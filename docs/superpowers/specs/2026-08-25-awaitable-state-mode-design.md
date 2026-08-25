# Awaitable 状态模式：值可重复读取的"最新值"语义

日期：2026-08-25（2026-08-25 修订：见 §0）

## 0. 修订记录

本文档初版把状态存放在一个**全流共享的"状态格"**上，所有消费者一起阻塞在它的条件变量上。该设计在实现并通过全部任务评审之后被推翻，原因是它**违背了 `ChannelHub` 架构自身的核心不变式**（见 `2026-08-20-channel-hub-flat-consumers-design.md`）：hub 只负责分发，每个 `Awaitable`（含 `shared()` 得到的订阅者）独占自己的队列，**每次 `await` 都等在自己队列的条件变量上**。

共享状态格使"谁在等"和"谁被唤醒"落在两个不同的对象上，由此直接导致最终评审发现的两个缺陷：句柄级 `close()` 唤不醒自己的等待者；消费者归零时 `markExhausted` 不关状态格，等待者永久挂死且无路可救。这两个后果不是可以打补丁的 bug，是错误结构的必然产物。

本次修订取消共享状态格：**状态存放在各消费者自己的队列里**（容量 1），`await` 用非破坏性读取读自己的队列。上述两个缺陷随结构一并消失。

同期还定下两项修改，见 §3.5 与 §3.6：状态读取命中值时必须让出协程；`Awaitable::close()` 改为终止整条流。

## 1. 背景与问题

`Awaitable<T>` 今天是**破坏性消费**的队列：`await()` 走 `queue_->pop()`，值取走即从队列移除。这对"事件流"是正确语义，但无法表达另一类需求：**值是一个状态，谁来读都读到它，读多少次都不变**。

现有代码里已经有一批**天然一次性**的来源，它们与队列语义并不契合：

- `coro(std::future<T>&&)` / `coro(const QFuture<T>&)` —— 只 push 一次结果；
- `CoroAbstractSocket::connectToHost()` / `waitForConnected()` —— 一次性等待。

对这些来源 `await` 第二次会**永久阻塞**：值已被第一次消费取走，而生产者不会再产生第二个。

## 2. 目标与非目标

### 目标

- 同一个 `Awaitable` 上，多次 `await()` 返回同一个值且不阻塞。
- 值可反复更新，读到的总是最新那个。
- 值可被清空，回到"无值"状态，此后 `await()` 重新阻塞，直到下一次赋值。
- 尚无值时 `await()` 阻塞等待首个值（让出协程，不阻塞线程）。
- `shared()` 得到的订阅者自动继承状态语义，且一接上就能读到当前值。
- 队列模式的现有行为**逐条不变**。

### 非目标

- **不做变化通知。** 状态是拿来读的；想知道"变了"只能自行轮询。
- 不引入新类型：状态语义作为 `Awaitable` 的一个模式存在。
- 不改变 `Awaitable` 的按值 move-only 语义，不引入虚函数、不引入常驻 fiber。

## 3. 语义规格

### 3.1 结构

状态模式是**流一级的属性**（存在 `ChannelHub` 上，`shared()` 共享 hub 故自动继承），但**状态本身存放在各消费者自己的队列里**：

```
                 ┌──────────────────────────────────────────┐
   生产者 lambda │  ChannelHub<T>                            │
   捕获这一个 ──►│  关闭状态 / 终止原因 / 清理钩子             │
                 │  vector<weak_ptr<FiberChannel<T>>> 消费者表│
                 │                                          │
                 │  队列模式：latest_ 为空                    │
                 │  状态模式：latest_ ── 最新值的一份副本，    │
                 │            仅用于给新订阅者播种，无人等待它 │
                 └───┬──────────┬──────────┬─────────────────┘
                     │ push 扇出 │          │
                 ┌───▼───┐  ┌───▼───┐  ┌───▼───┐
                 │queue A│  │queue B│  │queue C│  状态模式下容量恒为 1
                 └───▲───┘  └───▲───┘  └───▲───┘
                     │          │          │
                Awaitable   Awaitable   Awaitable
                 各自等在自己队列的条件变量上
```

**状态模式 = 容量 1 的队列 + 非破坏性读取。** `push` 照常扇出到每条消费者队列，容量 1 的既有语义使新值自动挤掉旧值——每个消费者手里都是最新值。读取用 `FiberChannel::wait_peek()`：拷贝队首但不弹出，因此可反复读到同一个值。

**扇出逻辑与队列模式完全一致**，不存在"状态模式不扇出"这类特例；两种模式的唯一差别是**队列容量固定为 1** 与**读取方式为 peek 而非 pop**。

**模式不设单独的枚举成员**：hub 的 `latest_` 是否启用（以一个 `std::unique_ptr<T>` 或等价物表示）即模式标志。模式在构造时确定、此后不可变。

### 3.2 已决策项

| 决策点 | 结论 | 理由 |
|---|---|---|
| 状态形态 | 可变状态（最新值），非一次性 promise | 值可反复更新 |
| 放置方式 | `Awaitable` 上的模式开关，非独立类型 | 用法统一 |
| 重复 `await()` | 立即返回同一个值 | 字面实现"多次消费值不变" |
| 变化通知 | 不提供 | 换来最简设计 |
| **状态存放位置** | **各消费者自己的队列（容量 1）**，hub 另存一份副本供新订阅者播种 | 守住"每次 await 都等在自己队列的 cv 上"这条架构不变式；见 §0 |
| 状态模式下的扇出 | 与队列模式完全一致 | 不为状态模式开特例 |
| 清空操作 | 映射到"丢弃待消费值"，两种模式共用一个方法 | 底层同为 `discard_pending` |
| **`Awaitable::close()` 的作用域** | **终止整条流**（关闭 hub 表里所有消费者队列并跑一次清理） | 见 §3.6 |
| 析构的作用域 | 只摘自己那条队列；表中再无存活且未关闭的队列时才收尾 | 想只退订自己就析构句柄 |
| 关闭且有值时的读取 | 返回 `closed`，**不返回值** | 状态模式下唯一可能的终止信号；见 §3.4 |
| 状态读取命中值时 | **必须让出协程** | 否则饿死同线程的一切；见 §3.5 |

### 3.3 边界行为

- **首值等待。** 自己的队列为空且未关闭时，`await()` 让出协程等待；某次赋值扇出到本队列后被唤醒并返回该值。
- **最新值覆盖。** 容量 1 使旧值被挤掉；此后读到的恒为最后一次赋的值。
- **订阅继承与播种。** `shared()` 复用同一个 hub，因而自动是状态模式。`attach()` 时若 hub 的 `latest_` 有值，先把它播种进新队列——新订阅者因此立刻能读到当前值，不需要任何 replay 机制。
- **清空后重新挂起。** 清空同时清掉 hub 的 `latest_` 与所有消费者队列，此后 `await()` 重新阻塞；下一次赋值扇出时唤醒各自的等待者。
- **无值时的容量语义。** 状态模式下容量固定为 1：`setCapacity()` 为空操作，`capacity()` 返回 1。
- **移动后的空壳。** `queue_` 为空时 `await()` 立即返回 `no_message`、`await_for()` 返回 `timed_out`，不触碰 `hub_`。

### 3.4 关闭即终止信号（`wait_peek` 与 `pop` 的刻意差异）

`pop()` 的既有行为是"已关闭但仍有余量时先返回值，取空后才报 `closed`"。**`wait_peek()` 不同：只要 channel 已关闭就返回 `closed`，无论队列中是否还有值，`out` 不写入。**

理由有两条：

1. peek 不消费，"余量"这个概念对它不成立——一个已关闭的状态不再是活状态。
2. 更重要的是，**这是状态模式下唯一可能的终止信号**。若关闭后仍返回值，`await()` 将永远返回成功、永不报错，消费者根本无从得知流已终止。

代价是最后一个值在关闭后不可再读。这是拿"最后的值"换"可检测的终止"。

两个 `wait_peek` 重载的 Doxygen 必须写明这一差异，否则读代码的人会以为是抄漏了 `pop` 的逻辑。

### 3.5 状态读取命中值时必须让出协程

状态模式命中值时，`wait_peek` 的 `cv.wait(lck, pred)` 谓词立即为真、无竞争的 `boost::fibers::mutex` 也不挂起——整个 `await()` **一次协程让出都不发生**。于是 `while(auto v = await(a))` 这类循环会**饿死同线程的一切**，包括关闭者、生产者与 Qt 泵协程；`makeTask` 默认粘连到创建线程、Qt 回调又由该线程的泵协程分发，因此"关闭者与消费者同线程"是默认情形而非特例。

实测（单线程调度器，一个满速读取的协程 + 一个 50ms 后关流的协程）：

| | 循环次数 | 关闭者被调度时的次数 | 结果 |
|---|---|---|---|
| 无 `yield` | 5,000,001（撞上兜底） | 5,000,001 | 关闭者全程未被调度 |
| 有 `yield` | 628,716 | 628,716 | 正常被 `close` 收敛 |

因此**状态分支在成功返回值之前必须 `boost::this_fiber::yield()` 一次**。代价是每次状态读取多一次协程切换（相对一次加锁加一次 `T` 拷贝并不夸张），且只影响状态模式。

有了它，"忙循环以流的寿命为界"这一说法才真正成立。

### 3.6 `Awaitable::close()` 终止整条流

`Awaitable::close(ec)` 关闭 hub 表里**所有**消费者队列，并触发一次清理（断开上游）。实现上是 `hub_->close(ec)` 后接 `hub_->notifyClosed(ec)`——关闭之后表中已无未关闭的队列，归零判定自然成立，清理照跑。

这**推翻**了 `2026-08-20-channel-hub-flat-consumers-design.md` §3.2 的"`Awaitable::close()` 只关自己这一路"。推翻的理由：业务代码持有源句柄调 `close()` 时，期望的是整条流终止；而订阅者对此毫不知情会导致它们继续等待一条已经没有生产者的流。

**析构不受影响**：`~Awaitable` 仍只摘自己那条队列，只有当表中再无"存活且未关闭"的队列时才收尾。**想只退订自己，析构句柄即可，不要调 `close()`。**

该语义对两种模式一视同仁——同一个方法在不同模式下作用域不同太容易记错。

`ChannelHub::close(ec)` 的既有行为（关闭所有消费者队列、不清空消费者表、不直接跑清理钩子）不变；工厂层的全部关闭路径本就走它，因此上游关闭一直是传播到所有订阅者的。

### 3.7 `push` 的唤醒方式为 `notify_all`

`FiberChannel::push()` 结尾使用 `notify_all()` 而非 `notify_one()`。`wait_peek` 的等待者不消费元素，同一条队列上若有多个等待者（`shared_ptr<Awaitable>` 被多个协程同时 `await` 即是），`notify_one` 只会唤醒其中一个，其余继续睡到下一次赋值。

安全性无虞：`pop` / `pop_wait_for` / `value_pop` / `wait_peek` 全部使用带谓词的 `wait`，多余唤醒会自行重新等待。代价仅是多消费者抢同一条队列时的一次惊群。

## 4. 接口

### 4.1 `Coro::FiberChannel<T>`（`coro/detail/fiberchannel.hpp`）

```cpp
/// 阻塞至有值或已关闭；有值时把队首**拷贝**到 out 但不弹出。
/// 与 pop 刻意不同：channel 已关闭即返回 closed，不论是否仍有值（见 §3.4）。
channel_status wait_peek(T& out);

/// 同上，最长等待 timeout_duration；超时返回 timeout。
template<typename Rep, typename Period>
channel_status wait_peek_for(T& out, std::chrono::duration<Rep, Period> const& timeout_duration);
```

`push()` 结尾为 `cv_consumer_.notify_all()`（见 §3.7）。无新增数据成员，`sizeof(FiberChannel<T>)` 保持 160。

**本节内容已由前序任务实现完毕，本次修订不改动 `fiberchannel.hpp`。**

### 4.2 `Coro::ChannelHub<T>`（`coro/detail/channelhub.hpp`）

```cpp
enum class AwaitMode { Queue, State };   // 置于 namespace Coro

explicit ChannelHub(AwaitMode mode = AwaitMode::Queue);
bool isState() const noexcept;
```

新增数据成员：`std::unique_ptr<T> latest_`（队列模式下恒为空指针）与一个标记模式的 `bool`（或以"模式已启用"的等价表示；不得放在 `guard_` 之后）。

**注意**：`latest_` 是"最新值的一份副本"，**没有任何消费者等待在它上面**，它只在 `attach()` 时用于给新队列播种。

各方法在状态模式下的行为：

- 构造：`AwaitMode::State` 时记录模式。
- `push(v)`：先更新 `latest_`，再**照常扇出到所有消费者队列**（与队列模式同一段代码，无特例）。hub 已关闭时返回 `closed`。
- `attach(q)`：状态模式下先 `q->setCapacity(1)`；若 `latest_` 有值则 `q->push(*latest_)` 播种；随后照常入表。hub 已关闭时的既有处理不变。
- `close(ec)`：不变（关闭所有消费者队列，不清空表，不跑清理钩子）。
- `discard_pending()`：清空 `latest_`，并照常清空所有消费者队列。
- `setCapacity(c)`：状态模式下为空操作；`capacity()` 状态模式下返回 1。
- `detach(q)` / `notifyClosed(ec)` / `markExhausted`：**一行不改**。

### 4.3 `Coro::Awaitable<T>` / `Awaitable<void>`（`coro/await/awaitable.hpp`）

```cpp
/// 以指定语义模式构造。默认构造仍为队列模式，行为不变。
explicit Awaitable(AwaitMode mode);

/// 丢弃待消费的值：状态模式下使其回到"无值"，此后 await 重新阻塞。
/// 只作用于本句柄自己那条队列；要清空整条流请用 channel()->discard_pending()。
void discardPending();
```

`await()` 按模式选择读取方式，**两种模式都等在自己队列的条件变量上**：

```cpp
Result<T, std::error_code> await(){
    if(!queue_){
        return std::make_error_code(std::errc::no_message);   // 移动后的空壳
    }
    T value{};
    const bool state = (hub_ && hub_->isState());
    auto status = state ? queue_->wait_peek(value) : queue_->pop(value);
    if(status == boost::fibers::channel_op_status::success){
        if(state){
            boost::this_fiber::yield();   // §3.5：否则饿死同线程的一切
        }
        return value;
    }
    return queue_->close_error();
}
```

`await_for(timeout)` 同构，改用 `wait_peek_for` / `pop_wait_for`，`timeout` 映射为 `std::errc::timed_out`。

`close(ec)` 改为终止整条流（§3.6）：

```cpp
void close(std::error_code error){
    if(hub_){
        hub_->close(error);          // 传播：关闭所有消费者队列
        hub_->notifyClosed(error);   // 归零判定 -> 跑一次清理
    }
}
```

`resolve(v)` / `shared()` / `isClosed()` / 析构不变。`Awaitable<void>` 同构（内部类型为 `ChannelHub<int>` / `FiberChannel<int>`）。

## 5. 实现要点

### 5.1 改动清单

| 文件 | 改动 |
|---|---|
| `coro/detail/fiberchannel.hpp` | **不改**（`wait_peek` 系列与 `notify_all` 已就位） |
| `coro/detail/channelhub.hpp` | 状态存储由"共享状态格"改为"`latest_` 播种副本"；`push` 取消状态特例、照常扇出；`attach` 增加设容量与播种 |
| `coro/await/awaitable.hpp` | `await` / `await_for` 改为读自己的队列并在状态成功路径 `yield`；`close` 改为终止整条流 |
| `test/testfiberawait/tst_testfiberawait.cpp` | 见 §7 |

各 `coro*` 工厂**不改动**。

### 5.2 锁顺序

不变，恒为 hub → queue。状态模式下 `push` 与 `attach` 在持 hub 锁时取消费者队列的锁，与既有扇出同向。`await()` 等待发生在**自己队列**的锁上，不持有 hub 锁。共享状态格取消后，不再存在"多个消费者阻塞于同一对象"的情形。

## 6. 存储与性能

`sizeof(FiberChannel<T>)` 保持 160。`sizeof(ChannelHub<T>)` 因 `latest_` 与模式标记而变化，**实测确定并重新钉死**（沿用 `test_case_channel_layout_size` 的做法；x86-64 / libstdc++ / glibc，换平台需重测）。

**状态模式内存为 O(N)**：N 个消费者各持有一份当前值的拷贝，加 hub 的一份播种副本，共 N+1 份。这与容量 1 的队列模式一致。初版设计曾宣称 O(1)，那是共享状态格的产物，随该结构一并作废。

性能：状态模式的赋值与队列模式同为一次扇出；读取是一次加锁、一次 `T` 拷贝、一次协程让出（§3.5）。

## 7. 已知限制

1. **状态模式下 `while(auto v = await(a))` 是忙循环**，`generate(a)` 同理会不断产出同一个值，直到流关闭才终止。有了 §3.5 的 `yield()`，它不再饿死同线程的其它协程，但仍会持续占用 CPU。`await()` 与 `AwaitMode` 的 Doxygen 必须显著标注。项目负责人已知悉并接受，且明确选择不让 `generate()` 对状态模式硬拒绝，仅以文档劝阻。
2. **无变化通知。** 想知道状态何时改变只能轮询。
3. **关闭后最后一个值不可再读**（§3.4）。
4. **任何句柄调 `close()` 都会终止整条流**（§3.6）。想只退订自己请析构句柄。
5. 状态模式内存为 O(N)，非 O(1)。

## 8. 测试计划

位置：`test/testfiberawait`。

### 需要改写的既有用例（因 §3.6 的 `close` 语义反转）

| 用例 | 改写方向 |
|---|---|
| `test_case_flat_close_scope_is_self_only` | 断言的正是被推翻的语义，改写为"任一句柄 `close()` 即全体收敛"，并相应改名 |
| `test_case_broadcast_mirror_close_isolated` | 其中的 `first->close()` 现在会终止整条流，重新设计断言或改用析构来表达"退订自己" |
| `test_case_broadcast_prune_preserves_later_mirror` | 同上 |
| `test_case_broadcast_closed_mirror_pruned` | 同上 |

上次改写中把 `source.close()` 换成 `source.channel()->close()` 的那几条**保持不动**——生产者侧关闭本就传播，语义未变。

### 需要改写的状态模式用例（因 §0 的结构修订）

| 用例 | 改写方向 |
|---|---|
| `test_case_hub_state_no_fanout_to_queues` | **作废**：状态模式现在就是要扇出的 |
| `test_case_hub_state_keeps_latest` | 改为经消费者队列观测最新值 |
| `test_case_state_memory_is_constant` | 改为验证"每个消费者各一份、且各自只保留一份"（O(N) 而非 O(1)） |
| `test_case_state_handle_close_isolated` | 反转为"句柄 `close()` 终止整条流" |
| `test_case_channel_layout_size` | 按实测更新 `ChannelHub` 的值 |

### 新增用例

| 用例 | 验证内容 |
|---|---|
| 阻塞中被 close 唤醒 | 一个协程阻塞在状态模式的 `await()` 上，另一协程对同一句柄 `close(ec)`，前者必须**当场返回** `ec` 而非继续阻塞（这是共享状态格时代无法做到、也是本次结构修订的直接收益） |
| 让出协程 | 同线程上一个满速 `while(await(a))` 与一个延时关流的协程，循环必须能被关闭收敛而不是饿死对方（钉死 §3.5 的 `yield`） |
| 订阅播种 | 赋值之后再 `shared()`，新订阅者立刻读到当前值 |
| 清空后重新挂起 | 清空后各消费者重新阻塞，下一次赋值各自被唤醒 |
| `void` 特化 | 上述关键路径在 `Awaitable<void>` 上重跑，含 `shared()` 继承与关闭终止 |

其余既有状态模式用例（重复读取、首值等待、最新值覆盖、容量固定、空壳句柄）语义不变，保留。

回归范围：`test/testfiberawait`、`test/testfibertask`、`test/testexecutor`、`test/test_scheduler` 全部通过。

## 9. 文档更新

`doc/使用说明.md`、`doc/软件设计说明.md`、`doc/架构设计.md`、`doc/类图与时序图.md`、`doc/img/sdd-05-csc_await.mmd` 中关于状态模式的描述需按本次修订同步；特别是"共享状态格"与"内存 O(1)"两处失效表述必须改写，以及 `close()` 作用域反转须在两份 spec 与使用说明中一致。
