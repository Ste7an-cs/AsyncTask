# Awaitable 状态模式：值可重复读取的"最新值"语义

日期：2026-08-25

## 1. 背景与问题

`Awaitable<T>` 今天是**破坏性消费**的队列：`await()` 走 `queue_->pop()`，值取走即从队列移除。这对"事件流"是正确语义，但无法表达另一类需求：**值是一个状态，谁来读都读到它，读多少次都不变**。

现有代码里已经有一批**天然一次性**的来源，它们与队列语义并不契合：

- `coro(std::future<T>&&)` / `coro(const QFuture<T>&)` —— 只 push 一次结果；
- `CoroAbstractSocket::connectToHost()` / `waitForConnected()` —— 一次性等待。

对这些来源 `await` 第二次会**永久阻塞**：值已被第一次消费取走，而生产者不会再产生第二个。这是一个今天就存在、只是没被触发的隐患。

## 2. 目标与非目标

### 目标

- 同一个 `Awaitable` 上，多次 `await()` 返回同一个值且不阻塞。
- 值可反复更新，读到的总是最新那个。
- 值可被清空，回到"无值"状态，此后 `await()` 重新阻塞，直到下一次赋值。
- 尚无值时 `await()` 阻塞等待首个值（让出协程，不阻塞线程）。
- `shared()` 得到的订阅者自动继承状态语义，且一接上就能读到当前值。
- 队列模式的现有行为**逐条不变**。

### 非目标

- **不做变化通知。** 状态是拿来读的；想知道"变了"只能自行轮询。这是明确排除的能力，不是缺陷。
- 不引入新类型：状态语义作为 `Awaitable` 的一个模式存在，而非独立的 `State<T>`。
- 不改变 `Awaitable` 的按值 move-only 语义，不引入虚函数、不引入常驻 fiber。

## 3. 语义规格

### 3.1 结构

状态模式是**流一级的属性**，存在 `ChannelHub` 上——`shared()` 共享 hub，因此所有消费者必然读到同一份状态。

```
                 ┌──────────────────────────────────────────┐
   生产者 lambda │  ChannelHub<T>                            │
   捕获这一个 ──►│  关闭状态 / 终止原因 / 清理钩子             │
                 │  vector<weak_ptr<FiberChannel<T>>> 消费者表│
                 │                                          │
                 │  队列模式：state_ == nullptr              │
                 │  状态模式：state_ ──► FiberChannel<T>     │
                 │              （容量 1 的状态格）           │
                 └───┬──────────┬──────────┬─────────────────┘
                     │          │          │
                 ┌───▼───┐  ┌───▼───┐  ┌───▼───┐
                 │queue A│  │queue B│  │queue C│  仅记账用；状态模式下不投递
                 └───────┘  └───────┘  └───────┘
                     ▲          ▲          ▲
                Awaitable   Awaitable   Awaitable
```

**状态格就是一条容量为 1 的 `FiberChannel<T>`。** 赋值即 `push`——容量 1 的既有语义会自动丢弃旧值、保留新值，正是"最新值"。读取用新增的非破坏性 `wait_peek()`：拷贝队首但不弹出。所有消费者 peek 同一格，因此"多次读值不变"是**结构性保证**，不依赖任何约定。

**模式不设单独的枚举成员**：`state_` 非空即状态模式。模式在构造时确定、此后不可变，杜绝"中途切模式"这种没人推得清楚的状态。

### 3.2 已决策项

| 决策点 | 结论 | 理由 |
|---|---|---|
| 状态形态 | 可变状态（最新值），非一次性 promise | 用户需求：值可反复更新 |
| 放置方式 | `Awaitable` 上的模式开关，非独立类型 | 用户选择；用法统一 |
| 重复 `await()` | 立即返回同一个值 | 用户选择；字面实现"多次消费值不变" |
| 变化通知 | 不提供 | 用户明确不需要；换来 O(1) 内存与最简设计 |
| 状态存储 | 复用容量 1 的 `FiberChannel<T>` | 不引入新类型；复用已被现有用例覆盖的机制 |
| 状态模式下的闲置队列 | 照常分配并 attach | 生命周期记账（消费者归零即断上游）一行不改 |
| 清空操作 | 映射到"丢弃待消费值"，两种模式共用一个方法 | 底层本就是同一个 `discard_pending` |
| 清空的调用方 | 生产者与句柄都可以 | 队列模式下句柄级只清自己那条队列；状态模式下没有"自己那一路"，只能清共享的状态格 |
| 关闭且有值时的读取 | 返回 `closed`，**不返回值** | 这是状态模式下唯一可能的终止信号（见 §3.4） |

### 3.3 边界行为

- **首值等待。** 状态格为空且未关闭时，`await()` 让出协程等待；某次赋值后被唤醒并返回该值。
- **最新值覆盖。** 连续赋值时，容量 1 使旧值被丢弃；此后读到的恒为最后一次赋的值。
- **订阅继承。** `shared()` 复用同一个 hub，因而自动是状态模式，且新订阅者立刻读到当前值——不需要任何 replay 机制，因为状态本就在那儿。
- **清空后重新挂起。** 清空使状态格回到空，此后 `await()` 重新阻塞；下一次赋值唤醒**所有**等待者（见 §3.5）。
- **句柄自关只影响自己。** `close()` 关掉本句柄那条（状态模式下闲置的）队列；`await()` 先查它，因此本句柄此后一律得到终止错误，其他消费者不受影响。"句柄管自己、生产者管整流"这条既有规则在状态模式下依然成立。
- **无值时的容量语义。** 状态模式下 `setCapacity()` 是空操作（状态天然容量 1），`capacity()` 返回 1。

### 3.4 关闭即终止信号（`wait_peek` 与 `pop` 的刻意差异）

`pop()` 的既有行为是"已关闭但仍有余量时先返回值，取空后才报 `closed`"。**`wait_peek()` 不同：只要 channel 已关闭就返回 `closed`，无论格中是否还有值，`out` 不写入。**

这个差异是刻意的，理由有两条：

1. peek 不消费，"余量"这个概念对它不成立——一个已关闭的状态不再是活状态。
2. 更重要的是，**这是状态模式下唯一可能的终止信号**。若关闭后仍返回值，`await()` 将永远返回成功、永不报错，消费者根本无从得知流已终止；生产者一旦消亡，所有消费者会对着一个陈旧的值空转下去。

代价是最后一个值在关闭后不可再读。这是拿"最后的值"换"可检测的终止"，且它把 §8 的死循环限制从"永不终止"降级为"以流的寿命为界"。

两个 `wait_peek` 重载的 Doxygen 必须写明这一差异，否则读代码的人会以为是抄漏了 `pop` 的逻辑。

### 3.5 `push` 的唤醒方式必须改为 `notify_all`

`FiberChannel::push()` 现在结尾是 `cv_consumer_.notify_one()`。队列模式下这是对的——一个值只应被一个消费者取走。

但状态模式下 N 个消费者都挂在同一个状态格上 peek，`notify_one` **只会唤醒其中一个**，其余继续睡到下一次赋值。这是会静默漏唤醒的缺陷，且"清空 → 全体重新阻塞 → 赋值唤醒"这条路径必然踩到。

因此 `push` 结尾改为 `notify_all()`。安全性无虞：`pop` / `pop_wait_for` / `value_pop` / `wait_peek` 全部使用带谓词的 `wait`，多余唤醒会自行重新等待。代价是队列模式下若某条队列确有多个竞争消费者（`test_case_broadcast_coexists_with_competing_consumers` 即是），一次 `push` 会唤醒全部而只有一个抢到值。以一次惊群换掉一个漏唤醒缺陷。

## 4. 接口

### 4.1 `Coro::FiberChannel<T>`（`coro/detail/fiberchannel.hpp`）

新增两个非破坏性读取方法：

```cpp
/// 阻塞至有值或已关闭；有值时把队首**拷贝**到 out 但不弹出。
/// 与 pop 刻意不同：channel 已关闭即返回 closed，不论是否仍有值（见 §3.4）。
channel_status wait_peek(T& out);

/// 同上，最长等待 timeout_duration；超时返回 timeout。
template<typename Rep, typename Period>
channel_status wait_peek_for(T& out, std::chrono::duration<Rep, Period> const& timeout_duration);
```

`push()` 结尾由 `cv_consumer_.notify_one()` 改为 `cv_consumer_.notify_all()`。

其余成员与签名不变。无新增数据成员，`sizeof(FiberChannel<T>)` 保持 160。

### 4.2 `Coro::ChannelHub<T>`（`coro/detail/channelhub.hpp`）

```cpp
enum class AwaitMode { Queue, State };   // 置于 namespace Coro

explicit ChannelHub(AwaitMode mode = AwaitMode::Queue);
bool isState() const noexcept;                                  // state_ != nullptr
const std::shared_ptr<FiberChannel<T>>& stateCell() const;      // 状态格；队列模式下为空
```

新增数据成员 `std::shared_ptr<FiberChannel<T>> state_`（队列模式下为空指针）。

各方法在状态模式下的行为差异：

- 构造：`AwaitMode::State` 时创建状态格并 `setCapacity(1)`。
- `push(v)`：写状态格（容量 1 自动丢旧留新），**不向消费者队列扇出**。hub 已关闭时仍返回 `closed`。
- `close(ec)`：同时关闭状态格与所有消费者队列，与队列模式一致。
- `discard_pending()`：同时清空状态格与所有消费者队列。
- `setCapacity(c)`：状态模式下为空操作；`capacity()` 返回 1。
- `attach(q)` / `detach(q)` / `notifyClosed(ec)`：**一行不改**。状态模式下消费者队列虽不投递，仍照常入表，生命周期记账（消费者归零即执行一次清理）完全复用。

### 4.3 `Coro::Awaitable<T>` / `Awaitable<void>`（`coro/await/awaitable.hpp`）

```cpp
/// 以指定模式构造。默认构造仍为队列模式，行为不变。
explicit Awaitable(AwaitMode mode);

/// 丢弃待消费的值。
/// 队列模式：只清空本句柄自己的队列，其他消费者不受影响。
/// 状态模式：清空**共享的**状态格，此后所有消费者的 await 都重新阻塞——
///   状态模式下不存在"自己那一路"，这是结构决定的，无法只清自己。
/// 无论哪种模式，要显式影响整条流请用 channel()->discard_pending()。
void discardPending();
```

`await()` / `await_for()` 按模式分派；两种模式都先检查本句柄是否已收尾：

```cpp
if(queue_ && queue_->is_closed()){
    return queue_->close_error();          // 本句柄已 close()，或整流已关闭
}
if(hub_ && hub_->isState()){
    return hub_->stateCell()->wait_peek(value);   // 状态模式：读状态格，不消费
}
return queue_->pop(value);                 // 队列模式：既有行为，逐字不变
```

`resolve(v)` 不变（仍是 `hub_->push(v)`，由 hub 按模式分派）。`shared()` 不变（复用 hub，自动继承模式）。`isClosed()` 不变（查自身队列）。

`Awaitable<void>` 同构：内部类型为 `ChannelHub<int>` / `FiberChannel<int>`，状态语义退化为"事件是否已发生"，同样支持清空使其回到未发生。

## 5. 实现要点

### 5.1 改动清单

| 文件 | 改动 |
|---|---|
| `coro/detail/fiberchannel.hpp` | 新增 `wait_peek` / `wait_peek_for`；`push` 的 `notify_one` 改 `notify_all` |
| `coro/detail/channelhub.hpp` | 新增 `AwaitMode` 枚举、`state_` 成员、模式构造函数、`isState()` / `stateCell()`；`push` / `close` / `discard_pending` / `setCapacity` 分派状态模式 |
| `coro/await/awaitable.hpp` | 两个特化各新增模式构造函数与 `discardPending()`；`await` / `await_for` 分派状态模式 |
| `test/testfiberawait/tst_testfiberawait.cpp` | 新增状态模式用例；更新 `sizeof` 钉死值 |

各 `coro*` 工厂**不改动**：它们默认构造 `Awaitable`，即队列模式，行为逐条不变。

### 5.2 锁顺序

不变，仍恒为 hub → queue：状态模式下 `push` 在持 hub 锁时取状态格的锁，与既有扇出同向。`await()` 读状态格时**不持 hub 锁**——它经 `stateCell()` 取到 `shared_ptr` 后即在格自己的锁上等待，因此长时间阻塞不会占住 hub。状态格的存活由 hub 保证（`Awaitable` 持 `hub_`，hub 持状态格）。

## 6. 存储与性能

**存储**：`FiberChannel` 只加方法，`sizeof` 保持 160。`ChannelHub` 多一个 `shared_ptr`，`sizeof` 由 160 预计增至 176——实测确定并重新钉死（沿用 `test_case_channel_layout_size` 的做法，测试环境 x86-64 / libstdc++ / glibc，换平台需重测）。每个 `Awaitable` 的开销不变。

**状态模式内存为 O(1)**：状态格最多持有一个值，与消费者数量、赋值次数均无关。代价是每个句柄仍分配一条闲置队列（160 字节），换取生命周期机制零改动。

**性能**：状态模式的赋值是一次容量 1 的入队加一次 `notify_all`，读取是一次加锁加一次 `T` 拷贝，**完全没有扇出**——消费者越多越比队列模式便宜。队列模式唯一的变化是 `notify_one` → `notify_all`（见 §3.5）。

## 7. 已知限制

1. **状态模式下 `while(auto v = await(a))` 是满速空转循环**，`generate(a)` 同理会不断产出同一个值，直到流关闭才终止。这是"重复 `await` 立即返回同一个值"这一决策的直接后果，项目负责人已知悉并明确接受。`await()` 与 `AwaitMode` 的 Doxygen 必须显著标注，使人在写下这种循环之前就看见。
2. **无变化通知。** 想知道状态何时改变只能轮询。属既定非目标。
3. **关闭后最后一个值不可再读**（§3.4）。这是换取可检测终止的代价。
4. **状态模式下每个句柄仍分配一条闲置队列。** 属既定取舍。

## 8. 测试计划

位置：`test/testfiberawait`。现有 101 条用例即为队列模式的回归基线，必须逐条通过。

| 用例 | 验证内容 |
|---|---|
| `wait_peek` 单测 | 空→阻塞；有值→返回且**不弹出**（连读两次仍得同值）；关闭且空→`closed`；**关闭且有值→`closed`**（§3.4 的刻意差异） |
| `wait_peek_for` 超时 | 空且未关闭时到期返回 `timeout`，channel 仍开放 |
| 状态基本读取 | 赋值后多次 `await` 返回同一值且不阻塞 |
| 首值等待 | 无值时 `await_for` 超时；赋值后再 `await` 立即返回 |
| 最新值覆盖 | 连续赋值三次，读到最后一个；且格中始终只存一个值 |
| 订阅继承 | `shared()` 得到的句柄是状态模式，且立刻读到当前值 |
| **清空后全员唤醒** | 清空后多个消费者重新阻塞，一次赋值必须唤醒**全部**——专钉 §3.5 的 `notify_all`，是本设计最关键的防线 |
| 关闭即终止 | 有值时关闭 → `await` 返回终止错误而非值；`while(await(a))` 因此能退出 |
| 句柄自关 | 本句柄得到终止错误，其他消费者照常读到状态 |
| 内存 O(1) | N 个消费者、M 次赋值后，只有一个值被持有（`weak_ptr` 观测） |
| 容量无效 | 状态模式下 `setCapacity` 无效、`capacity()` 返回 1 |
| `void` 特化 | 上述关键路径在 `Awaitable<void>` 上重跑 |
| 竞争消费者回归 | `notify_all` 改动后，同一队列上多个竞争消费者仍不重不漏（现有 `test_case_broadcast_coexists_with_competing_consumers`） |
| `sizeof` 钉死 | 更新 `ChannelHub` 实测值；确认 `FiberChannel` 仍为 160 |

回归范围：`test/testfiberawait`、`test/testfibertask`、`test/testexecutor`、`test/test_scheduler` 全部通过。

**运行时须清除代理环境变量**，否则 UDP 用例会因 Qt 把 `bind()` 路由进 SOCKS5 代理引擎而误报失败（见 `.superpowers/sdd/progress.md` 的调查记录）；相关用例已加 `setProxy(NoProxy)`，但新增用例若涉及 socket 需比照处理。

## 9. 文档更新

- `doc/使用说明.md`：新增"状态模式"小节，说明用法、与队列模式的差异、以及 §7.1 的死循环警告。
- `doc/软件设计说明.md`：在 `Awaitable` / `ChannelHub` 的单元设计中补充状态格与模式分派。
- `doc/架构设计.md`、`doc/类图与时序图.md`：类图补充状态格。
- `doc/需求规格说明.md`：新增状态语义的需求项。
