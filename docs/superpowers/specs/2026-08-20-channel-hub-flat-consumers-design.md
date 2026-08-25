# ChannelHub：把广播扇出移出 FiberChannel，消费者队列拉平

日期：2026-08-20

## 1. 背景与问题

`Awaitable<T>` 内部持有一个 `FiberChannel<T>`，`shared()` 通过 `FiberChannel::addMirror()` 把订阅者的 channel 注册为源 channel 的镜像。扇出发生在 `FiberChannel` 的 `push` / `close(error)` / `discard_pending` / `setCapacity` 四处，外加析构时的镜像收敛。设计依据见 `docs/superpowers/specs/2026-08-11-shared-awaitable-design.md` §3.2：生产者按约定只捕获 `channel()`、绝不持有 `Awaitable`，因此生产者能触达的只有 channel，广播只能由 channel 自己实现。

由此产生两个问题。

**其一，源队列在广播用法下常年囤积无人消费的值。** 典型写法是 `auto src = coro(sock).readAll(); auto a = src->shared();`，随后只消费 `a`，从不 `await(src)`。源 channel 自己的队列照样每次 `push` 都入队，停在 `capacity_`（默认 1024）条不再增长——2026-08-12 的有界化改动挡住了 OOM，但没解决囤积本身：

- 1024 条值被长期持有且永远不会被取走，等于常驻内存；
- 对承载资源的值（`nextConnection()` 的 `QTcpSocket*`、`QNetworkDatagram`）更严重：1024 个连接被扣在队列里没人处理，超出后还被静默丢弃，等于连接泄漏；
- 每次 `push` 多一次 `push_back`、到顶后多一次 `pop_front`，以及一次 `T` 拷贝。

镜像队列没有这个问题——它由订阅者 `Awaitable` 独占持有，句柄一析构队列就消失，源侧的 `weak_ptr` 随后被剔除。源队列享受不到同样待遇，是因为它长在 `FiberChannel` 身上，而 `FiberChannel` 被生产者 lambda 强持有，比源 `Awaitable` 活得久。

更进一步，源句柄今天**不能丢**：`Awaitable` 析构会跑 `guard_` 的清理、断开 Qt 连接，整条流随之终止（订阅者收到 `connection_aborted`）。所以广播用法下调用方被迫一直攥着 `src`，那 1024 条就是这么攒起来的。

**其二，`FiberChannel` 不再是一个通用队列。** 它带着 `Awaitable` 的前向声明、`template<typename U> friend class Awaitable`、镜像列表和四处扇出，无法脱离 await 层作为普通的跨线程队列复用。

## 2. 目标与非目标

### 目标

- 源队列与订阅者队列行为一致：句柄析构即不再持有任何值，也不再接收投递。
- 没有任何存活消费者时，`push` 直接丢弃，不产生 `T` 拷贝、不占内存。
- `FiberChannel<T>` 回归纯粹的跨线程/跨协程通用队列，不含 `Awaitable` 的任何痕迹，可独立复用。
- 消除"源 / 镜像"的身份不对称，链式 `shared()` 不再形成传播树。
- 生产者侧写法（各 `coro*` 工厂中的 `auto ch = a.channel(); ch->push(...)`）保持不变。

### 非目标

- 不做 replay。订阅之后产生的数据才对消费者可见，与今天一致。
- 不做背压。仍是有容量上限的 FIFO，超限丢弃队首最旧值，不阻塞生产者。
- 不改变 `Awaitable` 的按值 move-only 语义，不引入虚函数、不引入常驻 fiber。
- 不提供丢弃计数等运行期信号，容量仍是唯一的调节手段。

## 3. 语义规格

### 3.1 结构

```
                 ┌─────────────────────────────────────────┐
   生产者 lambda │  ChannelHub<T>      coro/detail/          │
   捕获这一个 ──►│  关闭状态 / 终止原因 / 容量默认值          │
                 │  vector<weak_ptr<FiberChannel<T>>>       │
                 │  AwaitableCloseGuard（断连清理钩子）      │
                 └───┬──────────┬──────────┬────────────────┘
                     │ push 扇出 │          │
                 ┌───▼───┐  ┌───▼───┐  ┌───▼───┐
                 │queue A│  │queue B│  │queue C│  FiberChannel<T>（纯队列）
                 └───▲───┘  └───▲───┘  └───▲───┘  mutex / cv / deque / capacity
                     │          │          │
                Awaitable   Awaitable   Awaitable  各自独占持有自己那条
                （源句柄）  （shared） （shared）
```

`ChannelHub` 持队列的 `weak_ptr`，队列的强引用只在 `Awaitable` 手里。源与订阅者在表上没有身份差别：`a->shared()->shared()` 只是同一张表上多一项，不再是今天那样的镜像树。

### 3.2 已决策项

| 决策点 | 结论 | 理由 |
|---|---|---|
| `FiberChannel` 的定位 | 纯通用队列，无镜像、无 friend | 本次改动的原始诉求；可脱离 await 层复用 |
| 生产者捕获的对象 | `ChannelHub<T>`，方法名与签名同今天 `FiberChannel` 的生产者侧 | 工厂里全是 `auto ch = a.channel()`，一个字不用改 |
| 源队列归属 | 归源 `Awaitable` 独占，与订阅者队列完全对称 | 句柄析构即释放队列与其中的值，正面解决囤积 |
| 上游存活 | 由全部句柄共同决定，最后一条未关闭的消费者队列消失时终止 | 使"丢掉 `src`、只留订阅者"成为合法用法；推翻 2026-08-11 §3.2 的"由源句柄持有" |
| `Awaitable::close()` 作用域 | 只关自己这一路 | 平表下无"源"身份可依；整流终止归生产者侧 `ch->close(ec)` |
| 清理钩子位置 | 内联进 `ChannelHub` | "消费者归零即终止上游"收敛为单一实现点 |
| 退订方式 | 纯 RAII（`Awaitable` 析构时 detach，`weak_ptr` 兜底） | 无需 `unsubscribe()` |
| 扇出时机 | 生产者线程同步完成 | 与今天一致，避免常驻泵 fiber |

### 3.3 边界行为

- **`close()` 不摘表，析构才摘。** `Awaitable::close()` 只把自己那条队列标记关闭并留在表里；`push` 跳过已关闭的队列（省一次加锁与一次 `T` 拷贝），`discard_pending()` 仍能触达它。这条约束由 `corotcpserver.hpp:135-137` 的用法逼出：消费者先 `close()` 掉流、再 `delete server`，`server` 析构时要靠 `discard_pending()` 清掉队列里即将悬空的 `QTcpSocket*`；若 `close()` 就把队列摘出表，消费者仍能 pop 出野指针。真正的摘除发生在 `Awaitable` 析构。
- **消费者归零。** 当表中不存在"既存活又未关闭"的队列时（由 `Awaitable` 析构或 `close()` 触发判定），hub 跑一次清理钩子并把自身置为关闭（终止原因 `no_message`），此后生产者 `push` 返回 `closed`。单消费者场景（绝大多数）行为与今天逐字一致：`close()` → 我是最后一路 → 立刻断连。
- **hub 已关闭时调 `shared()`。** 新句柄的队列立即以 hub 记录的终止原因关闭，不入表，避免订阅者永久挂起。与今天 `addMirror()` 的处理一致。
- **无 replay 与首值不丢。** 工厂构造 `Awaitable` 时队列即已入表，早于任何 `connect`，因此"刚连上就 push"的首个值仍落在源队列里，不存在 check-then-wait 竞态。`shared()` 之前产生的值对新订阅者不可见。
- **移动后的空壳。** `Awaitable` 的移动构造/赋值沿用默认实现，被移动方的 `hub_` / `queue_` 置空；其析构不 detach、不触发归零判定。表中的 `weak_ptr` 仍指向同一条队列，所有权随之转移，不受影响。
- **不再需要析构收敛。** 今天 `~FiberChannel()` 以 `connection_aborted` 关闭仍存活的镜像，防止"忘了 close 又丢了源句柄"导致订阅者永久挂起。新结构下队列由 `Awaitable` 独占、`Awaitable` 又持有 hub，因此 hub 析构时表中不可能还有存活队列，该兜底连同 `connection_aborted` 这一终止原因一并删除。
- **非 Qt 生产者忘记 close。** 生产者手动持有 hub 且既不 `close()` 也不释放时，消费者仍会永久挂起。今天同样如此（channel 被生产者持有就不会析构），无回退。

### 3.4 清理钩子的执行约束

两条必须成立，否则会自毁或死锁：

1. **清理必须在 hub 的 fiber mutex 之外执行。** 锁内只改表与判定，取出回调后出锁再调用。`AwaitableCloseGuard` 自身用独立的 `std::mutex`，与 hub 的 fiber mutex 无嵌套。
2. **触发方必须是持有 `hub_` 的 `Awaitable`。** 清理会 `QObject::disconnect`，进而销毁生产者 lambda、释放它持有的那份 hub 引用。之所以不会在 hub 成员函数执行期间把 hub 自己析构掉，正是因为触发方（`~Awaitable` 体内，成员尚未释放；或 `close()`，句柄更是活着）还攥着另一份引用。hub 自身析构时兜底跑一次清理是安全的——此时引用计数已归零，不存在重入销毁。

## 4. 接口

### 4.1 `Coro::FiberChannel<T>`（`coro/detail/fiberchannel.hpp`）

删除 `mirrors_`、`addMirror()`、`Awaitable` 前向声明与 friend 声明，以及 `push` / `close(error)` / `discard_pending` / `setCapacity` 四处扇出和析构函数。保留 `push` / `pop` / `pop_wait_for` / `value_pop` / `is_closed` / `close()` / `close(ec)` / `close_error` / `discard_pending` / `setCapacity` / `capacity`，签名不变。

### 4.2 `Coro::ChannelHub<T>`（新增 `coro/detail/channelhub.hpp`）

```cpp
template<class T>
class ChannelHub {
    using channel_status = boost::fibers::channel_op_status;
public:
    ChannelHub() = default;
    ~ChannelHub() = default;                         // guard_ 成员析构即兜底跑一次清理
    ChannelHub(const ChannelHub&) = delete;
    ChannelHub& operator=(const ChannelHub&) = delete;

    // 生产者侧：方法名与签名同今天 FiberChannel 的对应成员
    channel_status push(T value);
    void close() noexcept;
    void close(std::error_code error) noexcept;
    bool is_closed() const noexcept;
    std::error_code close_error() const;
    void discard_pending();
    void setCapacity(std::uint32_t capacity);
    std::uint32_t capacity() const;
    void setOnClose(std::function<void()> fn);

    // Awaitable 内部调用：挂载/摘除自己的队列、通知归零判定
    void attach(const std::shared_ptr<FiberChannel<T>>& queue);
    void detach(const FiberChannel<T>* queue);       // Awaitable 析构时调用
    void notifyClosed(std::error_code error);        // Awaitable::close(ec) 后触发归零判定

private:
    mutable boost::fibers::mutex mtx_;
    std::vector<std::weak_ptr<FiberChannel<T>>> consumers_;
    std::atomic_bool closed_{false};
    std::uint32_t capacity_{FiberChannel<T>::kDefaultCapacity};
    std::error_code close_error_{std::make_error_code(std::errc::no_message)};
    detail::AwaitableCloseGuard guard_;
};
```

`attach`/`detach`/`notifyClosed` 是 public，而非 spec 早先设想的 private + friend：与旧的
`FiberChannel::addMirror` 不同，这三个方法无需 private + friend 保护。`addMirror` 必须收口，
是因为两个同类型的 channel 能互相注册为对方的镜像而成环；`ChannelHub` 与 `FiberChannel` 是
两个不同的类型层，结构上无法互相挂载，环不可能构造出来。public 同时让 `ChannelHub` 可以脱离
`Awaitable` 独立单测。

各方法行为：

- `push(v)`：加锁；hub 已关闭返回 `closed`。遍历 `consumers_`：`weak_ptr` 失效的槽位 swap-and-pop 剔除；已关闭的队列跳过投递但保留；其余 `queue->push(v)`。hub 未关闭却一条也没投出去时返回 `success`——正常流程下不会发生（消费者归零必然已把 hub 置为关闭，见下），此处只是不做额外断言的防御写法。
- `close(ec)`：加锁；已关闭则返回；记录规范化后的终止原因（空错误码替换为 `no_message`）；置 `closed_`；扇出关闭所有存活队列；**不清空 `consumers_`**（`discard_pending()` 之后仍需触达）；**不跑清理钩子**（与今天"生产者关 channel 不触发 guard"一致）。
- `discard_pending()` / `setCapacity(c)`：加锁遍历扇出，顺带 swap-and-pop 剔除失效槽位。`setCapacity` 同时更新 `capacity_`，作为后续 `attach` 的默认值。
- `attach(q)`：加锁；hub 已关闭则出锁后 `q->close(close_error_)` 并返回，不入表；否则 `q->setCapacity(capacity_)` 后入表。
- `detach(q)`：加锁；移除对应槽位并剔除失效槽位；若表中已无"存活且未关闭"的队列，则置 `closed_` 与 `close_error_ = no_message`，出锁后跑一次清理钩子。
- `notifyClosed(ec)`：同 `detach` 的归零判定，但不移除槽位；归零时 hub 继承调用方传入的 `ec`（规范化后）作为 `close_error_`，而非退回 `no_message`——否则 `a.close(connection_reset)` 之后生产者查 `close_error()` 会丢失终止原因，与今天 `Awaitable::close(ec)` 直接把 `ec` 写进 channel 的行为不符。

### 4.3 `Coro::Awaitable<T>` / `Awaitable<void>`（`coro/await/awaitable.hpp`）

```cpp
std::shared_ptr<ChannelHub<T>>    hub_;     // void 特化为 ChannelHub<int>
std::shared_ptr<FiberChannel<T>>  queue_;   // void 特化为 FiberChannel<int>
```

- 默认构造：新建 hub 与队列，`hub_->attach(queue_)`。
- 私有构造 `Awaitable(std::shared_ptr<ChannelHub<T>>)`：复用传入的 hub，新建自己的队列并 attach。供 `shared()` 使用；用 tag 参数使其可被 `make_shared` 调用，避免多一次分配。
- 析构：`hub_` 与 `queue_` 均非空时 `hub_->detach(queue_.get())`。
- `channel()` → `hub_`（返回类型由 `shared_ptr<FiberChannel<T>>` 变为 `shared_ptr<ChannelHub<T>>`）。
- `shared()` → `make_shared<Awaitable<T>>(hub_, tag)`。
- `await()` / `await_for(t)` → `queue_->pop()` / `queue_->pop_wait_for(t)`，终止原因取自 `queue_->close_error()`。
- `resolve(v)` → `hub_->push(v)`；`a.resolve(42)` 后 `a.await()` 仍能取到 42（自己的队列也在表上）。
- `close(ec)` → `queue_->close(ec)`，随后 `hub_->notifyClosed()`。
- `setOnClose(fn)` → `hub_->setOnClose(fn)`；`setCapacity` / `capacity` → 透传 hub。
- **新增** `bool isClosed() const` → 查自身队列的关闭状态，供 `Generator` 使用（见 §5.3）。

`detail::AwaitableCloseGuard` 从 `awaitable.hpp` 移入 `channelhub.hpp`，保持 `Coro::detail` 命名空间与类名不变。

## 5. 实现要点

### 5.1 改动清单

| 文件 | 改动 |
|---|---|
| `coro/detail/fiberchannel.hpp` | 删除镜像列表、`addMirror`、friend 与前向声明、四处扇出、析构函数 |
| `coro/detail/channelhub.hpp` | **新增**：`AwaitableCloseGuard`（迁入）与 `ChannelHub<T>` |
| `coro/await/awaitable.hpp` | 两个特化改持 hub + 独占队列；`channel()` 返回类型变更；新增 `isClosed()`；`AwaitableCloseGuard` 迁出 |
| `coro/await/generator.hpp` | 三处（79 / 321 / 567）改用 `isClosed()` |
| `coro/all.hpp` | 增加 `#include "detail/channelhub.hpp"` |
| `AsyncTask.pri` | 通用 `HEADERS` 增加 `channelhub.hpp` |

各 `coro*` 工厂**不改动**：`auto ch = a.channel(); ch->push(...)` / `ch->close(ec)` / `ch->discard_pending()` / `ch->is_closed()` 全部沿用 `auto`，hub 提供同名同签名的成员。

### 5.2 锁顺序

恒为 hub → queue，单向无环：`push` / `close` / `discard_pending` / `setCapacity` 先持 hub 的 `mtx_`，再取各队列自己的 `mtx_`，从不反向。层数恒为 2——今天链式 `a->shared()->shared()` 是三层嵌套，新结构下消失。清理钩子的 `std::mutex` 始终在 hub 锁释放之后才取（见 §3.4）。

### 5.3 `Generator` 的判据

`Generator<T>::Yield::is_closed()` 今天查 `p_awaiter_->channel()->is_closed()`，语义是"消费端是否关闭了输出端"。改动后 `channel()` 是 hub，hub 未关不等于那条输出队列未关，须改查 `p_awaiter_->isClosed()`。`generate(std::shared_ptr<Awaitable<T>>)` 内部（`generator.hpp:567` 附近）超时后的续等判断同理改为 `a->isClosed()`。

## 6. 存储与性能

**存储。** 单消费者场景分配次数不变（今天 channel + guard 两次，改后 hub + queue 两次），但 hub 大于 guard，每个 `Awaitable` 净增几十字节。`FiberChannel` 卸下 `mirrors_` 后 `sizeof` 从 168 回落到 160。订阅者反而更省：今天每个订阅者是"完整 channel + 自己的 guard"，改后只是一条队列，hub 共用——订阅越多越划算。

x86-64 / libstdc++ / glibc 实测数字：`sizeof(Coro::FiberChannel<int>)` = **160**（改动前 168，卸下镜像列表后回落）；`sizeof(Coro::ChannelHub<int>)` = **160**。每个 `Awaitable` 仍是两次 `make_shared` 分配（改动前是 channel + 独立 guard，改动后是 hub + queue），单消费者场景总字节由 168+guard 增至 160+160=320，净增几十字节；反过来订阅者更省——改动前每个订阅者是"完整 channel 168 + 自己的 guard"，改动后只是一条 160 的队列，hub 共用，订阅越多越划算。这两个数字由 `test/testfiberawait/tst_testfiberawait.cpp` 的 `test_case_channel_layout_size`（`QCOMPARE(sizeof(Coro::FiberChannel<int>), std::size_t(160))` / `QCOMPARE(sizeof(Coro::ChannelHub<int>), std::size_t(160))`）钉死，换平台需重测。

**性能。** `push` 的临界区数量不变（今天源锁 + N 个镜像锁，改后 hub 锁 + N 个队列锁）。无存活消费者时 `push` 只做一次遍历即返回，省掉今天的 `push_back` 加到顶后的 `pop_front` 与一次 `T` 拷贝。链式 `shared()` 因锁层数从 3 降到 2 而变快。

## 7. 行为变化

1. **源句柄 `close()` 不再终止订阅者。** 只关自己这一路。整条流的终止归生产者侧 `ch->close(ec)`（socket 出错、来源析构），该路径仍扇出到所有消费者。
2. **丢掉源句柄不再终止上游。** 广播场景下这正是目的。代价是订阅者全部消失时上游会断开且不可恢复——2026-08-11 §3.2 当初回避的正是这个风险，本次明确接受。
3. **消费者归零后 `push` 返回 `closed`。** 今天最后一个 `Awaitable` 析构后，channel 仍被 lambda 持有且未关闭，`push` 返回 `success` 并把值存进无人取的队列；改后 hub 置为关闭，明确告知生产者已无接收方。
4. **`channel()` 返回类型变更**为 `shared_ptr<ChannelHub<T>>`。调用点均用 `auto`，不需改动；显式写出该类型的代码需要跟进。
5. **终止原因 `connection_aborted` 不再出现。** 产生它的析构收敛路径已被结构性消除（§3.3）。

## 8. 已知限制

- 消费者队列仍是有容量上限的 FIFO（默认 1024，`0` 表示无限），超限丢弃队首最旧值。对字节流是在流中间开洞而非丢尾部；对承载 `QTcpSocket*` / `QNetworkDatagram` 等自身即资源的值是丢失整个连接/数据报。不能容忍丢失的场景需调大容量或传 `0`，框架不提供丢弃计数。
- 无 replay。`shared()` 之前产生的数据对新订阅者不可见。
- 扇出在生产者线程同步完成，消费者越多每次 `push` 越慢。
- 订阅者全部消失即终止上游，不可恢复（§7.2）。

## 9. 测试计划

位置：`test/testfiberawait`。

现有用例全部保留，其中两条需要跟进：

| 用例 | 跟进 |
|---|---|
| 终止传播 | 改写为生产者侧 `ch->close(connection_reset)` 传播到每个消费者；源句柄 `close()` 改为验证"只关自己" |
| `sizeof` 钉死 | `FiberChannel` 更新为实测值，补 `ChannelHub` 一条 |

新增用例：

| 用例 | 验证内容 |
|---|---|
| 源句柄析构后订阅者继续 | `shared()` 两个订阅者后丢弃源句柄，上游不断、订阅者继续收齐后续全部数据 |
| 源队列随句柄释放 | 源句柄析构后继续 push，源那条队列不再持有任何值（以承载资源的值观测，配合 ASan） |
| 无消费者不囤积 | 所有句柄消失后 push 返回 `closed`，不产生 `T` 拷贝、不增长内存 |
| 清理恰好一次 | 最后一条未关闭的队列消失时清理跑一次；`close()` 后再析构不重复跑 |
| 平表级联 | `a->shared()->shared()` 后 `setCapacity` / `discard_pending` 到达每一条队列 |
| `close()` 只关自己 | 订阅者 `close()`，源与其他订阅者不受影响；源 `close()`，订阅者不受影响 |
| 已关闭队列仍可 discard | 消费者 `close()` 后 `discard_pending()` 仍清空其队列（`corotcpserver` 悬空指针回归） |
| `void` 特化 | 上述关键路径在 `Awaitable<void>` 上重跑 |
| 跨线程 | 订阅者在另一个 `QtFiberThread` 上消费 |
| 正常退出 | 进程能正常退出（回归已知的 `testfiberawait` 退出挂起问题） |

回归范围：`test/testfiberawait`、`test/testfibertask`、`test/testexecutor`、`test/test_scheduler` 全部通过。

## 10. 文档更新

- `doc/使用说明.md`：更新 `FiberChannel` 条目（改为通用队列）并新增 `ChannelHub`；§6.7 广播消费一节改写源队列与生命周期的描述。
- `doc/软件设计说明.md`：§5.5 与 `FiberChannel` / `Awaitable` 的单元设计段落按新结构重写，替换关于 `mirrors_` 的整段说明。
- `doc/架构设计.md` §2.2、`doc/类图与时序图.md` 的类图：加入 `ChannelHub`，关系由 `Awaitable *-- FiberChannel` 改为 `Awaitable --> ChannelHub`、`Awaitable *-- FiberChannel`、`ChannelHub o-- FiberChannel`。
- `doc/需求规格说明.md` RX_CHANNEL_DATA：区分通用队列与分发端两项职责。
- `skill/using-asynctask/SKILL.md`：如含 `channel()` 类型或广播用法的描述，同步跟进。
