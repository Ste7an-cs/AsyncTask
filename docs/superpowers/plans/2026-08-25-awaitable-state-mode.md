# Awaitable 状态模式 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 `Awaitable` 增加"状态模式"——值一旦产生就持久存在，多次 `await()` 立即返回同一个最新值；可清空使其回到挂起；队列模式行为逐条不变。

**Architecture:** 状态语义是流一级的属性，存在 `ChannelHub` 上：状态模式的 hub 额外持有一条**容量为 1** 的 `FiberChannel<T>` 作"状态格"。赋值即 `push`（容量 1 自动丢旧留新），读取用新增的非破坏性 `wait_peek()`。所有消费者 peek 同一格，因此"多次读值不变"是结构性保证。消费者队列在状态模式下不投递，仅用于既有的生命周期记账。

**Tech Stack:** C++17、boost.fiber（`/usr/local`，静态库）、Qt 5.15.3、qmake、QtTest、AddressSanitizer。

设计依据：`docs/superpowers/specs/2026-08-25-awaitable-state-mode-design.md`。实现中若发现与 spec 冲突，以 spec 为准并回头更新 spec，不要静默偏离。

## Global Constraints

- C++17，头文件-only 的模板代码放在 `coro/detail/` 与 `coro/await/`。
- `coro/detail/` 下的代码**不得依赖 Qt**：非 Qt 构建仍要能用 `Result` / `FiberChannel` / `ChannelHub` / `FiberScheduler` 基类 / `FiberTask`。
- 中文注释，Doxygen 风格（`@brief` / `@details` / `@param` / `@return` / `@code` / `@warning`），与现有文件保持一致的密度。
- 锁顺序恒为 hub → queue（含状态格），绝不反向；清理钩子必须在 hub 的 fiber mutex 之外执行。
- `ChannelHub` 的 `guard_` 成员**必须保持声明在最后**——析构时最先销毁，此时 `mtx_`/`consumers_` 仍存活，回调可安全重入 hub。新增成员一律加在 `guard_` **之前**。
- **队列模式的行为必须逐字节不变。** 特别是 `await()` 的队列分支：现有 `pop()` 在"已关闭但仍有余量"时先把余量取完，绝不可在队列分支前插入任何提前返回。
- 各 `coro*` 工厂文件（`corosignal.hpp` / `corofuture.hpp` / `coroiodevice.hpp` / `corosocket.hpp` / `corosslsocket.hpp` / `corotcpserver.hpp` / `corolocalsocket.hpp` / `corolocalserver.hpp` / `coroudpsocket.hpp`）**不应产生任何改动**——它们默认构造 `Awaitable`，即队列模式。
- 每个任务结束时全量测试必须通过，且**不得留下失败或被注释掉的用例**。

### 分支

本计划在特性分支上执行。执行前：

```bash
cd /home/david/zpj/Framework-dev/AsyncTask && git checkout -b feat/awaitable-state-mode
```

### 构建与运行命令（每个任务反复用到）

构建目录已配置好，直接用：

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait
```

构建目录若不存在，重建它（**不要**删除或重配已存在的目录）：

```bash
mkdir -p /tmp/at-build/testfiberawait && cd /tmp/at-build/testfiberawait && \
qmake /home/david/zpj/Framework-dev/AsyncTask/test/testfiberawait/testfiberawait.pro
```

只跑单个或若干用例：

```bash
cd /tmp/at-build/testfiberawait && ./testfiberawait test_case_channel_wait_peek_does_not_consume
```

**改了头文件后 qmake 的依赖可能不完整**，出现莫名其妙的旧行为时用 `make clean && make -j$(nproc)` 重来。

**基线（本计划开始前实测）：`Totals: 101 passed, 0 failed, 0 skipped`。**

其余三个测试工程的回归（Task 4 之前至少跑一次）：

```bash
for t in testfibertask testexecutor test_scheduler/testscheduler.pro; do
  d=/tmp/at-build/$(basename $t .pro); mkdir -p $d && cd $d && \
  qmake /home/david/zpj/Framework-dev/AsyncTask/test/$t && make -j$(nproc) && ./$(basename $d) || echo "FAILED: $t";
done
```

### 环境注意

本机 shell 导出了 SOCKS5 代理变量（`all_proxy=socks5://127.0.0.1:7897`）。Qt 会据此把 `QAbstractSocket::bind()` 路由进代理引擎，导致 `QUdpSocket::bind()` 返回 true 但 `localPort()` 恒为 0。现有 socket 用例已逐个 `setProxy(QNetworkProxy::NoProxy)` 规避，**本计划新增的用例都不涉及 socket，不受影响**。若某次运行出现 UDP 用例失败，先确认是否误改了那些 `setProxy` 调用，而不是去动网络配置。

---

### Task 1: `FiberChannel` 增加非破坏性读取，并把 `push` 改为唤醒全部

状态格需要"读而不取"，且需要一次赋值唤醒**所有**等待者。这两件事都落在 `FiberChannel` 上，与 `Awaitable` 无关，可独立测通。

**Files:**
- Modify: `coro/detail/fiberchannel.hpp`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: `FiberChannel<T>` 的现有成员 `push` / `pop` / `close` / `close_error` / `setCapacity` / `is_closed`。
- Produces:
  - `boost::fibers::channel_op_status FiberChannel<T>::wait_peek(T& out)`
  - `template<Rep,Period> boost::fibers::channel_op_status FiberChannel<T>::wait_peek_for(T& out, std::chrono::duration<Rep,Period> const&)`
  - `push()` 的唤醒语义由 `notify_one` 变为 `notify_all`

---

- [ ] **Step 1: 写失败测试**

在 `test/testfiberawait/tst_testfiberawait.cpp` 的 `private slots:` 段中，`void test_case_channel_layout_size();` 这一行**之前**插入四条声明：

```cpp
    void test_case_channel_wait_peek_does_not_consume();
    void test_case_channel_wait_peek_closed_even_with_value();
    void test_case_channel_wait_peek_for_timeout();
    void test_case_channel_push_wakes_all_peekers();
```

在文件中 `void TestFiberAwait::test_case_channel_layout_size()` 的实现**之前**插入这四个实现：

```cpp
/// @brief 验证 wait_peek 读取队首但不弹出，因此可反复读到同一个值。
void TestFiberAwait::test_case_channel_wait_peek_does_not_consume()
{
    auto ch = std::make_shared<Coro::FiberChannel<int>>();
    QCOMPARE(ch->push(7), boost::fibers::channel_op_status::success);

    int first{};
    QCOMPARE(ch->wait_peek(first), boost::fibers::channel_op_status::success);
    QCOMPARE(first, 7);

    int second{};
    QCOMPARE(ch->wait_peek(second), boost::fibers::channel_op_status::success);
    QCOMPARE(second, 7);                 // 仍是同一个值：peek 不消费

    // 值确实还在队列里——pop 依然能取到它
    int popped{};
    QCOMPARE(ch->pop(popped), boost::fibers::channel_op_status::success);
    QCOMPARE(popped, 7);
}

/// @brief 验证 channel 已关闭时 wait_peek 一律返回 closed，即使格中仍有值。
/// @details 这与 pop 的"先取完余量再报关闭"是**刻意的差异**：peek 不消费，
///          "余量"概念对它不成立；更重要的是，这是状态模式下唯一可能的终止信号。
///          同一用例里对照验证 pop 的余量语义未被本次改动破坏。
void TestFiberAwait::test_case_channel_wait_peek_closed_even_with_value()
{
    auto ch = std::make_shared<Coro::FiberChannel<int>>();
    QCOMPARE(ch->push(5), boost::fibers::channel_op_status::success);
    ch->close(std::make_error_code(std::errc::connection_reset));

    int peeked{-1};
    QCOMPARE(ch->wait_peek(peeked), boost::fibers::channel_op_status::closed);
    QCOMPARE(peeked, -1);                // out 不被写入
    QCOMPARE(ch->close_error(), std::make_error_code(std::errc::connection_reset));

    // 对照：pop 仍保留"先取完余量"的既有语义
    int popped{};
    QCOMPARE(ch->pop(popped), boost::fibers::channel_op_status::success);
    QCOMPARE(popped, 5);
    QCOMPARE(ch->pop(popped), boost::fibers::channel_op_status::closed);
}

/// @brief 验证空且未关闭时 wait_peek_for 到期返回 timeout，且 channel 保持开放。
void TestFiberAwait::test_case_channel_wait_peek_for_timeout()
{
    auto ch = std::make_shared<Coro::FiberChannel<int>>();
    int value{};
    QCOMPARE(ch->wait_peek_for(value, std::chrono::milliseconds(20)),
             boost::fibers::channel_op_status::timeout);
    QVERIFY(!ch->is_closed());

    // 超时不影响后续：有值之后照常读到
    QCOMPARE(ch->push(3), boost::fibers::channel_op_status::success);
    QCOMPARE(ch->wait_peek_for(value, std::chrono::milliseconds(20)),
             boost::fibers::channel_op_status::success);
    QCOMPARE(value, 3);
}

/// @brief 验证一次 push 会唤醒**全部**挂在 wait_peek 上的等待者。
/// @details 状态模式下 N 个消费者 peek 同一个状态格，谁都不消费，因此 notify_one
///          只会唤醒其中一个、其余继续睡到下一次赋值——这是会静默漏唤醒的缺陷。
///          本用例是该缺陷唯一的防线：改回 notify_one 时，两个等待者会超时而
///          woken 只到 1。
void TestFiberAwait::test_case_channel_push_wakes_all_peekers()
{
    auto cell = std::make_shared<Coro::FiberChannel<int>>();
    cell->setCapacity(1);
    std::atomic_int woken{0};

    auto peeker = [cell, &woken]{
        return Coro::makeTask([cell, &woken]{
            int v{};
            // 用带超时的版本：漏唤醒时表现为干净的失败而非整套挂死
            if(cell->wait_peek_for(v, std::chrono::seconds(2))
                    == boost::fibers::channel_op_status::success && v == 9){
                ++woken;
            }
            return 0;
        }, Coro::Priority::Normal, Coro::Affinity::sticky());
    };
    auto t1 = peeker();
    auto t2 = peeker();
    auto t3 = peeker();

    auto producer = Coro::makeTask([cell]{
        Coro::msleep(50);                // 先让三个等待者都挂上去
        cell->push(9);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    producer.get();
    t1.get();
    t2.get();
    t3.get();
    QCOMPARE(woken.load(), 3);
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) 2>&1 | tail -20
```

预期：编译失败，`error: 'class Coro::FiberChannel<int>' has no member named 'wait_peek'`。

- [ ] **Step 3: 新增 `wait_peek` 与 `wait_peek_for`**

在 `coro/detail/fiberchannel.hpp` 中，`pop_wait_for` 的实现之后、`value_pop` 的文档注释之前，插入：

```cpp
    /**
     * @brief 读取队首元素但**不弹出**，无可用值时阻塞/协程等待。
     * @details 与 pop() 有一处**刻意的差异**：只要 channel 已关闭就返回 closed，
     *          无论队列中是否仍有值，`out` 也不被写入。pop() 的"已关闭但仍有余量时
     *          先取完余量"对 peek 不成立——peek 不消费，"余量"概念无从谈起；而且
     *          这正是"状态"类用法唯一可能的终止信号：若关闭后仍返回值，读取方将
     *          永远成功、永不报错，无从得知来源已经终止。
     *          取到的是队首的**拷贝**，元素仍留在队列中，可被反复读取。
     * @param out 输出参数，队首元素的拷贝
     * @return 成功返回 success；channel 已关闭返回 closed
     * @code
     * int v{};
     * // 反复读同一个值：适合把 channel 当"状态格"用（配合 setCapacity(1)）
     * if(cell->wait_peek(v) == boost::fibers::channel_op_status::success) use(v);
     * @endcode
     */
    channel_status wait_peek(T& out){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        cv_consumer_.wait(lck, [this](){return !queue_.empty() || closed_.load();});
        if(closed_.load()){
            return channel_status::closed;
        }
        out = queue_.front();
        return channel_status::success;
    }
    /**
     * @brief 读取队首元素但不弹出，无可用值则等待，最长等待 timeout_duration。
     * @details 终止语义同 wait_peek()：channel 已关闭即返回 closed，不论是否仍有值。
     *          超时仅表示本次等待到期，channel 仍可保持开放。
     * @tparam Rep 时长的计数类型
     * @tparam Period 时长的周期类型
     * @param out 输出参数，队首元素的拷贝
     * @param timeout_duration 超时时间
     * @return 成功返回 success；超时返回 timeout；channel 已关闭返回 closed
     * @code
     * int v{};
     * auto st = cell->wait_peek_for(v, std::chrono::milliseconds(100));
     * if(st == boost::fibers::channel_op_status::timeout){
     *     // 仅本次等待到期，channel 仍开放
     * }
     * @endcode
     */
    template< typename Rep, typename Period >
    channel_status wait_peek_for( T & out,
                                  std::chrono::duration< Rep, Period > const& timeout_duration){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        const bool ready = cv_consumer_.wait_for(
                    lck, timeout_duration, [this](){return !queue_.empty() || closed_.load();});
        if(!ready){
            return channel_status::timeout;
        }
        if(closed_.load()){
            return channel_status::closed;
        }
        out = queue_.front();
        return channel_status::success;
    }
```

- [ ] **Step 4: 把 `push` 的唤醒改为全部**

在同一文件的 `push()` 中，把

```cpp
        cv_consumer_.notify_one();
```

改为

```cpp
        // notify_all 而非 notify_one：wait_peek 的等待者不消费元素，一次 push 必须
        // 唤醒全部，否则挂在同一条 channel 上的多个 peek 只会醒来一个（静默漏唤醒）。
        // 对 pop 一侧是安全的——pop/pop_wait_for/value_pop 均使用带谓词的 wait，
        // 多余唤醒会自行重新等待；代价仅是多消费者抢同一条队列时的一次惊群。
        cv_consumer_.notify_all();
```

同时在 `push()` 的 Doxygen `@details` 末尾补一句：

```
     *          唤醒采用 notify_all：非破坏性的 wait_peek() 等待者不消费元素，
     *          必须全部唤醒。
```

- [ ] **Step 5: 运行新用例，确认通过**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && \
./testfiberawait test_case_channel_wait_peek_does_not_consume \
  test_case_channel_wait_peek_closed_even_with_value \
  test_case_channel_wait_peek_for_timeout \
  test_case_channel_push_wakes_all_peekers
```

预期：`Totals: 6 passed, 0 failed`（4 个用例 + `initTestCase` + `cleanupTestCase`）。

- [ ] **Step 6: 全量回归**

```bash
cd /tmp/at-build/testfiberawait && ./testfiberawait 2>&1 | grep -E "^(FAIL|Totals)"
```

预期：`Totals: 105 passed, 0 failed, 0 skipped`（基线 101 + 新增 4）。

`notify_one` → `notify_all` 是行为改动，**特别留意 `test_case_broadcast_coexists_with_competing_consumers` 必须仍然通过**——它在同一条队列上放了两个竞争消费者，验证"合起来不重不漏"。若它挂了，说明惊群下的抢占出了问题，回头查而不是改测试。

- [ ] **Step 7: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/detail/fiberchannel.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(channel): FiberChannel 新增非破坏性 wait_peek，push 改为唤醒全部

wait_peek 读队首但不弹出，可反复读到同一个值；与 pop 刻意不同的是
channel 已关闭即返回 closed，不论是否仍有值——这是"状态"类用法唯一
可能的终止信号。push 相应改为 notify_all，否则多个 peek 等待者只会
醒来一个。"
```

---

### Task 2: `ChannelHub` 增加状态模式

hub 在状态模式下额外持有一条容量 1 的状态格，`push` 只写状态格而不扇出。本任务只改 hub，`Awaitable` 尚未接线。

**Files:**
- Modify: `coro/detail/channelhub.hpp`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: Task 1 的 `FiberChannel<T>::wait_peek` / `wait_peek_for`；`FiberChannel` 的既有成员。
- Produces:
  - `enum class Coro::AwaitMode { Queue, State };`
  - `explicit ChannelHub<T>::ChannelHub(AwaitMode mode = AwaitMode::Queue)`
  - `bool ChannelHub<T>::isState() const noexcept`
  - `const std::shared_ptr<FiberChannel<T>>& ChannelHub<T>::stateCell() const`
  - `push` / `close(ec)` / `discard_pending` / `setCapacity` / `capacity` 在状态模式下的分派行为

---

- [ ] **Step 1: 写失败测试**

在 `private slots:` 段中 Task 1 加的四条声明之后追加：

```cpp
    void test_case_hub_state_keeps_latest();
    void test_case_hub_state_no_fanout_to_queues();
    void test_case_hub_state_capacity_is_fixed();
    void test_case_hub_state_close_and_discard();
    void test_case_hub_queue_mode_defaults_unchanged();
```

在 Task 1 那四个实现之后追加：

```cpp
/// @brief 验证状态模式的 hub 只保留最新值，且可被反复读取。
void TestFiberAwait::test_case_hub_state_keeps_latest()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>(Coro::AwaitMode::State);
    QVERIFY(hub->isState());
    QVERIFY(hub->stateCell() != nullptr);

    QCOMPARE(hub->push(1), boost::fibers::channel_op_status::success);
    QCOMPARE(hub->push(2), boost::fibers::channel_op_status::success);
    QCOMPARE(hub->push(3), boost::fibers::channel_op_status::success);

    int v{};
    QCOMPARE(hub->stateCell()->wait_peek(v), boost::fibers::channel_op_status::success);
    QCOMPARE(v, 3);                      // 最新值覆盖
    QCOMPARE(hub->stateCell()->wait_peek(v), boost::fibers::channel_op_status::success);
    QCOMPARE(v, 3);                      // 反复读不变
}

/// @brief 验证状态模式下 push 不向消费者队列扇出——内存与消费者数量无关。
void TestFiberAwait::test_case_hub_state_no_fanout_to_queues()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>(Coro::AwaitMode::State);
    auto q1 = std::make_shared<Coro::FiberChannel<int>>();
    auto q2 = std::make_shared<Coro::FiberChannel<int>>();
    hub->attach(q1);
    hub->attach(q2);

    for(int i = 0; i < 5; ++i){
        QCOMPARE(hub->push(i), boost::fibers::channel_op_status::success);
    }

    // 消费者队列始终为空：状态模式只写状态格
    int v{};
    QCOMPARE(q1->wait_peek_for(v, std::chrono::milliseconds(20)),
             boost::fibers::channel_op_status::timeout);
    QCOMPARE(q2->wait_peek_for(v, std::chrono::milliseconds(20)),
             boost::fibers::channel_op_status::timeout);
}

/// @brief 验证状态模式下容量固定为 1，setCapacity 无效。
void TestFiberAwait::test_case_hub_state_capacity_is_fixed()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>(Coro::AwaitMode::State);
    QCOMPARE(hub->capacity(), std::uint32_t(1));

    hub->setCapacity(64);                // 状态天然容量 1，应被忽略
    QCOMPARE(hub->capacity(), std::uint32_t(1));

    QCOMPARE(hub->push(1), boost::fibers::channel_op_status::success);
    QCOMPARE(hub->push(2), boost::fibers::channel_op_status::success);
    int v{};
    QCOMPARE(hub->stateCell()->wait_peek(v), boost::fibers::channel_op_status::success);
    QCOMPARE(v, 2);                      // 仍只保留一个
}

/// @brief 验证状态模式下 close 关闭状态格、discard_pending 清空状态格。
void TestFiberAwait::test_case_hub_state_close_and_discard()
{
    // discard：值被清掉，状态格回到空但仍开放
    {
        auto hub = std::make_shared<Coro::ChannelHub<int>>(Coro::AwaitMode::State);
        QCOMPARE(hub->push(1), boost::fibers::channel_op_status::success);
        hub->discard_pending();
        int v{};
        QCOMPARE(hub->stateCell()->wait_peek_for(v, std::chrono::milliseconds(20)),
                 boost::fibers::channel_op_status::timeout);
        QVERIFY(!hub->stateCell()->is_closed());
    }
    // close：状态格随之关闭，且带上同一个终止原因
    {
        auto hub = std::make_shared<Coro::ChannelHub<int>>(Coro::AwaitMode::State);
        QCOMPARE(hub->push(1), boost::fibers::channel_op_status::success);
        hub->close(std::make_error_code(std::errc::connection_reset));
        QVERIFY(hub->stateCell()->is_closed());
        QCOMPARE(hub->stateCell()->close_error(),
                 std::make_error_code(std::errc::connection_reset));
        int v{};
        QCOMPARE(hub->stateCell()->wait_peek(v), boost::fibers::channel_op_status::closed);
        QCOMPARE(hub->push(2), boost::fibers::channel_op_status::closed);
    }
}

/// @brief 验证默认构造仍是队列模式，既有默认值不变。
void TestFiberAwait::test_case_hub_queue_mode_defaults_unchanged()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>();
    QVERIFY(!hub->isState());
    QVERIFY(hub->stateCell() == nullptr);
    QCOMPARE(hub->capacity(), Coro::FiberChannel<int>::kDefaultCapacity);

    // 扇出照常
    auto q = std::make_shared<Coro::FiberChannel<int>>();
    hub->attach(q);
    QCOMPARE(hub->push(42), boost::fibers::channel_op_status::success);
    int v{};
    QCOMPARE(q->pop(v), boost::fibers::channel_op_status::success);
    QCOMPARE(v, 42);
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) 2>&1 | tail -20
```

预期：编译失败，`error: 'AwaitMode' is not a member of 'Coro'`。

- [ ] **Step 3: 新增 `AwaitMode` 枚举**

在 `coro/detail/channelhub.hpp` 中，`} // namespace detail` 之后、`ChannelHub` 的文档注释之前，插入：

```cpp
/**
 * @brief 数据流的语义模式，构造时确定、此后不可变。
 * @warning **State 模式下 `await()` 不消费、立即返回当前值**，因此
 *          `while(auto v = Coro::await(a))` 与 `Coro::generate(a)` 会满速空转，
 *          直到流关闭才终止。这两种写法只适用于 Queue 模式；在 State 模式下
 *          请改用一次性的读取。
 */
enum class AwaitMode {
    Queue,   ///< 队列：破坏性消费，一个值只被取走一次（默认，与既有行为一致）
    State,   ///< 状态：非破坏性读取，多次读取返回同一个最新值
};
```

- [ ] **Step 4: 给 `ChannelHub` 加状态格**

在 `coro/detail/channelhub.hpp` 中做四处改动。

其一，把默认构造函数替换为带模式的构造函数（原 `ChannelHub() = default;` 及其上方注释）：

```cpp
    /**
     * @brief 构造一个没有任何消费者的分发端。
     * @details `AwaitMode::State` 时额外创建一条容量为 1 的"状态格"：赋值即
     *          push（容量 1 自动丢旧留新，正是"最新值"语义），读取走
     *          FiberChannel::wait_peek()（不消费）。队列模式下状态格为空指针，
     *          不产生任何堆分配。模式此后不可变。
     * @param mode 语义模式，默认为队列模式
     * @code
     * auto queueHub = std::make_shared<Coro::ChannelHub<int>>();
     * auto stateHub = std::make_shared<Coro::ChannelHub<int>>(Coro::AwaitMode::State);
     * @endcode
     */
    explicit ChannelHub(AwaitMode mode = AwaitMode::Queue){
        if(mode == AwaitMode::State){
            state_ = std::make_shared<FiberChannel<T>>();
            state_->setCapacity(1);
        }
    }
```

其二，在 `setOnClose` 的实现之后、`private:` 之前，新增两个查询方法：

```cpp
    /**
     * @brief 查询本流是否为状态模式
     * @return 状态模式返回 true
     * @code
     * if(hub->isState()) qDebug() << "值可反复读取";
     * @endcode
     */
    bool isState() const noexcept { return state_ != nullptr; }
    /**
     * @brief 取得状态格；队列模式下为空指针。
     * @details 供 Awaitable 在状态模式下读取当前值。状态格的存活由本 hub 保证，
     *          调用方持有 hub 即可安全地在格上长时间等待——等待发生在格自己的
     *          锁上，不占用 hub 的锁。
     * @return 状态格的 shared_ptr；队列模式下为空
     * @code
     * int v{};
     * if(hub->isState()) hub->stateCell()->wait_peek(v);
     * @endcode
     */
    const std::shared_ptr<FiberChannel<T>>& stateCell() const { return state_; }
```

其三，新增数据成员。在 `private:` 段中 `close_error_` 之后、`guard_` **之前**插入（`guard_` 必须保持在最后）：

```cpp
    std::shared_ptr<FiberChannel<T>> state_;///< 状态格：容量 1 的队列，非空即状态模式；队列模式下为空指针
```

其四，在 `push` 的关闭检查之后、扇出循环之前插入状态分派：

```cpp
        if(state_){
            // 状态模式：只写状态格（容量 1 自动丢旧留新），不向消费者队列扇出——
            // 因此内存与消费者数量无关，也不随赋值次数增长。
            state_->push(std::move(value));
            return channel_status::success;
        }
```

- [ ] **Step 5: 让 `close` / `discard_pending` / `setCapacity` / `capacity` 分派状态模式**

在 `close(std::error_code error)` 中，`closed_.store(true);` 之后、消费者扇出循环之前插入：

```cpp
        if(state_){
            state_->close(close_error_);
        }
```

在 `discard_pending()` 的加锁之后、遍历消费者表之前插入：

```cpp
        if(state_){
            state_->discard_pending();
        }
```

把 `setCapacity(std::uint32_t capacity)` 的函数体开头改为（加锁之后立即判断）：

```cpp
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(state_){
            // 状态天然容量 1，容量概念对它不适用，忽略本次设置
            return;
        }
        capacity_ = capacity;
```

把 `capacity()` 的返回改为：

```cpp
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return state_ ? std::uint32_t(1) : capacity_;
```

同时在 `setCapacity` 与 `capacity` 的 `@details` 中各补一句"状态模式下容量固定为 1，setCapacity 为空操作"。

- [ ] **Step 6: 运行新用例，确认通过**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && \
./testfiberawait test_case_hub_state_keeps_latest test_case_hub_state_no_fanout_to_queues \
  test_case_hub_state_capacity_is_fixed test_case_hub_state_close_and_discard \
  test_case_hub_queue_mode_defaults_unchanged
```

预期：`Totals: 7 passed, 0 failed`。

- [ ] **Step 7: 全量回归并更新 `sizeof` 钉死值**

```bash
cd /tmp/at-build/testfiberawait && ./testfiberawait 2>&1 | grep -E "^(FAIL|Totals)"
```

`test_case_channel_layout_size` 会失败——`ChannelHub` 多了一个 `shared_ptr` 成员。把该用例临时改成打印实际值：

```cpp
void TestFiberAwait::test_case_channel_layout_size()
{
    qDebug() << "FiberChannel" << sizeof(Coro::FiberChannel<int>)
             << "ChannelHub" << sizeof(Coro::ChannelHub<int>);
    QCOMPARE(sizeof(Coro::FiberChannel<int>), std::size_t(160));
    QCOMPARE(sizeof(Coro::ChannelHub<int>), std::size_t(160));
}
```

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait test_case_channel_layout_size
```

**记下 `QDEBUG` 行打印的两个实际数字**（`FiberChannel` 应仍为 160；`ChannelHub` 预计增至 176，但以实测为准，不要照抄这个预测值），然后把用例改回断言形式，用实测值填入：

```cpp
/// @brief 固定队列与分发端的布局大小，防止新增字段静默跨过 glibc 分配桶。
/// @details 两者都由 make_shared 创建，加 16 字节控制块后落入分配桶；体积跳变会
///          悄悄增加每条流的堆占用，因此在这里钉死。换平台需重新测定。
void TestFiberAwait::test_case_channel_layout_size()
{
    QCOMPARE(sizeof(Coro::FiberChannel<int>), std::size_t(<实测的 FiberChannel 值>));
    QCOMPARE(sizeof(Coro::ChannelHub<int>), std::size_t(<实测的 ChannelHub 值>));
}
```

再次全量：

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait 2>&1 | grep -E "^(FAIL|Totals)"
```

预期：`Totals: 110 passed, 0 failed, 0 skipped`（105 + 新增 5）。

- [ ] **Step 8: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/detail/channelhub.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(channel): ChannelHub 增加状态模式

状态模式的 hub 额外持有一条容量 1 的状态格，push 只写状态格而不向
消费者队列扇出，因此内存与消费者数量无关。state_ 非空即状态模式，
不设单独的枚举成员；模式构造时确定、此后不可变。"
```

---

### Task 3: `Awaitable` 接线到状态模式

两个特化各新增模式构造函数与 `discardPending()`，`await` / `await_for` 按模式分派。本任务落地全部面向使用者的语义。

**Files:**
- Modify: `coro/await/awaitable.hpp`（`Awaitable<T>` 与 `Awaitable<void>` 两个特化）
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: Task 2 的 `Coro::AwaitMode`、`ChannelHub<T>::ChannelHub(AwaitMode)`、`isState()`、`stateCell()`；Task 1 的 `wait_peek` / `wait_peek_for`。
- Produces:
  - `explicit Awaitable<T>::Awaitable(AwaitMode mode)`（`void` 特化同名）
  - `void Awaitable<T>::discardPending()`（`void` 特化同名）
  - `await()` / `await_for()` 在状态模式下的非破坏性语义

---

- [ ] **Step 1: 写失败测试**

在 `private slots:` 段中 Task 2 加的五条声明之后追加：

```cpp
    void test_case_state_repeated_await_same_value();
    void test_case_state_waits_for_first_value();
    void test_case_state_latest_wins();
    void test_case_state_shared_inherits_and_sees_current();
    void test_case_state_discard_suspends_then_all_resume();
    void test_case_state_close_terminates_even_with_value();
    void test_case_state_handle_close_isolated();
    void test_case_state_memory_is_constant();
    void test_case_state_capacity_noop();
    void test_case_state_void_specialization();
    void test_case_state_moved_from_shell();
```

在 Task 2 那五个实现之后追加：

```cpp
/// @brief 验证状态模式下多次 await 立即返回同一个值。
void TestFiberAwait::test_case_state_repeated_await_same_value()
{
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    QVERIFY(st.resolve(42));

    QCOMPARE(st.await().value(), 42);
    QCOMPARE(st.await().value(), 42);
    QCOMPARE(st.await().value(), 42);    // 值不因读取而消失
}

/// @brief 验证尚无值时 await 阻塞，赋值后才返回。
void TestFiberAwait::test_case_state_waits_for_first_value()
{
    using namespace std::chrono_literals;
    Coro::Awaitable<int> st{Coro::AwaitMode::State};

    auto pending = st.await_for(50ms);
    QVERIFY(!pending);
    QCOMPARE(pending.error(), std::make_error_code(std::errc::timed_out));

    QVERIFY(st.resolve(7));
    QCOMPARE(st.await().value(), 7);
}

/// @brief 验证连续赋值时读到的恒为最后一个值。
void TestFiberAwait::test_case_state_latest_wins()
{
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    QVERIFY(st.resolve(1));
    QVERIFY(st.resolve(2));
    QVERIFY(st.resolve(3));
    QCOMPARE(st.await().value(), 3);
}

/// @brief 验证 shared() 自动继承状态模式，且新订阅者立刻读到当前值。
/// @details 状态模式不需要 replay 机制——状态本就在共享的状态格上。
void TestFiberAwait::test_case_state_shared_inherits_and_sees_current()
{
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    QVERIFY(st.resolve(5));

    auto late = st.shared();             // 赋值之后才订阅
    QCOMPARE(late->await().value(), 5);  // 仍能读到当前值
    QCOMPARE(late->await().value(), 5);
    QCOMPARE(st.await().value(), 5);     // 源与订阅者读同一份
}

/// @brief 验证清空使状态回到挂起，且下一次赋值唤醒**全部**等待者。
/// @details 这是本设计最关键的防线：状态格上多个消费者都不消费，若 push 只
///          notify_one，则只有一个被唤醒、其余超时，woken 会停在 1。
void TestFiberAwait::test_case_state_discard_suspends_then_all_resume()
{
    using namespace std::chrono_literals;
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    auto sub1 = st.shared();
    auto sub2 = st.shared();

    QVERIFY(st.resolve(1));
    QCOMPARE(st.await().value(), 1);

    st.discardPending();                 // 回到无值：全体重新挂起
    auto suspended = Coro::await_for(sub1, 50ms);
    QVERIFY(!suspended);
    QCOMPARE(suspended.error(), std::make_error_code(std::errc::timed_out));

    std::atomic_int woken{0};
    auto waiter = [&woken](std::shared_ptr<Coro::Awaitable<int>> handle){
        return Coro::makeTask([handle, &woken]{
            auto r = Coro::await_for(handle, std::chrono::seconds(2));
            if(r && r.value() == 9) ++woken;
            return 0;
        }, Coro::Priority::Normal, Coro::Affinity::sticky());
    };
    auto t1 = waiter(sub1);
    auto t2 = waiter(sub2);
    auto producer = Coro::makeTask([ch = st.channel()]{
        Coro::msleep(50);                // 先让两个等待者都挂上去
        ch->push(9);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    producer.get();
    t1.get();
    t2.get();
    QCOMPARE(woken.load(), 2);
    QCOMPARE(st.await().value(), 9);
}

/// @brief 验证关闭是状态模式下的终止信号：即使格中仍有值，await 也返回终止错误。
/// @details 这是拿"最后的值"换"可检测的终止"。没有它，状态模式下 await 永远
///          成功、永不报错，消费者无从得知流已终止，会对着陈旧值空转。
void TestFiberAwait::test_case_state_close_terminates_even_with_value()
{
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    QVERIFY(st.resolve(3));
    QCOMPARE(st.await().value(), 3);

    st.channel()->close(std::make_error_code(std::errc::connection_reset));

    auto ended = st.await();
    QVERIFY(!ended);
    QCOMPARE(ended.error(), std::make_error_code(std::errc::connection_reset));

    // 因此 while(await(a)) 这类循环能够退出
    int spins = 0;
    while(auto v = st.await()){
        Q_UNUSED(v);
        if(++spins > 3) break;           // 若未收敛，这里会兜住并使断言失败
    }
    QCOMPARE(spins, 0);
}

/// @brief 验证句柄自己 close() 只影响自己，其他消费者照常读到状态。
void TestFiberAwait::test_case_state_handle_close_isolated()
{
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    auto sub = st.shared();
    QVERIFY(st.resolve(8));

    sub->close(std::make_error_code(std::errc::operation_canceled));

    auto closed = sub->await();
    QVERIFY(!closed);
    QCOMPARE(closed.error(), std::make_error_code(std::errc::operation_canceled));

    QCOMPARE(st.await().value(), 8);     // 源不受影响
}

/// @brief 验证状态模式内存为 O(1)：无论多少消费者、赋值多少次，只保留一个值。
void TestFiberAwait::test_case_state_memory_is_constant()
{
    Coro::Awaitable<std::shared_ptr<int>> st{Coro::AwaitMode::State};
    auto sub1 = st.shared();
    auto sub2 = st.shared();

    std::weak_ptr<int> older;
    {
        auto a = std::make_shared<int>(1);
        older = a;
        QVERIFY(st.resolve(a));
    }
    QVERIFY(!older.expired());           // 当前状态持有它

    std::weak_ptr<int> newer;
    {
        auto b = std::make_shared<int>(2);
        newer = b;
        QVERIFY(st.resolve(b));
    }
    QVERIFY(older.expired());            // 旧值被顶掉，不因三个消费者而多留副本
    QVERIFY(!newer.expired());

    QCOMPARE(*st.await().value(), 2);
    QCOMPARE(*sub1->await().value(), 2);
    QCOMPARE(*sub2->await().value(), 2);
}

/// @brief 验证状态模式下容量固定为 1，setCapacity 无效。
void TestFiberAwait::test_case_state_capacity_noop()
{
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    QCOMPARE(st.capacity(), std::uint32_t(1));
    st.setCapacity(64);
    QCOMPARE(st.capacity(), std::uint32_t(1));
}

/// @brief 验证 void 特化同样支持状态模式：事件是否已发生，可反复查询、可清空。
void TestFiberAwait::test_case_state_void_specialization()
{
    using namespace std::chrono_literals;
    Coro::Awaitable<void> st{Coro::AwaitMode::State};

    auto pending = st.await_for(50ms);
    QVERIFY(!pending);                   // 尚未发生

    QVERIFY(st.resolve());
    QVERIFY(st.await());                 // 已发生
    QVERIFY(st.await());                 // 反复查询不变

    st.discardPending();                 // 回到未发生
    auto again = st.await_for(50ms);
    QVERIFY(!again);
    QCOMPARE(again.error(), std::make_error_code(std::errc::timed_out));
}

/// @brief 验证被移动过的状态模式空壳句柄，await 立即返回而非挂死。
void TestFiberAwait::test_case_state_moved_from_shell()
{
    Coro::Awaitable<int> st{Coro::AwaitMode::State};
    Coro::Awaitable<int> moved(std::move(st));
    QVERIFY(moved.resolve(1));

    auto r = st.await();                 // 空壳：必须立即返回，不得阻塞
    QVERIFY(!r);
    QCOMPARE(r.error(), std::make_error_code(std::errc::no_message));

    QCOMPARE(moved.await().value(), 1);  // 接管的一路照常
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) 2>&1 | tail -20
```

预期：编译失败，`error: no matching function for call to 'Coro::Awaitable<int>::Awaitable(Coro::AwaitMode)'`。

- [ ] **Step 3: 给 `Awaitable<T>` 加模式构造与 `discardPending`**

在 `coro/await/awaitable.hpp` 的 `Awaitable<T>` 中，默认构造函数 `Awaitable(){ hub_->attach(queue_); }` 之后插入：

```cpp
    /**
     * @brief 以指定语义模式构造一条新数据流。
     * @details `AwaitMode::State` 时 await() 变为**非破坏性读取**：立即返回当前值，
     *          多次读取值不变；尚无值时阻塞等待首个值；discardPending() 可使其回到
     *          挂起。默认构造仍为队列模式，行为与既有完全一致。
     * @param mode 语义模式
     * @warning State 模式下 `while(auto v = Coro::await(a))` 与 `Coro::generate(a)`
     *          会满速空转直到流关闭——这两种写法只适用于 Queue 模式。
     * @code
     * Coro::Awaitable<int> st{Coro::AwaitMode::State};
     * st.resolve(42);
     * QCOMPARE(st.await().value(), 42);
     * QCOMPARE(st.await().value(), 42);   // 反复读同一个值
     * @endcode
     */
    explicit Awaitable(AwaitMode mode)
        : hub_(std::make_shared<ChannelHub<T>>(mode)){
        hub_->attach(queue_);
    }
```

在 `setOnClose` 的实现之后插入：

```cpp
    /**
     * @brief 丢弃待消费的值。
     * @details 队列模式：只清空本句柄自己的队列，其他消费者不受影响。
     *          状态模式：清空**共享的**状态格，此后所有消费者的 await() 都重新
     *          阻塞——状态模式下不存在"自己那一路"，这是结构决定的，无法只清自己。
     *          无论哪种模式，要显式影响整条流请用 `channel()->discard_pending()`。
     *          本操作不改变关闭状态，也不修改已保留的终止原因。
     * @code
     * st.discardPending();     // 状态回到"无值"，await 重新挂起
     * @endcode
     */
    void discardPending(){
        if(hub_ && hub_->isState()){
            if(const auto& cell = hub_->stateCell()){
                cell->discard_pending();
            }
            return;
        }
        if(queue_){
            queue_->discard_pending();
        }
    }
```

- [ ] **Step 4: 让 `Awaitable<T>` 的 `await` / `await_for` 分派状态模式**

把 `Awaitable<T>::await()` 的函数体整体替换为：

```cpp
    Result<T, std::error_code> await(){
        if(!queue_){
            return std::make_error_code(std::errc::no_message);   // 移动后的空壳
        }
        T value{};
        if(hub_ && hub_->isState()){
            // ---- 状态模式：读状态格，不消费 ----
            if(queue_->is_closed()){
                // 本句柄已 close()，或整流关闭时连带关掉了它。状态格不消费，
                // 没有"余量"概念，直接以本句柄的终止原因收尾。
                return queue_->close_error();
            }
            const auto& cell = hub_->stateCell();
            auto status = cell->wait_peek(value);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            return cell->close_error();
        }
        // ---- 队列模式：以下与既有逐字相同，不得插入任何提前返回 ----
        auto status = queue_->pop(value);
        if(status == boost::fibers::channel_op_status::success){
            return value;
        }
        return queue_->close_error();
    }
```

把 `Awaitable<T>::await_for()` 的函数体整体替换为：

```cpp
    template<typename Rep, typename Period>
    Result<T, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(!queue_){
            return std::make_error_code(std::errc::timed_out);    // 移动后的空壳
        }
        T value{};
        if(hub_ && hub_->isState()){
            if(queue_->is_closed()){
                return queue_->close_error();
            }
            const auto& cell = hub_->stateCell();
            auto status = cell->wait_peek_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return cell->close_error();
        }
        auto status = queue_->pop_wait_for(value, timeout);
        if(status == boost::fibers::channel_op_status::success){
            return value;
        }
        if(status == boost::fibers::channel_op_status::timeout){
            return std::make_error_code(std::errc::timed_out);
        }
        return queue_->close_error();
    }
```

同时在 `await()` 与 `await_for()` 的 Doxygen `@details` 中补一句：

```
     *          状态模式下本方法**不消费**：立即返回当前值，多次调用值不变；
     *          流关闭后返回终止错误（最后的值不再可读）。
```

- [ ] **Step 5: 对 `Awaitable<void>` 做同样的四处改动**

`Awaitable<void>` 的改法与 `Awaitable<T>` 完全一致，只是内部类型为 `ChannelHub<int>` / `FiberChannel<int>`，且 `await` / `await_for` 返回 `Result<void, std::error_code>`。

模式构造函数（插在默认构造之后）：

```cpp
    /**
     * @brief 以指定语义模式构造一条新数据流。
     * @details `AwaitMode::State` 时语义退化为"事件是否已发生"：已发生则 await()
     *          立即成功且可反复查询，discardPending() 使其回到未发生。默认构造仍为
     *          队列模式，行为与既有完全一致。
     * @param mode 语义模式
     * @warning State 模式下 `while(Coro::await(a))` 与 `Coro::generate(a)` 会满速
     *          空转直到流关闭。
     * @code
     * Coro::Awaitable<void> ready{Coro::AwaitMode::State};
     * ready.resolve();
     * QVERIFY(ready.await());      // 反复查询都成功
     * @endcode
     */
    explicit Awaitable(AwaitMode mode)
        : hub_(std::make_shared<ChannelHub<int>>(mode)){
        hub_->attach(queue_);
    }
```

`discardPending()`（插在 `setOnClose` 之后），与 `Awaitable<T>` 版本逐字相同：

```cpp
    /**
     * @brief 丢弃待消费的值。
     * @details 队列模式：只清空本句柄自己的队列，其他消费者不受影响。
     *          状态模式：清空**共享的**状态格，此后所有消费者的 await() 都重新
     *          阻塞——状态模式下不存在"自己那一路"，这是结构决定的，无法只清自己。
     *          无论哪种模式，要显式影响整条流请用 `channel()->discard_pending()`。
     *          本操作不改变关闭状态，也不修改已保留的终止原因。
     * @code
     * ready.discardPending();     // 回到"未发生"，await 重新挂起
     * @endcode
     */
    void discardPending(){
        if(hub_ && hub_->isState()){
            if(const auto& cell = hub_->stateCell()){
                cell->discard_pending();
            }
            return;
        }
        if(queue_){
            queue_->discard_pending();
        }
    }
```

`await()`：

```cpp
    Result<void, std::error_code> await(){
        if(!queue_){
            return std::make_error_code(std::errc::no_message);
        }
        int value{};
        if(hub_ && hub_->isState()){
            if(queue_->is_closed()){
                return queue_->close_error();
            }
            const auto& cell = hub_->stateCell();
            auto status = cell->wait_peek(value);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            return cell->close_error();
        }
        auto status = queue_->pop(value);
        if(status == boost::fibers::channel_op_status::success){
            return Result<void, std::error_code>();
        }
        return queue_->close_error();
    }
```

`await_for()`：

```cpp
    template<typename Rep, typename Period>
    Result<void, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(!queue_){
            return std::make_error_code(std::errc::timed_out);
        }
        int value{};
        if(hub_ && hub_->isState()){
            if(queue_->is_closed()){
                return queue_->close_error();
            }
            const auto& cell = hub_->stateCell();
            auto status = cell->wait_peek_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return cell->close_error();
        }
        auto status = queue_->pop_wait_for(value, timeout);
        if(status == boost::fibers::channel_op_status::success){
            return Result<void, std::error_code>();
        }
        if(status == boost::fibers::channel_op_status::timeout){
            return std::make_error_code(std::errc::timed_out);
        }
        return queue_->close_error();
    }
```

- [ ] **Step 6: 运行新用例，确认通过**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && \
./testfiberawait test_case_state_repeated_await_same_value test_case_state_waits_for_first_value \
  test_case_state_latest_wins test_case_state_shared_inherits_and_sees_current \
  test_case_state_discard_suspends_then_all_resume test_case_state_close_terminates_even_with_value \
  test_case_state_handle_close_isolated test_case_state_memory_is_constant \
  test_case_state_capacity_noop test_case_state_void_specialization \
  test_case_state_moved_from_shell
```

预期：`Totals: 13 passed, 0 failed`（11 个用例 + `initTestCase` + `cleanupTestCase`）。

- [ ] **Step 7: 全量回归**

```bash
cd /tmp/at-build/testfiberawait && ./testfiberawait 2>&1 | grep -E "^(FAIL|QFATAL|Totals)"
```

预期：`Totals: 121 passed, 0 failed, 0 skipped`（110 + 新增 11）。

**队列模式的既有用例必须一条不差。** 任何一条挂掉都说明状态分派污染了队列路径——最可能的原因是把"本句柄已收尾"的早退检查写到了模式分派之外，那会破坏 `pop()` "已关闭但仍有余量时先取完余量"的语义。回头查实现，不要改测试。

- [ ] **Step 8: 确认工厂文件零改动**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask && git status --short coro/await/coro*.hpp coro/await/detail/ coro/await/generator.hpp
```

预期：**无输出**。工厂默认构造 `Awaitable`，即队列模式，不应受本次改动影响。

- [ ] **Step 9: 其余三个测试工程回归**

```bash
for t in testfibertask testexecutor test_scheduler/testscheduler.pro; do
  d=/tmp/at-build/$(basename $t .pro); mkdir -p $d && cd $d && \
  qmake /home/david/zpj/Framework-dev/AsyncTask/test/$t && make -j$(nproc) && ./$(basename $d) 2>&1 | tail -2 || echo "FAILED: $t";
done
```

预期：三个工程都以 `0 failed` 结束。

- [ ] **Step 10: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/await/awaitable.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): Awaitable 支持状态模式

两个特化各新增 explicit Awaitable(AwaitMode) 与 discardPending()；
await/await_for 在状态模式下改为非破坏性读取——立即返回当前值、
多次读取值不变、可清空回到挂起、关闭即终止。队列模式路径逐字未动。"
```

---

### Task 4: 文档同步

**Files:**
- Modify: `doc/使用说明.md`（能力表、新增"状态模式"小节）
- Modify: `doc/软件设计说明.md`（`Awaitable` / `ChannelHub` 的单元设计段落）
- Modify: `doc/架构设计.md`（§2.2 与 await 层描述）
- Modify: `doc/类图与时序图.md` 与 `doc/img/sdd-05-csc_await.mmd`
- Modify: `doc/需求规格说明.md`（新增状态语义需求项）

**Interfaces:**
- Consumes: Task 1-3 落地的最终结构。
- Produces: 无代码接口。

---

- [ ] **Step 1: 更新 `doc/使用说明.md`**

- 能力表中 `FiberChannel<T>` 条目补上 `wait_peek`/`wait_peek_for`（非破坏性读取，已关闭即返回 closed）；`ChannelHub<T>` 条目补上状态模式与状态格。
- `Awaitable<T>` 条目补上 `AwaitMode` 构造与 `discardPending()`。
- 新增"状态模式"小节，内容覆盖：与队列模式的语义差异；`Coro::Awaitable<int> st{Coro::AwaitMode::State};` 的用法示例；`shared()` 自动继承且新订阅者立刻读到当前值；`discardPending()` 使其回到挂起；关闭是终止信号且最后的值不再可读；**以及醒目的警告：状态模式下 `while(auto v = await(a))` 与 `generate(a)` 会满速空转，直到流关闭才终止**。

- [ ] **Step 2: 更新 `doc/软件设计说明.md`**

在 `ChannelHub` / `Awaitable` 的单元设计段落中补充：状态格是一条容量 1 的 `FiberChannel`，`state_` 非空即状态模式；状态模式下 `push` 只写状态格不扇出，内存 O(1)；`wait_peek` 与 `pop` 在"已关闭且有值"时的刻意差异及其理由（唯一的终止信号）；`push` 改为 `notify_all` 的原因；`setCapacity` 在状态模式下为空操作。

- [ ] **Step 3: 更新 `doc/架构设计.md` 与类图**

- `架构设计.md` 的 await 层描述中补充状态模式。
- `doc/img/sdd-05-csc_await.mmd` 中给 `ChannelHub` 补上 `state_` 成员与到 `FiberChannel` 的组合关系。**只改 `.mmd` 源文件，不要手工编辑 `.svg`。** 本机无 mermaid 工具链，`doc/软件设计说明.md` 中该图下方已有"待重新导出"的标注，无需重复添加；若你所在环境有 `mmdc`/`npx`，可重新导出并在报告中说明。

- [ ] **Step 4: 更新 `doc/需求规格说明.md`**

新增一条状态语义的需求项：值可反复读取且不因读取而消失、可更新为最新值、可清空回到挂起、关闭即终止。

- [ ] **Step 5: 确认文档与代码一致**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask && grep -rn "AwaitMode\|状态模式\|wait_peek" doc/ | head -20
```

预期：能看到各文档中新增的描述；确认没有把 `wait_peek` 的终止语义写反（已关闭即 closed，**不是**先取余量）。

- [ ] **Step 6: 最终全量回归**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait 2>&1 | tail -3
```

预期：`Totals: 121 passed, 0 failed, 0 skipped`。

- [ ] **Step 7: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add doc/
git commit -m "docs: 同步 Awaitable 状态模式的设计与使用文档"
```
