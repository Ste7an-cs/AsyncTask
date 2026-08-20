# ChannelHub 与消费者队列拉平 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把广播扇出从 `FiberChannel` 移入新的 `ChannelHub`，让 `FiberChannel` 回归纯通用队列，并使源队列与订阅者队列一样随句柄析构而释放。

**Architecture:** 生产者捕获的对象由 `FiberChannel<T>` 换成 `ChannelHub<T>`（同名同签名的生产者侧方法，工厂代码不用改）。hub 持一张 `vector<weak_ptr<FiberChannel<T>>>` 消费者表，每条队列由一个 `Awaitable` 独占持有；源与订阅者在表上没有身份差别。清理钩子 `AwaitableCloseGuard` 内联进 hub，消费者归零时跑一次。

**Tech Stack:** C++17、boost.fiber（`/usr/local`，静态库）、Qt 5.15.13、qmake、QtTest、AddressSanitizer。

设计依据：`docs/superpowers/specs/2026-08-20-channel-hub-flat-consumers-design.md`。实现中若发现与 spec 冲突，以 spec 为准并回头更新 spec，不要静默偏离。

## Global Constraints

- C++17，头文件-only 的模板代码放在 `coro/detail/` 与 `coro/await/`，`.pri` 里以 `HEADERS` 登记。
- `coro/detail/` 下的代码**不得依赖 Qt**：非 Qt 构建仍要能用 `Result` / `FiberChannel` / `ChannelHub` / `FiberScheduler` 基类 / `FiberTask`。
- 中文注释，Doxygen 风格（`@brief` / `@details` / `@param` / `@return` / `@code`），与现有文件保持一致的密度。
- 锁顺序恒为 hub → queue，绝不反向；清理钩子必须在 hub 的 fiber mutex **之外**执行。
- 各 `coro*` 工厂文件（`corosignal.hpp` / `corofuture.hpp` / `coroiodevice.hpp` / `corosocket.hpp` / `corosslsocket.hpp` / `corotcpserver.hpp` / `corolocalsocket.hpp` / `corolocalserver.hpp` / `coroudpsocket.hpp`）**不应产生任何改动**——它们全部用 `auto ch = a.channel()`，hub 提供同名同签名成员。任务结束时若这些文件有 diff，说明设计被违反了，要停下来查。
- 每个任务结束时全量测试必须通过，且**不得留下失败或被注释掉的用例**。

### 构建与运行命令（每个任务反复用到）

首次准备构建目录：

```bash
mkdir -p /tmp/at-build/testfiberawait
cd /tmp/at-build/testfiberawait
qmake /home/david/zpj/Framework-dev/AsyncTask/test/testfiberawait/testfiberawait.pro
```

编译并跑全量：

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait
```

只跑单个用例：

```bash
cd /tmp/at-build/testfiberawait && ./testfiberawait test_case_hub_fanout_basic
```

**改了头文件后 qmake 的依赖可能不完整**，出现莫名其妙的旧行为时用 `make clean && make -j$(nproc)` 重来。

**基线（改动前，2026-08-20 实测）：`Totals: 85 passed, 0 failed, 0 skipped`，`sizeof(Coro::FiberChannel<int>) == 168`。**

其余三个测试工程的回归（Task 4 之前至少跑一次）：

```bash
for t in testfibertask testexecutor test_scheduler/testscheduler.pro; do
  d=/tmp/at-build/$(basename $t .pro); mkdir -p $d && cd $d && \
  qmake /home/david/zpj/Framework-dev/AsyncTask/test/$t && make -j$(nproc) && ./$(basename $d) || echo "FAILED: $t";
done
```

---

### Task 1: `ChannelHub<T>` 落地并独立单测

新建 hub 类型并把 `AwaitableCloseGuard` 迁进来。本任务**不改** `Awaitable`，因此除了 guard 换了个位置以外行为零变化，现有 85 个用例必须原样通过。

**Files:**
- Create: `coro/detail/channelhub.hpp`
- Modify: `coro/await/awaitable.hpp:13-79`（删除本地的 `detail` 命名空间与 `AwaitableCloseGuard` 定义，改为包含新头文件）
- Modify: `coro/all.hpp:34` 附近（增加 include）
- Modify: `AsyncTask.pri`（通用 `HEADERS` 增加 `channelhub.hpp`）
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: `Coro::FiberChannel<T>` 的现有公开成员 `push` / `pop` / `pop_wait_for` / `is_closed` / `close(std::error_code)` / `close_error` / `discard_pending` / `setCapacity` / `capacity` / `kDefaultCapacity`。
- Produces:
  - `Coro::detail::AwaitableCloseGuard`（迁移，接口不变：`set(std::function<void()>)` / `run()`）
  - `template<class T> class Coro::ChannelHub`，公开成员：
    - `boost::fibers::channel_op_status push(T value)`
    - `void close() noexcept` / `void close(std::error_code error) noexcept`
    - `bool is_closed() const noexcept`
    - `std::error_code close_error() const`
    - `void discard_pending()`
    - `void setCapacity(std::uint32_t capacity)` / `std::uint32_t capacity() const`
    - `void setOnClose(std::function<void()> fn)`
    - `void attach(const std::shared_ptr<FiberChannel<T>>& queue)`
    - `void detach(const FiberChannel<T>* queue)`
    - `void notifyClosed(std::error_code error)`

---

- [ ] **Step 1: 写失败测试——hub 扇出到多条队列**

在 `test/testfiberawait/tst_testfiberawait.cpp` 的 `private slots:` 段（第 219 行 `void test_case_channel_layout_size();` 之前）插入六条声明：

```cpp
    void test_case_hub_fanout_basic();
    void test_case_hub_attach_after_close();
    void test_case_hub_capacity_defaults_and_propagates();
    void test_case_hub_exhausted_runs_cleanup_once();
    void test_case_hub_discard_reaches_closed_queue();
    void test_case_hub_push_copy_parity();
```

在文件末尾 `test_case_channel_layout_size` 的实现之前插入这六个用例的实现：

```cpp
/// @brief 验证 ChannelHub 把每次 push 同步投递给每一条已挂载的队列。
void TestFiberAwait::test_case_hub_fanout_basic()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>();
    auto first = std::make_shared<Coro::FiberChannel<int>>();
    auto second = std::make_shared<Coro::FiberChannel<int>>();
    hub->attach(first);
    hub->attach(second);

    QCOMPARE(hub->push(1), boost::fibers::channel_op_status::success);
    QCOMPARE(hub->push(2), boost::fibers::channel_op_status::success);

    int value{};
    QCOMPARE(first->pop(value), boost::fibers::channel_op_status::success);
    QCOMPARE(value, 1);
    QCOMPARE(first->pop(value), boost::fibers::channel_op_status::success);
    QCOMPARE(value, 2);
    QCOMPARE(second->pop(value), boost::fibers::channel_op_status::success);
    QCOMPARE(value, 1);
    QCOMPARE(second->pop(value), boost::fibers::channel_op_status::success);
    QCOMPARE(value, 2);

    // 队列消亡后 hub 不再投递，且失效槽位被剔除（不崩溃即通过）
    second.reset();
    QCOMPARE(hub->push(3), boost::fibers::channel_op_status::success);
    QCOMPARE(first->pop(value), boost::fibers::channel_op_status::success);
    QCOMPARE(value, 3);
}

/// @brief 验证 hub 关闭后再挂载的队列立即以 hub 的终止原因收敛，且不入表。
void TestFiberAwait::test_case_hub_attach_after_close()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>();
    hub->close(std::make_error_code(std::errc::connection_refused));

    auto late = std::make_shared<Coro::FiberChannel<int>>();
    hub->attach(late);

    QVERIFY(late->is_closed());
    QCOMPARE(late->close_error(), std::make_error_code(std::errc::connection_refused));
    int value{};
    QCOMPARE(late->pop(value), boost::fibers::channel_op_status::closed);
    QCOMPARE(hub->push(1), boost::fibers::channel_op_status::closed);
}

/// @brief 验证 hub 的容量既作为新队列的默认值，也会级联到已挂载的队列。
void TestFiberAwait::test_case_hub_capacity_defaults_and_propagates()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>();
    QCOMPARE(hub->capacity(), Coro::FiberChannel<int>::kDefaultCapacity);

    auto early = std::make_shared<Coro::FiberChannel<int>>();
    hub->attach(early);
    hub->setCapacity(2);
    QCOMPARE(hub->capacity(), std::uint32_t(2));
    QCOMPARE(early->capacity(), std::uint32_t(2));      // 级联到已挂载的

    auto late = std::make_shared<Coro::FiberChannel<int>>();
    hub->attach(late);
    QCOMPARE(late->capacity(), std::uint32_t(2));       // 作为新挂载的默认值

    for(int i = 1; i <= 4; ++i){
        QCOMPARE(hub->push(i), boost::fibers::channel_op_status::success);
    }
    int value{};
    QCOMPARE(late->pop(value), boost::fibers::channel_op_status::success);
    QCOMPARE(value, 3);                                  // 只剩最近两条
    QCOMPARE(late->pop(value), boost::fibers::channel_op_status::success);
    QCOMPARE(value, 4);
}

/// @brief 验证消费者归零时清理钩子恰好跑一次，且 hub 随之关闭。
/// @details 覆盖两条触发路径：队列被关闭（notifyClosed）与队列被摘除（detach）。
void TestFiberAwait::test_case_hub_exhausted_runs_cleanup_once()
{
    // 路径一：唯一消费者被关闭
    {
        auto hub = std::make_shared<Coro::ChannelHub<int>>();
        auto queue = std::make_shared<Coro::FiberChannel<int>>();
        hub->attach(queue);
        int cleanups = 0;
        hub->setOnClose([&cleanups]{ ++cleanups; });

        queue->close(std::make_error_code(std::errc::connection_reset));
        hub->notifyClosed(std::make_error_code(std::errc::connection_reset));
        QCOMPARE(cleanups, 1);
        QVERIFY(hub->is_closed());
        // 终止原因必须被继承，而不是退回 no_message
        QCOMPARE(hub->close_error(), std::make_error_code(std::errc::connection_reset));

        hub->detach(queue.get());
        QCOMPARE(cleanups, 1);                           // 幂等，不重复跑
    }

    // 路径二：还有别的存活消费者时不得跑清理
    {
        auto hub = std::make_shared<Coro::ChannelHub<int>>();
        auto closing = std::make_shared<Coro::FiberChannel<int>>();
        auto living = std::make_shared<Coro::FiberChannel<int>>();
        hub->attach(closing);
        hub->attach(living);
        int cleanups = 0;
        hub->setOnClose([&cleanups]{ ++cleanups; });

        closing->close();
        hub->notifyClosed(std::make_error_code(std::errc::no_message));
        QCOMPARE(cleanups, 0);
        QVERIFY(!hub->is_closed());
        QCOMPARE(hub->push(9), boost::fibers::channel_op_status::success);

        hub->detach(living.get());
        QCOMPARE(cleanups, 1);
        QVERIFY(hub->is_closed());
    }
}

/// @brief 验证 discard_pending() 能触达已关闭但尚未摘除的队列。
/// @details 这是 corotcpserver 的悬空指针防线：消费者先 close()、再销毁来源，
///          来源析构时必须能清掉队列里即将悬空的值。
void TestFiberAwait::test_case_hub_discard_reaches_closed_queue()
{
    auto hub = std::make_shared<Coro::ChannelHub<int>>();
    auto queue = std::make_shared<Coro::FiberChannel<int>>();
    hub->attach(queue);

    QCOMPARE(hub->push(1), boost::fibers::channel_op_status::success);
    queue->close();                                      // 关闭但句柄仍在，未摘除
    hub->discard_pending();

    int value{};
    QCOMPARE(queue->pop(value), boost::fibers::channel_op_status::closed);
}

/// @brief 验证 hub 的扇出拷贝次数与旧的 FiberChannel 镜像实现持平。
/// @details 旧实现把值 move 进源队列、copy 给每条镜像。hub 必须同样把最后一个
///          接收者用 move 送达，否则每次 push 都会比旧实现多一次 T 拷贝。
void TestFiberAwait::test_case_hub_push_copy_parity()
{
    auto hub = std::make_shared<Coro::ChannelHub<CopyCounted>>();
    auto only = std::make_shared<Coro::FiberChannel<CopyCounted>>();
    hub->attach(only);

    CopyCounted::copies = 0;
    QCOMPARE(hub->push(CopyCounted(1)), boost::fibers::channel_op_status::success);
    QCOMPARE(CopyCounted::copies, 0);                    // 唯一接收者：全程 move

    auto second = std::make_shared<Coro::FiberChannel<CopyCounted>>();
    hub->attach(second);
    CopyCounted::copies = 0;
    QCOMPARE(hub->push(CopyCounted(2)), boost::fibers::channel_op_status::success);
    QCOMPARE(CopyCounted::copies, 1);                    // 两个接收者：一拷一移
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) 2>&1 | tail -20
```

预期：编译失败，`error: 'ChannelHub' is not a member of 'Coro'`。

- [ ] **Step 3: 新建 `coro/detail/channelhub.hpp`**

完整内容如下。注意 `AwaitableCloseGuard` 是从 `coro/await/awaitable.hpp:13-79` **原样迁入**（类名、命名空间、注释、`@code` 示例都不变），下面已包含迁移后的完整文本：

```cpp
#ifndef CHANNELHUB_HPP
#define CHANNELHUB_HPP

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <system_error>
#include <vector>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include "fiberchannel.hpp"

namespace Coro {

namespace detail {

/**
 * @brief 管理一条数据流的一次性终止清理回调。
 * @details 首次显式关闭或最后一个消费者消失时执行清理。回调在互斥锁外调用，
 *          避免清理过程重入时发生死锁。
 * @note 多次调用 run() 只会执行一次清理。
 * @code
 * // ChannelHub 内部持有本守卫；工厂经 Awaitable::setOnClose 注入清理，
 * // 消费者归零时恰好执行一次
 * Coro::Awaitable<int> a;
 * a.setOnClose([conn]{ QObject::disconnect(*conn); });
 * a.close();      // 此处断开连接；之后析构不会重复执行
 * @endcode
 */
class AwaitableCloseGuard {
    std::mutex mutex_;
    std::function<void()> cleanup_;
    bool closed_{false};
public:
    ~AwaitableCloseGuard(){ run(); }

    /**
     * @brief 设置终止清理回调。
     * @details 替换旧回调时，旧回调会在互斥锁外立即执行；若已终止，传入回调也会在锁外立即执行。
     * @param cleanup 终止时执行的清理回调。
     * @code
     * guard.set([conns]{ for(auto& c : conns) QObject::disconnect(*c); });
     * @endcode
     */
    void set(std::function<void()> cleanup){
        bool runImmediately = false;
        std::function<void()> previous;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(closed_){
                runImmediately = true;
            }else{
                previous = std::move(cleanup_);
                cleanup_ = std::move(cleanup);
            }
        }
        if(previous) previous();
        if(runImmediately && cleanup) cleanup();
    }

    /**
     * @brief 执行一次终止清理。
     * @details 首次调用取出回调并在互斥锁外执行，后续调用不再执行回调。
     * @code
     * guard.run();    // 执行清理
     * guard.run();    // 幂等：不再重复执行
     * @endcode
     */
    void run(){
        std::function<void()> cleanup;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(closed_) return;
            closed_ = true;
            cleanup = std::move(cleanup_);
        }
        if(cleanup) cleanup();
    }
};

} // namespace detail

/**
 * @brief 一条数据流的分发端：生产者投递一次，每个消费者队列各得一份。
 *
 * 生产者只捕获 hub（绝不捕获 Awaitable，否则成引用环），每个消费者由自己的
 * Awaitable 独占持有一条 FiberChannel 并挂到本 hub 上。源与订阅者在消费者表上
 * 没有身份差别：句柄析构即摘除，队列连同其中排队的值一起释放。
 *
 * 生产者侧的方法名与签名同 FiberChannel，因此各工厂里 `auto ch = a.channel()`
 * 之后的写法一字不变。
 *
 * @tparam T 传递的数据类型
 * @code
 * // 通常不直接使用，而是通过 Awaitable::channel() 取得
 * Coro::Awaitable<int> a;
 * auto hub = a.channel();
 * QObject::connect(obj, &Obj::valueChanged, [hub](int v){ hub->push(v); });
 * @endcode
 */
template<class T>
class ChannelHub{
    using channel_status = boost::fibers::channel_op_status;
public:
    /** @brief 默认构造，创建一个没有任何消费者的分发端 */
    ChannelHub() = default;
    /** @brief 析构：guard_ 成员析构时兜底执行一次清理 */
    ~ChannelHub() = default;
    /** @brief 禁止拷贝构造 */
    ChannelHub(const ChannelHub&) = delete;
    /** @brief 禁止拷贝赋值 */
    ChannelHub& operator=(const ChannelHub&) = delete;

    /**
     * @brief 挂载一条消费者队列，此后每次 push 都会向它投递一份。
     * @details hub 已关闭时不挂载，直接以 hub 记录的终止原因关闭该队列，避免消费者
     *          永久挂起。挂载时把 hub 当前的容量赋给该队列。
     *          与旧的 FiberChannel::addMirror 不同，本方法无需 private + friend 保护：
     *          hub 与队列是两个不同的类型层，结构上无法互相挂载成环。
     * @param queue 待挂载的消费者队列
     * @code
     * hub->attach(queue);      // 由 Awaitable 的构造函数调用
     * @endcode
     */
    void attach(const std::shared_ptr<FiberChannel<T>>& queue){
        if(!queue){
            return;
        }
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            const std::error_code error = close_error_;
            lck.unlock();
            queue->close(error);
            return;
        }
        queue->setCapacity(capacity_);
        consumers_.push_back(queue);
    }

    /**
     * @brief 摘除一条消费者队列；若消费者就此归零，关闭 hub 并执行一次清理。
     * @details 由 Awaitable 的析构函数调用。清理回调在锁外执行——它通常会
     *          QObject::disconnect，进而销毁生产者 lambda 并释放它持有的 hub 引用；
     *          之所以不会把 hub 自己析构掉，是因为调用方（正在析构的 Awaitable，
     *          此刻成员尚未释放）还攥着另一份引用。
     * @param queue 待摘除的队列裸指针，仅用于比对，不解引用
     * @code
     * hub->detach(queue.get());    // 由 ~Awaitable 调用
     * @endcode
     */
    void detach(const FiberChannel<T>* queue){
        bool cleanup = false;
        {
            std::unique_lock<boost::fibers::mutex> lck{mtx_};
            for(std::size_t i = 0; i < consumers_.size(); ){
                auto held = consumers_[i].lock();
                if(!held || held.get() == queue){
                    consumers_[i] = std::move(consumers_.back());
                    consumers_.pop_back();
                    continue;
                }
                ++i;
            }
            cleanup = markExhausted(std::make_error_code(std::errc::no_message));
        }
        if(cleanup){
            guard_.run();
        }
    }

    /**
     * @brief 通知 hub 某条消费者队列已被关闭，触发归零判定。
     * @details 与 detach 不同，本方法不摘除槽位——已关闭的队列要留在表里，
     *          discard_pending() 之后仍需经由它清掉即将悬空的值。归零时 hub
     *          继承传入的终止原因，使生产者查 close_error() 能拿到真正的原因。
     * @param error 消费者关闭时给出的终止原因
     * @code
     * hub->notifyClosed(error);    // 由 Awaitable::close(error) 调用
     * @endcode
     */
    void notifyClosed(std::error_code error){
        bool cleanup = false;
        {
            std::unique_lock<boost::fibers::mutex> lck{mtx_};
            cleanup = markExhausted(error);
        }
        if(cleanup){
            guard_.run();
        }
    }

    /**
     * @brief 向每一条存活且未关闭的消费者队列投递一份 value。
     * @details 失效槽位（消费者句柄已析构）在遍历中以 swap-and-pop 剔除；已关闭的
     *          队列跳过投递但保留在表中。投递延后一拍：最后一个接收者直接 move 送达，
     *          因此拷贝次数与旧的 FiberChannel 镜像实现持平。
     * @param value 待投递的元素
     * @return hub 已关闭返回 closed，否则返回 success
     * @code
     * if(hub->push(42) != boost::fibers::channel_op_status::success){
     *     // hub 已关闭，值被丢弃
     * }
     * @endcode
     */
    channel_status push(T value){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(BOOST_UNLIKELY(closed_.load())){
            return channel_status::closed;
        }
        std::shared_ptr<FiberChannel<T>> pending;
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            if(!queue->is_closed()){
                // 延后一拍：上一个接收者此刻才拿到拷贝，最后一个留到循环外 move
                if(pending){
                    pending->push(value);
                }
                pending = std::move(queue);
            }
            ++i;
        }
        if(pending){
            pending->push(std::move(value));
        }
        return channel_status::success;
    }

    /**
     * @brief 查询 hub 是否已关闭
     * @return 已关闭返回 true
     * @code
     * while(!hub->is_closed() && dev->bytesAvailable() > 0) hub->push(dev->readAll());
     * @endcode
     */
    bool is_closed() const noexcept {
        return closed_.load(std::memory_order_acquire);
    }

    /**
     * @brief 关闭整条流，唤醒并收敛所有消费者
     * @code
     * hub->close();    // 正常终止：消费者取完余量后观察到 no_message
     * @endcode
     */
    void close() noexcept {
        close(std::make_error_code(std::errc::no_message));
    }

    /**
     * @brief 关闭整条流并记录终止原因，唤醒并收敛所有消费者
     * @details 仅首次关闭记录终止原因，后续调用不覆盖。消费者表**不清空**——
     *          discard_pending() 在此之后仍需经由它触达各队列。本方法不执行清理钩子：
     *          清理由消费者归零触发（见 detach / notifyClosed），与旧实现中
     *          「生产者关 channel 不触发 guard」的语义一致。
     * @param error 终止原因
     * @code
     * hub->close(Coro::detail::socket_error_code(socket->error()));
     * @endcode
     */
    void close(std::error_code error) noexcept {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            return;
        }
        close_error_ = error == std::error_code{}
                ? std::make_error_code(std::errc::no_message)
                : error;
        closed_.store(true);
        for(auto& weak : consumers_){
            if(auto queue = weak.lock()){
                queue->close(close_error_);
            }
        }
    }

    /**
     * @brief 返回首次关闭时记录的终止原因
     * @return 终止原因；尚未关闭时为预置的 no_message
     * @code
     * qDebug() << hub->close_error().message().c_str();
     * @endcode
     */
    std::error_code close_error() const {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return close_error_;
    }

    /**
     * @brief 丢弃所有消费者队列中尚未被消费的值。
     * @details 不改变关闭状态，也不修改已保留的终止错误。已关闭但尚未摘除的队列
     *          同样会被清空——来源销毁时靠这条路径清掉即将悬空的 QTcpSocket*。
     * @code
     * QObject::connect(server, &QObject::destroyed, [hub]{ hub->discard_pending(); });
     * @endcode
     */
    void discard_pending(){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            queue->discard_pending();
            ++i;
        }
    }

    /**
     * @brief 设置容量上限：级联给所有存活队列，并作为后续挂载的默认值。
     * @details 各队列按新容量立即从队首丢弃多余的值。传 0 表示无限。承载了自身即
     *          代表资源的值（如 QTcpSocket*）的流应调大容量或传 0，否则丢弃意味着
     *          该资源永远得不到处理。
     * @param capacity 新的容量上限，0 表示无限
     * @code
     * hub->setCapacity(0);     // 取消上限，恢复无界队列（谨慎使用）
     * @endcode
     */
    void setCapacity(std::uint32_t capacity){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        capacity_ = capacity;
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            queue->setCapacity(capacity);
            ++i;
        }
    }

    /**
     * @brief 查询当前的容量上限
     * @return 容量上限；0 表示无限
     * @code
     * if(hub->capacity() == 0) qDebug() << "无界队列";
     * @endcode
     */
    std::uint32_t capacity() const {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return capacity_;
    }

    /**
     * @brief 注册消费者归零时执行一次的清理钩子
     * @param fn 清理回调（如断开信号连接）
     * @code
     * hub->setOnClose([conn]{ QObject::disconnect(*conn); });
     * @endcode
     */
    void setOnClose(std::function<void()> fn){
        guard_.set(std::move(fn));
    }

private:
    mutable boost::fibers::mutex mtx_;///< 保护消费者表与关闭状态的 fiber 互斥量
    std::vector<std::weak_ptr<FiberChannel<T>>> consumers_{};///< 消费者队列表，强引用在各 Awaitable 手里
    std::atomic_bool closed_{false};///< 关闭标志
    std::uint32_t capacity_{FiberChannel<T>::kDefaultCapacity};///< 容量上限，级联给队列并作为新挂载的默认值
    std::error_code close_error_{std::make_error_code(std::errc::no_message)};///< 首次关闭时保留的终止原因
    detail::AwaitableCloseGuard guard_;///< 消费者归零时执行一次的清理钩子

    /**
     * @brief 判定消费者是否已归零；归零则关闭 hub 并返回 true（需持有 mtx_ 调用）。
     * @details 「归零」指表中不存在既存活又未关闭的队列。顺带以 swap-and-pop 剔除
     *          失效槽位。已关闭的 hub 不覆盖已记录的终止原因，但仍返回 true——
     *          清理钩子本身幂等，重复触发无害，而漏触发会导致 Qt 连接迟迟不断开。
     * @param error 归零时采用的终止原因
     * @return 已归零返回 true
     */
    bool markExhausted(std::error_code error){
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            if(!queue->is_closed()){
                return false;
            }
            ++i;
        }
        if(!closed_.load()){
            close_error_ = error == std::error_code{}
                    ? std::make_error_code(std::errc::no_message)
                    : error;
            closed_.store(true);
        }
        return true;
    }
};

}

#endif // CHANNELHUB_HPP
```

- [ ] **Step 4: 从 `awaitable.hpp` 移除 guard 定义**

删除 `coro/await/awaitable.hpp` 第 13-79 行整段（`namespace detail { ... class AwaitableCloseGuard {...}; } // namespace detail`），并把第 8 行的 include 改成两条：

```cpp
#include "detail/fiberchannel.hpp"
#include "detail/channelhub.hpp"
#include "detail/result.hpp"
```

`Awaitable` 内部对 `detail::AwaitableCloseGuard` 的引用（第 116-117、333-334 行的 `guard_` 成员）保持不动——类名与命名空间没变，本步只是换了定义位置。

- [ ] **Step 5: 登记新头文件**

`coro/all.hpp` 第 34 行 `#include "detail/fiberchannel.hpp"  // FiberChannel<T>` 之后增加一行：

```cpp
#include "detail/channelhub.hpp"     // ChannelHub<T>
```

`AsyncTask.pri` 末尾的通用 `HEADERS +=` 段中，`$$PWD/coro/detail/fiberchannel.hpp \` 之后增加一行：

```
    $$PWD/coro/detail/channelhub.hpp \
```

- [ ] **Step 6: 运行新用例，确认通过**

```bash
cd /tmp/at-build/testfiberawait && qmake /home/david/zpj/Framework-dev/AsyncTask/test/testfiberawait/testfiberawait.pro && make -j$(nproc) && \
./testfiberawait test_case_hub_fanout_basic test_case_hub_attach_after_close \
  test_case_hub_capacity_defaults_and_propagates test_case_hub_exhausted_runs_cleanup_once \
  test_case_hub_discard_reaches_closed_queue test_case_hub_push_copy_parity
```

预期：`Totals: 8 passed, 0 failed`（6 个用例 + `initTestCase` + `cleanupTestCase`）。

- [ ] **Step 7: 全量回归**

```bash
cd /tmp/at-build/testfiberawait && ./testfiberawait
```

预期：`Totals: 91 passed, 0 failed, 0 skipped`（基线 85 + 新增 6）。本任务未触碰 `Awaitable`，旧行为必须一条不差。

- [ ] **Step 8: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/detail/channelhub.hpp coro/await/awaitable.hpp coro/all.hpp AsyncTask.pri test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(channel): 新增 ChannelHub 分发端并迁入 AwaitableCloseGuard"
```

---

### Task 2: `Awaitable` 接线到 hub，消费者队列拉平

把两个特化改成「共享 hub + 独占队列」，源与订阅者对称。本任务落地全部语义变化，是整个改动的核心。完成后 `FiberChannel` 的镜像代码变成无人调用的死代码（Task 3 再删）。

**Files:**
- Modify: `coro/await/awaitable.hpp`（`Awaitable<T>` 与 `Awaitable<void>` 两个特化）
- Modify: `coro/await/generator.hpp:79`、`:321`、`:567` 附近
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: Task 1 的 `Coro::ChannelHub<T>` 全部公开成员。
- Produces:
  - `Awaitable<T>::channel()` 返回类型由 `std::shared_ptr<FiberChannel<T>>` 变为 `std::shared_ptr<ChannelHub<T>>`（`void` 特化为 `ChannelHub<int>`）
  - 新增 `bool Awaitable<T>::isClosed() const` / `bool Awaitable<void>::isClosed() const`
  - `Awaitable<T>::shared()` / `Awaitable<void>::shared()` 签名不变，语义改为「同一 hub 上再挂一条队列」

---

- [ ] **Step 1: 写失败测试——本次改动的六条核心语义**

在 `private slots:` 段（Task 1 加的六条声明之后）追加：

```cpp
    void test_case_flat_source_handle_dropped_keeps_stream();
    void test_case_flat_source_queue_released_with_handle();
    void test_case_flat_no_consumer_stops_buffering();
    void test_case_flat_close_scope_is_self_only();
    void test_case_flat_cleanup_runs_once_on_last_handle();
    void test_case_flat_void_source_handle_dropped_keeps_stream();
```

在文件末尾追加实现：

```cpp
/// @brief 验证丢弃源句柄不再终止订阅者：上游继续活着，订阅者继续收数据。
/// @details 这是本次改动的核心诉求——广播用法下调用方不必再攥着源句柄，
///          因而源队列不会长期囤积无人消费的值。
void TestFiberAwait::test_case_flat_source_handle_dropped_keeps_stream()
{
    using namespace std::chrono_literals;
    std::shared_ptr<Coro::Awaitable<int>> subscriber;
    std::shared_ptr<Coro::ChannelHub<int>> producer;
    {
        Coro::Awaitable<int> source;
        subscriber = source.shared();
        producer = source.channel();          // 生产者侧句柄，模拟 Qt 槽的捕获
        QVERIFY(source.resolve(1));
    }                                          // 源句柄析构

    // 订阅者先收到丢源之前的值
    QCOMPARE(Coro::await_for(subscriber, 100ms).value(), 1);

    // 上游仍然活着：丢源之后生产的值照样送达
    QCOMPARE(producer->push(2), boost::fibers::channel_op_status::success);
    QCOMPARE(Coro::await_for(subscriber, 100ms).value(), 2);
    QVERIFY(!producer->is_closed());
}

/// @brief 验证源句柄析构后，源那条队列连同排队的值一起释放，不再接收投递。
void TestFiberAwait::test_case_flat_source_queue_released_with_handle()
{
    auto tracker = std::make_shared<int>(0);
    std::weak_ptr<int> alive = tracker;
    std::shared_ptr<Coro::Awaitable<std::shared_ptr<int>>> subscriber;
    std::shared_ptr<Coro::ChannelHub<std::shared_ptr<int>>> producer;
    {
        Coro::Awaitable<std::shared_ptr<int>> source;
        subscriber = source.shared();
        producer = source.channel();
        QVERIFY(source.resolve(tracker));      // 源队列与订阅者队列各持一份
    }                                           // 源句柄析构：源队列连同它那份一起没了

    tracker.reset();
    QVERIFY(!alive.expired());                 // 订阅者队列里还排着一份

    // 订阅者取走后，再没有任何队列持有它
    auto received = subscriber->await();
    QVERIFY(received);
    received.value().reset();
    QVERIFY(alive.expired());                  // 源队列确实没在偷偷囤着
}

/// @brief 验证所有消费者句柄消失后 push 直接失败，不再囤积任何值。
void TestFiberAwait::test_case_flat_no_consumer_stops_buffering()
{
    std::shared_ptr<Coro::ChannelHub<int>> producer;
    {
        Coro::Awaitable<int> only;
        producer = only.channel();
        QVERIFY(only.resolve(1));
    }                                           // 唯一消费者析构 → 消费者归零

    QVERIFY(producer->is_closed());
    QCOMPARE(producer->push(2), boost::fibers::channel_op_status::closed);
}

/// @brief 验证 close() 只作用于自己这一路，源与订阅者互不牵连。
void TestFiberAwait::test_case_flat_close_scope_is_self_only()
{
    using namespace std::chrono_literals;
    Coro::Awaitable<int> source;
    auto first = source.shared();
    auto second = source.shared();

    // 订阅者关自己：源与另一个订阅者照常
    first->close();
    QVERIFY(source.resolve(1));
    QCOMPARE(source.await().value(), 1);
    QCOMPARE(second->await().value(), 1);
    QVERIFY(!first->await());

    // 源关自己：两个订阅者不受影响
    source.close();
    QVERIFY(source.channel()->push(2) == boost::fibers::channel_op_status::success);
    QCOMPARE(Coro::await_for(second, 100ms).value(), 2);
    auto sourceEnded = source.await();
    QVERIFY(!sourceEnded);
}

/// @brief 验证清理钩子在最后一个消费者消失时恰好跑一次。
void TestFiberAwait::test_case_flat_cleanup_runs_once_on_last_handle()
{
    int cleanups = 0;
    {
        Coro::Awaitable<int> source;
        source.setOnClose([&cleanups]{ ++cleanups; });
        auto subscriber = source.shared();

        source.close();                        // 还有订阅者在，不能跑清理
        QCOMPARE(cleanups, 0);

        subscriber->close();                   // 最后一路关闭，跑一次
        QCOMPARE(cleanups, 1);
    }                                           // 两个句柄析构，不得重复跑
    QCOMPARE(cleanups, 1);
}

/// @brief 验证 void 特化同样享有平表语义：丢弃源句柄后订阅者继续收到事件。
void TestFiberAwait::test_case_flat_void_source_handle_dropped_keeps_stream()
{
    using namespace std::chrono_literals;
    std::shared_ptr<Coro::Awaitable<void>> subscriber;
    std::shared_ptr<Coro::ChannelHub<int>> producer;   // void 特化底层承载 int
    {
        Coro::Awaitable<void> source;
        subscriber = source.shared();
        producer = source.channel();
        QVERIFY(source.resolve());
    }                                           // 源句柄析构

    QVERIFY(Coro::await_for(subscriber, 100ms));           // 丢源之前的那次事件
    QCOMPARE(producer->push(1), boost::fibers::channel_op_status::success);
    QVERIFY(Coro::await_for(subscriber, 100ms));           // 丢源之后仍然送达
    QVERIFY(!subscriber->isClosed());
}
```

- [ ] **Step 2: 运行新用例，确认失败**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait test_case_flat_source_handle_dropped_keeps_stream
```

预期：编译失败（`producer` 被推导为 `shared_ptr<FiberChannel<int>>`，与显式写出的 `shared_ptr<ChannelHub<int>>` 不匹配），报 `conversion from ... FiberChannel ... to ... ChannelHub`。

- [ ] **Step 3: 改写 `Awaitable<T>`**

把 `coro/await/awaitable.hpp` 中 `Awaitable<T>`（原第 113-313 行）的成员与相关方法改成下面这样。未列出的方法（`await_for`、两个 `close` 重载的文档注释等）保持原有注释，只按下面的实现调整函数体。

成员与构造/析构：

```cpp
template<typename T>
class Awaitable{
    /** @brief 订阅构造的标记类型：私有，使订阅构造函数无法被外部调用 */
    struct SubscribeTag{};

    std::shared_ptr<ChannelHub<T>> hub_{std::make_shared<ChannelHub<T>>()};
    std::shared_ptr<FiberChannel<T>> queue_{std::make_shared<FiberChannel<T>>()};
public:
    /** @brief 默认构造：新建一条数据流，并把自己的队列挂上去 */
    Awaitable(){ hub_->attach(queue_); }
    /**
     * @brief 订阅构造：复用已有的 hub，挂一条属于自己的新队列。
     * @details 仅供 shared() 使用——SubscribeTag 是私有类型，外部无法构造。
     * @param hub 要订阅的数据流分发端
     */
    Awaitable(std::shared_ptr<ChannelHub<T>> hub, SubscribeTag)
        : hub_(std::move(hub)){
        if(hub_) hub_->attach(queue_);
    }
    /** @brief 析构：摘除自己那条队列；若消费者就此归零，hub 会执行一次清理 */
    ~Awaitable(){
        if(hub_ && queue_) hub_->detach(queue_.get());
    }
```

移动构造/赋值、拷贝的删除声明保持原样（`= default` / `= delete`）——移动后 `hub_` 与 `queue_` 均为空，析构中的判空正是为此。

`channel()` / `shared()` / `isClosed()`：

```cpp
    /**
     * @brief 生产者侧共享的分发端。生产者只捕获它、不持有整个 Awaitable。
     * @return 内部数据流分发端的 shared_ptr
     * @code
     * Coro::Awaitable<QByteArray> a;
     * QObject::connect(dev, &QIODevice::readyRead, [ch = a.channel(), dev]{
     *     ch->push(dev->readAll());
     * });
     * @endcode
     */
    std::shared_ptr<ChannelHub<T>> channel() const { return hub_; }

    /**
     * @brief 注册一个共享订阅者，此后源产生的每个值都会同步复制一份投递给它。
     *
     * 返回的是普通 Awaitable，因此 Coro::await / await_for / generate 均原样可用。
     * 订阅者与源在消费者表上没有身份差别：各得全量、互不竞争，句柄析构即自动退订。
     * 不做 replay：本次调用之前已产生的值对订阅者不可见。
     * 源句柄析构不会终止订阅者——上游活到最后一个句柄消失为止。
     * @return 共享订阅句柄；数据流已关闭时返回的句柄立即以其终止原因收敛
     * @code
     * auto src = Coro::coro(sock).readAll();
     * auto sync = src->shared();
     * auto audit = src->shared();
     * @endcode
     */
    std::shared_ptr<Awaitable<T>> shared(){
        return std::make_shared<Awaitable<T>>(hub_, SubscribeTag{});
    }

    /**
     * @brief 查询自己这一路是否已关闭。
     * @details 查的是本句柄独占的队列，而非整条流——订阅者关掉自己不影响别人。
     * @return 自己这一路已关闭返回 true
     * @code
     * while(!a.isClosed()) produce(a);
     * @endcode
     */
    bool isClosed() const { return queue_ && queue_->is_closed(); }
```

容量、清理钩子、消费与投递：

```cpp
    void setCapacity(std::uint32_t capacity){
        if(hub_) hub_->setCapacity(capacity);
    }
    std::uint32_t capacity() const {
        return hub_ ? hub_->capacity() : 0;
    }
    void setOnClose(std::function<void()> fn){
        if(hub_) hub_->setOnClose(std::move(fn));
    }

    Result<T, std::error_code> await(){
        if(queue_){
            T value{};
            auto status = queue_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::no_message);
    }

    template<typename Rep, typename Period>
    Result<T, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(queue_){
            T value{};
            auto status = queue_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::timed_out);
    }

    bool resolve(const T& value){
        if(hub_){
            if(hub_->is_closed()){
                return false;
            }
            return (boost::fibers::channel_op_status::success == hub_->push(value));
        }
        return false;
    }

    void close(){
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭自己这一路并记录终止原因，唤醒本路的等待者。
     * @details 只作用于本句柄的队列，源与其他订阅者不受影响。若本路是最后一条
     *          未关闭的消费者，hub 随之关闭并执行一次清理（断开上游）。
     * @param error 终止原因
     * @code
     * a.close(std::make_error_code(std::errc::connection_reset));
     * @endcode
     */
    void close(std::error_code error){
        if(queue_) queue_->close(error);
        if(hub_) hub_->notifyClosed(error);
    }
```

- [ ] **Step 4: 照同样的方式改写 `Awaitable<void>`**

`Awaitable<void>`（原第 330-521 行）的改法与 `Awaitable<T>` 完全一致，只是类型换成 `ChannelHub<int>` 与 `FiberChannel<int>`，且 `await` / `await_for` 返回 `Result<void, std::error_code>`、`resolve()` 无参：

```cpp
template<>
class Awaitable<void>{
    struct SubscribeTag{};

    std::shared_ptr<ChannelHub<int>> hub_{std::make_shared<ChannelHub<int>>()};
    std::shared_ptr<FiberChannel<int>> queue_{std::make_shared<FiberChannel<int>>()};
public:
    Awaitable(){ hub_->attach(queue_); }
    Awaitable(std::shared_ptr<ChannelHub<int>> hub, SubscribeTag)
        : hub_(std::move(hub)){
        if(hub_) hub_->attach(queue_);
    }
    ~Awaitable(){
        if(hub_ && queue_) hub_->detach(queue_.get());
    }
    // 移动/拷贝声明保持原样

    std::shared_ptr<ChannelHub<int>> channel() const { return hub_; }

    std::shared_ptr<Awaitable<void>> shared(){
        return std::make_shared<Awaitable<void>>(hub_, SubscribeTag{});
    }

    bool isClosed() const { return queue_ && queue_->is_closed(); }

    void setCapacity(std::uint32_t capacity){ if(hub_) hub_->setCapacity(capacity); }
    std::uint32_t capacity() const { return hub_ ? hub_->capacity() : 0; }
    void setOnClose(std::function<void()> fn){ if(hub_) hub_->setOnClose(std::move(fn)); }

    Result<void, std::error_code> await(){
        if(queue_){
            int value{};
            auto status = queue_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::no_message);
    }

    template<typename Rep, typename Period>
    Result<void, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(queue_){
            int value{};
            auto status = queue_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::timed_out);
    }

    bool resolve(void){
        if(hub_){
            if(hub_->is_closed()){
                return false;
            }
            return (boost::fibers::channel_op_status::success == hub_->push(1));
        }
        return false;
    }

    void close(){ close(std::make_error_code(std::errc::no_message)); }
    void close(std::error_code error){
        if(queue_) queue_->close(error);
        if(hub_) hub_->notifyClosed(error);
    }
};
```

原有的 Doxygen 注释全部保留，其中提到「内部队列」「镜像」的措辞按新结构改写（参照上一步 `Awaitable<T>` 的注释）。

- [ ] **Step 5: 改 `generator.hpp` 的关闭判据**

三处都在把「整条流是否关闭」误当成「本路输出是否关闭」，改用新的 `isClosed()`：

`coro/await/generator.hpp:79`（`Generator<T>::Yield`）与 `:321`（`Generator<void>::Yield`），两处均为：

```cpp
        bool is_closed() const{return p_awaiter_->isClosed();}
```

`coro/await/generator.hpp:567` 附近（`generate(std::shared_ptr<Awaitable<T>>)` 中超时后的续等判断），把

```cpp
                    auto channel = a->channel();
                    if(channel && !channel->is_closed()){
                        continue;
                    }
```

改为

```cpp
                    if(!a->isClosed()){
                        continue;
                    }
```

- [ ] **Step 6: 编译并跑新用例**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && \
./testfiberawait test_case_flat_source_handle_dropped_keeps_stream \
  test_case_flat_source_queue_released_with_handle test_case_flat_no_consumer_stops_buffering \
  test_case_flat_close_scope_is_self_only test_case_flat_cleanup_runs_once_on_last_handle \
  test_case_flat_void_source_handle_dropped_keeps_stream
```

预期：`Totals: 8 passed, 0 failed`（6 个用例 + `initTestCase` + `cleanupTestCase`）。

- [ ] **Step 7: 全量回归，定位受语义变更影响的旧用例**

```bash
cd /tmp/at-build/testfiberawait && ./testfiberawait 2>&1 | grep -E "^(FAIL|Totals)"
```

预期失败三条，分别对应 spec §7 的行为变化。**只允许改这三条，其余任何一条挂掉都说明实现有 bug，必须回头查而不是改测试。**

- [ ] **Step 8: 改写 `test_case_broadcast_terminal_error`**

原用例断言「源句柄 `close(ec)` 传播到所有订阅者」，这条语义已归生产者侧。整体替换为：

```cpp
/// @brief 验证生产者侧关闭整条流时，终止原因传播到每个消费者且不被覆盖。
/// @details 消费者侧的 close() 只关自己这一路（见 test_case_flat_close_scope_is_self_only），
///          整流终止是生产者的职责，走 channel()（即 ChannelHub）上的 close。
void TestFiberAwait::test_case_broadcast_terminal_error()
{
    Coro::Awaitable<int> source;
    auto first = source.shared();
    auto second = source.shared();
    auto producer = source.channel();

    QVERIFY(source.resolve(7));
    producer->close(std::make_error_code(std::errc::connection_reset));
    producer->close(std::make_error_code(std::errc::timed_out));   // 首次错误不得被覆盖

    QCOMPARE(first->await().value(), 7);
    QCOMPARE(first->await().error(), std::make_error_code(std::errc::connection_reset));
    QCOMPARE(second->await().value(), 7);
    QCOMPARE(second->await().error(), std::make_error_code(std::errc::connection_reset));
    QCOMPARE(source.await().value(), 7);
    QCOMPARE(source.await().error(), std::make_error_code(std::errc::connection_reset));
}
```

- [ ] **Step 9: 改写源析构相关的两条用例**

`connection_aborted` 收敛路径已被结构性消除（spec §3.3、§7.5），这两条改为断言新语义。先把 `private slots:` 里的两条声明改名：

```cpp
    void test_case_broadcast_source_destroyed_keeps_mirror_open();
    void test_case_broadcast_source_destroyed_keeps_chain_open();
```

再把两个实现整体替换：

```cpp
/// @brief 验证源句柄析构后订阅者不再被强行收敛，而是继续等待上游。
/// @details 旧实现下源 channel 析构会以 connection_aborted 关闭镜像；新结构里
///          上游活到最后一个句柄消失，订阅者只是暂时无值可取。
void TestFiberAwait::test_case_broadcast_source_destroyed_keeps_mirror_open()
{
    using namespace std::chrono_literals;
    std::shared_ptr<Coro::Awaitable<int>> subscriber;
    {
        Coro::Awaitable<int> source;
        subscriber = source.shared();
        QVERIFY(source.resolve(5));
    }   // 源析构，未调用 close()

    // 排队值仍先被消费
    auto queued = Coro::await_for(subscriber, 100ms);
    QVERIFY(queued);
    QCOMPARE(queued.value(), 5);

    // 随后既不收敛也不崩溃，只是没有新值
    auto pending = Coro::await_for(subscriber, 50ms);
    QVERIFY(!pending);
    QCOMPARE(pending.error(), std::make_error_code(std::errc::timed_out));
    QVERIFY(!subscriber->isClosed());
}

/// @brief 验证链式订阅在源句柄析构后同样保持开放。
/// @details 链式订阅在新结构里就是同一个 hub 上的两条平级队列，不再是镜像树。
void TestFiberAwait::test_case_broadcast_source_destroyed_keeps_chain_open()
{
    using namespace std::chrono_literals;
    std::shared_ptr<Coro::Awaitable<int>> mirror;
    std::shared_ptr<Coro::Awaitable<int>> nested;
    std::shared_ptr<Coro::ChannelHub<int>> producer;
    {
        Coro::Awaitable<int> source;
        mirror = source.shared();
        nested = mirror->shared();
        producer = source.channel();
        QVERIFY(source.resolve(7));
    }   // 源析构，未 close()

    // 两级都先取到排队值
    QCOMPARE(Coro::await_for(mirror, 100ms).value(), 7);
    QCOMPARE(Coro::await_for(nested, 100ms).value(), 7);

    // 两级都仍然开放，且继续接收上游后续的值
    QCOMPARE(producer->push(8), boost::fibers::channel_op_status::success);
    QCOMPARE(Coro::await_for(mirror, 100ms).value(), 8);
    QCOMPARE(Coro::await_for(nested, 100ms).value(), 8);
}
```

- [ ] **Step 10: 全量回归**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait
```

预期：`Totals: 97 passed, 0 failed, 0 skipped`（91 + 新增 6）。

- [ ] **Step 11: 确认工厂文件零改动**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask && git status --short coro/await/coro*.hpp coro/await/detail/
```

预期：**无输出**。有输出说明 hub 的生产者侧接口与 `FiberChannel` 不兼容，回头补齐 hub 的成员而不是改工厂。

- [ ] **Step 12: 其余三个测试工程回归**

```bash
for t in testfibertask testexecutor test_scheduler/testscheduler.pro; do
  d=/tmp/at-build/$(basename $t .pro); mkdir -p $d && cd $d && \
  qmake /home/david/zpj/Framework-dev/AsyncTask/test/$t && make -j$(nproc) && ./$(basename $d) || echo "FAILED: $t";
done
```

预期：三个工程都以 `0 failed` 结束，且没有 `FAILED:` 输出。

- [ ] **Step 13: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/await/awaitable.hpp coro/await/generator.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "refactor(await): Awaitable 改用 ChannelHub，消费者队列拉平

源与订阅者在消费者表上不再有身份差别，队列由各自句柄独占。
丢弃源句柄不再终止上游，源队列随句柄一起释放；close() 只关自己这一路。"
```

---

### Task 3: 删除 `FiberChannel` 的镜像机制

Task 2 之后镜像代码已无人调用。删干净，让 `FiberChannel` 成为纯通用队列。

**Files:**
- Modify: `coro/detail/fiberchannel.hpp`
- Test: `test/testfiberawait/tst_testfiberawait.cpp:1194-1200`（`test_case_channel_layout_size`）

**Interfaces:**
- Consumes: 无。
- Produces: `FiberChannel<T>` 不再有 `addMirror`、`mirrors_`、`Awaitable` 前向声明与 friend 声明，也不再有用户声明的析构函数。公开成员签名全部不变。

---

- [ ] **Step 1: 确认镜像机制确已无人调用**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask && grep -rn "addMirror\|mirrors_" --include='*.hpp' --include='*.cpp' --include='*.h' .
```

预期：只在 `coro/detail/fiberchannel.hpp` 内部命中。若 `awaitable.hpp` 仍有命中，说明 Task 2 没做完，停下来补齐。

- [ ] **Step 2: 删除镜像相关代码**

在 `coro/detail/fiberchannel.hpp` 中删除这些内容：

1. 第 16 行的前向声明 `template<typename T> class Awaitable;`
2. 第 61-69 行整个析构函数 `~FiberChannel()`（连同其上的文档注释）
3. `push()` 中第 89-106 行的镜像扇出段（`if(mirrors_){ ... }` 整块），保留其后的 `queue_.push_back(...)` 与容量裁剪
4. `close(std::error_code)` 中第 245-252 行的镜像扇出段（`// 终止必须传播...` 注释连同 `if(mirrors_){ ... }`）
5. `discard_pending()` 中第 280-294 行的镜像扇出段，只保留 `queue_.clear();`
6. `setCapacity()` 中第 322-335 行的镜像扇出段
7. 第 355 行的成员 `mirrors_`
8. 第 357 行的 `template<typename U> friend class Awaitable;`
9. 第 358-381 行的 `addMirror()` 及其文档注释
10. 头部不再需要的 `#include <vector>`

删除后各方法体应是：

```cpp
    channel_status push(T value){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if ( BOOST_UNLIKELY( is_closed() ) ) {
            return channel_status::closed;
        }
        queue_.push_back(std::move(value));
        // capacity_ == 0 表示无限容量；否则丢弃队首最旧值，净大小停留在上限
        while(capacity_ != 0 && queue_.size() > capacity_){
            queue_.pop_front();
        }
        cv_consumer_.notify_one();
        return channel_status::success;
    }
```

```cpp
    void close(std::error_code error) noexcept {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            return;
        }
        close_error_ = error == std::error_code{}
                ? std::make_error_code(std::errc::no_message)
                : error;
        closed_.store(true);
        cv_consumer_.notify_all();
    }
```

```cpp
    void discard_pending(){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        queue_.clear();
    }
```

```cpp
    void setCapacity(std::uint32_t capacity){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        capacity_ = capacity;
        if(capacity_ != 0){
            while(queue_.size() > capacity_){
                queue_.pop_front();
            }
        }
    }
```

- [ ] **Step 3: 更新类的文档注释**

文件头 `FiberChannel` 的 `@brief` 段（第 18-39 行）里以 `Awaitable::channel()` 为例的 `@code` 块已经不再准确——`channel()` 现在返回 `ChannelHub`。改为直接使用队列的示例：

```cpp
/**
 * @brief 跨线程/跨协程安全的通用队列，可用于两个线程/协程间数据传递。
 *
 * 代替 boost::fibers::unbuffered_channel——原生的 unbuffered_channel 在
 * 非协程线程上 pop 时会 crash。内部用 fiber 版 mutex/condition_variable，
 * 等待时让出协程而非阻塞线程。不依赖 await 层，可独立使用。
 * @tparam T 队列元素类型
 * @code
 * auto q = std::make_shared<Coro::FiberChannel<int>>();
 * auto prod = Coro::makeTask([q]{
 *     for(int i = 0; i < 3; i++) q->push(i);
 *     q->close();                                 // 结束流，消费者自然收敛
 *     return 0;
 * });
 * auto cons = Coro::makeTask([q]{
 *     int v{};
 *     while(q->pop(v) == boost::fibers::channel_op_status::success) qDebug() << v;
 *     return 0;
 * });
 * @endcode
 */
```

同时把 `push()` 文档里「每个镜像按各自的容量独立丢弃，与源互不影响」这句删掉，`setCapacity()` 文档里关于镜像传播与链式订阅的整段（第 303-308 行）删掉——那些行为现在归 `ChannelHub`。

- [ ] **Step 4: 测出新的 `sizeof` 并更新布局用例**

先把用例改成打印实际值：

```cpp
void TestFiberAwait::test_case_channel_layout_size()
{
    qDebug() << "FiberChannel" << sizeof(Coro::FiberChannel<int>)
             << "ChannelHub" << sizeof(Coro::ChannelHub<int>);
    QCOMPARE(sizeof(Coro::FiberChannel<int>), std::size_t(168));
}
```

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait test_case_channel_layout_size
```

预期：FAIL，并在 `QDEBUG` 行打印出两个实际值。**记下这两个数字**（`FiberChannel` 预计回落到 160，但以实测为准，不要照抄这个预测值）。

- [ ] **Step 5: 用实测值钉死布局**

把用例替换为下面这段，`<实测的 FiberChannel 值>` / `<实测的 ChannelHub 值>` 用上一步打印的数字填入：

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

- [ ] **Step 6: 全量回归**

```bash
cd /tmp/at-build/testfiberawait && make clean && make -j$(nproc) && ./testfiberawait
```

预期：`Totals: 97 passed, 0 failed, 0 skipped`。

- [ ] **Step 7: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/detail/fiberchannel.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "refactor(channel): 删除 FiberChannel 的镜像机制，回归通用队列"
```

---

### Task 4: 文档同步

代码里已经没有「镜像」这个概念了，五份文档还在按旧结构描述。

**Files:**
- Modify: `doc/使用说明.md:216`（能力表）、§6.7（广播消费一节）、`:350`（生产者只捕获 channel 的约定）、`:18` 与 `:205`（非 Qt 构建的能力清单）
- Modify: `doc/软件设计说明.md:41`、`:46`、`:51-52`、`:99`、`:134`、`:263`、`:267`、`:328`、`:333`、`:337`
- Modify: `doc/架构设计.md:19`、§2.2、`:85`
- Modify: `doc/类图与时序图.md:29`、`:35`、`:180`、`:208`
- Modify: `doc/需求规格说明.md:220`（RX_CHANNEL_DATA）、`doc/需求文档.md:75`（NFR-2）
- Modify: `skill/using-asynctask/SKILL.md`（若含 `channel()` 类型或广播用法的描述）
- Modify: `docs/superpowers/specs/2026-08-20-channel-hub-flat-consumers-design.md` §4.2

**Interfaces:**
- Consumes: Task 1-3 落地的最终结构。
- Produces: 无代码接口。

---

- [ ] **Step 1: 修正 spec 中 attach/detach 的可见性描述**

实现时把 `attach` / `detach` / `notifyClosed` 定为 **public**，与 spec §4.2 写的 private + friend 不符。spec 是设计的事实来源，要跟上而不是放任分叉。把 §4.2 代码块里的 `private:` 段拆开——三个方法移到 `public:` 段，并在其后补一句理由：

> 与旧的 `FiberChannel::addMirror` 不同，这三个方法无需 private + friend 保护。`addMirror` 必须收口，是因为两个同类型的 channel 能互相注册为对方的镜像而成环；`ChannelHub` 与 `FiberChannel` 是两个不同的类型层，结构上无法互相挂载，环不可能构造出来。public 同时让 `ChannelHub` 可以脱离 `Awaitable` 独立单测。

- [ ] **Step 2: 更新 `doc/使用说明.md`**

- 第 18 行与第 205 行的非 Qt 能力清单：`FiberChannel` 后补上 `ChannelHub`。
- 第 216 行的能力表：`FiberChannel<T>` 条目改为「跨线程/跨协程安全的通用数据队列，**有容量上限**（默认 1024，0 为无限），超限 `push` 丢弃队首最旧值；`push`/`pop`/`close(error)`/`setCapacity`」，并新增一行 `ChannelHub<T>`：「一条数据流的分发端，`Awaitable::channel()` 返回它；生产者投递一次，每个消费者队列各得一份；`push`/`close(error)`/`discard_pending`/`setCapacity`」。
- 第 350 行的约定改为「生产者只捕获 `channel()`（`shared_ptr<ChannelHub<T>>`），绝不捕获整个 `Awaitable`」，理由段保持不变。
- §6.7 广播消费一节：删去「源队列保留全量」的旧描述，改为说明源与订阅者对称、源句柄可安全丢弃、上游活到最后一个句柄消失；补充 `close()` 只关自己这一路，整流终止走生产者侧。

- [ ] **Step 3: 更新 `doc/软件设计说明.md`**

第 337 行那一整段关于 `mirrors_` 与 `addMirror` 的设计说明已完全失效，整段重写为 `ChannelHub` 的结构说明，覆盖：平表消费者表与 `weak_ptr` 退订；`close()` 不摘表、析构才摘（及其 `corotcpserver` 悬空指针成因）；消费者归零跑一次清理；清理必须在 hub 锁外执行、且触发方持有 hub 引用（spec §3.4）；锁顺序恒为 hub → queue 且层数恒为 2；push 延后一拍投递以保持拷贝次数与旧实现持平。

第 41、46、51-52、134、263、267、328、333 行按新结构逐处订正（`Awaitable` 组合的是 hub + 独占队列；`FiberChannel` 的字段清单删去 `mirrors_`；CSC_DETAIL 的主要内容补上 `ChannelHub`）。第 99 行的图例说明同步改写。

- [ ] **Step 4: 更新 `doc/架构设计.md` 与 `doc/类图与时序图.md`**

- `架构设计.md` 第 19 行 detail 层清单补上 `ChannelHub`；§2.2 增补 hub 的小节；第 85 行改为「内部 `shared_ptr<ChannelHub<T>>` + 独占的 `shared_ptr<FiberChannel<T>>`」。
- `类图与时序图.md` 第 29、35、180、208 行：类图加入 `ChannelHub`，关系改为 `Awaitable --> ChannelHub`、`Awaitable *-- FiberChannel`、`ChannelHub o-- FiberChannel`。若对应的 `.mmd`/`.svg` 由源文件生成，同步更新 `doc/img/sdd-05-csc_await.mmd` 并重新导出 svg；无法导出时在提交信息里注明 svg 待更新。

- [ ] **Step 5: 更新需求文档与 skill**

- `doc/需求规格说明.md:220` 的 RX_CHANNEL_DATA 拆成两项职责：通用队列（`FiberChannel`）与分发端（`ChannelHub`）。
- `doc/需求文档.md:75` 的 NFR-2 中 `Awaitable`/`FiberChannel` 补上 `ChannelHub`。
- 检查并更新 skill：

```bash
cd /home/david/zpj/Framework-dev/AsyncTask && grep -n "channel()\|FiberChannel\|shared()\|镜像" skill/using-asynctask/SKILL.md
```

命中处按新结构订正；无命中则跳过。

- [ ] **Step 6: 确认文档里没有残留的旧概念**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask && grep -rn "addMirror\|mirrors_\|connection_aborted" doc/ skill/ ReadMe.md
```

预期：无输出。`docs/superpowers/` 下的历史 spec 与 plan 是存档，**不要改**。

- [ ] **Step 7: 最终全量回归**

```bash
cd /tmp/at-build/testfiberawait && make -j$(nproc) && ./testfiberawait 2>&1 | tail -3
for t in testfibertask testexecutor test_scheduler/testscheduler.pro; do
  d=/tmp/at-build/$(basename $t .pro); mkdir -p $d && cd $d && \
  qmake /home/david/zpj/Framework-dev/AsyncTask/test/$t && make -j$(nproc) && ./$(basename $d) 2>&1 | tail -2 || echo "FAILED: $t";
done
```

预期：`testfiberawait` 为 `97 passed, 0 failed`，其余三个工程均 `0 failed`。

- [ ] **Step 8: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add doc/ skill/ docs/superpowers/specs/2026-08-20-channel-hub-flat-consumers-design.md
git commit -m "docs: 按 ChannelHub 新结构同步设计与使用文档"
```
