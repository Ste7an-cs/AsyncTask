# FiberTask::cancel 中止 Awaitable Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `FiberTask::cancel()` 在中止当前任务正在等待的 `Awaitable` 时返回 `std::errc::interrupted`，并让该任务后续所有 `await()`/`await_for()` 立即返回 `interrupted`。

**Architecture:** 任务链的 `SharedState` 持有一个单活动 await 的 `TaskCancelSlot`；当前 fiber 通过 `MetaContext` 指向它。`Awaitable::await()`/`await_for()` 在等待前把“关闭本句柄消费者队列”的栈上节点登记到槽；`cancel()` 关闭当前句柄的 `FiberChannel`，唤醒等待。后续 await 只在槽上直接失败，不关闭新句柄。`FiberChannel`、调度器、线程池、QtLocal 和关机流程均不改。

**Tech Stack:** C++17、boost.fiber >= 1.89、Qt >= 5.12（QtTest/ASan）、qmake、Linux/gcc >= 9。

## Global Constraints

- 编译器/标准：gcc >= 9，C++17。
- boost >= 1.89（本机 WSL 当前的 boost 1.72 缺少 `boost::fibers::waker`，不能用于编译本仓库；执行前按 `ReadMe` 安装/使用既有 CI 的 boost 1.89）。
- Qt >= 5.12；测试 `.pro` 已启用 ASan，保留 `-static-libasan`，不要额外加 `-lasan`。
- 取消错误码统一为 `std::make_error_code(std::errc::interrupted)`。
- 取消是协作式的：不抛取消异常、不强制 unwind、不覆盖任务最终返回值。
- 只关闭被取消任务的当前 `Awaitable` **句柄消费者队列**；同 hub 的其他 `shared()` 句柄不受影响。
- `FiberChannel` 零修改；不新增面向用户的公共 API。
- 保持现有 `then` 后继“启动前检查 `is_runnable`”语义；不要改 `SharedState::add_nodes/done_nodes`。
- 所有构建/测试在 Linux/qmake 环境执行；测试生成物在 `test/<name>/` 下，Makefile 已被 `.gitignore` 忽略。
- 每个任务一个独立 commit；提交信息按本仓库风格使用中文 `feat/fix/docs` 前缀。

---

### Task 1: TaskCancelSlot 与 MetaContext 取消槽

**Files:**
- Create: `coro/detail/taskcancel.hpp`
- Create: `coro/detail/taskcancel.cpp`
- Modify: `coro/executor/scheduler/fiberproperty.h`
- Modify: `AsyncTask.pri`
- Test: `test/testfibertask/tst_test_fiber_task.cpp`

**Interfaces:**
- Produces:
  - `struct Coro::detail::AwaitableCancelNode { using CloseFn = void (*)(void*, std::error_code) noexcept; void* obj; CloseFn close; };`
  - `class Coro::detail::TaskCancelSlot`：`bool add(AwaitableCancelNode*); void remove(AwaitableCancelNode*) noexcept; void cancel(std::error_code) noexcept; bool isCancelled() const noexcept;`
  - `class Coro::detail::TaskCancelScope`：构造时把当前 fiber 的取消槽设为给定槽，析构时恢复。
  - `TaskCancelSlot* Coro::detail::currentTaskCancelSlot() noexcept;`
  - `void Coro::detail::setCurrentTaskCancelSlot(TaskCancelSlot*) noexcept;`
  - `MetaContext::taskCancelSlot()` / `setTaskCancelSlot()`。

- [ ] **Step 1: 写失败测试（组件级）**

在 `test/testfibertask/tst_test_fiber_task.cpp` 顶部加入：

```cpp
#include "await/awaitable.hpp"
#include "detail/taskcancel.hpp"
#include <QElapsedTimer>
#include <atomic>
#include <chrono>
#include <functional>
#include <system_error>
#include <thread>
```

在 `test_fiber_task` 的 `private slots:` 中、`void begin();` 之后加入：

```cpp
    void test_case_cancel_slot_aborts_active_node();
```

在 `void test_fiber_task::begin()` 之后、`test_case_chain_task` 之前加入：

```cpp
/**
 * @brief TaskCancelSlot 的独立组件测试：登记、取消、注销与幂等。
 */
void test_fiber_task::test_case_cancel_slot_aborts_active_node()
{
    struct Probe {
        int closed{0};
        std::error_code seen{};
        static void close(void* obj, std::error_code ec) noexcept
        {
            auto* self = static_cast<Probe*>(obj);
            ++self->closed;
            self->seen = ec;
        }
    } probe;

    Coro::detail::TaskCancelSlot slot;
    Coro::detail::AwaitableCancelNode node{&probe, &Probe::close};

    QVERIFY(slot.add(&node));
    QVERIFY(!slot.isCancelled());

    slot.cancel(std::make_error_code(std::errc::interrupted));
    QVERIFY(slot.isCancelled());
    QCOMPARE(probe.closed, 1);
    QVERIFY(probe.seen == std::make_error_code(std::errc::interrupted));

    QVERIFY(!slot.add(&node));                  // 已取消后不再登记
    slot.remove(&node);                         // active 已置空，安全空转
    slot.cancel(std::make_error_code(std::errc::interrupted));
    QCOMPARE(probe.closed, 1);                  // 幂等
}
```

- [ ] **Step 2: 运行测试，确认因缺少头文件而失败**

Run:
```bash
cd test/testfibertask
qmake testfibertask.pro
make -j"$(nproc)"
```
Expected: 编译失败，包含
`fatal error: detail/taskcancel.hpp: No such file or directory`。

- [ ] **Step 3: 创建 `coro/detail/taskcancel.hpp`**

```cpp
#ifndef TASKCANCEL_HPP
#define TASKCANCEL_HPP

#include <atomic>
#include <cassert>
#include <mutex>
#include <system_error>

namespace Coro {
namespace detail {

/**
 * @brief 一次 Awaitable 等待的取消登记项。
 * @details 对象位于等待协程的栈上；cancel() 通过函数指针关闭对应句柄的消费者队列。
 */
struct AwaitableCancelNode {
    /** @brief 关闭回调类型；必须 noexcept，只做队列 close */
    using CloseFn = void (*)(void* obj, std::error_code ec) noexcept;

    void*   obj{nullptr};   ///< 正在等待的 Awaitable 句柄
    CloseFn close{nullptr}; ///< 调该句柄的私有 closeConsumerQueue
};

/**
 * @brief 每个任务链一个的取消槽。
 * @details 同一任务 fiber 同一时刻至多一个活动 await；add/remove/cancel 由 mtx_ 串行化。
 */
class TaskCancelSlot {
public:
    TaskCancelSlot() = default;
    TaskCancelSlot(const TaskCancelSlot&) = delete;
    TaskCancelSlot& operator=(const TaskCancelSlot&) = delete;

    /**
     * @brief 登记当前等待；槽已取消时返回 false
     * @param node 栈上节点，调用方保证其存活到 remove()
     * @return 登记成功返回 true
     */
    bool add(AwaitableCancelNode* node)
    {
        assert(node != nullptr);
        assert(node->close != nullptr);
        std::lock_guard<std::mutex> lk(mtx_);
        if(cancelled_.load(std::memory_order_acquire)){
            return false;
        }
        assert(active_ == nullptr);
        active_ = node;
        return true;
    }

    /**
     * @brief 注销当前等待；节点未被登记或已被 cancel() 取走时安全空转
     * @param node 之前传给 add() 的节点
     */
    void remove(AwaitableCancelNode* node) noexcept
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if(active_ == node){
            active_ = nullptr;
        }
    }

    /**
     * @brief 置取消标志并关闭当前登记的消费者队列
     * @param ec 关闭错误码
     * @details 持锁调用 node->close；等待者返回后 remove() 会阻塞到本函数退出。
     *          close 只做 noexcept 的 FiberChannel::close，不跑用户清理。
     */
    void cancel(std::error_code ec) noexcept
    {
        cancelled_.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lk(mtx_);
        if(active_ != nullptr){
            AwaitableCancelNode* node = active_;
            active_ = nullptr;
            node->close(node->obj, ec);
        }
    }

    /** @brief 是否已请求取消 */
    bool isCancelled() const noexcept
    {
        return cancelled_.load(std::memory_order_acquire);
    }

private:
    std::mutex           mtx_;
    AwaitableCancelNode* active_{nullptr};
    std::atomic_bool     cancelled_{false};
};

TaskCancelSlot* currentTaskCancelSlot() noexcept;
void setCurrentTaskCancelSlot(TaskCancelSlot* slot) noexcept;

/**
 * @brief 将当前 fiber 的 MetaContext 临时指向任务取消槽。
 * @details makeTask/then 在调用用户函数前构造，函数返回/抛出后析构恢复。
 */
class TaskCancelScope {
public:
    explicit TaskCancelScope(TaskCancelSlot* slot) noexcept
        : previous_(currentTaskCancelSlot()){
        setCurrentTaskCancelSlot(slot);
    }
    ~TaskCancelScope(){
        setCurrentTaskCancelSlot(previous_);
    }
    TaskCancelScope(const TaskCancelScope&) = delete;
    TaskCancelScope& operator=(const TaskCancelScope&) = delete;
private:
    TaskCancelSlot* previous_{nullptr};
};

} // namespace detail
} // namespace Coro

#endif // TASKCANCEL_HPP
```

- [ ] **Step 4: 创建 `coro/detail/taskcancel.cpp`**

```cpp
#include "taskcancel.hpp"
#include "executor/scheduler/fiberproperty.h"

#include <boost/fiber/context.hpp>

namespace Coro {
namespace detail {

/**
 * @brief 读取当前 fiber 的 MetaContext 上登记的任务取消槽。
 * @details 非框架 fiber / 非 fiber 上下文返回 nullptr；不调用会 yield 的
 *          boost::this_fiber::properties()。
 */
TaskCancelSlot* currentTaskCancelSlot() noexcept
{
    try {
        boost::fibers::context* ctx = boost::fibers::context::active();
        if(ctx == nullptr){
            return nullptr;
        }
        auto* meta = dynamic_cast<MetaContext*>(ctx->get_properties());
        return meta != nullptr ? meta->taskCancelSlot() : nullptr;
    } catch (...) {
        return nullptr;
    }
}

/**
 * @brief 设置当前 fiber 的 MetaContext 上的任务取消槽。
 * @details 非框架 fiber / 非 fiber 上下文为空操作。
 */
void setCurrentTaskCancelSlot(TaskCancelSlot* slot) noexcept
{
    try {
        boost::fibers::context* ctx = boost::fibers::context::active();
        if(ctx == nullptr){
            return;
        }
        if(auto* meta = dynamic_cast<MetaContext*>(ctx->get_properties())){
            meta->setTaskCancelSlot(slot);
        }
    } catch (...) {
        // 非 fiber 上下文：不支持任务取消，保持无操作。
    }
}

} // namespace detail
} // namespace Coro
```

- [ ] **Step 5: 在 `coro/executor/scheduler/fiberproperty.h` 增加 MetaContext 字段**

在 `namespace Coro {` 之后、`enum class Priority` 之前加入前置声明：

```cpp
namespace detail {
class TaskCancelSlot;
}
```

把 `MetaContext` 的拷贝赋值改为：

```cpp
    MetaContext& operator=(const MetaContext& meta){
        if(this != &meta){
            this->ctx_ = meta.ctx_;
            this->priority_ = meta.priority_;
            this->affinity_ = meta.affinity_;
            this->name_ = meta.name_;
            this->task_cancel_slot_ = meta.task_cancel_slot_;
        }
        return *this;
    }
```

把拷贝构造改为：

```cpp
    MetaContext(const MetaContext& other): boost::fibers::fiber_properties(other.ctx_), priority_(other.priority_), affinity_(other.affinity_), name_(other.name_), task_cancel_slot_(other.task_cancel_slot_){
    }
```

在 `MetaContext` 的 `name_` 成员之后加入：

```cpp
    detail::TaskCancelSlot*       task_cancel_slot_{nullptr};///< 当前任务的取消槽；非任务 fiber 为空
```

在 `MetaContext` 的 public 方法区（`name()` 之后即可）加入：

```cpp
    /**
     * @brief 读取当前 fiber 所属任务的取消槽
     * @return 取消槽指针；无任务上下文时返回 nullptr
     */
    detail::TaskCancelSlot* taskCancelSlot() const noexcept {
        return task_cancel_slot_;
    }
    /**
     * @brief 设置当前 fiber 所属任务的取消槽
     * @param slot 任务链的取消槽
     */
    void setTaskCancelSlot(detail::TaskCancelSlot* slot) noexcept {
        task_cancel_slot_ = slot;
    }
```

- [ ] **Step 6: 把新文件加入 `AsyncTask.pri`**

在 `HEADERS` 中 `$$PWD/coro/detail/fiberchannel.hpp \` 附近加入：

```qmake
    $$PWD/coro/detail/taskcancel.hpp \
```

在 `SOURCES` 中 `$$PWD/coro/detail/asyncdefine.cpp \` 之后加入：

```qmake
    $$PWD/coro/detail/taskcancel.cpp \
```

- [ ] **Step 7: 重新构建并跑组件测试**

Run:
```bash
cd test/testfibertask
qmake testfibertask.pro
make -j"$(nproc)"
./testfibertask test_case_cancel_slot_aborts_active_node
```
Expected: `Totals: 1 passed, 0 failed, 0 skipped`。

- [ ] **Step 8: Commit**

```bash
git add coro/detail/taskcancel.hpp coro/detail/taskcancel.cpp \
        coro/executor/scheduler/fiberproperty.h AsyncTask.pri \
        test/testfibertask/tst_test_fiber_task.cpp
git commit -m "feat: add per-task await cancel slot"
```

---

### Task 2: 中止当前 await 并短路后续 await

**Files:**
- Modify: `coro/task/fibertask.h`
- Modify: `coro/await/awaitable.hpp`
- Test: `test/testfibertask/tst_test_fiber_task.cpp`

**Interfaces:**
- Consumes: 来自 Task 1 的 `TaskCancelSlot`、`AwaitableCancelNode`、`TaskCancelScope`、`currentTaskCancelSlot()`。
- Produces:
  - `SharedState::cancel_slot`（`detail::TaskCancelSlot`）与 `SharedState::cancel() noexcept`。
  - `FiberTask<T>::cancel()` / `FiberTask<void>::cancel()` 调用 `SharedState::cancel()`。
  - `Awaitable<T>::closeConsumerQueue(std::error_code) noexcept` / `Awaitable<void>::closeConsumerQueue(...)` 私有方法。
  - `Awaitable::await()` 的任务取消登记、注销、`interrupted` 返回与 hub 归零通知。

- [ ] **Step 1: 写失败测试（当前 await + 后续 await + hub 清理）**

在 `test/testfibertask/tst_test_fiber_task.cpp` 的 `#define TQVERIFY` 之后、`class SigObject` 之前加入匿名命名空间：

```cpp
namespace {

std::error_code interruptedError()
{
    return std::make_error_code(std::errc::interrupted);
}

/**
 * @brief 有界等待 done；超时则执行 cleanup 再等，避免失败用例把任务永久挂起。
 * @return done 最终是否为 true
 */
bool waitForDone(std::atomic_bool& done, int timeoutMs,
                 const std::function<void()>& cleanup)
{
    QElapsedTimer timer;
    timer.start();
    while(!done.load() && timer.elapsed() < timeoutMs){
        QTest::qWait(1);
    }
    if(!done.load()){
        if(cleanup){
            cleanup();
        }
        timer.restart();
        while(!done.load() && timer.elapsed() < 2000){
            QTest::qWait(1);
        }
    }
    return done.load();
}

} // namespace
```

在 `private slots:` 中追加：

```cpp
    void test_case_cancel_aborts_current_await();
    void test_case_cancel_keeps_following_awaits_immediate();
    void test_case_cancel_closes_last_consumer_hub();
```

在 `test_fiber_task::end()` 之前追加：

```cpp
/**
 * @brief cancel() 时任务正阻塞在 await()，当前等待应立即以 interrupted 收敛。
 */
void test_fiber_task::test_case_cancel_aborts_current_await()
{
    Coro::Awaitable<int> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0}; // 1 成功，2 interrupted，3 其他错误

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r = source.await();
        if(r){
            state.store(1);
        }else if(r.error() == interruptedError()){
            state.store(2);
        }else{
            state.store(3);
        }
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);                    // 让协程真正进入 await
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not leave await()");
    QCOMPARE(state.load(), 2);
}

/**
 * @brief 取消后，同一任务后续 await 必须立即中断，且不再关闭新句柄。
 */
void test_fiber_task::test_case_cancel_keeps_following_awaits_immediate()
{
    Coro::Awaitable<int> first;
    Coro::Awaitable<int> second;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  firstState{0};
    std::atomic_int  secondState{0};
    std::atomic_llong secondMs{-1};

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r1 = first.await();
        firstState.store(r1 ? 1 : (r1.error() == interruptedError() ? 2 : 3));

        const auto t0 = std::chrono::steady_clock::now();
        auto r2 = second.await();
        secondMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - t0).count());
        secondState.store(r2 ? 1 : (r2.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        first.close(std::make_error_code(std::errc::operation_canceled));
        second.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(firstState.load(), 2);
    QCOMPARE(secondState.load(), 2);
    QVERIFY(secondMs.load() >= 0);
    QVERIFY(secondMs.load() < 1000);
    QVERIFY(!second.isClosed());
}

/**
 * @brief 当前句柄是唯一消费者时，取消引起的队列关闭应让 hub 归零并跑一次清理。
 */
void test_fiber_task::test_case_cancel_closes_last_consumer_hub()
{
    Coro::Awaitable<int> source;
    std::atomic_int cleanups{0};
    source.setOnClose([&]{ cleanups.fetch_add(1); });

    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0};

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r = source.await();
        state.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(state.load(), 2);
    QTRY_VERIFY_WITH_TIMEOUT(cleanups.load() == 1, 1000);
    QVERIFY(source.channel()->is_closed());
}
```

- [ ] **Step 2: 运行测试，确认失败**

Run:
```bash
cd test/testfibertask
qmake testfibertask.pro
make -j"$(nproc)"
for t in test_case_cancel_aborts_current_await \
         test_case_cancel_keeps_following_awaits_immediate \
         test_case_cancel_closes_last_consumer_hub; do
    ./testfibertask "$t"
done
```
Expected: 每个用例输出 `FAIL!`；取消尚未接入 Awaitable，`state`/`firstState`/`secondState` 为 3，
`cleanups` 为 1（由 cleanup 的 `operation_canceled` 触发），但 `state` 不是 2。

- [ ] **Step 3: 修改 `coro/task/fibertask.h`：SharedState 与 cancel()**

在文件头部 include 区加入：

```cpp
#include "detail/taskcancel.hpp"
#include <system_error>
```

在 `SharedState` 内部加入字段与取消方法：

```cpp
    detail::TaskCancelSlot cancel_slot;///< 当前节点 await 的取消槽

    /**
     * @brief 取消任务链并中止当前 await
     * @details 置 is_runnable=false（后继短路），并通知当前等待的 Awaitable 句柄
     *          以 interrupted 收敛。
     */
    void cancel() noexcept {
        is_runnable.store(false, std::memory_order_release);
        cancel_slot.cancel(std::make_error_code(std::errc::interrupted));
    }
```

`FiberTask<T>::cancel()` 改为：

```cpp
    void cancel(){
        if(state_ptr_){
            this->state_ptr_->cancel();
        }
    }
```

`FiberTask<void>::cancel()` 同样改为：

```cpp
    void cancel(){
        if(state_ptr_){
            this->state_ptr_->cancel();
        }
    }
```

- [ ] **Step 4: 在 `makeTask` / `then` 的协程体里设置 TaskCancelScope**

`makeTask` 的 fiber lambda 内，把 `try` 块改为：

```cpp
        try {
            detail::TaskCancelScope cancel_scope(&state_ptr->cancel_slot);
            if constexpr (std::is_void_v<ReturnType>){
                func();
                p_ptr->set_value(Result<ReturnType>());
            }else{
                ReturnType val = func();
                p_ptr->set_value(Result<ReturnType>(val));
            }
        } catch (...) {
            p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
        }
```

`FiberTask<T>::then` 的 fiber lambda 内，把 `if(val.has_value() && true == s_ptr->is_runnable.load())` 块改为：

```cpp
                if(val.has_value() && true == s_ptr->is_runnable.load()){
                    detail::TaskCancelScope cancel_scope(&s_ptr->cancel_slot);
                    if constexpr (std::is_void_v<ReturnType>){
                        func(val.value());
                        p_ptr->set_value(Result<ReturnType>());
                    }else{
                        p_ptr->set_value(Result<ReturnType>(func(val.value())));
                    }
                }else{
                    p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
                }
```

`FiberTask<void>::then` 的 fiber lambda 内，把 `if(val.has_value() && true == s_ptr->is_runnable.load())` 块改为：

```cpp
                if(val.has_value() && true == s_ptr->is_runnable.load()){
                    detail::TaskCancelScope cancel_scope(&s_ptr->cancel_slot);
                    if constexpr (std::is_void_v<ReturnType>){
                        func();
                        p_ptr->set_value(Result<ReturnType>());
                    }else{
                        p_ptr->set_value(Result<ReturnType>(func()));
                    }
                }else{
                    p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
                }
```

- [ ] **Step 5: 修改 `coro/await/awaitable.hpp`：私有关闭 + 当前 await 接入**

在 include 区加入：

```cpp
#include "detail/taskcancel.hpp"
#include <system_error>
```

在 `Awaitable<T>` 类末尾（`close(std::error_code)` 之后、类的 `};` 之前）加入：

```cpp
private:
    /**
     * @brief 只关闭本句柄的消费者队列（任务取消路径）
     * @param ec 关闭错误码
     */
    void closeConsumerQueue(std::error_code ec) noexcept {
        if(queue_){
            queue_->close(ec);
        }
    }
    /** @brief 类型擦除 thunk，供 TaskCancelSlot 调用 */
    static void closeConsumerQueueThunk(void* obj, std::error_code ec) noexcept {
        static_cast<Awaitable*>(obj)->closeConsumerQueue(ec);
    }
```

把 `Awaitable<T>::await()` 整个替换为：

```cpp
    Result<T, std::error_code> await(){
        auto* cancel = detail::currentTaskCancelSlot();
        if(cancel && cancel->isCancelled()){
            return std::make_error_code(std::errc::interrupted);
        }
        if(!queue_){
            return std::make_error_code(std::errc::no_message);
        }
        detail::AwaitableCancelNode cancel_node{this, &Awaitable::closeConsumerQueueThunk};
        const bool tracked = cancel && cancel->add(&cancel_node);
        if(cancel && !tracked){
            return std::make_error_code(std::errc::interrupted);
        }

        T value{};
        const bool state = (hub_ && hub_->isState());
        auto status = state ? queue_->wait_peek(value) : queue_->pop(value);
        if(tracked){
            cancel->remove(&cancel_node);
        }
        if(status == boost::fibers::channel_op_status::success){
            if(state){
                boost::this_fiber::yield();
            }
            return value;
        }

        const std::error_code error = queue_->close_error();
        if(hub_){
            hub_->notifyClosed(error);
        }
        return error;
    }
```

在 `Awaitable<void>` 类末尾加入同样的私有方法（`queue_` 为 `FiberChannel<int>`）：

```cpp
private:
    /**
     * @brief 只关闭本句柄的消费者队列（任务取消路径）
     * @param ec 关闭错误码
     */
    void closeConsumerQueue(std::error_code ec) noexcept {
        if(queue_){
            queue_->close(ec);
        }
    }
    /** @brief 类型擦除 thunk，供 TaskCancelSlot 调用 */
    static void closeConsumerQueueThunk(void* obj, std::error_code ec) noexcept {
        static_cast<Awaitable*>(obj)->closeConsumerQueue(ec);
    }
```

把 `Awaitable<void>::await()` 整个替换为：

```cpp
    Result<void, std::error_code> await(){
        auto* cancel = detail::currentTaskCancelSlot();
        if(cancel && cancel->isCancelled()){
            return std::make_error_code(std::errc::interrupted);
        }
        if(!queue_){
            return std::make_error_code(std::errc::no_message);
        }
        detail::AwaitableCancelNode cancel_node{this, &Awaitable::closeConsumerQueueThunk};
        const bool tracked = cancel && cancel->add(&cancel_node);
        if(cancel && !tracked){
            return std::make_error_code(std::errc::interrupted);
        }

        int value{};
        const bool state = (hub_ && hub_->isState());
        auto status = state ? queue_->wait_peek(value) : queue_->pop(value);
        if(tracked){
            cancel->remove(&cancel_node);
        }
        if(status == boost::fibers::channel_op_status::success){
            if(state){
                boost::this_fiber::yield();
            }
            return Result<void, std::error_code>();
        }

        const std::error_code error = queue_->close_error();
        if(hub_){
            hub_->notifyClosed(error);
        }
        return error;
    }
```

- [ ] **Step 6: 构建并跑三个用例**

Run:
```bash
cd test/testfibertask
qmake testfibertask.pro
make -j"$(nproc)"
for t in test_case_cancel_aborts_current_await \
         test_case_cancel_keeps_following_awaits_immediate \
         test_case_cancel_closes_last_consumer_hub; do
    ./testfibertask "$t"
done
```
Expected: 三个用例均为 `Totals: 1 passed, 0 failed, 0 skipped`。

- [ ] **Step 7: Commit**

```bash
git add coro/task/fibertask.h coro/await/awaitable.hpp \
        test/testfibertask/tst_test_fiber_task.cpp
git commit -m "feat: abort current await on task cancel"
```

---

### Task 3: 取消 await_for

**Files:**
- Modify: `coro/await/awaitable.hpp`
- Test: `test/testfibertask/tst_test_fiber_task.cpp`

**Interfaces:**
- Consumes: Task 1 的取消槽、Task 2 的 `closeConsumerQueue` 与 `await` 接入。
- Produces: `Awaitable<T>::await_for()` / `Awaitable<void>::await_for()` 在取消时返回 `interrupted`；超时与取消同时发生时取消优先。

- [ ] **Step 1: 写失败测试**

在 `private slots:` 中追加：

```cpp
    void test_case_cancel_await_for_returns_interrupted();
```

在 `test_fiber_task::end()` 之前追加：

```cpp
/**
 * @brief cancel() 时任务阻塞在 await_for()，应返回 interrupted 而不是 timed_out。
 */
void test_fiber_task::test_case_cancel_await_for_returns_interrupted()
{
    Coro::Awaitable<int> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0}; // 1 成功，2 interrupted，3 timed_out，4 其他
    std::atomic_llong elapsedMs{-1};

    auto task = Coro::makeTask([&]{
        started.store(true);
        const auto t0 = std::chrono::steady_clock::now();
        auto r = source.await_for(std::chrono::seconds(5));
        elapsedMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count());
        if(r){
            state.store(1);
        }else if(r.error() == interruptedError()){
            state.store(2);
        }else if(r.error() == std::make_error_code(std::errc::timed_out)){
            state.store(3);
        }else{
            state.store(4);
        }
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not leave await_for()");
    QCOMPARE(state.load(), 2);
    QVERIFY(elapsedMs.load() >= 0);
    QVERIFY(elapsedMs.load() < 1000);
}
```

- [ ] **Step 2: 运行测试，确认失败**

Run:
```bash
cd test/testfibertask
qmake testfibertask.pro
make -j"$(nproc)"
./testfibertask test_case_cancel_await_for_returns_interrupted
```
Expected: `FAIL!`；`state` 为 4（cleanup 用 `operation_canceled` 关闭），不是 2。

- [ ] **Step 3: 修改 `Awaitable<T>::await_for()`**

把整个函数替换为：

```cpp
    template<typename Rep, typename Period>
    Result<T, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        auto* cancel = detail::currentTaskCancelSlot();
        if(cancel && cancel->isCancelled()){
            return std::make_error_code(std::errc::interrupted);
        }
        if(!queue_){
            return std::make_error_code(std::errc::timed_out);
        }
        detail::AwaitableCancelNode cancel_node{this, &Awaitable::closeConsumerQueueThunk};
        const bool tracked = cancel && cancel->add(&cancel_node);
        if(cancel && !tracked){
            return std::make_error_code(std::errc::interrupted);
        }

        T value{};
        const bool state = (hub_ && hub_->isState());
        auto status = state ? queue_->wait_peek_for(value, timeout)
                            : queue_->pop_wait_for(value, timeout);
        if(tracked){
            cancel->remove(&cancel_node);
        }
        if(status == boost::fibers::channel_op_status::success){
            if(state){
                boost::this_fiber::yield();
            }
            return value;
        }
        if(status == boost::fibers::channel_op_status::timeout){
            if(cancel && cancel->isCancelled()){
                if(queue_->is_closed() && hub_){
                    hub_->notifyClosed(queue_->close_error());
                }
                return std::make_error_code(std::errc::interrupted);
            }
            return std::make_error_code(std::errc::timed_out);
        }

        const std::error_code error = queue_->close_error();
        if(hub_){
            hub_->notifyClosed(error);
        }
        return error;
    }
```

- [ ] **Step 4: 修改 `Awaitable<void>::await_for()`**

把整个函数替换为：

```cpp
    template<typename Rep, typename Period>
    Result<void, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        auto* cancel = detail::currentTaskCancelSlot();
        if(cancel && cancel->isCancelled()){
            return std::make_error_code(std::errc::interrupted);
        }
        if(!queue_){
            return std::make_error_code(std::errc::timed_out);
        }
        detail::AwaitableCancelNode cancel_node{this, &Awaitable::closeConsumerQueueThunk};
        const bool tracked = cancel && cancel->add(&cancel_node);
        if(cancel && !tracked){
            return std::make_error_code(std::errc::interrupted);
        }

        int value{};
        const bool state = (hub_ && hub_->isState());
        auto status = state ? queue_->wait_peek_for(value, timeout)
                            : queue_->pop_wait_for(value, timeout);
        if(tracked){
            cancel->remove(&cancel_node);
        }
        if(status == boost::fibers::channel_op_status::success){
            if(state){
                boost::this_fiber::yield();
            }
            return Result<void, std::error_code>();
        }
        if(status == boost::fibers::channel_op_status::timeout){
            if(cancel && cancel->isCancelled()){
                if(queue_->is_closed() && hub_){
                    hub_->notifyClosed(queue_->close_error());
                }
                return std::make_error_code(std::errc::interrupted);
            }
            return std::make_error_code(std::errc::timed_out);
        }

        const std::error_code error = queue_->close_error();
        if(hub_){
            hub_->notifyClosed(error);
        }
        return error;
    }
```

- [ ] **Step 5: 构建并跑用例**

Run:
```bash
cd test/testfibertask
qmake testfibertask.pro
make -j"$(nproc)"
./testfibertask test_case_cancel_await_for_returns_interrupted
```
Expected: `Totals: 1 passed, 0 failed, 0 skipped`。

- [ ] **Step 6: Commit**

```bash
git add coro/await/awaitable.hpp test/testfibertask/tst_test_fiber_task.cpp
git commit -m "feat: cancel await_for with interrupted"
```

---

### Task 4: 句柄隔离、取消前等待、已完成任务与跨线程取消

**Files:**
- Test: `test/testfibertask/tst_test_fiber_task.cpp`

**Interfaces:**
- Consumes: Task 2/3 的完整取消实现。
- Produces: 以下边界行为的回归用例。

- [ ] **Step 1: 写测试**

在 `private slots:` 中追加：

```cpp
    void test_case_cancel_keeps_sibling_shared_handle();
    void test_case_cancel_before_await_is_immediate();
    void test_case_cancel_completed_task_is_noop();
    void test_case_cancel_from_other_thread();
```

在 `test_fiber_task::end()` 之前追加：

```cpp
/**
 * @brief 取消一个 shared() 句柄不影响同 hub 的其它句柄。
 */
void test_fiber_task::test_case_cancel_keeps_sibling_shared_handle()
{
    Coro::Awaitable<int> source;
    auto h1 = source.shared();
    auto h2 = source.shared();

    std::atomic_bool started1{false};
    std::atomic_bool started2{false};
    std::atomic_bool done1{false};
    std::atomic_bool done2{false};
    std::atomic_int  state1{0};
    std::atomic_int  value2{-1};

    auto t1 = Coro::makeTask([h1, &started1, &done1, &state1]{
        started1.store(true);
        auto r = h1->await();
        state1.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done1.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    auto t2 = Coro::makeTask([h2, &started2, &done2, &value2]{
        started2.store(true);
        auto r = h2->await();
        if(r){
            value2.store(r.value());
        }
        done2.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started1.load() && started2.load(), 3000);
    QTest::qWait(100);
    t1.cancel();
    const bool done1Ok = waitForDone(done1, 2000, cleanup);
    if(!done1Ok || state1.load() != 2){
        // 红阶段/失败路径：cleanup 已关闭 source，等 h2 也收尾，避免提前 return 后局部变量被使用
        (void)waitForDone(done2, 2000, cleanup);
    }
    QVERIFY2(done1Ok, "h1 waiter did not finish");
    QCOMPARE(state1.load(), 2);
    QVERIFY(!done2.load());
    QVERIFY(h1->isClosed());
    QVERIFY(!h2->isClosed());

    QVERIFY(source.resolve(7));
    QVERIFY2(waitForDone(done2, 2000, cleanup), "h2 waiter did not finish");
    QCOMPARE(value2.load(), 7);
    QVERIFY(!h2->isClosed());
}

/**
 * @brief 取消发生在任务进入 await 之前，下一次 await 应立即中断且不关闭句柄。
 */
void test_fiber_task::test_case_cancel_before_await_is_immediate()
{
    Coro::Awaitable<int> source;
    std::atomic_bool entered{false};
    std::atomic_bool gate{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0};
    std::atomic_llong elapsedMs{-1};

    auto task = Coro::makeTask([&]{
        entered.store(true);
        while(!gate.load()){
            Coro::msleep(1);
        }
        const auto t0 = std::chrono::steady_clock::now();
        auto r = source.await();
        elapsedMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count());
        state.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        gate.store(true);
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(entered.load(), 3000);
    task.cancel();
    gate.store(true);
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(state.load(), 2);
    QVERIFY(elapsedMs.load() >= 0);
    QVERIFY(elapsedMs.load() < 1000);
    QVERIFY(!source.isClosed());
}

/**
 * @brief 已完成任务再 cancel 不应崩溃；FiberTask::get() 是单消费者，不能重复取。
 */
void test_fiber_task::test_case_cancel_completed_task_is_noop()
{
    auto task = Coro::makeTask([]{ return 42; },
                               Coro::Priority::Normal,
                               Coro::Affinity::sticky());
    auto first = task.get();
    QVERIFY(first.has_value());
    QCOMPARE(first.value(), 42);

    // 已完成：没有活动 await；cancel 只置位标志，不得崩溃。
    task.cancel();
}

/**
 * @brief 从没有 fiber 调度器的普通 std::thread 调用 cancel() 也要安全。
 */
void test_fiber_task::test_case_cancel_from_other_thread()
{
    Coro::Awaitable<int> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0};

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r = source.await();
        state.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);

    std::thread canceller([task]() mutable { task.cancel(); });
    canceller.join();

    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(state.load(), 2);
}
```

- [ ] **Step 2: 构建并运行四个用例**

Run:
```bash
cd test/testfibertask
qmake testfibertask.pro
make -j"$(nproc)"
for t in test_case_cancel_keeps_sibling_shared_handle \
         test_case_cancel_before_await_is_immediate \
         test_case_cancel_completed_task_is_noop \
         test_case_cancel_from_other_thread; do
    ./testfibertask "$t"
done
```
Expected: 四个用例均为 `Totals: 1 passed, 0 failed, 0 skipped`。若失败，按 spec 的语义修正 Task 2/3 的生产代码，再重跑本步骤；不要修改测试以绕过行为。

- [ ] **Step 3: Commit**

```bash
git add test/testfibertask/tst_test_fiber_task.cpp
git commit -m "test: cover task cancel edge cases"
```

---

### Task 5: 文档与全量回归

**Files:**
- Modify: `coro/task/fibertask.h`（仅 Doxygen 注释）
- Modify: `coro/await/awaitable.hpp`（仅 Doxygen 注释）
- Modify: `ReadMe.md`
- Modify: `doc/使用说明.md`
- Modify: `doc/架构设计.md`
- Modify: `doc/需求文档.md`
- Modify: `doc/需求规格说明.md`
- Modify: `doc/软件设计说明.md`
- Modify: `doc/类图与时序图.md`
- Modify: `skill/using-asynctask/SKILL.md`

**Interfaces:**
- Consumes: 已实现的取消语义。
- Produces: 文档与全量回归结果。

- [ ] **Step 1: 更新 `ReadMe.md`**

把取消示例行：

```cpp
// task.cancel();                            // 需要时取消任务链
```

改为：

```cpp
// task.cancel();                            // 取消：中止当前 await，后继短路
```

在该代码块之后加入：

```markdown
`cancel()` 是协作式取消：若任务当前阻塞在 `Awaitable::await()` / `await_for()`，
该 Awaitable **句柄**的消费者队列会以 `std::errc::interrupted` 关闭，当前等待
立即返回；取消后同一任务上的后续 await 也立即返回 `interrupted`。同一 hub 的
其他 `shared()` 句柄不受影响；同一个 Awaitable 句柄上的多个等待者会一起被唤醒，
需要按任务隔离时请使用独立的 `shared()` 句柄。框架不抛取消异常，也不强制结束
任务函数。
```

- [ ] **Step 2: 更新 `doc/使用说明.md`**

把术语表里 `FiberTask\<T\>` 行的取消描述改为：

```markdown
| FiberTask\<T\> | 一次协程任务的句柄：`then` 链式后继、`on_finally` 结束回调、`cancel` 取消（中止当前 await、后续等待立即中断）、`get()` 让出式取结果 |
```

把 §6.1 代码块中的：

```cpp
// task.cancel();                            // 取消：尚未开始的后继短路返回中断
```

改为：

```cpp
// task.cancel();                            // 取消：中止当前 await，后续等待与后继都中断
```

在 §6.1 代码块之后、`多协程汇合（join）` 之前加入：

```markdown
`cancel()` 会让任务当前等待的 Awaitable 句柄以 `std::errc::interrupted` 关闭，
当前 await 立即返回；取消后同一任务的后续 await 也立即返回 `interrupted`。
同 hub 的其他 `shared()` 句柄不受影响；同一个 Awaitable 句柄上的多个等待者会
一起被唤醒。取消是协作式的：框架不抛异常，也不强制任务函数结束。
```

- [ ] **Step 3: 更新 `doc/架构设计.md`**

把 §4.1 的：

```markdown
- `cancel()`：置 `SharedState.is_runnable=false`；后继协程运行前检查该标志，取消则以中断错误短路。
```

改为：

```markdown
- `cancel()`：置 `SharedState.is_runnable=false` 并触发任务的取消槽；当前阻塞在 `Awaitable::await()`/`await_for()` 的句柄消费者队列以 `interrupted` 关闭，当前等待立即返回；取消后后续 await 也立即返回 `interrupted`；后继协程运行前检查该标志并短路。取消只影响该句柄，不影响同 hub 的其他 `shared()` 句柄。
```

- [ ] **Step 4: 更新 `doc/需求文档.md`**

把 FR-3.3 改为：

```markdown
- FR-3.3 `cancel()`：取消任务链；当前正在等待的 Awaitable 句柄以 `interrupted` 收敛，取消后后续 await/await_for 与未开始的后继均返回中断错误。取消是协作式的，不强制结束正在执行的任务函数。
```

- [ ] **Step 5: 更新 `doc/需求规格说明.md`**

把 §3.1.3.1 的句子改为：

```markdown
应提供任务链式编排能力：在前驱完成后自动以其结果为入参执行后继任务；在整条任务链全部结束时统一触发收尾回调；发起取消后，当前正在等待的 Awaitable 立即以中断结果返回，尚未开始的后续任务在启动前即短路并返回中断结果，取消后的后续 await 也立即中断；取回任务结果时以协程让出方式等待而不阻塞线程。
```

把 §3.1.3.3 的取消条目改为：

```markdown
- 取消：对任务链发起取消后，当前等待的 Awaitable 句柄以中断错误收敛；后续 await/await_for 立即返回中断；尚未开始的后续任务在启动前短路、返回中断结果。
```

把 §3.2.2 的取消条目改为：

```markdown
- 取消一条任务链：中止当前 Awaitable 等待，使后续 await 立即中断，并使尚未开始的后续节点在启动前短路、不再执行。
```

把 RX_TASK_STATE_DATA 的 SharedState 描述改为：

```markdown
- **RX_TASK_STATE_DATA** 任务链状态（SharedState）：任务链各节点共享，含在途节点计数（原子）、取消标志（原子）、结束回调列表与当前 await 的任务取消槽。
```

- [ ] **Step 6: 更新 `doc/软件设计说明.md`**

把 §2.1.1 图例中 SharedState 的描述补上取消槽：

```markdown
`pending_cnt` 记在途节点数、`is_runnable` 为取消标志、`cancel_slot` 登记当前 await 的取消节点、`finally_cbs` 收集结束回调。
```

把 §2.2 任务链时序图例说明的“取消是协作式的，只令尚未开始的节点短路”改为：

```markdown
取消是协作式的：当前正在 `Awaitable::await()` 的节点会因句柄消费者队列以 `interrupted` 关闭而提前返回；后续 await 也立即中断；尚未开始的节点短路。取消不抛异常、不强制结束正在执行的 CPU 代码。
```

把单个节点状态图例说明的“取消是**协作式**的——只令尚未开始的节点短路，不打断正在运行的协程”改为：

```markdown
取消是**协作式**的——当前 await 会提前返回 `interrupted`，后续 await 立即中断，尚未开始的节点短路；不打断正在执行的 CPU 计算，也不强制覆盖任务最终返回值。
```

把 §5.3 的 `cancel()` 描述改为：

```markdown
`cancel()`：`is_runnable = false`，并触发 `cancel_slot`，使当前等待的 Awaitable 句柄以 `interrupted` 关闭；取消后后续 await 立即返回 `interrupted`，未开始的后继短路。
```

- [ ] **Step 7: 更新头文件 Doxygen 注释**

`coro/task/fibertask.h` 中 `FiberTask<T>::cancel()` 与 `FiberTask<void>::cancel()`
的注释都改为：

```cpp
    /**
     * @brief 取消任务链并中止当前 await
     * @details 当前若阻塞在 Awaitable::await()/await_for()，该句柄的消费者队列
     *          以 interrupted 关闭，当前等待立即返回；取消后后续 await 立即中断；
     *          尚未开始的后继在启动前短路。取消是协作式的，不强制结束任务函数。
     * @code
     * auto task = Coro::makeTask(step1).then(step2);
     * task.cancel();      // 中止当前 await；后续等待/后继返回中断
     * @endcode
     */
```

`coro/await/awaitable.hpp` 中两个 `await()` 与两个 `await_for()` 的 Doxygen 注释各加一行：

```cpp
     * @note 任务 cancel() 中止当前等待时，本句柄的消费者队列以 interrupted 关闭；
     *       同 hub 的其他 shared() 句柄不受影响。
```

- [ ] **Step 8: 更新 `doc/类图与时序图.md` 与 skill**

在 §2.3 任务链时序代码块之后加入：

```markdown
> 若当前节点阻塞在 `Awaitable::await()`，`cancel()` 会关闭该句柄的消费者队列，
> 当前等待返回 `interrupted`；取消后新的 await 也立即返回 `interrupted`，
> 同 hub 的其他 `shared()` 句柄不受影响。
```

把 `skill/using-asynctask/SKILL.md` 速查表的：

```markdown
| 取消任务链 | `t.cancel();`（只令尚未开始的节点短路） |
```

改为：

```markdown
| 取消任务链 | `t.cancel();`（中止当前 await，后续 await 立即中断，未开始后继短路） |
```

- [ ] **Step 9: 构建全部测试**

Run:
```bash
cd test
qmake TestFiber.pro
make -j"$(nproc)"
```
Expected: 全部子工程编译成功，无错误。

- [ ] **Step 10: 运行全部测试**

Run:
```bash
cd test
QT_QPA_PLATFORM=offscreen ./testfibertask/testfibertask
QT_QPA_PLATFORM=offscreen ./testfiberawait/testfiberawait
QT_QPA_PLATFORM=offscreen ./testexecutor/testexecutor
QT_QPA_PLATFORM=offscreen ./testquit/testquit
QT_QPA_PLATFORM=offscreen ./test_scheduler/testscheduler
QT_QPA_PLATFORM=offscreen ./testlatency/testlatency
QT_QPA_PLATFORM=offscreen ./testProfile/testProfile
```
Expected: 每个可执行文件输出 `0 failed`；`testquit` 的辅助子进程 15s 内完成；ASan 无报错。

- [ ] **Step 11: 记录性能基准**

Run:
```bash
cd test
./testlatency/testlatency 2>&1 | tee /tmp/task-cancel-latency.txt
./testProfile/testProfile 2>&1 | tee /tmp/task-cancel-profile.txt
```
Expected: 两程序正常退出；把 p50/p99 与 CPU 结果记录到 commit message 或交付说明。若相对 master 出现明显退化（数量级或 p99 显著恶化），在 Task 5 内先分析 `dynamic_cast`/槽锁开销，再决定是否优化；不要在未说明的情况下放宽测试。

- [ ] **Step 12: Commit**

```bash
git add coro/task/fibertask.h coro/await/awaitable.hpp \
        ReadMe.md doc/使用说明.md doc/架构设计.md doc/需求文档.md \
        doc/需求规格说明.md doc/软件设计说明.md doc/类图与时序图.md \
        skill/using-asynctask/SKILL.md
git commit -m "docs: document task cancel await interruption"
```

---

## Self-Review

**Spec coverage:**
- 当前 await 中止：Task 2（`await` 登记 + cancel 关闭句柄队列）。
- 后续 await 立即中断：Task 2（预检查 `isCancelled`，不关闭新句柄）。
- `await_for` 取消：Task 3。
- 只关句柄、同 hub 其他 shared 不受影响：Task 2 `closeConsumerQueue` + Task 4 隔离用例。
- hub 最后一个消费者归零清理：Task 2 `notifyClosed` + `test_case_cancel_closes_last_consumer_hub`。
- 跨线程/取消前/已完成：Task 4。
- 不改 FiberChannel、无新公共 API：Task 2/3 代码路径。
- 文档与回归：Task 5。

**Placeholder scan:** 无占位符；所有代码步骤带完整代码，命令带预期输出。

**Type consistency:**
- `TaskCancelSlot`/`AwaitableCancelNode`/`TaskCancelScope` 名称贯穿 Task 1–4。
- `closeConsumerQueue` / `closeConsumerQueueThunk` 在 Task 2/3 一致。
- `interruptedError()` 与 `waitForDone()` 在 Task 2 定义，Task 3/4 复用。
- `SharedState::cancel()` 在 Task 2 定义，`FiberTask::cancel()` 调用。
