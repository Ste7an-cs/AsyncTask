# FiberTask::cancel 提前中止 Awaitable 设计

- 日期：2026-10-27
- 状态：待评审
- 范围：`FiberTask::cancel()` 对当前任务正在等待的 `Awaitable` 的协作式取消
- 相关文档：`doc/架构设计.md`、`doc/使用说明.md`、`doc/需求文档.md`

## 1. 背景

现状 `FiberTask::cancel()` 只设置 `SharedState::is_runnable = false`：

- 后继 `then` 节点在启动前检查该标志并短路；
- 当前正在运行的节点不受影响；
- 如果当前节点正阻塞在 `Awaitable::await()` / `await_for()`，没有任何机制把它唤醒，任务会一直挂到数据到达或进程退出。

本设计增加一条**按任务**的取消通知：取消时让当前等待的 Awaitable 提前返回 `std::errc::interrupted`，并让该任务后续的 await 也立即失败。它不改变现有“未开始的后继短路”语义。

## 2. 目标与非目标

### 2.1 目标

- `FiberTask::cancel()` 在任务当前等待 `Awaitable` 时，使该次 await 立即返回 `interrupted`。
- 取消之后，同一任务上的所有后续 `await()` / `await_for()` 立即返回 `interrupted`，不再挂起。
- 只关闭当前等待的那个 Awaitable **句柄的消费者队列**；同一 hub 的其他 `shared()` 句柄不受影响。
- 取消可从任意线程调用；不需要新增公共 API；`FiberChannel` 零修改。
- 取消是协作式的，不抛异常、不强制 unwind。

### 2.2 非目标

- 不覆盖 `FiberTask::get()`、`Coro::msleep()`、直接使用 boost 同步原语的阻塞。
- 不关闭整条 hub；只有当前句柄是最后一个未关闭消费者时，hub 才按既有归零规则关闭。
- 不强制中止正在执行的 CPU 计算，也不强制覆盖任务最终返回值。
- 不把取消异常注入协程栈；用户代码收到 `interrupted` 后自行决定是否 return。
- 不改变 `then` 后继“启动前检查 is_runnable”的既有行为。
- 不处理 `on_finally` 回调内部发起的 await。

## 3. 关键决策

| 决策 | 结论 |
|---|---|
| 触发 | `FiberTask::cancel()`（任务链任一 handle） |
| 当前等待 | 关闭该 Awaitable 句柄的消费者队列 → await 返回 `interrupted` |
| 后续等待 | 立即返回 `interrupted`，不挂起、不关闭新句柄 |
| 错误码 | `std::make_error_code(std::errc::interrupted)` |
| 关闭粒度 | 句柄级消费者队列，不是 hub、不是 per-waiter |
| 结束方式 | 协作式；不抛异常、不强制 unwind、不覆盖最终结果 |
| 当前任务槽 | 每个任务链一个 `TaskCancelSlot`，由当前 fiber 的 `MetaContext` 指向 |
| 线程局部 | 不用 `thread_local`；用 per-fiber 的 `MetaContext` 字段 |
| 容器 | 单槽（一个任务同一时刻只能有一个活动 await），无 list、无堆分配 |

## 4. 架构

### 4.1 `TaskCancelSlot`（新增 `coro/detail/taskcancel.hpp` / `.cpp`）

```cpp
namespace Coro::detail {

struct AwaitableCancelNode {
    using CloseFn = void (*)(void* obj, std::error_code ec) noexcept;

    void*   obj{nullptr};    ///< 正在等待的 Awaitable 句柄
    CloseFn close{nullptr};  ///< 调该句柄的私有 closeConsumerQueue
};

class TaskCancelSlot {
public:
    bool add(AwaitableCancelNode* node);   ///< 已取消返回 false
    void remove(AwaitableCancelNode* node) noexcept;
    void cancel(std::error_code ec) noexcept;
    bool isCancelled() const noexcept;

private:
    std::mutex            mtx_;
    AwaitableCancelNode*  active_{nullptr};
    std::atomic_bool      cancelled_{false};
};

class TaskCancelScope {
public:
    explicit TaskCancelScope(TaskCancelSlot* slot) noexcept
        : previous_(currentTaskCancelSlot()) {
        setCurrentTaskCancelSlot(slot);
    }
    ~TaskCancelScope() {
        setCurrentTaskCancelSlot(previous_);
    }
    TaskCancelScope(const TaskCancelScope&) = delete;
    TaskCancelScope& operator=(const TaskCancelScope&) = delete;

private:
    TaskCancelSlot* previous_{nullptr};
};

TaskCancelSlot* currentTaskCancelSlot() noexcept;
void setCurrentTaskCancelSlot(TaskCancelSlot* slot) noexcept;

} // namespace Coro::detail
```

实现要点：

- `add()`：持 `mtx_`；`cancelled_` 为真返回 false；否则断言 `active_ == nullptr`，
  记录 `active_` 并返回 true。
- `remove()`：持 `mtx_`；`active_ == node` 时置空。
- `cancel(ec)`：先 `cancelled_.store(true, release)`，再持 `mtx_`；若 `active_`
  非空，先取走指针并置空，再调用 `active_->close(active_->obj, ec)`。
  - close 只做 `FiberChannel::close()`（`noexcept`），不跑用户清理；
  - 等待者被唤醒后返回 `await()`，`remove()` 需要同一把锁，因此节点在 close
    期间不会被销毁。
- `isCancelled()`：原子读，acquire。
- 同一任务同一时刻只允许一个活动 await；`add()` 中的断言在 release 构建被移除，
  但这是调用方（单协程顺序执行）保证的不变量。

### 4.2 当前 fiber 的取消槽

`MetaContext` 增加一个字段：

```cpp
detail::TaskCancelSlot* task_cancel_slot_{nullptr};

detail::TaskCancelSlot* taskCancelSlot() const noexcept;
void setTaskCancelSlot(detail::TaskCancelSlot* slot) noexcept;
```

- `fiberproperty.h` 只前置声明 `Coro::detail::TaskCancelSlot`，不引入 taskcancel 实现。
- `MetaContext` 的拷贝构造/赋值一并复制该指针；它不拥有对象。
- `currentTaskCancelSlot()` 实现（`taskcancel.cpp`）：

```cpp
TaskCancelSlot* currentTaskCancelSlot() noexcept {
    try {
        auto* ctx = boost::fibers::context::active();
        if (!ctx) return nullptr;
        auto* meta = dynamic_cast<MetaContext*>(ctx->get_properties());
        return meta ? meta->taskCancelSlot() : nullptr;
    } catch (...) {
        return nullptr;
    }
}
```

- 直接用 `context::active()->get_properties()`，不用
  `boost::this_fiber::properties<MetaContext>()`——后者在 properties 为空时会
  `yield()`，不适合 await 热路径。
- `setCurrentTaskCancelSlot()` 同样取当前 context 的 `MetaContext` 并写字段；
  非框架 fiber（无 `MetaContext`）是空操作。
- 使用 per-fiber 的 `MetaContext`，避免 `thread_local` 在同一 OS 线程的多个协程
  之间串味。

### 4.3 `SharedState` 与 `FiberTask::cancel()`

`SharedState` 增加：

```cpp
detail::TaskCancelSlot cancel_slot;

void cancel() noexcept {
    is_runnable.store(false, std::memory_order_release);
    cancel_slot.cancel(std::make_error_code(std::errc::interrupted));
}
```

`FiberTask<T>` / `FiberTask<void>` 的 `cancel()` 改为：

```cpp
void cancel() {
    if (state_ptr_) {
        state_ptr_->cancel();
    }
}
```

- `is_runnable` 仍供 `then` 后继启动前检查。
- 取消链上任一 handle 都调用同一个 `SharedState::cancel()`。
- `FiberTask` 默认构造的空句柄仍是空操作。

### 4.4 `makeTask` / `then` 设置当前取消槽

在调用用户函数前，用 `TaskCancelScope` 把当前 fiber 的 `MetaContext`
指向 `SharedState::cancel_slot`；函数返回/抛出后恢复：

- `makeTask` 的首个节点：
  ```cpp
  try {
      detail::TaskCancelScope cancel_scope(&state_ptr->cancel_slot);
      if constexpr (std::is_void_v<ReturnType>) {
          func();
          p_ptr->set_value(Result<ReturnType>());
      } else {
          ReturnType val = func();
          p_ptr->set_value(Result<ReturnType>(val));
      }
  } catch (...) {
      p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
  }
  state_ptr->done_nodes();
  ```
- `then` 的后继节点：
  - 先 `f_ptr->get()` 等待前驱 future（不在取消槽范围内）；
  - `val.has_value() && is_runnable` 通过后，在 `TaskCancelScope` 内调用 `func`；
  - 任何异常仍由既有 `catch(...)` 转成 `interrupted`；
  - `done_nodes()` 在 scope 结束后执行，因此 `on_finally` 回调不受该任务的取消槽影响。

### 4.5 `Awaitable` 接入

`Awaitable<T>` / `Awaitable<void>` 各增加私有方法：

```cpp
void closeConsumerQueue(std::error_code ec) noexcept {
    if (queue_) queue_->close(ec);   // 只关本句柄的消费者队列
}

static void closeConsumerQueueThunk(void* obj, std::error_code ec) noexcept {
    static_cast<Awaitable*>(obj)->closeConsumerQueue(ec);
}
```

`await()`：

```cpp
if (!queue_) return no_message;

auto* cancel = detail::currentTaskCancelSlot();
if (cancel && cancel->isCancelled()) {
    return std::make_error_code(std::errc::interrupted);
}

detail::AwaitableCancelNode cancel_node{this, &Awaitable::closeConsumerQueueThunk};
const bool tracked = cancel && cancel->add(&cancel_node);
if (cancel && !tracked) {
    return std::make_error_code(std::errc::interrupted);
}

T value{};
const bool state = (hub_ && hub_->isState());
auto status = state ? queue_->wait_peek(value) : queue_->pop(value);
if (tracked) cancel->remove(&cancel_node);

if (status == boost::fibers::channel_op_status::success) {
    if (state) boost::this_fiber::yield();
    return value;
}

const std::error_code ec = queue_->close_error();
if (hub_) hub_->notifyClosed(ec);   // 锁外；仅最后一个消费者时真正关 hub
return ec;
```

`await_for()`：

- 登记/注销逻辑相同；
- 成功取到值 → 返回成功；
- `timeout` 后：
  - 若 `cancel && cancel->isCancelled()` → 若句柄已被取消关闭则锁外
    `notifyClosed(queue_->close_error())`，返回 `interrupted`；
  - 否则返回 `timed_out`；
- 其它 `closed` → 锁外 `notifyClosed(ec)` 后返回 `ec`。

`close()`、`close(error)`、`shared()`、`setOnClose()`、`FiberChannel` 均不修改。

### 4.6 `FiberChannel`（零修改）

- 现有 `close(error)` 已满足“只关本句柄消费者队列”。
- 不增加取消参数、不增加唤醒接口。
- 等待谓词、容量、状态模式行为保持现状。

## 5. 取消语义与竞态

| 场景 | 行为 |
|---|---|
| cancel 时任务正阻塞在 await | 关闭当前句柄消费者队列；await 返回 `interrupted` |
| cancel 后任务再次 await/await_for | 立即 `interrupted`，不挂起、不关闭新句柄 |
| cancel 发生在 await 已成功取到值之后 | 本次返回值照常；后续 await 立即 `interrupted` |
| cancel 与 await 成功同时 | 若 pop 已成功，返回值；否则 `interrupted` |
| cancel 发生在任务未处于 await 时 | 当前代码继续；下一次 await 立即 `interrupted` |
| cancel 发生在任务启动前 | 首个节点仍开始执行；第一次 await 时立即 `interrupted`；若函数从不 await，则继续运行 |
| await_for 超时与 cancel 同时 | cancel 优先：返回 `interrupted`，不是 `timed_out` |
| 任务已结束后再 cancel | 无活动 await；仅置位取消标志，无外部副作用 |
| await 已被其他路径 close（用户 close / hub 关闭） | 首次关闭错误优先；可能不是 `interrupted`，但后续 await 仍返回 `interrupted` |
| 同一个 Awaitable 句柄被多个等待者使用 | 该句柄队列关闭，所有等待者都收到 `interrupted` |
| 同一 hub 的不同 shared() 句柄 | 只影响被登记的那一个；其他句柄照常工作 |
| 当前句柄是 hub 最后一个未关闭消费者 | 被唤醒的 await 在锁外 `notifyClosed`，hub 关闭并执行一次清理 |
| cancel 重复调用 | `cancelled_` 幂等；`active_` 置空后不再重复关闭 |
| 后继节点等待前驱 future（`then` 内的 `get()`） | 不属于 Awaitable 等待，cancel 不会立刻唤醒；前驱结束后按 `is_runnable` 短路为 `interrupted` |
| `on_finally` 回调内部 await | 不在取消槽范围内，不受本次 cancel 影响 |

## 6. 并发与生命周期

- `TaskCancelSlot` 的 `mtx_` 串行化 `add` / `remove` / `cancel`；
  `cancelled_` 是原子快路径。
- `cancel()` 持槽锁调用 `AwaitableCloseNode::close`；close 只做
  `FiberChannel::close()`，`noexcept`、不跑用户清理。等待者醒来后 `remove()`
  需要同一把锁，因此节点在 cancel 遍历期间不会被销毁。
- `hub_->notifyClosed()` 在 `remove()` 之后、槽锁之外由等待方调用；用户清理
  （如 `QObject::disconnect`）不会持槽锁执行，也不会与 cancel 自锁。
- `AwaitableCancelNode` 在 `await()` 栈上；没有堆分配。
- `MetaContext` 是 per-fiber 的，取消槽指针随 fiber 上下文走；同一 OS 线程上
  的其它协程不会看到它。
- `SharedState` 被任务链上的 fiber 以 `shared_ptr` 持有，寿命覆盖整个等待。
- `cancel()` 可从任意线程调用；它最终落到 `FiberChannel::close`，与现有
  `Awaitable::close()` 依赖同一条跨线程路径。若调用线程完全没有 boost.fiber
  调度器，仍受 `FiberChannel` 既有跨线程限制约束（与本次改动无关）。
- `currentTaskCancelSlot()` 用 `dynamic_cast` 检查 `MetaContext`；非框架 fiber
  返回 null。它在 await 热路径上调用；若性能测试退化，再考虑在调度器
  `pick_next()` 缓存当前 `MetaContext*`（本次不做）。

## 7. 测试计划

### 7.1 新增

1. `cancel_aborts_current_await`
   - 任务（sticky/Shared，跑在池 worker）等待一个永不 resolve 的 Awaitable；
   - 主线程调用 `task.cancel()`；
   - 断言 await 返回 `interrupted`，任务在超时内完成。
2. `cancel_makes_following_awaits_immediate`
   - 任务第一次 await 被 cancel 唤醒后，再 await 另一个永不 resolve 的句柄；
   - 第二次 await 立即返回 `interrupted`，不关闭第二个句柄。
3. `cancel_await_for_returns_interrupted`
   - 任务 `await_for(h, 5s)`；cancel 后返回 `interrupted`，耗时远小于 5s。
4. `cancel_keeps_sibling_shared_handle`
   - source 生成 h1、h2；两个任务分别等待；
   - cancel 等待 h1 的任务：h1 的 await 返回 `interrupted`，h2 任务不受影响；
   - `source.resolve(7)` 后 h2 任务拿到 7。
5. `cancel_before_await_is_immediate`
   - 任务先等一个普通原子闸门；cancel 后放行闸门，第一次 await 立即
     `interrupted`。
6. `cancel_completed_task_is_noop`
   - 任务正常完成后 cancel；`get()` 结果不变，无崩溃。
7. `cancel_closes_last_consumer_hub`
   - 被取消的句柄挂 `setOnClose`，且是唯一消费者；
   - cancel 后断言清理执行一次、hub 已关闭。
8. `cancel_from_other_thread`
   - 从 `std::thread` 调用 `cancel()`，验证跨线程唤醒安全。

### 7.2 回归

- 既有 `then` 后继短路、`cancel()` 语义、`testfibertask`、`testfiberawait`、
  `testexecutor`、`testquit`、`test_scheduler` 全绿。
- ASan 下无 UAF/泄漏。
- `testlatency` / `testProfile` 记录 p50/p99；`dynamic_cast` 与槽锁不得造成
  明显退化，若有回退再考虑在 `pick_next()` 缓存 `MetaContext*`。

### 7.3 验收标准

- 取消时当前 await 在有限时间内返回 `interrupted`，不再挂起。
- 取消后同一任务后续 await/await_for 立即返回 `interrupted`。
- 只关闭当前句柄消费者队列；同 hub 其他 `shared()` 句柄不受影响。
- 无新公共 API；`FiberChannel` 零修改。
- 取消是协作式的：不抛异常、不强制 unwind、不覆盖任务最终返回值。
- 现有测试与新增测试全部通过。

## 8. 文件清单

### 新增

- `coro/detail/taskcancel.hpp`
- `coro/detail/taskcancel.cpp`
- `docs/superpowers/specs/2026-10-27-task-cancel-awaitable-design.md`

### 修改

- `coro/executor/scheduler/fiberproperty.h`：`MetaContext` 增加取消槽字段与访存器。
- `coro/task/fibertask.h`：`SharedState::cancel_slot` / `cancel()`；`makeTask` / `then`
  设置 `TaskCancelScope`；`FiberTask::cancel()` 调用 `SharedState::cancel()`。
- `coro/await/awaitable.hpp`：私有 `closeConsumerQueue` / thunk；`await()` /
  `await_for()` 登记、注销与取消检查。
- `AsyncTask.pri`：加入 `taskcancel.hpp/.cpp`。
- `test/testfibertask/tst_test_fiber_task.cpp`：新增取消用例。
- `test/testfiberawait/tst_testfiberawait.cpp`：新增 per-handle 隔离用例（如放在此处更合适）。
- `ReadMe.md`、`doc/使用说明.md`、`doc/架构设计.md`、`doc/需求文档.md`、
  `doc/需求规格说明.md`、`doc/软件设计说明.md`、`skill/using-asynctask/SKILL.md`：
  同步取消语义。

## 9. 风险与限制

- **协作式**：取消只保证在 await 点提前返回；不打断 CPU 计算，也不强制用户
  `return`。用户忽略 `interrupted` 时任务继续运行。
- **不覆盖的阻塞**：`FiberTask::get()`、`msleep`、直接使用 boost 同步原语的等待
  不会被取消唤醒。
- **句柄级影响**：同一个 `Awaitable` 句柄被多个任务/等待者共享时，关闭该句柄会
  唤醒所有等待者；需要按任务隔离时应使用彼此独立的 `shared()` 句柄。
- **结果不覆盖**：取消时任务最终返回值仍由用户函数决定；框架不强制把 `get()`
  结果改成 `interrupted`。
- **热路径成本**：每次 await 多一次 `dynamic_cast` 和（在任务内）一次槽锁
  add/remove；无堆分配。实测退化明显再优化。
- **单活动 await 不变量**：一个任务 fiber 同一时刻只能有一个活动 await；
  `add()` 用断言守护。若未来支持同一任务并发多等待，需要改为列表/计数。
- **普通线程调用**：从没有 boost.fiber 调度器的普通 `std::thread` 调 `cancel()`
  会走到 `FiberChannel::close`，受既有跨线程实现约束；从协程/Qt 线程调用是主路径。
- **`on_finally` 不参与取消**：清理回调在取消槽恢复后执行；其中的 await 只受
  线程退出策略（本次未实现）影响。
- **首个节点启动**：cancel 早于首个节点启动时，首个节点仍会开始执行，直到第一次
  await 才立即返回 `interrupted`；若函数从不 await，则继续运行。

## 10. 开放问题

无。已确认：

- 关闭单位：当前 Awaitable 句柄的消费者队列；
- 当前 await 返回 `interrupted`；
- 后续所有 await/await_for 立即返回 `interrupted`；
- 不抛取消异常、不强制结束 task、不覆盖最终结果；
- `FiberChannel` 零修改。
