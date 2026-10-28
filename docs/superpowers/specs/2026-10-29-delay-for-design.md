# Coro::delay_for 设计

- 日期：2026-10-29
- 状态：待评审
- 范围：基于 `Awaitable<void>::await_for` 的可取消协程延时，替代 fiber 版 `sleep`/`msleep`
- 相关：`coro/await/awaitable.hpp`、`coro/detail/asyncdefine.h`、`docs/superpowers/specs/2026-10-27-task-cancel-awaitable-design.md`

## 1. 背景与目标

`Coro::sleep()` / `Coro::msleep()` 直接调用 `boost::this_fiber::sleep_for`，协程睡在 boost.fiber 的 sleep queue 里：

- 不能被 `FiberTask::cancel()` 唤醒；
- 任务已经取消，仍要睡满剩余时间；
- 长延时会把任务和线程的收尾拖住。

目标：提供一个**协程可等待、可被 task.cancel 提前中止**的延时接口，正常到期返回 `Result<void>` 成功，取消返回 `std::errc::interrupted`。不引入定时线程、不依赖 Qt、不改 `FiberChannel`/`Awaitable`。

## 2. 命名

公开入口：**`Coro::delay_for`**。

- `delay` 表达“延时/休眠”语义；
- 与现有 fiber 版 `Coro::sleep()` / `Coro::msleep()` 明确区分；
- 不引入公开的 `DelayAwaitable`/`CoroSleep` 类：内部只复用 `Awaitable<void>`，没有需要暴露的新类型。

不采用 `sleep_for`（与 fiber sleep 概念过近）、`co_sleep_for`（与 C++20 `co_await` 易混淆）。

## 3. API

新增 Qt 无关的 header-only 头文件：

```cpp
// coro/await/delay.hpp
namespace Coro {

template<class Rep, class Period>
Result<void> delay_for(const std::chrono::duration<Rep, Period>& duration);

}
```

用法：

```cpp
using namespace std::chrono_literals;

auto r = Coro::delay_for(100ms);
if(!r && r.error() == std::make_error_code(std::errc::interrupted)){
    // 任务被 cancel
}
```

## 4. 语义

| 场景 | 返回值 / 行为 |
|---|---|
| 正常到期 | `Result<void>` 成功；不暴露 `timed_out` |
| `FiberTask::cancel()` 中止当前 await | `std::errc::interrupted`，提前返回 |
| `duration <= 0` | 不挂起，立即成功 |
| 未在 task 内 / 没有 task cancel | 只能等正常到期；不再响应其它外部信号 |
| `Coro::quit()` / 线程结束 | **不会**提前唤醒；仍会等满 duration（本设计不覆盖，见 §7） |

要点：`delay_for` 只把 `await_for` 的 **`timed_out` 视为成功**；其它错误原样返回。`interrupted` 是唯一的预期提前终止原因。

## 5. 实现

```cpp
// coro/await/delay.hpp
#include <chrono>
#include <system_error>
#include "awaitable.hpp"

namespace Coro {

template<class Rep, class Period>
Result<void> delay_for(const std::chrono::duration<Rep, Period>& duration)
{
    using duration_type = std::chrono::duration<Rep, Period>;
    if(duration <= duration_type::zero()){
        return Result<void>();
    }

    Awaitable<void> a;   // 没有生产者：只等超时或被 task cancel 关闭
    auto r = a.await_for(duration);
    if(r){
        return r;
    }
    if(r.error() == std::make_error_code(std::errc::timed_out)){
        return Result<void>();
    }
    return r;            // interrupted / 其它错误
}

}
```

- `Awaitable<void>` 的默认构造会创建 hub + 本句柄队列；`await_for` 把它登记到当前任务的取消槽（若在 task 内）。
- 正常超时：`await_for` 返回 `timed_out`，映射为成功；局部 Awaitable 随后析构，队列从 hub 摘除。
- 任务 cancel：`FiberTask::cancel()` 关闭当前句柄的消费者队列，`await_for` 返回 `interrupted`，直接传播。
- `duration <= 0`：不创建等待，立即成功；不主动 `yield`。
- 不修改 `FiberChannel`、`ChannelHub`、`await_for` 或取消槽。

## 6. 文件与测试

### 新增/修改

- 新增：`coro/await/delay.hpp`
- 修改：`coro/all.hpp`（await 层引入）
- 修改：`AsyncTask.pri`（HEADERS）
- 测试：`test/testfibertask/tst_test_fiber_task.cpp`
- 文档：`ReadMe.md`、`doc/使用说明.md`、`doc/架构设计.md`、`doc/软件设计说明.md`、`doc/需求文档.md`、`doc/需求规格说明.md`、`skill/using-asynctask/SKILL.md`

### 测试

1. **正常到期**
   - sticky task 调用 `delay_for(50ms)`；
   - 结果成功；耗时不少于约 30ms、且小于 1s。
2. **任务 cancel 提前中止**
   - sticky task 调用 `delay_for(300ms)`，记录 started；
   - 主线程等待片刻后 `task.cancel()`；
   - 结果错误为 `interrupted`，done 在 2s 内（实际应远小于 300ms）；
   - 若取消未生效，任务最多在 300ms 后完成并以成功返回，测试失败但不会挂死。
3. **零时长**
   - task 调用 `delay_for(0ms)`，立即成功，不挂起。

测试用现有 `interruptedError()` / `waitForDone()` 风格；使用 WSL 本地 wrapper（Qt 5.12.8、boost 1.83、ASan），最终 boost 1.89 验收后补。

## 7. 风险与限制

- **只响应 task.cancel**：`Coro::quit()`、线程退出不会提前关闭 delay 的局部 Awaitable；应用关机时若存在长 delay，`drainUntilIdle()` 不会被它唤醒（按已确认的范围）。
- 每次 delay 创建一条 hub + 队列（`Awaitable<void>` 默认构造）；没有额外线程或定时器。高频短延时会有分配开销，必要时后续再做池化/专用定时服务。
- 正常到期依赖 `boost::fibers::condition_variable::wait_for` 的超时调度；线程被严重积压时实际耗时会变大，与 fiber sleep 的时基特性一致。
- `duration` 使用 `std::chrono::duration` 模板；不提供裸 `unsigned long` 重载，避免与 `msleep` 的单位混淆。
- 在非 task 上下文（裸 fiber/无取消槽）调用时，只能等正常到期；这是 `await_for` 的既有行为。
- `timed_out` 不再暴露给调用方；`delay_for` 的“超时”就是正常完成，唯一预期的失败是 `interrupted`（或底层意外错误）。
- 文档需明确：`delay_for` 不替代 `Coro::sleep` 的 boost sleep queue 语义，只提供可取消的协程延时。

## 8. 验收标准

- `delay_for(50ms)` 返回成功，确实等待约 50ms；
- 任务 cancel 时 `delay_for(300ms)` 返回 `interrupted` 并提前完成；
- `delay_for(0ms)` 立即成功；
- 不新增定时线程，不引入 Qt 依赖，`FiberChannel`/`ChannelHub`/`await_for` 零修改；
- 新测试在本地 WSL ASan wrapper 通过，现有 `testfibertask` 全量保持通过；
- 文档与 `all.hpp`/`AsyncTask.pri` 同步。
