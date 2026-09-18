# Qt 事件循环与 Fiber 调度器的按需耦合

对应 issue [#6](https://github.com/Ste7an-cs/AsyncTask/issues/6)。

## 1. 问题

`QtFiberScheduler::suspend_until` 启动的 Qt 事件泵每轮 `processEvents()` 之后固定睡 1ms
（`coro/executor/scheduler/qtfiberscheduler.cpp:22-23`，间隔常量在 `qtfiberscheduler.h:41`）。
这给一切经 Qt 事件分发的 I/O 压出一条硬地板：下游 `Transport` 实测 TCP 回环请求-响应
往返 mean 3488µs / min 3128µs，而绕开本泵的 Fast DDS 是 300µs。payload 从 16B 涨到
1024B 几乎不影响（3488 → 3823µs），说明是**每跳固定延迟**而非带宽问题。

min 落在 3128µs 而不是随机分布，是因为一次往返要串行穿过 3~4 次「等下一轮泵」：
客户端 write 等写通知器刷出、服务端读就绪、服务端 write 刷出、客户端读就绪。每跳都
要等满一个完整间隔，所以是地板不是抖动。

把间隔改成 0 能把 min 拉到 583µs，但泵协程变忙转，CPU 从 2.4s 涨到 49.4s（同样 10.5s
墙钟，约 20 倍），不是可行解。

### 1.1 还有两条 issue 未提到的地板

**主线程上旋钮已经换了地方。** issue 基于 `6f42255`，但 `feat/gui-quit-native-eventloop`
分支的 `Coro::exec()` 已不走泵协程（`coro/task/fiberapplication.cpp:88` 显式
`stopCurrentThreadPump()`），改为 `aboutToBlock` 钩子里 `Coro::msleep(kFiberSliceMs)` +
`wakeUp()`（`fiberapplication.cpp:74-84`，常量在 `fiberapplication.h:86-88`）。形状变了，
1ms 地板没变，而且 `static constexpr` 比 `pump_interval_ms_` 更不可配。

**跨线程唤醒是轮询的。** `FiberScheduler::suspend_until` 在 `time_point::max()` 时只等
200µs（`fiberscheduler.cpp:150`），因为 `awakened()` 把非 pinned 协程 `detach()` 丢进
`FiberGlobalQueue` 后**没有任何机制通知目标线程**（`fiberscheduler.cpp:56-61`）。这是
「把间隔改 0 之后 mean 仍有 ~2000µs、抖动反而变大」的第二因素。

## 2. 目标与非目标

**目标**

- 消除 Qt 系 I/O 的固定延迟地板：fd 就绪到协程被唤醒之间不再有任何固定睡眠
- 空闲时线程真正睡在 `poll()` 里，CPU 归零（而非 1ms 轮询或忙转）
- 保持「Qt 槽运行在协程栈上」的现有语义，槽内 `Coro::await` 继续可用
- 保持 GUI 关窗退出语义（上一个 commit 引入的 `QCoreApplication::exec()` 原生退出路径）
- 不引入需要调用方调参才能达到合理性能的旋钮

**非目标**

- 不修复 `FiberGlobalQueue` 的亲和错配语义（协程迁移到其他线程时可能进不了对应线程的
  就绪队列）。本次只解决「线程叫不醒」，不碰「进错队列」。
- 不做 Qt 事件与协程的统一调度。评估过：需要自研 `QAbstractEventDispatcher` 让 fiber
  调度器成为唯一阻塞点，把 Qt 的 fd/timer 并入同一个 `poll()`。时延上并不优于本方案
  （两者都已达系统调用量级），收益是架构收敛与共同的优先级标尺，但代价是重新实现 Qt
  定时器语义 + 平台矩阵（Linux/Windows/macOS 各一套），**代价过大，本次不做**。

## 3. 核心机制：把「该等多久」从 boost.fiber 交棒给 Qt

一个线程只能有一个阻塞点。本方案选择**让 Qt 阻塞在 `poll()`**，由 fiber 调度器告诉它
可以睡多久。

难点在于「最近一个协程截止时刻」只存在于 boost.fiber 的 `sleep_queue_` 里，唯一的出口
是 `algorithm::suspend_until(tp)` 回调。但该回调跑在 **dispatcher context** 上，在那里
执行会挂起的用户代码会切栈崩溃（与 `Coro::quit()` 不能在 POSIX 信号处理函数中调用是
同一个坑）。

所以让 `suspend_until` **只交棒、不睡觉**：

```cpp
// QtFiberScheduler 新增
static thread_local QtFiberScheduler*   t_self_;        // 构造时登记
boost::fibers::waker                    qt_waker_{};    // 被挂起的「Qt 持有者」协程
std::atomic_bool                        parked_{false};
std::chrono::steady_clock::time_point   next_deadline_{};

/// 由持有 Qt 事件循环的那个协程调用；挂起自身，返回「可安全阻塞至」的时刻
std::chrono::steady_clock::time_point QtFiberScheduler::parkUntilIdle(){
    auto* self = t_self_;
    auto* ctx  = boost::fibers::context::active();
    self->qt_waker_ = ctx->create_waker();
    self->parked_.store(true, std::memory_order_release);
    ctx->suspend();                         // 本协程不再 ready
    return self->next_deadline_;
}

void QtFiberScheduler::suspend_until(time_point tp) noexcept {
    if(parked_.exchange(false, std::memory_order_acq_rel)){
        next_deadline_ = tp;                // 其余协程的最近截止时刻，无则 max
        qt_waker_.wake();                   // 仅入队，不切栈
        return;                             // 自己不睡
    }
    FiberScheduler::suspend_until(tp);       // 泵未起 / 已退出时退化为基类行为
}
```

**为什么不丢唤醒**：`parked_.store(true)` 与 `ctx->suspend()` 之间没有让出点（同线程、
无抢占），而 `suspend_until` 只在 dispatcher context 上运行，后者只有在本协程挂起之后
才拿得到控制权。

**为什么在 dispatcher context 上调 `wake()` 是安全的**：`waker::wake()` →
`context::wake(epoch)` → `schedule()` → `algorithm::awakened()`，后者只做入队（持
`global_mtx` 这个 `std::mutex`，短暂阻塞线程但不切换协程）。全程无 fiber 切栈。

**为什么不用 `boost::fibers::condition_variable`**：它的 `notify_one()` 内部要获取
fiber mutex，理论上可在 dispatcher context 中挂起调用者 → 偶发崩溃。

### 3.1 依赖的 boost.fiber API

`context::create_waker()` / `waker::wake()` / `context::suspend()` 位于
`boost/fiber/waker.hpp` 与 `boost/fiber/context.hpp` 的 public 段，但不在官方文档中，
属半公开 API。**已知取舍：接受升级 boost 时被改的风险**，换取无锁、不切栈的交棒。
`AsyncTask.pri` 目前锁定 boost 1.89。

## 4. 同一个原语，两处使用

### 4.1 工作线程（`FibersPool` → `QtFiberScheduler`）

泵协程（`qtfiberscheduler.cpp:19-25`）改为：

```cpp
while(!s_exit_ && !t_stop_){
    eventloop.processEvents(QEventLoop::AllEvents);      // 排空；槽在本协程栈上跑
    if(s_exit_ || t_stop_) break;

    auto tp = parkUntilIdle();                           // 交出线程，等交棒
    if(s_exit_ || t_stop_) break;

    // 交棒之后、本协程真正跑起来之前，可能又有协程被远端投递进来
    if(has_ready_fibers()) continue;

    int ms = clampToRange(tp - steady_clock::now(), 0, Coro::maxEventBlockMs());
    if(ms > 0){
        deadline_timer_.start(ms);                       // 单次 QTimer 成员，打断 poll
        eventloop.processEvents(QEventLoop::AllEvents
                              | QEventLoop::WaitForMoreEvents);   // 真阻塞
        deadline_timer_.stop();
    }
}
```

`parkUntilIdle()` 返回后到阻塞前必须重新检查 `has_ready_fibers()`：交棒之后 dispatcher
可能先调度了别的协程，等泵真正跑起来时早已有活可干，此时不能再进 `poll()` 睡。

`deadline_timer_` 是 `QTimer` 成员，`setSingleShot(true)`，不需要接槽 —— 定时器事件本身
就足以让 `processEvents` 返回。它随调度器构造在本线程上，工作线程已有 `QEventLoop`，
事件分发器存在。

### 4.2 主线程（`Coro::exec()`）

主线程不能改用泵协程：`QCoreApplication::exec()` 是 GUI 关窗退出的前提（见
`fiberapplication.cpp:47-59` 的注释 —— 只有它会置位 `in_exec` 并把 `QEventLoop` 压进
`threadData->eventLoops`，缺了这两样 `emitLastWindowClosed()` 根本不发退出事件）。

但跑 `QCoreApplication::exec()` 的 `qt-loop` 协程在 `aboutToBlock` 槽里就是「Qt 持有
者」，机制完全一样。`fiberapplication.cpp:74-84` 替换为：

```cpp
block_conn_ = QObject::connect(disp, &QAbstractEventDispatcher::aboutToBlock, this,
                               [this, disp]{
    auto tp = QtFiberScheduler::parkUntilIdle();     // 协程跑够了才回来
    int ms = clampToRange(tp - steady_clock::now(), 0, Coro::maxEventBlockMs());
    if(ms > 0) deadline_timer_.start(ms);            // 让随后的 poll() 至多睡 ms
    else       disp->wakeUp();                       // 立刻回来继续跑协程
});
```

`deadline_timer_` 是 `FiberApplication` 新增的 `QTimer` 成员（单次），与 4.1 中泵协程
用的是同一种手法。

`kEventDrainBudget`（64 轮）与 `kFiberSliceMs`（1ms）**删除**。`parkUntilIdle()` 天然
实现了「协程要跑多久就跑多久，跑完才让 Qt 阻塞」，两个启发式常量失去意义。

**已知限制（与现状相同，不在本次修复范围）**：`aboutToBlock` 只在 Qt 准备阻塞时发出。
持续的 Qt 事件风暴会让 `canWait` 一直为假，钩子不触发，协程被饿死。现有实现有同样的
问题，本方案不改变它。

## 5. 唤醒路径

泵一旦可以长时间阻塞在 `poll()` 里，所有让它醒来的路径都必须显式建立，缺一条就是挂死
而不是变慢。

建立一张唤醒登记表。为了不让 Qt 侵入非 Qt 的 `FiberScheduler` 基类，表里存的是回调而
非 `QAbstractEventDispatcher*`：

```cpp
// fiberscheduler.h —— 基类只认回调，不认 Qt
class FiberScheduler {
    static void registerWaker(void* key, std::function<void()> wake);
    static void unregisterWaker(void* key);
    static void wakeAllBlocked();                    // 仅在 s_blocked_count_ > 0 时遍历
    static std::atomic_int s_blocked_count_;         // 睡在 poll() 里的线程数
};
```

`QtFiberScheduler` 构造时登记 `[disp]{ disp->wakeUp(); }`（`wakeUp()` 是 Qt 明确保证
线程安全的少数函数之一），析构时注销。泵进入阻塞前 `++s_blocked_count_`，返回后 `--`。

| 触发点 | 动作 |
| --- | --- |
| `FiberScheduler::awakened()` 跨线程入队 | `wakeAllBlocked()` |
| `FiberScheduler::notify()` | `base::notify()` + 唤醒本线程 |
| `signalExit()` / `stopCurrentThreadPump()` | `wakeAllBlocked()`，否则关机卡在 poll |

### 5.1 广播必须限流

无条件广播会造成惊群：每次协程就绪都把 N 个工作线程全叫醒一次，正是 issue 抱怨的那种
CPU 浪费。两道闸：

1. `s_blocked_count_ == 0` 时直接返回，不遍历、不调 `wakeUp()`
2. 入队的协程亲和是 `Affinity::fixed(本线程)` 时跳过广播 —— 没有别的线程需要它

广播点在 `FiberScheduler::awakened()`（`fiberscheduler.cpp:46-62`）**释放 `global_mtx`
之后**，不能塞进 `FiberGlobalQueue::emplace_back()`：持着全局锁回调进 Qt 是锁序风险，
而且全局队列应保持为纯容器。需要把现有那个覆盖到函数末尾的 `lock_guard` 收进作用域。

### 5.2 安全封顶

`maxBlockMs()` 默认 10ms，作为广播若有遗漏时的兜底 —— 让症状退化成「慢」而不是「死」。
`pump_interval_ms_` 删除（不再有固定间隔），取而代之暴露：

```cpp
namespace Coro {
    void setMaxEventBlockMs(int ms);   // 默认 10
    int  maxEventBlockMs();
}
```

这同时满足 issue 的「优先级 1 —— 变成可配」，但语义不同：它不是分发间隔（那个已经没
有了），而是唤醒广播的失效保险。

## 6. 退出路径

阻塞语义变了，关机是最可能挂死的地方，逐条确认：

- `signalExit()` 置位 `s_exit_` 后调 `wakeAllBlocked()` → 各线程 `poll()` 返回 → 泵在
  循环顶部看到标志退出
- 泵处于 parked 状态时：线程本就空闲，dispatcher 会再次调 `suspend_until` → 交棒 → 泵
  醒来 → 看到标志退出
- `FiberApplication::shutdown()` 先 `disconnect(block_conn_)`，收尾期间不再 park
- `drainUntilIdle()`（`fiberapplication.cpp:23-33`）不受影响，它自己驱动 `processEvents`
  + `yield`

## 7. 净效果

`pump_interval_ms_` 不是变可配，而是**消失**。Qt fd 就绪 → `poll()` 立即返回 → 槽 push
channel → 泵 park → 等待协程被调度，全程无固定睡眠。空闲时线程真正睡在 `poll()` 里。

issue 的优先级 1 被优先级 2 吸收。

## 8. 验证

**基准（可复现，不进 CI 断言）**
新增 `example/latency_pingpong/`：单进程内两个 `QTcpSocket` 回环 ping-pong，
payload 16B，300 样本，输出 min / mean / stdev 与进程 CPU 时间。用于与 issue 中
`Transport` 的口径对照。

**回归断言**
新增 `test/testlatency/`：loopback 请求-响应往返 `min < 1ms`。阈值宽松（当前是 3128µs），
只为守住「地板回归」，不追求精确。时间敏感，标注为可能在极端负载的 CI 上不稳。

**现有测试全绿**
`test/testquit`、`test/testfiberawait`、`test/testfibertask`、`test/testProfile`、
`test/testexecutor`、`test/test_scheduler`。重点在 `testquit` —— 阻塞语义变了，关机路径
风险最高。

**手工验证**
`example/gui_quit` 关窗退出；`example/socket_pingpong`、`example/signal_quit`、
`example/thread_init` 正常跑完退出。

**环境注意**
跑测试前 `unset all_proxy`（SOCKS5 代理会让 Qt 的 bind 失败，导致 UDP 用例假阳性）。

## 9. 涉及文件

| 文件 | 改动 |
| --- | --- |
| `coro/executor/scheduler/qtfiberscheduler.h/.cpp` | 交棒原语 `parkUntilIdle()`、`suspend_until` 覆写、`notify` 覆写、泵循环重写、`deadline_timer_`、删 `pump_interval_ms_` |
| `coro/executor/scheduler/fiberscheduler.h/.cpp` | 唤醒登记表、`s_blocked_count_`、`awakened()` 收窄锁作用域并广播、`signalExit`/`stopCurrentThreadPump` 广播 |
| `coro/task/fiberapplication.h/.cpp` | `aboutToBlock` 钩子改为 park/arm，新增 `deadline_timer_` 成员，删 `kEventDrainBudget`、`kFiberSliceMs` |
| `coro/detail/asyncdefine.h/.cpp` | `setMaxEventBlockMs()` / `maxEventBlockMs()`，转发到 `QtFiberScheduler` 的静态值 |
| `example/latency_pingpong/` | 新增基准 |
| `test/testlatency/` | 新增回归断言 |

## 10. 已知风险

1. **半公开 boost API**：`context::create_waker()` / `waker::wake()` / `context::suspend()`
   不在 boost.fiber 官方文档中。升级 boost 需重新验证。已接受。
2. **`maxBlockMs` 默认值**：10ms 是保险丝而非性能参数。设大更省电，但广播遗漏时症状更
   隐蔽。取 10ms 偏保守。
3. **事件风暴下协程饥饿**：见 4.2，与现状相同，不在本次范围。
4. **`test/testlatency` 时间敏感**：CI 负载高时可能偶发失败。
