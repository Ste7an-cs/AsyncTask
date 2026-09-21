#ifndef QTFIBERSCHEDULER_H
#define QTFIBERSCHEDULER_H

#include "fiberscheduler.h"
#include <QEventLoop>
#include <QTimer>
#include <QAbstractEventDispatcher>
#include <boost/fiber/waker.hpp>
#include <atomic>
#include <chrono>
#include <mutex>

namespace Coro {

/**
 * @brief 支持 Qt 事件循环的调度器。
 *
 * 首次 suspend_until 用 std::call_once 创建一个绑定当前线程的常驻“事件泵协程”。
 * 泵协程排空 Qt 事件后调 parkUntilIdle() 交出线程；调度器无就绪协程时通过
 * suspend_until 把最近的协程截止时刻交棒回来，泵据此阻塞在 Qt 的 poll() 上。
 * 没有固定分发间隔：fd 就绪即刻返回，空闲时线程真正休眠。
 *
 * 本泵有意不参与跨线程唤醒广播（原委与代价见 pumpLoop() 的 @warning）：
 * Coro::maxEventBlockMs()（默认 10ms）既是新协程投递到本线程的拾取延迟上界，
 * 也是退出流程的兜底 —— Coro::quit() 只设全局退出标志，真正让睡着的泵醒来发现
 * 该标志的是 pumpLoop() 里的 deadline_timer_ 这道保险丝，而不是唤醒广播。
 *
 * Qt 槽运行在泵协程的栈上，因此槽内可以 Coro::await()。
 * @code
 * // 工作线程上安装本调度器：既能调度协程，也能分发 Qt 事件
 * // （因此 QTimer / socket 等 Qt 对象在工作线程上也可正常工作）
 * boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
 * @endcode
 */
class QtFiberScheduler : public FiberScheduler
{
public:
    QtFiberScheduler(void);
    ~QtFiberScheduler(void) override;

    /**
     * @brief 无就绪协程时：首次创建常驻事件协程（call_once），随后委托基类阻塞
     * @code
     * // 由 boost.fiber 调度器在无就绪协程时自动回调，使用方不直接调用。
     * // 首次进入创建常驻泵协程，之后委托基类做真正的 cv 阻塞（空闲不空转）
     * @endcode
     */
    void suspend_until(std::chrono::steady_clock::time_point const& time_point) noexcept override;

    /**
     * @brief 挂起调用协程，直到本线程再无就绪协程；返回可安全阻塞至的时刻
     * @details 由**持有 Qt 事件循环的那个协程**调用：工作线程是泵协程，主线程是
     *          Coro::exec() 的 qt-loop 协程在 aboutToBlock 中。挂起之后调度器才
     *          会回调 suspend_until，那里是唯一能拿到「最近一个协程截止时刻」的
     *          地方，它把该时刻交棒回来并唤醒本协程。
     * @code
     * auto tp = Coro::QtFiberScheduler::parkUntilIdle();
     * int ms = 可阻塞毫秒数(tp);
     * if(ms > 0) eventloop.processEvents(QEventLoop::AllEvents
     *                                  | QEventLoop::WaitForMoreEvents);
     * @endcode
     * @return 四种出口：<br>
     *         1. 正常交棒 —— suspend_until 交回来的「最近一个协程截止时刻」；
     *            本线程无任何定时协程时 boost.fiber 给的就是 time_point::max()；<br>
     *         2. 本线程没装 QtFiberScheduler（t_self_ 为空）—— 不挂起，直接返回
     *            now()，调用方据此算出的可阻塞时长为 0，退化为不阻塞的轮询；<br>
     *         3. 挂起期间被 unparkLocal() 放出（本线程正在交出 Qt 持有权）——
     *            返回 now()，同样表示「立刻返回，别再阻塞」；<br>
     *         4. 本线程已有协程持有 Qt 事件循环（重复 park）—— 拒绝挂起，
     *            qWarning 提示后同样返回 now()。
     */
    static std::chrono::steady_clock::time_point parkUntilIdle(void);

    /**
     * @brief 唤醒挂起的调度器线程，并戳破本线程可能正在进行的 poll()
     * @details 可能被 boost.fiber 从任意线程回调（远端就绪走 notify），因此戳
     *          分发器这一步必须走 wakeDispatcher() 的加锁路径。
     */
    void notify(void) noexcept override;

    /**
     * @brief 解除本线程 Qt 持有者协程的挂起（交出 Qt 持有权前调用）
     * @details Coro::exec() 会先 stopCurrentThreadPump() 停掉泵协程、再让
     *          qt-loop 协程接手。若此刻泵正挂在 parkUntilIdle() 上，不先把它放
     *          出来，Qt 持有者这个唯一席位就会被一个永远不会醒的协程占死。
     */
    static void unparkLocal(void);

    /**
     * @brief Qt 持有者被扣留的上限（毫秒）：饥饿守卫的周期
     * @details 见 releaseParkedIfOverdue() 的长注释。
     */
    static constexpr int kMaxParkMs = 100;

    /**
     * @brief 与本线程的事件分发器解绑（线程收尾时经基类钩子调用）
     * @details 注入 FiberScheduler::local_detach_hook_，由
     *          FiberThreadBlock::wait() 在停泵并让出之后调用。靠 thread_local
     *          t_self_ 找到本线程的调度器实例；本线程没装本调度器时是空操作。
     * @warning 调用之后本线程不再登记唤醒回调，也不再持有分发器指针 ——
     *          远端唤醒对它彻底失效，只能用在线程即将返回的最后一步。
     */
    static void detachLocal(void);

protected:
    QEventLoop eventloop;                    ///< 本线程 Qt 事件循环
    std::once_flag pump_once_;               ///< 每线程一次

    /**
     * @brief 常驻事件泵协程主体：排空 → 交出线程 → 按需阻塞在 poll()
     * @details 绑定创建它的线程（Fixed 亲和），循环分发 Qt 事件直到全局退出标志
     *          或本线程停止标志置位。没有固定分发间隔：本线程无就绪协程时直接
     *          阻塞在 Qt 的 poll() 上，fd 就绪即返回；阻塞时长取「最近一个协程
     *          截止时刻」与 Coro::maxEventBlockMs() 的较小者。
     *
     * @warning **本泵有意不参与 FiberScheduler 的跨线程唤醒广播**，即不调
     *          enterBlocked() / leaveBlocked()。于是 blockedCount() 恒为 0、
     *          wakeAllBlocked() 每次都在第一道闸返回，整张登记表处于休眠态。
     *          代价要说清楚：**一个睡在 poll() 里的工作线程不会被远端投递的
     *          Shared 协程叫醒，它要睡满 maxEventBlockMs（默认 10ms）才自醒。**
     *          旧泵（processEvents + msleep(1)）对这种活的拾取延迟是 ~1ms，所以
     *          这是一次 ~1ms → ≤10ms 的跨线程拾取回归，换来 2 倍的 CPU 改善
     *          （testProfile：CPU 266.5s→119.8s，墙钟 209.8s→112.0s，
     *          系统时间 63.9s→6.3s）。
     *          **代价的另一半同样要说清楚：关机与「停某一个线程的泵」也一并被推迟
     *          至多 maxEventBlockMs。** signalExit() / stopCurrentThreadPump() 里的
     *          wakeAllBlocked() 现在是空操作，真正让泵醒来发现退出标志的是本函数
     *          里的 deadline_timer_ 保险丝（详见 fiberscheduler.cpp 两处注释）。
     *          注意这个上界没有封顶：setMaxEventBlockMs() 只夹低端（非正值夹到 1），
     *          调大多少，关机就可能慢多少。
     *          急需更低跨线程延迟的场景可用 Coro::setMaxEventBlockMs() 调小；
     *          注意实测中墙钟反而比旧泵更快，那是因为压力下线程几乎不进 poll()，
     *          是本基准的性质，不是普遍保证。
     *
     * @note 为什么不打开广播：打开后实测 CPU 从 136.5s 飙到 1371.7s（testProfile
     *       直接超时失败）。原因是这条路是**电平触发**的 —— 每有一个协程入队就
     *       广播一次，而被戳醒的线程里只有 25.8% 真能找到活干，其余 74.2% 醒来
     *       空跑一轮再睡回去。把 maxEventBlockMs 调到 1ms 当替代也不行（CPU
     *       371.0s，比旧泵还差）。正确解法（边沿触发／wake-one／限流闸）是
     *       **另一个任务**的题目，此处不留开关、不留注释掉的分支。
     *
     * @warning 留给后续任务的**顺序约束**（曾在计划里搞错过一次，必须留存）：
     *          日后让本泵参与广播时，enterBlocked() 必须排在 hasReadyWork()
     *          复查**之前**，且每一条出口（复查命中、ms<=0、poll 返回、break）
     *          都要配对 leaveBlocked()。先复查后计数会开一个丢唤醒的窗口：复查
     *          与入队之间别的线程投递协程并广播，采样到计数为 0 便跳过，这次唤醒
     *          就丢了，只能等保险丝（症状是偶发慢 10ms，极难定位）。而漏掉一次
     *          减计数则更糟 —— 计数永久 >0，第一道闸从此形同虚设，每次广播都变成
     *          N 个线程的惊群。
     */
    void pumpLoop(void);

    /**
     * @brief 饥饿守卫：Qt 持有者被扣留超过 kMaxParkMs 就地放人
     *
     * @details 由本线程调度器的 pick_next() 在每次取协程之前调用。
     *          parkUntilIdle() 的交棒是**按需**的：只有调度器无就绪协程、
     *          boost.fiber 回调 suspend_until() 时，挂起的 Qt 持有者才会被唤醒。
     *          代价是这个唤醒条件依赖「线程变空闲」，而主线程的 pick_next() 会去
     *          全局队列里偷 Shared 协程 —— 只要还有协程排队，主线程就永远不空闲，
     *          Qt 一个事件都不分发。GUI 表现为窗口不重绘、点不动、关不掉，且时长
     *          没有上界。本函数给这个「按需」加一条下限：**持有者最多被扣留
     *          kMaxParkMs（100ms）**。
     *
     * @details 买到什么、付出什么，两边都要说清楚：<br>
     *          - 买到：**上界是 park 被扣留的时长**，不是端到端的分发间隔——
     *            持有者最多被扣留 100ms 就会被本函数放回就绪队列，但它是
     *            Priority::Normal 的 Fixed(本线程)，真正轮到它执行还要排在
     *            同一优先级桶内更靠前的项之后、等 Priority::High 的
     *            Fixed(本线程) 协程排空（fibertaskqueue.h 里的桶是按优先级降序
     *            取的 std::set，不是 FIFO）。实测 104～106ms 只是因为
     *            当时没有更高优先级的协程与它竞争这一桶，不能当成保证。持续积压下
     *            肉眼可见的卡顿，但输入、重绘、关窗都还活着，不是冻死。<br>
     *          - 付出：**只是一道保险丝**。正常路径分毫未动 —— 线程一空闲
     *            suspend_until() 仍然立刻交棒，fd 就绪仍然让 poll() 立刻返回，
     *            Task 5 的时延收益（testlatency 每样本 p50 2112us→150us）不受影响。<br>
     *          - 空闲唤醒频率不变：空闲时 pick_next() 取不到协程，boost.fiber 立刻
     *            回调 suspend_until()，持有者在那里就被交棒放走了，本函数的 100ms
     *            根本轮不到到期。空闲阻塞上限仍由 Coro::maxEventBlockMs() 管，
     *            本函数一次都不会把它缩短。
     *
     * @warning 为什么**不能**写成「一个睡 100ms 的常驻守卫协程」——这是实测结论，
     *          别再走回头路：`Coro::msleep()` 把协程挂进 boost 的 sleep_queue_，
     *          而它只由 scheduler::dispatch() 里的 sleep2ready_() 搬回就绪态；
     *          dispatch() 跑在 dispatcher context 上，那是个 pinned context，被
     *          awakened() 放进 main_queue_，而两份 pick_next() 都把 main_queue_
     *          排在全局队列**之后**。于是积压期间 dispatcher context 本身就被饿死，
     *          没有任何睡着的协程会醒来。实测（testquit 的 starve 档）：主线程
     *          Fixed 协程 msleep(200) 实际睡了 1497ms —— 正好是整个积压窗口。
     *          同理，跨线程唤醒（remote_ready_queue_）也要 dispatcher 才搬得动，
     *          所以守卫也不能靠别的线程把它叫醒。能不被饿死的只有两条：本线程
     *          pick_next() 里同步做掉（即本函数），或由别的线程新建一个
     *          Fixed(本线程) 协程投进全局队列。取前者：不多一条线程、不多一份
     *          收尾逻辑，也就没有「守卫协程拖住 ~scheduler」的风险。
     *
     * @note 覆盖面：本函数由 QtLocalFiberScheduler::pick_next() 调用，覆盖主线程
     *       与 QtFiberThread。线程池工作线程装的是 QtFiberScheduler，走基类
     *       pick_next()，**不在覆盖范围内** —— 它们的事件泵同样会被积压饿死，
     *       只是那里没有窗口可冻。要一并覆盖就得把钩子塞进基类的热路径，代价是
     *       17 条线程每次取协程都多读一次表，故本次不做。
     */
    void releaseParkedIfOverdue(void) noexcept;

    /**
     * @brief 戳醒本线程的 Qt 事件分发器（可跨线程调用）
     * @details 全程持 disp_mtx_：这把锁把「读 disp_ + 调 wakeUp()」和
     *          detachDispatcher() 里的「置空 disp_」串成互斥的两段，否则分发器
     *          在自己线程上被销毁时，别的线程手里的裸指针就是 use-after-free。
     */
    void wakeDispatcher(void) noexcept;
    /**
     * @brief 与本线程的事件分发器解绑，此后不再碰它
     * @details 幂等，三个触发点都在本调度器自己的线程上：QCoreApplication 析构
     *          （主线程分发器是它的子对象）、分发器自身析构、本调度器析构。
     */
    void detachDispatcher(void) noexcept;

    static thread_local QtFiberScheduler* t_self_;///< 本线程的调度器实例
    boost::fibers::waker  qt_waker_{};            ///< 挂起中的 Qt 持有者协程
    std::atomic_bool      parked_{ false };       ///< Qt 持有者是否正挂起
    std::chrono::steady_clock::time_point next_deadline_{};///< 交棒过来的截止时刻
    std::chrono::steady_clock::time_point park_started_{};///< 本次挂起的起点（饥饿守卫计时用）
    /**
     * @brief 正在挂起中的持有者 context（饥饿守卫的 TOCTOU 防护用）
     * @details boost::fibers::scheduler::suspend() 对 pick_next() 的调用发生在
     *          挂起协程自己的栈上、**栈切换之前**——那一刻 context::active() 还是
     *          这个即将挂起的协程本身。releaseParkedIfOverdue() 据此判断：若
     *          parking_ctx_ 等于当前 active()，说明挂起尚未真正完成，本轮先不
     *          放人，交给下一次 pick_next() 判断（届时已经切到别的栈）。
     */
    boost::fibers::context* parking_ctx_{ nullptr };
    std::mutex            disp_mtx_{};            ///< 串行化 disp_ 的取用与失效
    QAbstractEventDispatcher* disp_{ nullptr };   ///< 本线程的事件分发器（disp_mtx_ 保护）
    QMetaObject::Connection disp_conn_{};         ///< disp_ 的 destroyed 连接（仅本线程用）
    QMetaObject::Connection app_conn_{};          ///< qApp 的 destroyed 连接（仅主线程调度器有）
    QTimer                deadline_timer_{};      ///< 单次定时器，用于打断 poll()
};

}

#endif // QTFIBERSCHEDULER_H
