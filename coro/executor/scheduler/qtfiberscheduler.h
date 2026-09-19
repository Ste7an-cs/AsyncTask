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
 * suspend_until 委托基类阻塞（cv 等待，不空转）。首次 suspend_until 用
 * std::call_once 创建一个绑定当前线程的常驻“事件协程”持续分发 Qt 事件。
 * 退出流程：Coro::quit() 调 signalExit() 设全局退出标志，各线程的泵协程醒
 * 来后自行退出，从而 ~scheduler 无需等待无限协程即可干净析构。
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
     * @brief 常驻事件泵协程主体
     * @details 绑定创建它的线程（Fixed 亲和），循环分发 Qt 事件直到全局退出标志
     *          或本线程停止标志置位。
     */
    void pumpLoop(void);

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
    std::mutex            disp_mtx_{};            ///< 串行化 disp_ 的取用与失效
    QAbstractEventDispatcher* disp_{ nullptr };   ///< 本线程的事件分发器（disp_mtx_ 保护）
    QMetaObject::Connection disp_conn_{};         ///< disp_ 的 destroyed 连接（仅本线程用）
    QMetaObject::Connection app_conn_{};          ///< qApp 的 destroyed 连接（仅主线程调度器有）
    QTimer                deadline_timer_{};      ///< 单次定时器，用于打断 poll()
};

}

#endif // QTFIBERSCHEDULER_H
