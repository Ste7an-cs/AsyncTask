#ifndef QTFIBERSCHEDULER_H
#define QTFIBERSCHEDULER_H

#include "fiberscheduler.h"
#include <QEventLoop>
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

protected:
    QEventLoop eventloop;                    ///< 本线程 Qt 事件循环
    std::once_flag pump_once_;               ///< 每线程一次
    int pump_interval_ms_{ 1 };              ///< 事件分发间隔（ms）
};

}

#endif // QTFIBERSCHEDULER_H
