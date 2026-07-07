#ifndef QTFIBERSCHEDULER_H
#define QTFIBERSCHEDULER_H

#include "fiberscheduler.h"
#include <QEventLoop>
#include <mutex>
#include <atomic>

namespace Coro {

/**
 * @brief 支持 Qt 事件循环的调度器。
 *
 * 调度器空闲时委托基类阻塞（cv 等待）。Qt 事件由常驻“事件协程”在 worker
 * 上下文分发：首次 idle 时用 std::call_once 创建、绑定当前线程，复用至线程
 * 结束。退出时由 Coro::quit() 调 signalExit() 置全局标志令其自行终止，从而
 * scheduler 析构不会因无限协程卡死。
 */
class QtFiberScheduler : public FiberScheduler
{
public:
    QtFiberScheduler(void);
    ~QtFiberScheduler(void) override;

    /**
     * @brief 无就绪协程时：首次空闲时创建事件协程，此后委托基类阻塞
     * @param time_point 唤醒时刻
     */
    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept override;

    /** @brief 设全局退出标志，令各线程泵协程退出（Coro::quit() 中调） */
    static void signalExit(void);
protected:
    QEventLoop eventloop;                 ///< 本线程事件循环
    static std::atomic_bool s_exit_;      ///< 全局退出标志
    std::once_flag pump_once_;            ///< 每线程一次
    int pump_interval_ms_{ 1 };           ///< 事件分发间隔 (ms)
};

}

#endif // QTFIBERSCHEDULER_H
