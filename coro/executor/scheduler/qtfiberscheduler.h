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
 * suspend_until 委托基类阻塞（cv 等待，不空转）。首次 suspend_until 用
 * std::call_once 创建一个绑定当前线程的常驻“事件协程”持续分发 Qt 事件。
 * 退出流程：Coro::quit() 调 signalExit() 设全局退出标志，各线程的泵协程醒
 * 来后自行退出，从而 ~scheduler 无需等待无限协程即可干净析构。
 */
class QtFiberScheduler : public FiberScheduler
{
public:
    QtFiberScheduler(void);
    ~QtFiberScheduler(void) override;

    /**
     * @brief 无就绪协程时：首次创建常驻事件协程（call_once），随后委托基类阻塞
     * @param time_point 下一个唤醒的时刻
     */
    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept override;

    /** @brief 设置全局退出标志，令所有泵协程退出（Coro::quit() 中调用） */
    static void signalExit(void);
protected:
    QEventLoop eventloop;                  ///< 本线程事件循环（构造即创建事件派发器）
    static std::atomic_bool s_exit_;       ///< 全局退出标志：quit() 置位，泵协程读到后退出
    std::once_flag pump_once_;             ///< 每线程一次
    int pump_interval_ms_{ 1 };            ///< 事件协程分发间隔(ms)
};

}

#endif // QTFIBERSCHEDULER_H
