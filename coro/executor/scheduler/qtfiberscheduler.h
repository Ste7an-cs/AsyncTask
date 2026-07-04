#ifndef QTFIBERSCHEDULER_H
#define QTFIBERSCHEDULER_H

#include "fiberscheduler.h"
#include <QEventLoop>

namespace Coro {

/**
 * @brief 支持 Qt 事件循环的调度器。
 *
 * 覆写 suspend_until，在无就绪协程时用 QEventLoop::processEvents 泵一轮 Qt
 * 事件，使协程调度与 Qt 事件循环共存于同一线程；用于工作线程池。
 */
class QtFiberScheduler : public FiberScheduler
{
public:
    /** @brief 构造 */
    QtFiberScheduler(void);
    /** @brief 析构 */
    ~QtFiberScheduler(void) override;

    /**
     * @brief 没有可运行的协程时，休眠线程并驱动一轮 Qt 事件
     * @param time_point 下一个唤醒的时刻
     */
    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept override;
    /**
     * @brief 唤醒挂起的调度器线程（同时唤醒 Qt 事件循环）
     */
    void notify(void) noexcept override;
protected:
    QEventLoop loop;///< 备用事件循环
    std::atomic_bool interrupt_flg{false};///< 中断标志
    boost::fibers::fiber event_fiber_;///< 事件协程（预留）
    QEventLoop eventloop;///< 驱动 Qt 事件的事件循环
};

}

#endif // QTFIBERSCHEDULER_H
