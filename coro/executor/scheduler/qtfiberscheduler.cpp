#include "qtfiberscheduler.h"
#include <QCoreApplication>
#include <QAbstractEventDispatcher>
#include <QThread>
#include "detail/asyncdefine.h"
#include <boost/fiber/all.hpp>
#include <QDebug>

/**
 * @brief 构造
 */
Coro::QtFiberScheduler::QtFiberScheduler(void):FiberScheduler()
{
}

/**
 * @brief 析构
 */
Coro::QtFiberScheduler::~QtFiberScheduler(void)
{
}

/**
 * @brief 无可运行协程时休眠线程，同时用 processEvents 驱动一轮 Qt 事件
 * @param time_point 下一个唤醒的时刻
 */
void Coro::QtFiberScheduler::suspend_until(const std::chrono::steady_clock::time_point &time_point) noexcept
{
    constexpr QEventLoop::ProcessEventsFlags flags = QEventLoop::AllEvents | QEventLoop::WaitForMoreEvents;
    if ( (std::chrono::steady_clock::time_point::max)() == time_point) {
        eventloop.processEvents(flags, 200);
        flag_ = false;
    } else {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(time_point - std::chrono::steady_clock::now()).count();
        if(duration > 0){
            eventloop.processEvents(flags, duration);
        }
        flag_ = false;
    }
}

/**
 * @brief 唤醒挂起的调度器线程（唤醒 Qt 事件循环）
 */
void Coro::QtFiberScheduler::notify() noexcept
{
    flag_ = true;
    eventloop.wakeUp();
}
