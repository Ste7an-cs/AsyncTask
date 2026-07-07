#include "qtfiberscheduler.h"
#include <thread>
#include "detail/asyncdefine.h"
#include <boost/fiber/all.hpp>

std::atomic_bool Coro::QtFiberScheduler::s_exit_{ false };

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
 * @brief 设置全局退出标志，令所有泵协程退出（由 Coro::quit 调用）
 */
void Coro::QtFiberScheduler::signalExit(void)
{
    s_exit_.store(true, std::memory_order_release);
}

/**
 * @brief 无就绪协程时：首次创建常驻事件协程（call_once），随后委托基类阻塞
 * @param time_point 下一个唤醒的时刻
 */
void Coro::QtFiberScheduler::suspend_until(const std::chrono::steady_clock::time_point &time_point) noexcept
{
    std::call_once(pump_once_, [this]{
        boost::fibers::fiber(launch_properties([this]{
            while (!s_exit_.load(std::memory_order_acquire)) {
                eventloop.processEvents(QEventLoop::AllEvents);
                Coro::msleep(pump_interval_ms_);
            }
        }, Priority::High, Affinity::fixed(std::this_thread::get_id()))).detach();
    });
    FiberScheduler::suspend_until(time_point);
}
