#include "qtfiberscheduler.h"
#include <thread>
#include "detail/asyncdefine.h"
#include <boost/fiber/all.hpp>

std::atomic_bool Coro::QtFiberScheduler::s_exit_{ false };

Coro::QtFiberScheduler::QtFiberScheduler(void):FiberScheduler()
{
}

Coro::QtFiberScheduler::~QtFiberScheduler(void)
{
}

void Coro::QtFiberScheduler::signalExit(void)
{
    s_exit_.store(true, std::memory_order_release);
}

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
