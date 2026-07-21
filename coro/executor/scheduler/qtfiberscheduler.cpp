#include "qtfiberscheduler.h"
#include <thread>
#include "detail/asyncdefine.h"
#include <boost/fiber/all.hpp>

Coro::QtFiberScheduler::QtFiberScheduler(void):FiberScheduler()
{
}

Coro::QtFiberScheduler::~QtFiberScheduler(void)
{
}

void Coro::QtFiberScheduler::suspend_until(const std::chrono::steady_clock::time_point &time_point) noexcept
{
    bool startedPump = false;
    std::call_once(pump_once_, [this, &startedPump]{
        startedPump = true;
        boost::fibers::fiber(launch_properties([this]{
            while (!FiberScheduler::s_exit_.load(std::memory_order_acquire)
                   && !FiberScheduler::t_stop_.load(std::memory_order_acquire)) {
                eventloop.processEvents(QEventLoop::AllEvents);
                Coro::msleep(pump_interval_ms_);
            }
        }, Priority::High, Affinity::fixed(std::this_thread::get_id()))).detach();
    });
    if(startedPump) return;
    FiberScheduler::suspend_until(time_point);
}
