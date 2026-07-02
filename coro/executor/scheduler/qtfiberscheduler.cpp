#include "qtfiberscheduler.h"
#include <QCoreApplication>
#include <QAbstractEventDispatcher>
#include <QThread>
#include "detail/asyncdefine.h"
#include <boost/fiber/all.hpp>
#include <QDebug>

Coro::QtFiberScheduler::QtFiberScheduler(void):FiberScheduler()
{
}

Coro::QtFiberScheduler::~QtFiberScheduler(void)
{
}

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

void Coro::QtFiberScheduler::notify() noexcept
{
    flag_ = true;
    eventloop.wakeUp();
}
