#include "qtfiberscheduler.h"
#include <thread>
#include "detail/asyncdefine.h"
#include <boost/fiber/all.hpp>

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
 * @brief 无就绪协程时：起一个一次性协程分发一轮 Qt 事件，随后委托基类阻塞
 *
 * 一次性协程绑定当前线程、运行在 worker 上下文，`processEvents` 一轮即退出——因此不残留
 * 无限协程，线程析构时不会卡在 boost.fiber 的 ~scheduler 等待它终止。Qt 回调也在 worker
 * 上下文里执行（回调里可安全做协程阻塞）。
 * @param time_point 下一个唤醒的时刻
 */
void Coro::QtFiberScheduler::suspend_until(const std::chrono::steady_clock::time_point &time_point) noexcept
{
    boost::fibers::fiber(launch_properties([this]{
        eventloop.processEvents(QEventLoop::AllEvents);   // 分发一轮 Qt 事件后退出
    }, Priority::High, Affinity::fixed(std::this_thread::get_id()))).detach();
    FiberScheduler::suspend_until(time_point);            // 委托基类阻塞
}
