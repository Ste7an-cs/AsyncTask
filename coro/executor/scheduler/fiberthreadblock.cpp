#include "fiberthreadblock.h"
#include <boost/fiber/scheduler.hpp>
#include <boost/fiber/operations.hpp>
#include <chrono>
#include "fiberscheduler.h"

/**
 * @brief 构造
 */
Coro::FiberThreadBlock::FiberThreadBlock()
{

}

/**
 * @brief 在协程条件变量上等待关闭标志（等待期该线程仍可调度协程）
 */
void Coro::FiberThreadBlock::schedulerWait()
{
    std::unique_lock<boost::fibers::mutex> lck(mtx_);
    cond_.wait(lck, [this](){return is_closed_;});
    lck.unlock();
}

/**
 * @brief 阻塞当前线程直至 close。
 *
 * close() 唤醒后，自动停止本线程的常驻（泵）协程并短暂让出使其退出——之后再返回，
 * 保证 boost.fiber 的 ~scheduler 不会因残留的无限泵协程而挂死。
 */
void Coro::FiberThreadBlock::wait()
{
    this->schedulerWait();
    FiberScheduler::stopCurrentThreadPump();
    boost::this_fiber::sleep_for(std::chrono::milliseconds(5));
}

/**
 * @brief 解除阻塞：置关闭标志并唤醒所有等待者
 */
void Coro::FiberThreadBlock::close()
{

    if(true == is_closed_){
        return;
    }
    std::lock_guard<boost::fibers::mutex> lck(mtx_);
    is_closed_ = true;
    cond_.notify_all();
}

/**
 * @brief 查询是否已关闭
 * @return 已关闭返回 true
 */
bool Coro::FiberThreadBlock::isClosed()
{
    return is_closed_;
}
