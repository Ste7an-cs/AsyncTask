#include "fiberthreadblock.h"
#include <boost/fiber/scheduler.hpp>
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
 * @brief 阻塞当前线程直至 close
 */
void Coro::FiberThreadBlock::wait()
{
    this->schedulerWait();
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
