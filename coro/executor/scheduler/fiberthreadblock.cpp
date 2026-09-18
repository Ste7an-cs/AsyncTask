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
 * @details 返回前的最后一步是与本线程的事件分发器解绑。三步的先后不可调换：
 *          停泵 → 让出 5ms 让泵协程和其余就绪协程真正跑完 → 解绑。解绑之后本
 *          线程就再也叫不醒了（唤醒回调已注销、分发器指针已丢弃），提前解绑会
 *          把还有活干的协程永远睡死在这条线程上。
 *          之所以要在这里解绑：非主线程的事件分发器只在自己的 ~QObject 里发
 *          destroyed，那时派生析构（~QEventDispatcherGlib 等）已经跑完，别的
 *          线程此刻若正在 wakeUp() 就是在碰一个半毁的对象。本函数返回时线程还
 *          健在，是框架唯一能抢在分发器析构之前动手的时机。
 */
void Coro::FiberThreadBlock::wait()
{
    this->schedulerWait();
    FiberScheduler::stopCurrentThreadPump();
    boost::this_fiber::sleep_for(std::chrono::milliseconds(5));
    FiberScheduler::detachCurrentThreadDispatcher();
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
