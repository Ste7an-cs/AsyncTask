#ifndef FIBERTHREADBLOCK_H
#define FIBERTHREADBLOCK_H
#include <mutex>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/condition_variable.hpp>

namespace Coro {

/**
 * @brief 用于阻止用户创建的 thread 退出。
 *
 * thread 将阻塞在 wait 处，但 Fiber 可以正常调度（“阻止线程退出但允许协程
 * 调度”的基元）；取消阻塞时调用 close。
 * @code
 * // FibersPool 的工作线程与 QtFiberThread 均以它挂起待命
 * class Worker {
 *     Coro::FiberThreadBlock block;
 *     void run(){
 *         boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
 *         block.wait();          // 线程不退出，但可持续调度协程
 *     }
 *     void stop(){ block.close(); }   // 唤醒并让线程退出
 * };
 * @endcode
 */
class FiberThreadBlock
{
public:
    /**
     * @brief 构造
     * @code
     * Coro::FiberThreadBlock block;   // 通常作为线程类的成员
     * @endcode
     */
    FiberThreadBlock();
    /**
     * @brief 阻塞当前线程直至 close（阻塞期该线程仍可调度协程）
     * @code
     * // 工作线程主体：安装调度器后挂起，线程不退出但可持续调度协程
     * boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
     * block.wait();
     * @endcode
     */
    void wait();
    /**
     * @brief 解除阻塞，唤醒等待的线程
     * @code
     * block.close();        // 唤醒 wait() 中的线程，使其退出
     * @endcode
     */
    void close();
    /**
     * @brief 查询是否已关闭
     * @code
     * if(!block.isClosed()) block.close();
     * @endcode
     * @return 已关闭返回 true
     */
    bool isClosed();
protected:
    /** @brief 在协程条件变量上等待关闭标志 */
    void schedulerWait();
private:
    bool is_closed_{false};///< 关闭标志
    boost::fibers::mutex mtx_;///< 保护关闭标志的 fiber 互斥量
    boost::fibers::condition_variable cond_;///< 等待关闭的条件变量
};
}
#endif // FIBERTHREADBLOCK_H
