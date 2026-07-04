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
 */
class FiberThreadBlock
{
public:
    /** @brief 构造 */
    FiberThreadBlock();
    /** @brief 阻塞当前线程直至 close（阻塞期该线程仍可调度协程） */
    void wait();
    /** @brief 解除阻塞，唤醒等待的线程 */
    void close();
    /**
     * @brief 查询是否已关闭
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
