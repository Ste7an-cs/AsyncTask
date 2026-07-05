#ifndef QTFIBERSCHEDULER_H
#define QTFIBERSCHEDULER_H

#include "fiberscheduler.h"
#include <QEventLoop>

namespace Coro {

/**
 * @brief 支持 Qt 事件循环的调度器。
 *
 * suspend_until 委托基类做阻塞（cv 等待，不空转）；每次进入空闲时起一个**一次性协程**
 * （绑定当前线程、worker 上下文），分发一轮 Qt 事件后自行退出。一次性协程是有限的，
 * 不残留、不会在线程析构时阻塞 boost.fiber 的 ~scheduler。
 */
class QtFiberScheduler : public FiberScheduler
{
public:
    /** @brief 构造 */
    QtFiberScheduler(void);
    /** @brief 析构 */
    ~QtFiberScheduler(void) override;

    /**
     * @brief 无就绪协程时：起一个一次性协程分发一轮 Qt 事件，随后委托基类阻塞
     * @param time_point 下一个唤醒的时刻
     */
    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept override;
protected:
    QEventLoop eventloop;   ///< 驱动 Qt 事件的事件循环（构造即创建本线程事件派发器）
};

}

#endif // QTFIBERSCHEDULER_H
