#ifndef FIBERSCHEDULER_H
#define FIBERSCHEDULER_H

#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <deque>
#include <queue>
#include "fiberproperty.h"
#include "fibertaskqueue.h"

namespace Coro {

using fiber = boost::fibers::fiber;
///
/// \brief The FiberScheduler class 带优先级和线程模型的调度器
///
class FiberScheduler
        : public boost::fibers::algo::algorithm_with_properties<MetaContext>
{
public:
    using FbCtx = boost::fibers::context;

    FiberScheduler(void);
    ~FiberScheduler(void) override;
    FiberScheduler( FiberScheduler const&) = delete;
    FiberScheduler( FiberScheduler &&) = delete;

    FiberScheduler & operator=( FiberScheduler const&) = delete;
    FiberScheduler & operator=( FiberScheduler &&) = delete;

    ///
    /// \brief async::FiberScheduler::awakened 协程调度器唤醒处理函数
    ///    该函数在以下条件触发：1.在协程从休眠转至就绪时；2.新增加协程时；3.手动触发notify时
    /// \param ctx  被唤醒的fiber 上下文
    /// \param props    fiber的属性
    ///
    void awakened( boost::fibers::context * ctx, MetaContext &props) noexcept override;

    ///
    /// \brief pick_next 协程调度器获取下一个可运行的fiber,若有可用的fiber（has_ready_fibers返回true），一定要返回一个可用的context
    /// \return 被唤醒的fiber 上下文
    ///
    boost::fibers::context * pick_next() noexcept override;

    ///
    /// \brief has_ready_fibers 判断当前调度器线程中是否有可用的协程
    /// \return
    ///
    bool has_ready_fibers(void) const noexcept override;
    ///
    /// \brief suspend_until    没有可运行的协程时，休眠线程
    /// \param time_point       下一个唤醒的时刻
    ///
    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept override;

    void notify(void) noexcept override;
    ///
    /// \brief property_change 属性改变时的调度函数
    /// \param ctx
    /// \param props
    ///
    void property_change( boost::fibers::context * ctx, MetaContext & props) noexcept override;

protected://全局
    static std::mutex                       global_mtx;//全局锁
protected:
    std::queue<boost::fibers::context*>     main_queue_{};//调度器自身的就绪队列，包括dispatch任务和线程的主循环
    boost::mutex                            mtx_{};
    boost::condition_variable               cnd_{};
    bool                                    flag_{ false };
};

}

#endif // FIBERSCHEDULER_H
