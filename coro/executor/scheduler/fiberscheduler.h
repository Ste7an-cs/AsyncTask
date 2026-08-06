#ifndef FIBERSCHEDULER_H
#define FIBERSCHEDULER_H

#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <atomic>
#include <deque>
#include <queue>
#include "fiberproperty.h"
#include "fibertaskqueue.h"

namespace Coro {

using fiber = boost::fibers::fiber;
/**
 * @brief 带优先级和线程模型的调度器。
 * @code
 * // 作为 boost.fiber 的调度算法安装到线程上（一般由框架完成）
 * boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
 * @endcode
 *
 * 实现 boost.fiber 的 algorithm_with_properties<MetaContext> 调度算法接口，
 * 是自定义调度的基类：就绪协程按亲和放入全局队列，各线程按"本线程 Fixed →
 * Sticky → 未分配 Sticky → Shared"的顺序取出执行。
 *
 * 同时提供调度器退出所需的线程级 / 全局级停止接口（派生类可用，线程函数
 * block.wait 返回后需停所在线程的常驻协程时调用）。
 */
class FiberScheduler
        : public boost::fibers::algo::algorithm_with_properties<MetaContext>
{
public:
    using FbCtx = boost::fibers::context;

    /** @brief 构造 */
    FiberScheduler(void);
    /** @brief 析构 */
    ~FiberScheduler(void) override;
    /** @brief 禁止拷贝构造 */
    FiberScheduler( FiberScheduler const&) = delete;
    /** @brief 禁止移动构造 */
    FiberScheduler( FiberScheduler &&) = delete;

    /** @brief 禁止拷贝赋值 */
    FiberScheduler & operator=( FiberScheduler const&) = delete;
    /** @brief 禁止移动赋值 */
    FiberScheduler & operator=( FiberScheduler &&) = delete;

    /**
     * @brief 协程调度器唤醒处理函数。
     * @code
     * // 由 boost.fiber 在协程就绪时回调，使用方不直接调用。
     * // pinned 上下文留在本线程主队列，其余 detach 后入全局就绪集合
     * @endcode
     *
     * 在以下条件触发：1. 协程从休眠转至就绪时；2. 新增加协程时；3. 手动触发 notify 时。
     * pinned 上下文留在本线程就绪队列，其余 detach 后放入全局队列。
     * @param ctx 被唤醒的 fiber 上下文
     * @param props fiber 的属性
     */
    void awakened( boost::fibers::context * ctx, MetaContext &props) noexcept override;

    /**
     * @brief 获取下一个可运行的 fiber。
     * @code
     * // 由 boost.fiber 调度循环自动回调，使用方不直接调用。
     * // 取用顺序：本线程 Fixed -> 本线程 Sticky -> 未分配 Sticky -> Shared
     * @endcode
     *
     * 若有可用的 fiber（has_ready_fibers 返回 true），一定要返回一个可用的 context。
     * @return 下一个可运行的 fiber 上下文；无则返回 nullptr
     */
    boost::fibers::context * pick_next() noexcept override;

    /**
     * @brief 判断当前调度器线程中是否有可用的协程
     * @code
     * // 由调度器内部调用，决定是否进入 suspend_until 阻塞
     * @endcode
     * @return 有可运行协程返回 true
     */
    bool has_ready_fibers(void) const noexcept override;
    /**
     * @brief 没有可运行的协程时，休眠线程
     * @code
     * // 由 boost.fiber 回调；以条件变量真正阻塞而非空转，
     * // Qt 版本会在此之前先启动常驻事件泵协程
     * @endcode
     * @param time_point 下一个唤醒的时刻
     */
    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept override;

    /**
     * @brief 唤醒挂起的调度器线程
     * @code
     * // 由 boost.fiber 在有新协程就绪时回调，使 suspend_until 提前返回
     * @endcode
     */
    void notify(void) noexcept override;
    /**
     * @brief 协程属性改变时的调度函数（重新入队）
     * @code
     * // 在协程内改变自身属性会触发本回调：
     * auto& prop = boost::this_fiber::properties<Coro::MetaContext>();
     * prop.setPriority(Coro::Priority::High);    // 已就绪则解除就绪态并重新入队
     * @endcode
     * @param ctx fiber 上下文
     * @param props 变更后的属性
     */
    void property_change( boost::fibers::context * ctx, MetaContext & props) noexcept override;

    // —— 调度器退出接口 ——
    /**
     * @brief 设置全局退出标志，令所有线程的常驻（泵）协程退出（Coro::quit() 中调用）
     * @code
     * // Coro::quit() 关机流程中调用；各线程泵协程醒来看到标志后自行终止
     * Coro::FiberScheduler::signalExit();
     * @endcode
     */
    static void signalExit(void);
    /**
     * @brief 停止当前线程的常驻（泵）协程（单线程提前退出时调用）
     * @code
     * // FiberThreadBlock::wait() 返回前自动调用，确保 ~scheduler 不挂死；
     * // 使用方一般无需手动调用
     * Coro::FiberScheduler::stopCurrentThreadPump();
     * @endcode
     */
    static void stopCurrentThreadPump(void);

protected://全局
    static std::mutex                       global_mtx;///< 全局锁，串行化全局队列的跨线程访问
protected:
    std::queue<boost::fibers::context*>     main_queue_{};///< 调度器自身的就绪队列，包括 dispatch 任务和线程的主循环
    boost::mutex                            mtx_{};///< 保护 suspend_until 等待的互斥量
    boost::condition_variable               cnd_{};///< suspend_until 的条件变量
    bool                                    flag_{ false };///< 唤醒标志

    // 退出控制（static，所有调度器实例共享；子类可直接用）
    static std::atomic_bool s_exit_;         ///< 全局退出标志
    static thread_local std::atomic_bool t_stop_;///< 当前线程退出标志
};

}

#endif // FIBERSCHEDULER_H
