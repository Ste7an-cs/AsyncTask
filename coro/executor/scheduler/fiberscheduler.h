#ifndef FIBERSCHEDULER_H
#define FIBERSCHEDULER_H

#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <atomic>
#include <deque>
#include <functional>
#include <queue>
#include <unordered_map>
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
    /**
     * @brief 让本线程与它的平台事件分发器彻底解绑（线程收尾的最后一步）
     * @code
     * // FiberThreadBlock::wait() 在停泵并让出之后调用；使用方一般无需手动调用
     * Coro::FiberScheduler::detachCurrentThreadDispatcher();
     * @endcode
     * @details Qt 实现注入的钩子会注销本线程的唤醒回调、并丢弃缓存的
     *          QAbstractEventDispatcher 裸指针。必须排在 stopCurrentThreadPump()
     *          和随后的让出之后：解绑之后本线程就再也叫不醒了，远端就绪的唤醒
     *          彻底失效，提前调用只能靠 suspend_until 的定时轮询自醒——上限是
     *          轮询周期，或者本线程最近一个定时协程的截止时刻（以先到者为准），
     *          不是永久睡死，但也不总是亚毫秒级的。
     * @warning 只覆盖走 FiberThreadBlock::wait() 收尾的线程（框架创建的线程都走）。
     *          用户自建、装了调度器又不经 wait() 就返回的线程仍有残留窗口。
     */
    static void detachCurrentThreadDispatcher(void);

    // —— 阻塞唤醒登记表 ——
    /// @brief 唤醒回调类型（Qt 实现注入 QAbstractEventDispatcher::wakeUp）
    using WakeFn = std::function<void()>;
    /**
     * @brief 登记本线程的唤醒回调
     * @code
     * // QtFiberScheduler 构造时注入 Qt 的线程安全唤醒函数
     * FiberScheduler::registerWaker(this, [disp]{ disp->wakeUp(); });
     * @endcode
     * @param key 登记键（用调度器实例地址），注销时用同一个键
     * @param wake 可跨线程调用的唤醒回调
     * @details 登记的同时把**调用线程**的阻塞标志一并记下，wakeAllBlocked() 据此
     *          只戳真正阻塞着的线程。因此必须在要被唤醒的那个线程上调用。
     * @warning wakeAllBlocked() 在持有 waker_mtx_（非递归 std::mutex）期间调用
     *          登记的回调。回调绝不能在同一线程上再入 registerWaker /
     *          unregisterWaker / wakeAllBlocked，否则会自锁死锁。
     */
    static void registerWaker(void* key, WakeFn wake);
    /**
     * @brief 注销唤醒回调（调度器析构时调用）
     * @param key 登记时用的键
     */
    static void unregisterWaker(void* key);
    /**
     * @brief 叫醒所有阻塞中的线程
     * @details 两道闸：① 全局计数为 0 时直接返回，连登记表都不遍历；② 遍历时
     *          只戳「此刻确实阻塞着」的那几个线程，忙着跑协程的线程一个都不戳。
     * @details 第二道闸不是优化而是必需。只有第一道闸时，实测 testProfile：
     *          ~1.9 万次广播/秒 × 17 个登记线程 = **32.4 万次 wakeUp() 系统调用/秒**，
     *          而任一时刻真正阻塞的只有 0~4 个线程 —— 16 个白叫，进程 CPU 从
     *          266s 涨到 1805s（issue #6 那个忙转的另一种形态）。第一道闸在事件泵
     *          改为按需阻塞之前一直是「计数恒为 0」，所以这条惊群路径此前从未点亮。
     */
    static void wakeAllBlocked(void);
    /**
     * @brief 进入阻塞前调用（置本线程阻塞标志，阻塞线程计数 +1）
     * @warning 必须在真正要阻塞的那个线程上调用，且与 leaveBlocked() 严格配对：
     *          漏一次减计数，计数就永久 >0，wakeAllBlocked() 的第一道闸从此失效。
     */
    static void enterBlocked(void);
    /** @brief 离开阻塞后调用（阻塞线程计数 -1，清本线程阻塞标志） */
    static void leaveBlocked(void);
    /** @brief 当前阻塞中的线程数（供测试断言） */
    static int  blockedCount(void);

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

    /// @brief 登记表条目：唤醒回调 + 该线程「此刻是否真的阻塞着」的标志
    struct WakerEntry {
        WakeFn                            fn;      ///< 唤醒回调
        std::shared_ptr<std::atomic_bool> blocked; ///< 归属线程的阻塞标志
    };
    static std::mutex                        waker_mtx_;   ///< 保护登记表
    static std::unordered_map<void*, WakerEntry> wakers_;  ///< 各线程的唤醒回调
    static thread_local std::shared_ptr<std::atomic_bool> t_blocked_flag_;///< 本线程阻塞标志
    static std::atomic_int                   s_blocked_count_;///< 阻塞中的线程数

    /**
     * @brief 交出 Qt 持有权前解除本线程挂起协程的钩子（Qt 实现注入）
     * @details 存的是无捕获静态函数的指针而非 std::function：每个线程装调度器时
     *          都会注入一次，std::function 的赋值不是原子操作，多线程并发写会与
     *          stopCurrentThreadPump() 的读构成数据竞争。
     */
    static std::atomic<void(*)(void)> local_unpark_hook_;

    /**
     * @brief 线程收尾时与本线程事件分发器解绑的钩子（Qt 实现注入）
     * @details 与 local_unpark_hook_ 同样用原子函数指针存放，基类因此不必认识
     *          任何 Qt 类型；钩子内部靠 thread_local 自己找到本线程的调度器实例。
     */
    static std::atomic<void(*)(void)> local_detach_hook_;
};

}

#endif // FIBERSCHEDULER_H
