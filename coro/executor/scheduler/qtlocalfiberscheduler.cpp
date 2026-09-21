#include "qtlocalfiberscheduler.h"
#include <QDebug>
#include <boost/fiber/type.hpp>

/**
 * @brief 构造
 */
Coro::QtLocalFiberScheduler::QtLocalFiberScheduler():QtFiberScheduler()
{

}

/**
 * @brief 析构
 */
Coro::QtLocalFiberScheduler::~QtLocalFiberScheduler()
{

}

/**
 * @brief 取下一个可运行协程：只取“本线程 Fixed → Shared”，命中则 attach 到本线程
 * @return 下一个可运行的 fiber 上下文；无则返回 nullptr
 */
boost::fibers::context *Coro::QtLocalFiberScheduler::pick_next() noexcept
{
    /// @details 饥饿守卫，必须排在取全局锁之前（它内部会走 awakened()，那里要拿
    /// 同一把 global_mtx）。放在这里是因为**本函数是积压期间唯一还在跑的框架代码**：
    /// 线程不空闲 → suspend_until() 不回调 → 挂起的 Qt 持有者无人唤醒。原委与
    /// 「为什么不能写成一个睡 100ms 的守卫协程」见 QtFiberScheduler 头文件。
    releaseParkedIfOverdue();

    boost::fibers::context *ctx{nullptr};
    // 先从当前调度器的fixed_queue_中，取出一个fixed模式且未分配的Fiber
    do{
        std::lock_guard<std::mutex> guard(global_mtx);
        // 先从共享队列中取出，取出一个fixed模式且属于该线程的fiber
        std::optional<MetaContext> g_fixed_context = FiberGlobalQueue::instance()->pop_front_affinity(Affinity{AffinityMode::FixedId, std::this_thread::get_id()});
        if(g_fixed_context){
            ctx = g_fixed_context.value().context();
            break;
        }
        // 如果上述模式均为空，取出一个shared模式的Fiber
        std::optional<MetaContext> shared_context = FiberGlobalQueue::instance()->pop_front_affinity(Affinity{AffinityMode::Shared, std::nullopt});
        if(shared_context){
            ctx = shared_context.value().context();
            break;
        }
    }while(0);

    if(ctx != nullptr){
        boost::fibers::context::active()->attach(ctx);
    }else{
        //如果队列都没有，取出main队列取出一个fiber自身的调度任务
        if(!main_queue_.empty()){
            ctx = main_queue_.front();
            main_queue_.pop();
        }
    }
    return ctx;
}

/**
 * @brief 判断本线程是否有可用协程（Shared、本线程 Fixed，或本线程主队列非空）
 * @return 有则返回 true
 */
bool Coro::QtLocalFiberScheduler::has_ready_fibers() const noexcept
{
    std::lock_guard<std::mutex> guard(global_mtx);
    // 判断是否存在可用的fiber
    if(
            FiberGlobalQueue::instance()->getQueueSize(Affinity::shared()) > 0
            || FiberGlobalQueue::instance()->getQueueSize(Affinity::fixed(std::this_thread::get_id())) > 0
            || main_queue_.size()>0){
        return true;
    }else{
        return false;
    }
}

/**
 * @brief 本线程是否有真正可跑的协程（只认 Shared 与本线程 Fixed，排除 dispatcher）
 * @return 有真实可跑协程返回 true
 */
bool Coro::QtLocalFiberScheduler::hasReadyWork(void) const noexcept
{
    std::lock_guard<std::mutex> guard(global_mtx);
    auto* q = FiberGlobalQueue::instance();
    if(q->getQueueSize(Affinity::shared()) > 0
       || q->getQueueSize(Affinity::fixed(std::this_thread::get_id())) > 0){
        return true;
    }
    /// @details 同基类：main_queue_ 里常驻的 dispatcher context 不算活。
    if(main_queue_.empty()) return false;
    if(main_queue_.size() > 1) return true;
    return !main_queue_.front()->is_context(boost::fibers::type::dispatcher_context);
}
