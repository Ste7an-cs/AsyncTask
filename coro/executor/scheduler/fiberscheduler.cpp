#include "fiberscheduler.h"
#include "boost/fiber/type.hpp"
#include <boost/context/detail/prefetch.hpp>
using namespace boost::fibers;
using namespace boost::fibers::algo;
using namespace Coro;

std::mutex FiberScheduler::global_mtx{};
std::atomic_bool FiberScheduler::s_exit_{ false };
thread_local std::atomic_bool FiberScheduler::t_stop_{ false };

/**
 * @brief 构造
 */
FiberScheduler::FiberScheduler()
{

}

/**
 * @brief 析构
 */
FiberScheduler::~FiberScheduler()
{
}

/**
 * @brief 设全局退出标志，令所有线程的常驻（泵）协程退出
 */
void FiberScheduler::signalExit(void)
{
    s_exit_.store(true, std::memory_order_release);
}

/**
 * @brief 停止当前线程的常驻（泵）协程
 */
void FiberScheduler::stopCurrentThreadPump(void)
{
    t_stop_.store(true, std::memory_order_release);
}

/**
 * @brief 协程就绪时的处理：pinned 上下文留在本线程主队列，其余 detach 后入全局队列
 */
void Coro::FiberScheduler::awakened(boost::fibers::context *ctx, Coro::MetaContext &props) noexcept
{
    // fiber自身的事件，必须绑定至scheduler所在的线程，这里缓存至main_queue_
    if ( ctx->is_context( boost::fibers::type::pinned_context) ) { /*<
            recognize when we're passed this thread's main fiber (or an
            implicit library helper fiber): never put those on the shared
            queue
        >*/
        main_queue_.push(ctx);
    } else {
        // 根据协程的属性分配队列
        ctx->detach();

        std::lock_guard<std::mutex> guard(global_mtx);
        FiberGlobalQueue::instance()->emplace_back(props);
    }
}

/**
 * @brief 取下一个可运行协程：按“本线程 Fixed → 本线程 Sticky → 未分配 Sticky → Shared”取用。
 *
 * 命中全局队列则 attach 到本线程；否则从本线程主队列取调度任务。
 * @return 下一个可运行的 fiber 上下文；无则返回 nullptr
 */
boost::fibers::context * Coro::FiberScheduler::pick_next() noexcept{

    boost::fibers::context *ctx{nullptr};
    std::string name{};
    // 先从当前调度器的fixed_queue_中，取出一个fixed模式且未分配的Fiber
    do{
        std::lock_guard<std::mutex> guard(global_mtx);
        // 先从共享队列中取出，取出一个fixed模式且属于该线程的fiber
        std::optional<MetaContext> g_fixed_context = FiberGlobalQueue::instance()->pop_front_affinity(Affinity::fixed(std::this_thread::get_id()));
        if(g_fixed_context){
            ctx = g_fixed_context.value().context();
            name = g_fixed_context.value().name();
            break;
        }
        // 从共享队列中取出，取出一个sticky模式且属于该线程的fiber
        std::optional<MetaContext> g_sticky_context = FiberGlobalQueue::instance()->pop_front_affinity(Affinity{AffinityMode::Sticky, std::this_thread::get_id()});
        if(g_sticky_context){
            ctx = g_sticky_context.value().context();
            name = g_sticky_context.value().name();
            break;
        }
        // 如果共享队列中没有当前线程id的Fiber，取出一个Sticky模式且未分配的Fiber
        std::optional<MetaContext> sticky_context = FiberGlobalQueue::instance()->pop_front_affinity(Affinity{AffinityMode::Sticky, std::nullopt});
        if(sticky_context){
            //将Sticky模式的Fiber绑定至该线程，并修改为FixedId模式
            ctx = sticky_context.value().context();
            MetaContext* meta = dynamic_cast<MetaContext*>(ctx->get_properties());
            meta->affinity_.fixed_id = std::this_thread::get_id();
            name = sticky_context.value().name();
            break;
        }
        // 如果上述模式均为空，取出一个shared模式的Fiber
        std::optional<MetaContext> shared_context = FiberGlobalQueue::instance()->pop_front_affinity(Affinity{AffinityMode::Shared, std::nullopt});
        if(shared_context){
            ctx = shared_context.value().context();
            name = shared_context.value().name();
            break;
        }
    }while(0);

    if(ctx != nullptr){
        context::active()->attach(ctx);
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
 * @brief 判断本线程是否有可运行协程（全局队列中可被本线程取用者，或本线程主队列非空）
 * @return 有则返回 true
 */
bool Coro::FiberScheduler::has_ready_fibers() const noexcept
{
    std::lock_guard<std::mutex> guard(global_mtx);
    // 判断是否存在可用的fiber
    if(
            FiberGlobalQueue::instance()->getQueueSize(Affinity::shared()) > 0
            || FiberGlobalQueue::instance()->getQueueSize(Affinity::fixed(std::this_thread::get_id())) > 0
            || FiberGlobalQueue::instance()->getQueueSize(Affinity::sticky()) > 0
            || FiberGlobalQueue::instance()->getQueueSize(Affinity{AffinityMode::Sticky, std::this_thread::get_id()}) > 0
            || main_queue_.size()>0){
        return true;
    }else{
        return false;
    }
}

/**
 * @brief 无可运行协程时挂起线程至指定时刻或被 notify 唤醒
 * @param time_point 下一个唤醒的时刻
 */
void Coro::FiberScheduler::suspend_until(const std::chrono::steady_clock::time_point &time_point) noexcept
{
    if ( (std::chrono::steady_clock::time_point::max)() == time_point) {
        boost::unique_lock<boost::mutex> lock(mtx_);
        cnd_.wait_for(lock, boost::chrono::microseconds(200), [this](){ return flag_; });
        flag_ = false;
    } else {
        boost::unique_lock<boost::mutex> lock(mtx_);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(time_point - std::chrono::steady_clock::now()).count();
        if(duration > 0){
            cnd_.wait_for(lock,
                          boost::chrono::microseconds(duration),
                          [this](){ return flag_; });
        }
        flag_ = false;
    }
}

/**
 * @brief 唤醒挂起在 suspend_until 的调度器线程
 */
void Coro::FiberScheduler::notify(void) noexcept
{
    boost::unique_lock< boost::mutex > lk{ mtx_ };
    flag_ = true;
    lk.unlock();
    cnd_.notify_all();
}

/**
 * @brief 协程属性变更时：若已就绪则解除就绪态并重新调度
 * @param ctx fiber 上下文
 * @param props 变更后的属性
 */
void FiberScheduler::property_change(context *ctx, MetaContext &props) noexcept
{
    ///当fiber已经就绪时（已经由pick_next返回），解除就绪态并重新调度
    if ( ctx->ready_is_linked()) {
        ctx->ready_unlink();
        awakened( ctx, props);
    }
}

