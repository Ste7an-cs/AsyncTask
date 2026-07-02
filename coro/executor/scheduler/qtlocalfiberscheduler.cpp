#include "qtlocalfiberscheduler.h"
#include <QDebug>

Coro::QtLocalFiberScheduler::QtLocalFiberScheduler():QtFiberScheduler()
{

}

Coro::QtLocalFiberScheduler::~QtLocalFiberScheduler()
{

}

boost::fibers::context *Coro::QtLocalFiberScheduler::pick_next() noexcept
{
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
