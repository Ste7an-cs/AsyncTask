#include "fibertaskqueue.h"
#include <memory>

using namespace Coro;

Coro::FiberGlobalQueue::FiberGlobalQueue(void)
{

}

FiberTaskQueue::FiberTaskQueue(void)
{

}

void Coro::FiberTaskQueue::emplace_back(MetaContext &&meta)
{
    queue.emplace(meta);
}

std::optional<MetaContext> Coro::FiberTaskQueue::pop_front(void)
{
    if(queue.empty()){
        return std::nullopt;
    }else{
        auto iter = queue.begin();
        auto meta = *iter;
        queue.erase(iter);
        return meta;
    }
}

int FiberTaskQueue::size(void) const
{
    return queue.size();
}
Coro::FiberGlobalQueue *Coro::FiberGlobalQueue::instance(void)
{
    static FiberGlobalQueue fiber_queue;
    return &fiber_queue;
}

void Coro::FiberGlobalQueue::emplace_back(const MetaContext &meta)
{
//    std::lock_guard<std::mutex> guard(mtx);
    context_map[meta.context()] = meta;
    auto iter = queue_map.find(meta.affinity());
    if(iter != queue_map.end()){
        queue_map[meta.affinity()].emplace(meta);
    }else{
        MetaQueueType queue;
        queue.emplace(meta);
        queue_map.insert({meta.affinity(), queue});
    }
}

std::optional<MetaContext> Coro::FiberGlobalQueue::pop_front_affinity(const Coro::Affinity &affine)
{
//    std::lock_guard<std::mutex> guard(mtx);
    auto iter = queue_map.find(affine);
    if(iter != queue_map.end()){
        auto& queue = queue_map[affine];
        if(queue.size()>0){
            auto iter = queue.begin();
            auto meta = *iter;
            context_map.erase(meta.context());
            queue.erase(iter);
            return meta;
        }else{
            return std::nullopt;
        }
    }else{
        return std::nullopt;
    }
}

int FiberGlobalQueue::getQueueSize(const Affinity& affine) const
{
//    std::lock_guard<std::mutex> guard(mtx);
    auto iter = queue_map.find(affine);
    if(iter != queue_map.end()){
        return queue_map.at(affine).size();
    }else{
        return 0;
    }
}

void FiberGlobalQueue::remove_meta(const MetaContext &meta)
{
    auto iter = queue_map.find(meta.affinity());
    if(iter != queue_map.end()){
        auto& queue = queue_map[meta.affinity()];
        queue.erase(meta);
        context_map.erase(meta.context());
    }
}

void FiberGlobalQueue::remove_context(boost::fibers::context *ctx)
{
    auto iter = context_map.find(ctx);
    if(iter != context_map.end()){
        auto meta = iter->second;
        remove_meta(meta);
    }
}

int FiberGlobalQueue::size() const
{
//    std::lock_guard<std::mutex> guard(mtx);
    int size_all{0};
    for(auto iter = queue_map.cbegin(); iter!= queue_map.cend(); iter++){
        size_all += iter->second.size();
    }

    return size_all;
}

