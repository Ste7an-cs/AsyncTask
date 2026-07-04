#include "fibertaskqueue.h"
#include <memory>

using namespace Coro;

/**
 * @brief 构造（单例，受保护）
 */
Coro::FiberGlobalQueue::FiberGlobalQueue(void)
{

}

/**
 * @brief 构造
 */
FiberTaskQueue::FiberTaskQueue(void)
{

}

/**
 * @brief 添加一个 MetaContext
 * @param meta 待添加的协程属性
 */
void Coro::FiberTaskQueue::emplace_back(MetaContext &&meta)
{
    queue.emplace(meta);
}

/**
 * @brief 取出优先级最高的 MetaContext
 * @return 可用则返回 MetaContext，否则返回 nullopt
 */
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

/**
 * @brief 队列元素数量
 * @return 元素数量
 */
int FiberTaskQueue::size(void) const
{
    return queue.size();
}
/**
 * @brief 获取全局单例
 * @return 单例指针
 */
Coro::FiberGlobalQueue *Coro::FiberGlobalQueue::instance(void)
{
    static FiberGlobalQueue fiber_queue;
    return &fiber_queue;
}

/**
 * @brief 添加一个 MetaContext：更新上下文索引并放入对应亲和的分桶
 * @param meta 待添加的协程属性
 */
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

/**
 * @brief 取出符合线程模型的 MetaContext（对应桶内优先级最高者），并同步移除上下文索引
 * @param affine 线程模型（分桶键）
 * @return 可用则返回 MetaContext，否则返回 nullopt
 */
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

/**
 * @brief 获取某个线程模型的可用数量
 * @param affine 线程模型（分桶键）
 * @return 对应桶内元素数量
 */
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

/**
 * @brief 删除一个 MetaContext（同步移除分桶与上下文索引）
 * @param meta 待删除的协程属性
 */
void FiberGlobalQueue::remove_meta(const MetaContext &meta)
{
    auto iter = queue_map.find(meta.affinity());
    if(iter != queue_map.end()){
        auto& queue = queue_map[meta.affinity()];
        queue.erase(meta);
        context_map.erase(meta.context());
    }
}

/**
 * @brief 根据 context 删除对应的 MetaContext
 * @param ctx fiber 上下文
 */
void FiberGlobalQueue::remove_context(boost::fibers::context *ctx)
{
    auto iter = context_map.find(ctx);
    if(iter != context_map.end()){
        auto meta = iter->second;
        remove_meta(meta);
    }
}

/**
 * @brief 所有分桶的元素总数
 * @return 元素总数
 */
int FiberGlobalQueue::size() const
{
//    std::lock_guard<std::mutex> guard(mtx);
    int size_all{0};
    for(auto iter = queue_map.cbegin(); iter!= queue_map.cend(); iter++){
        size_all += iter->second.size();
    }

    return size_all;
}

