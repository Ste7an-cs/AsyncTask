#ifndef FIBERTASKQUEUE_H
#define FIBERTASKQUEUE_H

#include "fiberproperty.h"
#include <set>
#include <mutex>

namespace Coro {

typedef std::set<MetaContext, std::greater<MetaContext>>  MetaQueueType;

class FiberTaskQueue
{
public:
    FiberTaskQueue(void);
    FiberTaskQueue( FiberTaskQueue const&) = delete;
    FiberTaskQueue( FiberTaskQueue &&) = delete;
    ///
    /// \brief emplace_back 添加一个MetaContext
    /// \param meta
    ///
    void emplace_back(MetaContext&& meta);
    ///
    /// \brief pop_front 返回一个MetaContext，如果无可用的，返回nullopt
    /// \return
    ///
    std::optional<MetaContext> pop_front(void);
    ///
    /// \brief size MetaContext的可用个数
    /// \return
    ///
    int size() const;

    FiberTaskQueue & operator=( FiberTaskQueue const&) = delete;
    FiberTaskQueue & operator=( FiberTaskQueue &&) = delete;

    MetaQueueType queue;
};
///
/// \brief The FiberGlobalQueue class 全局队列，Fiber将根据线程模型分别存储带优先级的队列中
///
class FiberGlobalQueue
{
public:
    static FiberGlobalQueue* instance(void);
    ///
    /// \brief emplace_back 添加一个MetaContext
    /// \param meta
    ///
    void emplace_back(const MetaContext &);
    ///
    /// \brief pop_front_affinity 返回一个符合线程模型的 MetaContext，如果无可用的，返回nullopt
    /// \param affine
    /// \return
    ///
    std::optional<MetaContext> pop_front_affinity(const Affinity& affine);
    ///
    /// \brief getQueueSize 获取某个线程模型的可用数量
    /// \param affine
    /// \return
    ///
    int getQueueSize(const Affinity& affine) const;
    /// 删除一个MetaContext
    void remove_meta(const MetaContext& meta);
    /// 根据context删除
    void remove_context(boost::fibers::context* ctx);
    int size() const;
protected:
    FiberGlobalQueue(void);
    FiberGlobalQueue( FiberGlobalQueue const&) = delete;
    FiberGlobalQueue( FiberGlobalQueue &&) = delete;

    FiberGlobalQueue & operator=( FiberGlobalQueue const&) = delete;
    FiberGlobalQueue & operator=( FiberGlobalQueue &&) = delete;
    std::unordered_map<Affinity, MetaQueueType, Hash_Affinity>   queue_map;
    std::unordered_map<boost::fibers::context*, MetaContext>               context_map;
};
}

#endif // FIBERTASKQUEUE_H
