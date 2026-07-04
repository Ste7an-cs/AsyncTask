#ifndef FIBERTASKQUEUE_H
#define FIBERTASKQUEUE_H

#include "fiberproperty.h"
#include <set>
#include <mutex>

namespace Coro {

/** @brief 按优先级降序排列的 MetaContext 集合类型 */
typedef std::set<MetaContext, std::greater<MetaContext>>  MetaQueueType;

/**
 * @brief 单桶的就绪协程队列（按优先级排序），供测试与内部使用
 */
class FiberTaskQueue
{
public:
    /** @brief 构造 */
    FiberTaskQueue(void);
    /** @brief 禁止拷贝构造 */
    FiberTaskQueue( FiberTaskQueue const&) = delete;
    /** @brief 禁止移动构造 */
    FiberTaskQueue( FiberTaskQueue &&) = delete;
    /**
     * @brief 添加一个 MetaContext
     * @param meta 待添加的协程属性
     */
    void emplace_back(MetaContext&& meta);
    /**
     * @brief 取出一个 MetaContext（优先级最高者）
     * @return 可用则返回 MetaContext，否则返回 nullopt
     */
    std::optional<MetaContext> pop_front(void);
    /**
     * @brief MetaContext 的可用个数
     * @return 队列元素数量
     */
    int size() const;

    /** @brief 禁止拷贝赋值 */
    FiberTaskQueue & operator=( FiberTaskQueue const&) = delete;
    /** @brief 禁止移动赋值 */
    FiberTaskQueue & operator=( FiberTaskQueue &&) = delete;

    MetaQueueType queue;///< 底层按优先级排序的集合
};
/**
 * @brief 全局就绪队列（单例）。
 *
 * Fiber 根据线程模型分别存储在带优先级的分桶队列中：按 Affinity 分桶、
 * 桶内按优先级排序，并以 context 索引支持移除。
 */
class FiberGlobalQueue
{
public:
    /**
     * @brief 获取全局单例
     * @return 单例指针
     */
    static FiberGlobalQueue* instance(void);
    /**
     * @brief 添加一个 MetaContext（放入对应亲和的分桶）
     * @param meta 待添加的协程属性
     */
    void emplace_back(const MetaContext &);
    /**
     * @brief 取出一个符合线程模型的 MetaContext（对应桶内优先级最高者）
     * @param affine 线程模型（分桶键）
     * @return 可用则返回 MetaContext，否则返回 nullopt
     */
    std::optional<MetaContext> pop_front_affinity(const Affinity& affine);
    /**
     * @brief 获取某个线程模型的可用数量
     * @param affine 线程模型（分桶键）
     * @return 对应桶内元素数量
     */
    int getQueueSize(const Affinity& affine) const;
    /**
     * @brief 删除一个 MetaContext
     * @param meta 待删除的协程属性
     */
    void remove_meta(const MetaContext& meta);
    /**
     * @brief 根据 context 删除对应的 MetaContext
     * @param ctx fiber 上下文
     */
    void remove_context(boost::fibers::context* ctx);
    /**
     * @brief 所有分桶的元素总数
     * @return 元素总数
     */
    int size() const;
protected:
    /** @brief 构造（单例，受保护） */
    FiberGlobalQueue(void);
    /** @brief 禁止拷贝构造 */
    FiberGlobalQueue( FiberGlobalQueue const&) = delete;
    /** @brief 禁止移动构造 */
    FiberGlobalQueue( FiberGlobalQueue &&) = delete;

    /** @brief 禁止拷贝赋值 */
    FiberGlobalQueue & operator=( FiberGlobalQueue const&) = delete;
    /** @brief 禁止移动赋值 */
    FiberGlobalQueue & operator=( FiberGlobalQueue &&) = delete;
    std::unordered_map<Affinity, MetaQueueType, Hash_Affinity>   queue_map;///< 按亲和分桶的就绪队列
    std::unordered_map<boost::fibers::context*, MetaContext>               context_map;///< 按上下文索引，供删除
};
}

#endif // FIBERTASKQUEUE_H
