#ifndef FIBERPROPERTY_H
#define FIBERPROPERTY_H
#include <boost/fiber/properties.hpp>
#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/context.hpp>
#include <optional>
#include <thread>
#include <unordered_map>

namespace Coro {

/**
 * @brief 优先级标识
 */
enum class Priority : uint8_t {
    Low      = 0,///< 低优先级
    Normal   = 1,///< 普通优先级
    High     = 2,///< 高优先级
};

/**
 * @brief 线程调度模式
 *
 * - Shared：共享调度，Fiber 就绪时调度器将其分配至任一可用线程中执行；
 * - Sticky：粘连调度，Fiber 总是调度至同一个线程执行，创建时分配至任一线程并绑定；
 * - FixedId：指定线程调度，调度器将 Fiber 分配至指定线程 Id 的调度器中执行。
 */
enum class AffinityMode {
    Shared, // 共享调度
    Sticky, // 粘连调度
    FixedId // 指定线程调度
};

/**
 * @brief 线程模型。
 *
 * 依据线程调度模式枚举对象的数据结构，包括 AffinityMode 与依附线程的 id。
 */
struct Affinity {
    /**
     * @brief 创建一个共享模式的线程模型
     * @return 共享模式的 Affinity
     */
    static Affinity shared()    { return {AffinityMode::Shared, std::nullopt}; }

    /**
     * @brief 创建一个粘连调度的线程模型
     * @return 粘连模式的 Affinity
     */
    static Affinity sticky() { return {AffinityMode::Sticky, std::nullopt}; }

    /**
     * @brief 创建一个指定线程调度的线程模型
     * @param id 绑定的线程 id
     * @return 指定线程模式的 Affinity
     */
    static Affinity fixed(const std::thread::id& id) {
        return {AffinityMode::FixedId, id};
    }

    /**
     * @brief 相等比较
     * @param other 另一个线程模型
     * @return 模式与依附线程 id 均相同返回 true
     */
    bool operator==(const Affinity& other) const{
        return (mode == other.mode) && (fixed_id == other.fixed_id);
    }
    /**
     * @brief 大于比较（先比模式，再比依附线程 id）
     * @param other 另一个线程模型
     * @return 自身大于 other 返回 true
     */
    bool operator>(const Affinity& other) const{
        if(this->mode != other.mode){
            return this->mode > other.mode;
        }else{
            return this->fixed_id > other.fixed_id;
        }
    }
    /**
     * @brief 小于比较（先比模式，再比依附线程 id）
     * @param other 另一个线程模型
     * @return 自身小于 other 返回 true
     */
    bool operator<(const Affinity& other) const{
        if(this->mode != other.mode){
            return this->mode < other.mode;
        }else{
            return this->fixed_id < other.fixed_id;
        }
    }
    AffinityMode                   mode{AffinityMode::Shared};///< 调度模式
    std::optional<std::thread::id> fixed_id;///< 依附线程的线程 id
};

/**
 * @brief Affinity 的哈希函数对象，供以 Affinity 为键的哈希容器使用
 */
struct Hash_Affinity{
    /**
     * @brief 计算线程模型的哈希值
     * @param affine 线程模型
     * @return 哈希值
     */
    size_t operator()(const Affinity& affine) const{
        size_t h1 = std::hash<AffinityMode>()(affine.mode);
        size_t h2 = std::hash<std::optional<std::thread::id>>()(affine.fixed_id);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >>2));//自定义hash函数，减少hash碰撞
    }
};

/**
 * @brief boost::fiber 的属性，用于在 scheduler 中指定某个协程的线程模型和优先级
 */
struct MetaContext : boost::fibers::fiber_properties {
    friend class FiberScheduler;
    /** @brief 默认构造 */
    explicit MetaContext(): boost::fibers::fiber_properties(nullptr){}
    /**
     * @brief 由上下文构造
     * @param ctx boost fiber 上下文
     */
    explicit MetaContext(boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx) {}
    /**
     * @brief 由优先级、线程模型、上下文构造
     * @param pri 优先级
     * @param affine 线程模型
     * @param ctx boost fiber 上下文
     */
    explicit MetaContext(Priority pri, Affinity affine, boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx), priority_(pri), affinity_(affine), name_("") {}
    /**
     * @brief 由优先级、线程模型、名称、上下文构造
     * @param pri 优先级
     * @param affine 线程模型
     * @param name 协程名称
     * @param ctx boost fiber 上下文
     */
    explicit MetaContext(Priority pri, Affinity affine, std::string name, boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx), priority_(pri), affinity_(affine), name_(name) {}
    /**
     * @brief 拷贝赋值（复制上下文、优先级、线程模型与名称）
     * @param meta 被拷贝的源对象
     * @return 自身引用
     */
    MetaContext& operator=(const MetaContext& meta){
        if(this != &meta){
            this->ctx_ = meta.ctx_;
            this->priority_ = meta.priority_;
            this->affinity_ = meta.affinity_;
            this->name_ = meta.name_;
        }
        return *this;
    }
    /**
     * @brief 拷贝构造
     * @param other 被拷贝的源对象
     */
    MetaContext(const MetaContext& other): boost::fibers::fiber_properties(other.ctx_), priority_(other.priority_), affinity_(other.affinity_), name_(other.name_){
    }

    /**
     * @brief 设置 fiber 的优先级和线程模型，属性发生改变时通知 scheduler 变更
     * @warning fiber 运行后不能更改线程模型，fiber 可能调度至其他线程，
     *          导致 fiber 中创建的对象跨线程调用
     * @param pri 优先级
     * @param affine 线程模型
     */
    void setProperties(const Priority pri, const Affinity& affine){
        if(pri == priority_ && affinity_ == affine){
            return;
        }
        priority_ = pri;
        affinity_ = affine;
        notify();//通知scheduler
    }
    /**
     * @brief 设置 fiber 的优先级，属性发生改变时通知 scheduler 变更
     * @param pri 优先级
     */
    void setPriority(const Priority pri){
        if(pri == priority_){
            return;
        }
        priority_ = pri;
        notify();//通知scheduler
    }
    /**
     * @brief 设置 fiber 的线程模型，属性发生改变时通知 scheduler 变更
     * @warning fiber 运行后不建议更改线程模型，fiber 可能调度至其他线程，
     *          导致 fiber 中创建的对象跨线程调用
     * @param affine 线程模型
     */
    void setAffinity(const Affinity& affine){
        if(affinity_ == affine){
            return;
        }
        affinity_ = affine;
        notify();//通知scheduler
    }
    /**
     * @brief 设置 fiber 名称
     * @param name 名称
     */
    void setName(const std::string& name){
        name_ = name;
    }
    /**
     * @brief 读取 fiber 的优先级
     * @return 优先级
     */
    Priority priority() const{
        return priority_;
    }
    /**
     * @brief 读取 fiber 的线程模型
     * @return 线程模型
     */
    const Affinity affinity() const{
        return affinity_;
    }
    /**
     * @brief 读取 fiber 名称
     * @return 名称
     */
    const std::string name() const{
        return name_;
    }
    /**
     * @brief 读取关联的 boost fiber 上下文
     * @return 上下文指针
     */
    boost::fibers::context* context() const {
        return boost::fibers::fiber_properties::ctx_;
    }
    /**
     * @brief 小于比较（先比优先级，再比上下文地址）
     * @param other 另一个属性
     * @return 自身小于 other 返回 true
     */
    bool operator<(const MetaContext& other) const{
        if(this->priority_ != other.priority_){
            return this->priority_ < other.priority_;
        }else{
            return this->ctx_ < other.ctx_;
        }
    }
    /**
     * @brief 大于比较（先比优先级，再比上下文地址），供就绪集合按优先级降序排序
     * @param other 另一个属性
     * @return 自身大于 other 返回 true
     */
    bool operator>(const MetaContext& other) const{
        if(this->priority_ != other.priority_){
            return this->priority_ > other.priority_;
        }else{
            return this->ctx_ > other.ctx_;
        }
    }
    /**
     * @brief 相等比较（优先级与上下文均相同）
     * @param other 另一个属性
     * @return 相等返回 true
     */
    bool operator==(const MetaContext& other) const{
        return this->priority_ == other.priority_ && this->ctx_ == other.ctx_;
    }

private:
    Priority                       priority_{Priority::Normal};///< 优先级
    Affinity                       affinity_{};///< 线程模型
    std::string                    name_{""};///< 名称
};

}
#endif // FIBERPROPERTY_H
