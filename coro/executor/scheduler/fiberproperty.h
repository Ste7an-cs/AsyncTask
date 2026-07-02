#ifndef FIBERPROPERTY_H
#define FIBERPROPERTY_H
#include <boost/fiber/properties.hpp>
#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/context.hpp>
#include <optional>
#include <thread>
#include <unordered_map>

namespace Coro {

///
/// \brief The Priority enum 优先级标识
///
enum class Priority : uint8_t {
    Low      = 0,
    Normal   = 1,
    High     = 2,
};

///
/// \brief The AffinityMode enum 线程调度模式
/// \value Shared 共享调度，Fiber就绪时，调度器将分配至任一可用的线程中执行
/// \value Sticky 粘连调度，Fiber总是调度至同一个线程中执行，在创建时分配至任一线程中执行，并绑定该线程
/// \value FixedId 指定线程调度，调度器将Fiber分配至指定线程Id的调度器中执行
///
enum class AffinityMode {
    Shared, // 共享调度
    Sticky, // 粘连调度
    FixedId // 指定线程调度
};

///
/// \brief The Affinity struct 线程模型
/// 依据线程调度模式枚举对象的数据结构，包括AffinityMode和依附线程的id
///
struct Affinity {
    ///
    /// \brief shared 创建一个共享模式的线程模型
    /// \return
    ///
    static Affinity shared()    { return {AffinityMode::Shared, std::nullopt}; }

    ///
    /// \brief sticky 创建一个粘连调度的线程模型
    /// \return
    ///
    static Affinity sticky() { return {AffinityMode::Sticky, std::nullopt}; }

    ///
    /// \brief fixed 创建一个指定线程调度的线程模型
    /// \param id 绑定的线程id
    /// \return
    ///
    static Affinity fixed(const std::thread::id& id) {
        return {AffinityMode::FixedId, id};
    }

    bool operator==(const Affinity& other) const{
        return (mode == other.mode) && (fixed_id == other.fixed_id);
    }
    bool operator>(const Affinity& other) const{
        if(this->mode != other.mode){
            return this->mode > other.mode;
        }else{
            return this->fixed_id > other.fixed_id;
        }
    }
    bool operator<(const Affinity& other) const{
        if(this->mode != other.mode){
            return this->mode < other.mode;
        }else{
            return this->fixed_id < other.fixed_id;
        }
    }
    AffinityMode                   mode{AffinityMode::Shared};//调度模式
    std::optional<std::thread::id> fixed_id;//依附线程的线程id
};

struct Hash_Affinity{
    size_t operator()(const Affinity& affine) const{
        size_t h1 = std::hash<AffinityMode>()(affine.mode);
        size_t h2 = std::hash<std::optional<std::thread::id>>()(affine.fixed_id);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >>2));//自定义hash函数，减少hash碰撞
    }
};

///
/// \brief The MetaContext struct boost::fiber的属性
///     用于在scheduler中指定某个线程的线程模型和优先级
///
struct MetaContext : boost::fibers::fiber_properties {
    friend class FiberScheduler;
    ///构造函数
    explicit MetaContext(): boost::fibers::fiber_properties(nullptr){}
    explicit MetaContext(boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx) {}
    explicit MetaContext(Priority pri, Affinity affine, boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx), priority_(pri), affinity_(affine), name_("") {}
    explicit MetaContext(Priority pri, Affinity affine, std::string name, boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx), priority_(pri), affinity_(affine), name_(name) {}
    MetaContext& operator=(const MetaContext& meta){
        if(this != &meta){
            this->ctx_ = meta.ctx_;
            this->priority_ = meta.priority_;
            this->affinity_ = meta.affinity_;
            this->name_ = meta.name_;
        }
        return *this;
    }
    MetaContext(const MetaContext& other): boost::fibers::fiber_properties(other.ctx_), priority_(other.priority_), affinity_(other.affinity_), name_(other.name_){
    }

    ///
    /// \brief setProperties  设置fiber的优先级和线程模型
    ///     属性发生改变时，通知scheduler变更
    /// \warning fiber运行后不能更改线程模型，fiber可能调度至其他线程，导致fiber中创建的对象跨线程调用
    /// \param pri              优先级
    /// \param affine           线程模型
    ///
    void setProperties(const Priority pri, const Affinity& affine){
        if(pri == priority_ && affinity_ == affine){
            return;
        }
        priority_ = pri;
        affinity_ = affine;
        notify();//通知scheduler
    }
    ///
    /// \brief setPriority  设置fiber的优先级
    ///     属性发生改变时，通知scheduler变更
    /// \param pri              优先级
    ///
    void setPriority(const Priority pri){
        if(pri == priority_){
            return;
        }
        priority_ = pri;
        notify();//通知scheduler
    }
    ///
    /// \brief setAffinity  设置fiber的线程模型
    ///     属性发生改变时，通知scheduler变更
    /// \warning fiber运行后不建议更改线程模型，fiber可能调度至其他线程，导致fiber中创建的对象跨线程调用
    /// \param affine           线程模型
    ///
    void setAffinity(const Affinity& affine){
        if(affinity_ == affine){
            return;
        }
        affinity_ = affine;
        notify();//通知scheduler
    }
    void setName(const std::string& name){
        name_ = name;
    }
    ///
    /// \brief priority 读取fiber的优先级
    /// \return
    ///
    Priority priority() const{
        return priority_;
    }
    ///
    /// \brief affinity 读取fiber的线程模型
    /// \return
    ///
    const Affinity affinity() const{
        return affinity_;
    }
    const std::string name() const{
        return name_;
    }
    boost::fibers::context* context() const {
        return boost::fibers::fiber_properties::ctx_;
    }
    bool operator<(const MetaContext& other) const{
        if(this->priority_ != other.priority_){
            return this->priority_ < other.priority_;
        }else{
            return this->ctx_ < other.ctx_;
        }
    }
    bool operator>(const MetaContext& other) const{
        if(this->priority_ != other.priority_){
            return this->priority_ > other.priority_;
        }else{
            return this->ctx_ > other.ctx_;
        }
    }
    bool operator==(const MetaContext& other) const{
        return this->priority_ == other.priority_ && this->ctx_ == other.ctx_;
    }

private:
    Priority                       priority_{Priority::Normal};
    Affinity                       affinity_{};
    std::string                    name_{""};//名称
};

}
#endif // FIBERPROPERTY_H
