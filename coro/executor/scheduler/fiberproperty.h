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
 * @code
 * // 同一线程内 High 先于 Normal、Normal 先于 Low 被取用
 * Coro::makeTask([]{ return urgent(); }, Coro::Priority::High);
 * @endcode
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
 * @code
 * // 一般不直接用枚举，而是用 Affinity 的三个工厂函数
 * auto a = Coro::Affinity::shared();     // AffinityMode::Shared
 * auto b = Coro::Affinity::sticky();     // AffinityMode::Sticky
 * auto c = Coro::Affinity::fixed(std::this_thread::get_id());  // FixedId
 * @endcode
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
 * @code
 * // 纯计算任务：任意工作线程都可执行，吞吐最高
 * Coro::makeTask([]{ return compute(); }, Coro::Priority::Normal,
 *                Coro::Affinity::shared());
 *
 * // 要操作 Qt 对象：固定到该对象所属线程，避免违反 Qt 线程亲和
 * Coro::makeTask([sock]{ sock->write("x"); return 0; }, Coro::Priority::Normal,
 *                Coro::Affinity::fixed(sock->thread()->currentThreadId()));
 * @endcode
 */
struct Affinity {
    /**
     * @brief 创建一个共享模式的线程模型
     * @return 共享模式的 Affinity
     * @code
     * // 协程可被任意空闲工作线程取用；切勿在其中操作 Qt 对象
     * Coro::makeTask([]{ return heavyCompute(); },
     *                Coro::Priority::Normal, Coro::Affinity::shared());
     * @endcode
     */
    static Affinity shared()    { return {AffinityMode::Shared, std::nullopt}; }

    /**
     * @brief 创建一个粘连调度的线程模型
     * @return 粘连模式的 Affinity
     * @code
     * // 首次执行时绑定到某线程，此后固定在该线程上运行
     * Coro::makeTask([]{ return statefulWork(); },
     *                Coro::Priority::Normal, Coro::Affinity::sticky());
     * @endcode
     */
    static Affinity sticky() { return {AffinityMode::Sticky, std::nullopt}; }

    /**
     * @brief 创建一个指定线程调度的线程模型
     * @param id 绑定的线程 id
     * @return 指定线程模式的 Affinity
     * @code
     * // 绑定到当前线程（makeTask 的默认亲和即为此）
     * auto here = Coro::Affinity::fixed(std::this_thread::get_id());
     * Coro::makeTask([]{ return 0; }, Coro::Priority::Normal, here);
     * @endcode
     */
    static Affinity fixed(const std::thread::id& id) {
        return {AffinityMode::FixedId, id};
    }

    /**
     * @brief 相等比较
     * @param other 另一个线程模型
     * @return 模式与依附线程 id 均相同返回 true
     * @code
     * // 判断两个线程模型是否等价
     * bool same = (Coro::Affinity::shared() == Coro::Affinity::shared());
     * @endcode
     */
    bool operator==(const Affinity& other) const{
        return (mode == other.mode) && (fixed_id == other.fixed_id);
    }
    /**
     * @brief 大于比较（先比模式，再比依附线程 id）
     * @param other 另一个线程模型
     * @return 自身大于 other 返回 true
     * @code
     * // 供就绪集合按亲和分桶排序使用
     * bool gt = Coro::Affinity::sticky() > Coro::Affinity::shared();
     * @endcode
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
     * @code
     * std::map<Coro::Affinity, int> buckets;   // 依赖 operator< 排序
     * @endcode
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
 * @code
 * // 就绪协程集合即以 Affinity 为键分桶
 * std::unordered_map<Coro::Affinity, Bucket, Coro::Hash_Affinity> buckets;
 * @endcode
 */
struct Hash_Affinity{
    /**
     * @brief 计算线程模型的哈希值
     * @param affine 线程模型
     * @return 哈希值
     * @code
     * size_t h = Coro::Hash_Affinity{}(Coro::Affinity::shared());
     * @endcode
     */
    size_t operator()(const Affinity& affine) const{
        size_t h1 = std::hash<AffinityMode>()(affine.mode);
        size_t h2 = std::hash<std::optional<std::thread::id>>()(affine.fixed_id);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >>2));//自定义hash函数，减少hash碰撞
    }
};

/**
 * @brief boost::fiber 的属性，用于在 scheduler 中指定某个协程的线程模型和优先级
 * @code
 * // 每个协程都携带一份 MetaContext；launch_properties 内部即用它创建 fiber
 * // 在协程内可读取自身属性：
 * Coro::makeTask([]{
 *     auto prop = boost::this_fiber::properties<Coro::MetaContext>();
 *     qDebug() << int(prop.priority());
 *     return 0;
 * });
 * @endcode
 */
struct MetaContext : boost::fibers::fiber_properties {
    friend class FiberScheduler;
    /**
     * @brief 默认构造
     * @code
     * Coro::MetaContext meta;      // 默认 Normal 优先级 + Shared 亲和
     * @endcode
     */
    explicit MetaContext(): boost::fibers::fiber_properties(nullptr){}
    /**
     * @brief 由上下文构造
     * @param ctx boost fiber 上下文
     * @code
     * Coro::MetaContext meta(ctx);     // 绑定到指定 boost.fiber 上下文
     * @endcode
     */
    explicit MetaContext(boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx) {}
    /**
     * @brief 由优先级、线程模型、上下文构造
     * @param pri 优先级
     * @param affine 线程模型
     * @param ctx boost fiber 上下文
     * @code
     * Coro::MetaContext meta(Coro::Priority::High, Coro::Affinity::shared(), nullptr);
     * @endcode
     */
    explicit MetaContext(Priority pri, Affinity affine, boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx), priority_(pri), affinity_(affine), name_("") {}
    /**
     * @brief 由优先级、线程模型、名称、上下文构造
     * @param pri 优先级
     * @param affine 线程模型
     * @param name 协程名称
     * @param ctx boost fiber 上下文
     * @code
     * // 带名称便于调试；launch_properties 的 name 参数最终传到这里
     * Coro::MetaContext meta(Coro::Priority::High, Coro::Affinity::shared(),
     *                        "io-worker", nullptr);
     * @endcode
     */
    explicit MetaContext(Priority pri, Affinity affine, std::string name, boost::fibers::context* ctx)
        : boost::fibers::fiber_properties(ctx), priority_(pri), affinity_(affine), name_(name) {}
    /**
     * @brief 拷贝赋值（复制上下文、优先级、线程模型与名称）
     * @param meta 被拷贝的源对象
     * @return 自身引用
     * @code
     * Coro::MetaContext a, b;
     * b = a;                       // 就绪队列中转移属性时使用
     * @endcode
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
     * @code
     * auto prop = boost::this_fiber::properties<Coro::MetaContext>();
     * Coro::MetaContext copy(prop);    // 继承当前协程的优先级与亲和
     * @endcode
     */
    MetaContext(const MetaContext& other): boost::fibers::fiber_properties(other.ctx_), priority_(other.priority_), affinity_(other.affinity_), name_(other.name_){
    }

    /**
     * @brief 设置 fiber 的优先级和线程模型，属性发生改变时通知 scheduler 变更
     * @warning fiber 运行后不能更改线程模型，fiber 可能调度至其他线程，
     *          导致 fiber 中创建的对象跨线程调用
     * @param pri 优先级
     * @param affine 线程模型
     * @code
     * // 仅建议在协程启动前设置；运行期改亲和不受支持（见 RX_OTHER_AFFINITY）
     * meta.setProperties(Coro::Priority::High, Coro::Affinity::sticky());
     * @endcode
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
     * @code
     * meta.setPriority(Coro::Priority::High);   // 变更后调度器重新入队
     * @endcode
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
     * @code
     * // 应在协程启动前设置；运行期迁移线程不受支持
     * meta.setAffinity(Coro::Affinity::fixed(std::this_thread::get_id()));
     * @endcode
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
     * @code
     * meta.setName("io-worker");        // 便于调试时区分协程
     * @endcode
     * @param name 名称
     */
    void setName(const std::string& name){
        name_ = name;
    }
    /**
     * @brief 读取 fiber 的优先级
     * @code
     * auto prop = boost::this_fiber::properties<Coro::MetaContext>();
     * if(prop.priority() == Coro::Priority::High) fastPath();
     * @endcode
     * @return 优先级
     */
    Priority priority() const{
        return priority_;
    }
    /**
     * @brief 读取 fiber 的线程模型
     * @code
     * // Generator 即以此继承当前协程的调度属性
     * auto prop = boost::this_fiber::properties<Coro::MetaContext>();
     * Coro::launch_properties(fn, prop.priority(), prop.affinity());
     * @endcode
     * @return 线程模型
     */
    const Affinity affinity() const{
        return affinity_;
    }
    /**
     * @brief 读取 fiber 名称
     * @code
     * qDebug() << QString::fromStdString(meta.name());
     * @endcode
     * @return 名称
     */
    const std::string name() const{
        return name_;
    }
    /**
     * @brief 读取关联的 boost fiber 上下文
     * @code
     * // 调度器据此把协程 attach 到本线程并恢复执行
     * boost::fibers::context* ctx = meta.context();
     * @endcode
     * @return 上下文指针
     */
    boost::fibers::context* context() const {
        return boost::fibers::fiber_properties::ctx_;
    }
    /**
     * @brief 小于比较（先比优先级，再比上下文地址）
     * @code
     * bool lt = (metaLow < metaHigh);      // 供有序容器排序
     * @endcode
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
     * @code
     * // 就绪集合用 std::set<MetaContext, std::greater<>> 实现"高优先级先出"
     * std::set<Coro::MetaContext, std::greater<Coro::MetaContext>> ready;
     * @endcode
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
     * @code
     * bool same = (a == b);       // 同一上下文且优先级相同
     * @endcode
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
