#ifndef AWAITABLE_HPP
#define AWAITABLE_HPP

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include "detail/fiberchannel.hpp"
#include "detail/channelhub.hpp"
#include "detail/result.hpp"

namespace Coro {

/**
 * @brief 异步等待器，生产者/消费者模型。
 *
 * 生产者通过 channel() 拿到共享的数据流分发端（ChannelHub）并 push 消息，
 * 消费者 await 等待消息。跨线程安全（由 FiberChannel/ChannelHub 保证）。
 *
 * 该类与具体来源(Qt/std)解耦：持有一个共享 hub 与一条自己独占的队列。工厂层
 * 通过 setOnClose 注入清理逻辑(如断开信号)，最后一个消费者句柄消失时自动执行
 * 一次，从而实现及时取消订阅。关闭且已排队值耗尽后，消费者只能观察到首次
 * 记录的终止错误。
 *
 * move-only，按值传递；hub 为 shared_ptr，生产者只捕获 channel() 而不持有
 * 整个 Awaitable，避免引用环。
 *
 * @tparam T 等待/传递的数据类型
 * @code
 * // 1) 作为等待载体：由 coro() 工厂产出，用 await 顺序取值
 * auto r = Coro::await(Coro::coro(obj, &Obj::valueChanged));
 *
 * // 2) 作为生产者/消费者通道：生产者只捕获 channel()，避免引用环
 * Coro::Awaitable<int> a;
 * auto prod = Coro::makeTask([ch = a.channel()]{
 *     for(int i = 0; i < 10; i++) ch->push(i);
 *     ch->close();
 *     return 0;
 * });
 * auto cons = Coro::makeTask([&a]{
 *     while(auto v = a.await()) qDebug() << v.value();
 *     return 0;
 * });
 * @endcode
 */
template<typename T>
class Awaitable{
    /** @brief 订阅构造的标记类型：私有，使订阅构造函数无法被外部调用 */
    struct SubscribeTag{};

    std::shared_ptr<ChannelHub<T>> hub_{std::make_shared<ChannelHub<T>>()};
    std::shared_ptr<FiberChannel<T>> queue_{std::make_shared<FiberChannel<T>>()};
public:
    /** @brief 默认构造：新建一条数据流，并把自己的队列挂上去 */
    Awaitable(){ hub_->attach(queue_); }
    /**
     * @brief 订阅构造：复用已有的 hub，挂一条属于自己的新队列。
     * @details 仅供 shared() 使用——SubscribeTag 是私有类型，外部无法构造。
     * @param hub 要订阅的数据流分发端
     */
    Awaitable(std::shared_ptr<ChannelHub<T>> hub, SubscribeTag)
        : hub_(std::move(hub)){
        if(hub_) hub_->attach(queue_);
    }
    /** @brief 析构：摘除自己那条队列；若消费者就此归零，hub 会执行一次清理 */
    ~Awaitable(){
        if(hub_ && queue_) hub_->detach(queue_.get());
    }
    /**
     * @brief 移动构造（move-only）
     * @param other 被移动的源对象
     */
    Awaitable(Awaitable&& other) noexcept = default;
    /**
     * @brief 移动赋值（move-only）
     * @details 先摘除自己原有的队列，再接管 other 的 hub 与队列。摘除不能省：
     *          若本句柄是原数据流的最后一个消费者，仅释放 shared_ptr 不会触发 hub
     *          的归零判定（归零只在 detach/notifyClosed 里做，push 不做），原流的
     *          清理钩子将永不执行、上游连接迟迟不断。
     * @param other 被移动的源对象
     * @return 自身引用
     */
    Awaitable& operator=(Awaitable&& other) noexcept {
        if(this != &other){
            if(hub_ && queue_) hub_->detach(queue_.get());
            hub_ = std::move(other.hub_);
            queue_ = std::move(other.queue_);
        }
        return *this;
    }
    /** @brief 禁止拷贝构造（避免多个持有者语义混乱） */
    Awaitable(const Awaitable&) = delete ;
    /** @brief 禁止拷贝赋值 */
    Awaitable& operator=(const Awaitable&) = delete ;

    /**
     * @brief 生产者侧共享的分发端。生产者只捕获它、不持有整个 Awaitable。
     * @return 内部数据流分发端的 shared_ptr
     * @code
     * Coro::Awaitable<QByteArray> a;
     * QObject::connect(dev, &QIODevice::readyRead, [ch = a.channel(), dev]{
     *     ch->push(dev->readAll());
     * });
     * @endcode
     */
    std::shared_ptr<ChannelHub<T>> channel() const { return hub_; }

    /**
     * @brief 注册一个共享订阅者，此后源产生的每个值都会同步复制一份投递给它。
     *
     * 返回的是普通 Awaitable，因此 Coro::await / await_for / generate 均原样可用。
     * 订阅者与源在消费者表上没有身份差别：各得全量、互不竞争，句柄析构即自动退订。
     * 不做 replay：本次调用之前已产生的值对订阅者不可见。
     * 源句柄析构不会终止订阅者——上游活到最后一个句柄消失为止。
     * @return 共享订阅句柄；数据流已关闭时返回的句柄立即以其终止原因收敛
     * @code
     * auto src = Coro::coro(sock).readAll();
     * auto sync = src->shared();
     * auto audit = src->shared();
     * @endcode
     */
    std::shared_ptr<Awaitable<T>> shared(){
        return std::make_shared<Awaitable<T>>(hub_, SubscribeTag{});
    }

    /**
     * @brief 查询自己这一路是否已关闭。
     * @details 查的是本句柄独占的队列，而非整条流——订阅者关掉自己不影响别人。
     * @return 自己这一路已关闭返回 true
     * @code
     * while(!a.isClosed()) produce(a);
     * @endcode
     */
    bool isClosed() const { return queue_ && queue_->is_closed(); }

    /**
     * @brief 设置整条数据流（hub）的容量上限，超出时丢弃队首最旧的值。
     * @details 作用于 hub 而非本句柄的队列：会级联给该流当前所有存活的消费者队列，
     *          并作为后续新订阅者（shared()）的默认容量。透传给内部
     *          ChannelHub::setCapacity()，详见其文档。
     * @param capacity 新的容量上限，0 表示无限
     * @code
     * a.setCapacity(0);    // 承载 QTcpSocket* 等自身即资源的值时，取消丢弃
     * @endcode
     */
    void setCapacity(std::uint32_t capacity){
        if(hub_) hub_->setCapacity(capacity);
    }
    /**
     * @brief 查询整条数据流（hub）当前的容量上限
     * @return 容量上限；0 表示无限
     * @code
     * if(a.capacity() != 0) qDebug() << "有界队列，上限" << a.capacity();
     * @endcode
     */
    std::uint32_t capacity() const {
        return hub_ ? hub_->capacity() : 0;
    }

    /**
     * @brief 注册整条数据流关闭时执行一次的清理钩子（消费者归零时触发）。
     *
     * 钩子挂在 hub 上、属于整条流，而非某一个消费者句柄：当消费者归零（表中不再
     * 存在既存活又未关闭的队列）时执行一次，用于及时取消订阅（如断开上游信号）。
     * 与 Qt 解耦：仅保存 std::function，不含任何 Qt 类型。
     *
     * @warning 替换旧回调时，旧回调会在**锁外立即执行**；若数据流已关闭，传入的新
     *          回调同样会在锁外立即执行。因此在 shared() 得到的订阅句柄上调用
     *          setOnClose 会当场跑掉工厂注入的清理逻辑（如 disconnectAll），
     *          静默掐断整条流——任一句柄设置都会替换并立即执行前一个回调，
     *          不区分是源句柄还是订阅句柄。
     * @param fn 清理回调（如断开信号连接）
     * @code
     * // 扩展自定义来源时：把断连清理挂到 Awaitable 的收尾钩子
     * Coro::Awaitable<QByteArray> a;
     * auto conn = std::make_shared<QMetaObject::Connection>();
     * *conn = QObject::connect(dev, &QIODevice::readyRead,
     *                          [ch = a.channel(), dev]{ ch->push(dev->readAll()); });
     * a.setOnClose([conn]{ QObject::disconnect(*conn); });   // 消费者归零时断开
     * @endcode
     */
    void setOnClose(std::function<void()> fn){
        if(hub_) hub_->setOnClose(std::move(fn));
    }

    /**
     * @brief 等待一条消息（无数据时让出当前协程，不阻塞线程）
     * @return 取到数据返回 Result 值；队列关闭后返回首次终止错误，默认关闭为 no_message
     * @code
     * Coro::makeTask([&a]{
     *     while(auto v = a.await()){        // 关闭后循环自然结束
     *         use(v.value());
     *     }
     *     return 0;
     * });
     * @endcode
     */
    Result<T, std::error_code> await(){
        if(queue_){
            T value{};
            auto status = queue_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::no_message);
    }
    /**
     * @brief 等待一条消息，最长等待 timeout 时长
     * @tparam Rep 时长的计数类型
     * @tparam Period 时长的周期类型
     * @param timeout 最长等待时长
     * @return 取到数据返回 Result 值；超时返回 timed_out，关闭返回首次终止错误
     * @code
     * auto r = a.await_for(std::chrono::milliseconds(500));
     * if(!r && r.error() == std::make_error_code(std::errc::timed_out)){
     *     // 仅本次等待到期：来源未被取消，可继续等
     * }
     * @endcode
     */
    template<typename Rep, typename Period>
    Result<T, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(queue_){
            T value{};
            auto status = queue_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::timed_out);
    }

    /**
     * @brief 生产者侧投递一条数据
     * @param value 待投递的数据
     * @return 成功入队返回 true；数据流不存在或已关闭返回 false
     * @code
     * Coro::Awaitable<int> a;
     * a.resolve(42);                       // 消费侧 a.await() 即可取到 42
     * @endcode
     */
    bool resolve(const T& value){
        if(hub_){
            if(hub_->is_closed()){
                return false;
            }
            return (boost::fibers::channel_op_status::success == hub_->push(value));
        }
        return false;
    }
    /**
     * @brief 关闭自己这一路，唤醒本路的等待者
     * @code
     * a.close();       // 正常终止：已排队值仍先被消费，随后得到 no_message
     * @endcode
     */
    void close(){
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭自己这一路并记录终止原因，唤醒本路的等待者。
     * @details 只作用于本句柄的队列，源与其他订阅者不受影响。若本路是最后一条
     *          未关闭的消费者，hub 随之关闭并执行一次清理（断开上游）。
     * @param error 终止原因
     * @code
     * a.close(std::make_error_code(std::errc::connection_reset));
     * @endcode
     */
    void close(std::error_code error){
        if(queue_) queue_->close(error);
        if(hub_) hub_->notifyClosed(error);
    }
};

/**
 * @brief 异步等待器 void 特化。
 *
 * 无数据负载，仅表达"事件发生一次"；内部用 ChannelHub<int> / FiberChannel<int>
 * 承载信号。关闭且已排队值耗尽后，消费者只能观察到首次记录的终止错误；生命周期
 * 清理在最后一个消费者句柄消失时执行一次。
 * @code
 * // 等待"某事发生一次"，无数据负载；结果可直接当 bool 用
 * if(Coro::await(Coro::coro(sock).waitForConnected())){
 *     // 已连接
 * }
 * // 等待无参信号同样得到 Awaitable<void>
 * Coro::await(Coro::coro(&timer, &QTimer::timeout));
 * @endcode
 */
template<>
class Awaitable<void>{
    struct SubscribeTag{};

    std::shared_ptr<ChannelHub<int>> hub_{std::make_shared<ChannelHub<int>>()};
    std::shared_ptr<FiberChannel<int>> queue_{std::make_shared<FiberChannel<int>>()};
public:
    /** @brief 默认构造：新建一条数据流，并把自己的队列挂上去 */
    Awaitable(){ hub_->attach(queue_); }
    /**
     * @brief 订阅构造：复用已有的 hub，挂一条属于自己的新队列。
     * @details 仅供 shared() 使用——SubscribeTag 是私有类型，外部无法构造。
     * @param hub 要订阅的数据流分发端
     */
    Awaitable(std::shared_ptr<ChannelHub<int>> hub, SubscribeTag)
        : hub_(std::move(hub)){
        if(hub_) hub_->attach(queue_);
    }
    /** @brief 析构：摘除自己那条队列；若消费者就此归零，hub 会执行一次清理 */
    ~Awaitable(){
        if(hub_ && queue_) hub_->detach(queue_.get());
    }
    /**
     * @brief 移动构造（move-only）
     * @param other 被移动的源对象
     */
    Awaitable(Awaitable&& other) noexcept = default;
    /**
     * @brief 移动赋值（move-only）
     * @details 先摘除自己原有的队列，再接管 other 的 hub 与队列。摘除不能省：
     *          若本句柄是原数据流的最后一个消费者，仅释放 shared_ptr 不会触发 hub
     *          的归零判定（归零只在 detach/notifyClosed 里做，push 不做），原流的
     *          清理钩子将永不执行、上游连接迟迟不断。
     * @param other 被移动的源对象
     * @return 自身引用
     */
    Awaitable& operator=(Awaitable&& other) noexcept {
        if(this != &other){
            if(hub_ && queue_) hub_->detach(queue_.get());
            hub_ = std::move(other.hub_);
            queue_ = std::move(other.queue_);
        }
        return *this;
    }
    /** @brief 禁止拷贝构造 */
    Awaitable(const Awaitable&) = delete ;
    /** @brief 禁止拷贝赋值 */
    Awaitable& operator=(const Awaitable&) = delete ;

    /**
     * @brief 生产者侧共享的分发端
     * @return 内部数据流分发端的 shared_ptr
     * @code
     * Coro::Awaitable<void> a;
     * // void 特化内部用 ChannelHub<int> 承载"发生一次"，push 任意值即可
     * QObject::connect(obj, &Obj::done, [ch = a.channel()]{ ch->push(1); });
     * @endcode
     */
    std::shared_ptr<ChannelHub<int>> channel() const { return hub_; }

    /**
     * @brief 注册一个共享订阅者，此后每次 resolve() 都会同步通知它一次。
     *
     * 语义与 Awaitable<T>::shared() 相同：订阅者与源在消费者表上没有身份差别，
     * 各得全量、互不竞争，不做 replay，句柄析构即自动退订。
     * 源句柄析构不会终止订阅者——上游活到最后一个句柄消失为止。
     * @return 共享订阅句柄；数据流已关闭时返回的句柄立即以其终止原因收敛
     * @code
     * Coro::Awaitable<void> done;
     * auto watcher = done.shared();     // 与直接 await(done) 的消费者各得一份
     * @endcode
     */
    std::shared_ptr<Awaitable<void>> shared(){
        return std::make_shared<Awaitable<void>>(hub_, SubscribeTag{});
    }

    /**
     * @brief 查询自己这一路是否已关闭。
     * @details 查的是本句柄独占的队列，而非整条流——订阅者关掉自己不影响别人。
     * @return 自己这一路已关闭返回 true
     * @code
     * while(!a.isClosed()) produce(a);
     * @endcode
     */
    bool isClosed() const { return queue_ && queue_->is_closed(); }

    /**
     * @brief 设置整条数据流（hub）的容量上限，超出时丢弃队首最旧的值。
     * @details 作用于 hub 而非本句柄的队列：会级联给该流当前所有存活的消费者队列，
     *          并作为后续新订阅者（shared()）的默认容量。透传给内部
     *          ChannelHub::setCapacity()，详见其文档。
     * @param capacity 新的容量上限，0 表示无限
     * @code
     * a.setCapacity(0);    // 取消丢弃限制
     * @endcode
     */
    void setCapacity(std::uint32_t capacity){
        if(hub_) hub_->setCapacity(capacity);
    }
    /**
     * @brief 查询整条数据流（hub）当前的容量上限
     * @return 容量上限；0 表示无限
     * @code
     * if(a.capacity() != 0) qDebug() << "有界队列，上限" << a.capacity();
     * @endcode
     */
    std::uint32_t capacity() const {
        return hub_ ? hub_->capacity() : 0;
    }

    /**
     * @brief 注册整条数据流关闭时执行一次的清理钩子（消费者归零时触发）。
     * @details 钩子挂在 hub 上、属于整条流，而非某一个消费者句柄：当消费者归零
     *          （表中不再存在既存活又未关闭的队列）时执行一次。
     * @warning 替换旧回调时，旧回调会在**锁外立即执行**；若数据流已关闭，传入的新
     *          回调同样会在锁外立即执行。因此在 shared() 得到的订阅句柄上调用
     *          setOnClose 会当场跑掉工厂注入的清理逻辑（如 disconnectAll），
     *          静默掐断整条流——任一句柄设置都会替换并立即执行前一个回调。
     * @param fn 清理回调
     * @code
     * Coro::Awaitable<void> a;
     * auto conn = std::make_shared<QMetaObject::Connection>();
     * *conn = QObject::connect(obj, &Obj::done, [ch = a.channel()]{ ch->push(1); });
     * a.setOnClose([conn]{ QObject::disconnect(*conn); });
     * @endcode
     */
    void setOnClose(std::function<void()> fn){
        if(hub_) hub_->setOnClose(std::move(fn));
    }

    /**
     * @brief 等待事件发生一次（无数据时让出协程）
     * @return 事件到达返回成功 Result；队列关闭后返回首次终止错误，默认关闭为 no_message
     * @code
     * Coro::makeTask([sock]{
     *     if(Coro::coro(sock).waitForConnected()->await()) startWork();
     *     return 0;
     * });
     * @endcode
     */
    Result<void, std::error_code> await(){
        if(queue_){
            int value{};
            auto status = queue_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::no_message);
    }

    /**
     * @brief 等待事件发生一次，最长等待 timeout 时长
     * @tparam Rep 时长的计数类型
     * @tparam Period 时长的周期类型
     * @param timeout 最长等待时长
     * @return 事件到达返回成功 Result；超时返回 timed_out，关闭返回首次终止错误
     * @code
     * auto ok = Coro::coro(sock).waitForConnected()->await_for(std::chrono::seconds(2));
     * if(!ok) qWarning() << ok.error().message().c_str();
     * @endcode
     */
    template<typename Rep, typename Period>
    Result<void, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(queue_){
            int value{};
            auto status = queue_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return queue_->close_error();
        }
        return std::make_error_code(std::errc::timed_out);
    }

    /**
     * @brief 生产者侧发出一次"事件发生"信号
     * @return 成功入队返回 true；数据流不存在或已关闭返回 false
     * @code
     * Coro::Awaitable<void> a;
     * a.resolve();          // 通知"事件发生一次"，等待方的 await() 随即返回成功
     * @endcode
     */
    bool resolve(void){
        if(hub_){
            if(hub_->is_closed()){
                return false;
            }
            // void 特化底层同样是有界 ChannelHub<int>（默认上限 kDefaultCapacity），
            // 承载的是"事件发生一次"这种不可区分的标记，push 溢出时与其他类型一样会
            // 丢弃队首最旧的标记。标记彼此没有区别，丢掉一个只丢失一次计数，
            // 不会像 T 类型那样丢失"哪一次事件"的信息，因此这里不需要特殊处理。
            return (boost::fibers::channel_op_status::success == hub_->push(1));
        }
        return false;
    }
    /**
     * @brief 关闭自己这一路，唤醒本路的等待者
     * @code
     * a.close();       // 正常终止：已排队值仍先被消费，随后得到 no_message
     * @endcode
     */
    void close(){
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭自己这一路并记录终止原因，唤醒本路的等待者。
     * @details 只作用于本句柄的队列，源与其他订阅者不受影响。若本路是最后一条
     *          未关闭的消费者，hub 随之关闭并执行一次清理（断开上游）。
     * @param error 终止原因
     * @code
     * a.close(std::make_error_code(std::errc::connection_reset));
     * @endcode
     */
    void close(std::error_code error){
        if(queue_) queue_->close(error);
        if(hub_) hub_->notifyClosed(error);
    }
};

/**
 * @brief 消费一个 Awaitable：取一次消息（左值重载，具名可反复取）。
 * @tparam T 等待/传递的数据类型
 * @param a 待消费的等待器
 * @return 取到数据返回 Result 值；来源关闭返回首次终止错误，默认关闭为 no_message
 * @code
 * // 具名 Awaitable：建一次、反复取（推荐用法）
 * auto stream = Coro::coro(obj, &Obj::valueChanged);
 * auto first  = Coro::await(stream);
 * auto second = Coro::await(stream);
 * @endcode
 */
template<typename T>
Result<T> await(Awaitable<T>& a){
    return a.await();
}
/**
 * @brief 消费一个临时 Awaitable：取一次消息（右值重载）。
 * @tparam T 等待/传递的数据类型
 * @param a 待消费的等待器（右值临时对象）
 * @return 取到数据返回 Result 值；来源关闭返回首次终止错误，默认关闭为 no_message
 * @code
 * // 一次性等待：直接消费临时对象
 * auto r = Coro::await(Coro::coro(obj, &Obj::finished));
 * @endcode
 */
template<typename T>
Result<T> await(Awaitable<T>&& a){
    return a.await();
}
/**
 * @brief 通过共享句柄消费一个 Awaitable：取一次消息。
 * @tparam T 等待/传递的数据类型
 * @param a 待消费等待器的共享句柄
 * @return 取到数据返回 Result 值；空句柄返回 invalid_argument；来源关闭返回首次终止错误，默认关闭为 no_message
 * @code
 * // socket 族方法返回 shared_ptr<Awaitable<T>>，可直接消费
 * auto data = Coro::await(Coro::coro(sock).readAll());
 * @endcode
 */
template<typename T>
Result<T> await(const std::shared_ptr<Awaitable<T>>& a){
    if(!a){
        return std::make_error_code(std::errc::invalid_argument);
    }
    return a->await();
}
/**
 * @brief 消费一个 Awaitable：带超时取一次消息（左值重载）。
 *
 * 与不带超时的 await(a) 分属两个名字（而非 await 的重载），使自由函数命名与
 * Awaitable 成员方法 await()/await_for(timeout) 保持一致的语义。
 * @tparam T 等待/传递的数据类型
 * @tparam Rep 时长的计数类型
 * @tparam Period 时长的周期类型
 * @param a 待消费的等待器
 * @param timeout 最长等待时长
 * @return 取到数据返回 Result 值；超时返回 timed_out，关闭返回首次终止错误
 * @code
 * using namespace std::chrono_literals;
 * auto stream = Coro::coro(sock).readAll();
 * auto chunk  = Coro::await_for(stream, 2s);   // 超时不会取消订阅，可继续等
 * @endcode
 */
template<typename T, typename Rep, typename Period>
Result<T> await_for(Awaitable<T>& a, const std::chrono::duration<Rep, Period>& timeout){
    return a.await_for(timeout);
}
/**
 * @brief 消费一个临时 Awaitable：带超时取一次消息（右值重载）。
 * @tparam T 等待/传递的数据类型
 * @tparam Rep 时长的计数类型
 * @tparam Period 时长的周期类型
 * @param a 待消费的等待器（右值临时对象）
 * @param timeout 最长等待时长
 * @return 取到数据返回 Result 值；超时返回 timed_out，关闭返回首次终止错误
 * @code
 * using namespace std::chrono_literals;
 * auto r = Coro::await_for(Coro::coro(obj, &Obj::finished), 500ms);
 * @endcode
 */
template<typename T, typename Rep, typename Period>
Result<T> await_for(Awaitable<T>&& a, const std::chrono::duration<Rep, Period>& timeout){
    return a.await_for(timeout);
}
/**
 * @brief 通过共享句柄带超时消费一个 Awaitable。
 * @tparam T 等待/传递的数据类型
 * @tparam Rep 时长的计数类型
 * @tparam Period 时长的周期类型
 * @param a 待消费等待器的共享句柄
 * @param timeout 最长等待时长
 * @return 取到数据返回 Result 值；空句柄返回 invalid_argument；超时返回 timed_out，关闭返回首次终止错误
 * @code
 * using namespace std::chrono_literals;
 * auto ok = Coro::await_for(Coro::coro(sock).connectToHost(host, port), 2s);
 * if(!ok) qWarning() << ok.error().message().c_str();
 * @endcode
 */
template<typename T, typename Rep, typename Period>
Result<T> await_for(const std::shared_ptr<Awaitable<T>>& a,
                    const std::chrono::duration<Rep, Period>& timeout){
    if(!a){
        return std::make_error_code(std::errc::invalid_argument);
    }
    return a->await_for(timeout);
}

}

#endif // AWAITABLE_HPP
