#ifndef AWAITABLE_HPP
#define AWAITABLE_HPP

#include <functional>
#include <memory>
#include <mutex>
#include "detail/fiberchannel.hpp"
#include "detail/result.hpp"

namespace Coro {

namespace detail {

/**
 * @brief 管理 Awaitable 的一次性终止清理回调。
 * @details 首次显式关闭或最后一个共享守卫析构时执行清理。回调在互斥锁外调用，
 *          避免清理过程重入时发生死锁。
 * @note 多次调用 run() 只会执行一次清理。
 * @code
 * // Awaitable 内部持有本守卫；工厂经 setOnClose 注入清理，
 * // 首次 close() 或最后一个持有者析构时恰好执行一次
 * Coro::Awaitable<int> a;
 * a.setOnClose([conn]{ QObject::disconnect(*conn); });
 * a.close();      // 此处断开连接；之后析构不会重复执行
 * @endcode
 */
class AwaitableCloseGuard {
    std::mutex mutex_;
    std::function<void()> cleanup_;
    bool closed_{false};
public:
    ~AwaitableCloseGuard(){ run(); }

    /**
     * @brief 设置终止清理回调。
     * @details 替换旧回调时，旧回调会在互斥锁外立即执行；若已终止，传入回调也会在锁外立即执行。
     * @param cleanup 终止时执行的清理回调。
     * @code
     * guard->set([conns]{ for(auto& c : conns) QObject::disconnect(*c); });
     * @endcode
     */
    void set(std::function<void()> cleanup){
        bool runImmediately = false;
        std::function<void()> previous;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(closed_){
                runImmediately = true;
            }else{
                previous = std::move(cleanup_);
                cleanup_ = std::move(cleanup);
            }
        }
        if(previous) previous();
        if(runImmediately && cleanup) cleanup();
    }

    /**
     * @brief 执行一次终止清理。
     * @details 首次调用取出回调并在互斥锁外执行，后续调用不再执行回调。
     * @code
     * guard->run();    // 执行清理
     * guard->run();    // 幂等：不再重复执行
     * @endcode
     */
    void run(){
        std::function<void()> cleanup;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(closed_) return;
            closed_ = true;
            cleanup = std::move(cleanup_);
        }
        if(cleanup) cleanup();
    }
};

} // namespace detail

/**
 * @brief 异步等待器，生产者/消费者模型。
 *
 * 生产者通过 channel() 拿到共享队列并 push 消息，消费者 await 等待消息。
 * 跨线程安全（由 FiberChannel 保证）。
 *
 * 该类与具体来源(Qt/std)解耦：只持有一个共享 channel 与一个不透明的
 * 生命周期守卫 guard_。工厂层通过 setOnClose 注入清理逻辑(如断开信号)，
 * 首次关闭或最终析构时自动执行，从而实现及时取消订阅。关闭且已排队值耗尽后，
 * 消费者只能观察到首次记录的终止错误。
 *
 * move-only，按值传递；内部 channel 为 shared_ptr，生产者只捕获 channel()
 * 而不持有整个 Awaitable，避免引用环。
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
    std::shared_ptr<FiberChannel<T>> ch_{std::make_shared<FiberChannel<T>>()};
    std::shared_ptr<detail::AwaitableCloseGuard> guard_{
        std::make_shared<detail::AwaitableCloseGuard>()};
public:
    /** @brief 默认构造，内部自动创建一个空的共享队列 */
    Awaitable() = default;
    /** @brief 析构，尚未关闭时触发 guard_ 的清理钩子 */
    ~Awaitable() = default;
    /**
     * @brief 移动构造（move-only）
     * @param other 被移动的源对象
     */
    Awaitable(Awaitable&& other) noexcept = default;
    /**
     * @brief 移动赋值（move-only）
     * @param other 被移动的源对象
     * @return 自身引用
     */
    Awaitable& operator=(Awaitable&& other) noexcept = default;
    /** @brief 禁止拷贝构造（避免多个持有者语义混乱） */
    Awaitable(const Awaitable&) = delete ;
    /** @brief 禁止拷贝赋值 */
    Awaitable& operator=(const Awaitable&) = delete ;

    /**
     * @brief 生产者侧共享的队列。生产者只捕获它、不持有整个 Awaitable。
     * @return 内部共享队列的 shared_ptr
     * @code
     * Coro::Awaitable<QByteArray> a;
     * // 生产端只捕 channel：即使回调常驻，也不会延长 Awaitable 寿命
     * QObject::connect(dev, &QIODevice::readyRead, [ch = a.channel(), dev]{
     *     ch->push(dev->readAll());
     * });
     * @endcode
     */
    std::shared_ptr<FiberChannel<T>> channel() const { return ch_; }

    /**
     * @brief 注册一个共享订阅者，此后源产生的每个值都会同步复制一份投递给它。
     *
     * 返回的是普通 Awaitable，因此 Coro::await / await_for / generate 均原样可用。
     * 订阅者之间互为广播（各得全量），与直接 await 本对象的抢占式消费者也不竞争。
     * 不做 replay：本次调用之前已产生的值对订阅者不可见。订阅句柄析构即自动退订。
     * @return 共享订阅句柄；源已关闭时返回的句柄立即以源的终止原因收敛
     * @code
     * auto src = Coro::coro(sock).readAll();
     * auto sync = src->shared();      // 数据同步
     * auto audit = src->shared();     // 日志分发
     * Coro::makeTask([sync]{ while(auto c = Coro::await(sync)) apply(c.value()); return 0; });
     * Coro::makeTask([audit]{ for(const auto& c : Coro::generate(audit)) log(c); return 0; });
     * @endcode
     */
    std::shared_ptr<Awaitable<T>> shared(){
        auto sub = std::make_shared<Awaitable<T>>();
        if(ch_){
            ch_->addMirror(sub->channel());
        }
        return sub;
    }

    /**
     * @brief 注册 Awaitable 关闭或析构时执行一次的清理钩子。
     *
     * 与 Qt 解耦：仅保存 std::function，不含任何 Qt 类型。替换旧回调时，旧回调会在
     * 锁外立即执行；若 Awaitable 已关闭，传入回调也会在锁外立即执行。
     * @param fn 清理回调（如断开信号连接）
     * @code
     * // 扩展自定义来源时：把断连清理挂到 Awaitable 的收尾钩子
     * Coro::Awaitable<QByteArray> a;
     * auto conn = std::make_shared<QMetaObject::Connection>();
     * *conn = QObject::connect(dev, &QIODevice::readyRead,
     *                          [ch = a.channel(), dev]{ ch->push(dev->readAll()); });
     * a.setOnClose([conn]{ QObject::disconnect(*conn); });   // close 或析构时断开
     * @endcode
     */
    void setOnClose(std::function<void()> fn){
        if(guard_) guard_->set(std::move(fn));
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
        if(ch_){
            T value{};
            auto status = ch_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            return ch_->close_error();
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
        if(ch_){
            T value{};
            auto status = ch_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return ch_->close_error();
        }
        return std::make_error_code(std::errc::timed_out);
    }

    /**
     * @brief 生产者侧投递一条数据
     * @param value 待投递的数据
     * @return 成功入队返回 true；队列不存在或已关闭返回 false
     * @code
     * Coro::Awaitable<int> a;
     * a.resolve(42);                       // 消费侧 a.await() 即可取到 42
     * @endcode
     */
    bool resolve(const T& value){
        if(ch_){
            if(ch_->is_closed()){
                return false;
            }
            return (boost::fibers::channel_op_status::success == ch_->push(value));
        }
        return false;
    }
    /**
     * @brief 关闭内部队列，唤醒并收敛所有等待者
     * @code
     * a.close();       // 正常终止：已排队值仍先被消费，随后得到 no_message
     * @endcode
     */
    void close(){
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭内部队列并记录终止原因，唤醒并收敛所有等待者
     * @details 只有首次关闭记录的终止原因可被消费者观察，后续关闭不会覆盖该错误。
     * @param error 终止原因
     * @code
     * // 异常终止：保留首个错误码，消费者据此区分正常结束与出错
     * a.close(std::make_error_code(std::errc::connection_reset));
     * @endcode
     */
    void close(std::error_code error){
        if(ch_){
            ch_->close(error);
        }
        if(guard_) guard_->run();
    }
};

/**
 * @brief 异步等待器 void 特化。
 *
 * 无数据负载，仅表达"事件发生一次"；内部用 FiberChannel<int> 承载信号。
 * 关闭且已排队值耗尽后，消费者只能观察到首次记录的终止错误；生命周期清理在
 * 首次关闭或最后一个共享守卫析构时执行一次。
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
    std::shared_ptr<FiberChannel<int>> ch_{std::make_shared<FiberChannel<int>>()};
    std::shared_ptr<detail::AwaitableCloseGuard> guard_{
        std::make_shared<detail::AwaitableCloseGuard>()};
public:
    /** @brief 默认构造 */
    Awaitable() = default;
    /** @brief 析构 */
    ~Awaitable() = default;
    /**
     * @brief 移动构造（move-only）
     * @param other 被移动的源对象
     */
    Awaitable(Awaitable&& other) noexcept = default;
    /**
     * @brief 移动赋值（move-only）
     * @param other 被移动的源对象
     * @return 自身引用
     */
    Awaitable& operator=(Awaitable&& other) noexcept = default;
    /** @brief 禁止拷贝构造 */
    Awaitable(const Awaitable&) = delete ;
    /** @brief 禁止拷贝赋值 */
    Awaitable& operator=(const Awaitable&) = delete ;

    /**
     * @brief 生产者侧共享的队列
     * @return 内部共享队列的 shared_ptr
     * @code
     * Coro::Awaitable<void> a;
     * // void 特化内部用 FiberChannel<int> 承载"发生一次"，push 任意值即可
     * QObject::connect(obj, &Obj::done, [ch = a.channel()]{ ch->push(1); });
     * @endcode
     */
    std::shared_ptr<FiberChannel<int>> channel() const { return ch_; }

    /**
     * @brief 注册 Awaitable 关闭或析构时执行一次的清理钩子
     * @details 替换旧回调时，旧回调会在锁外立即执行；若 Awaitable 已关闭，传入回调
     *          也会在锁外立即执行。
     * @param fn 清理回调
     * @code
     * Coro::Awaitable<void> a;
     * auto conn = std::make_shared<QMetaObject::Connection>();
     * *conn = QObject::connect(obj, &Obj::done, [ch = a.channel()]{ ch->push(1); });
     * a.setOnClose([conn]{ QObject::disconnect(*conn); });
     * @endcode
     */
    void setOnClose(std::function<void()> fn){
        if(guard_) guard_->set(std::move(fn));
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
        if(ch_){
            int value;
            auto status = ch_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            return ch_->close_error();
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
        if(ch_){
            int value{};
            auto status = ch_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
            if(status == boost::fibers::channel_op_status::timeout){
                return std::make_error_code(std::errc::timed_out);
            }
            return ch_->close_error();
        }
        return std::make_error_code(std::errc::timed_out);
    }

    /**
     * @brief 生产者侧发出一次"事件发生"信号
     * @return 成功入队返回 true；队列不存在或已关闭返回 false
     * @code
     * Coro::Awaitable<void> a;
     * a.resolve();          // 通知"事件发生一次"，等待方的 await() 随即返回成功
     * @endcode
     */
    bool resolve(void){
        if(ch_){
            if(ch_->is_closed()){
                return false;
            }
            // void类型，如果队列满了，不用弹出旧的数据
            // 因为队列不为空就表明可用，不考虑数据的时效性
            return (boost::fibers::channel_op_status::success == ch_->push(1));
        }
        return false;
    }
    /**
     * @brief 关闭内部队列，唤醒并收敛所有等待者
     * @code
     * a.close();       // 正常终止：已排队值仍先被消费，随后得到 no_message
     * @endcode
     */
    void close(){
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭内部队列并记录终止原因，唤醒并收敛所有等待者
     * @details 只有首次关闭记录的终止原因可被消费者观察，后续关闭不会覆盖该错误。
     * @param error 终止原因
     * @code
     * // 异常终止：保留首个错误码，消费者据此区分正常结束与出错
     * a.close(std::make_error_code(std::errc::connection_reset));
     * @endcode
     */
    void close(std::error_code error){
        if(ch_){
            ch_->close(error);
        }
        if(guard_) guard_->run();
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
