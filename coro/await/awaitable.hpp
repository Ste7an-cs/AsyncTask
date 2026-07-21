#ifndef AWAITABLE_HPP
#define AWAITABLE_HPP

#include <functional>
#include <memory>
#include <mutex>
#include "detail/fiberchannel.hpp"
#include "detail/result.hpp"

namespace Coro {

namespace detail {

class AwaitableCloseGuard {
    std::mutex mutex_;
    std::function<void()> cleanup_;
    bool closed_{false};
public:
    ~AwaitableCloseGuard(){ run(); }

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
 * 首次关闭或最终析构时自动执行，从而实现及时取消订阅。
 *
 * move-only，按值传递；内部 channel 为 shared_ptr，生产者只捕获 channel()
 * 而不持有整个 Awaitable，避免引用环。
 *
 * @tparam T 等待/传递的数据类型
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
     */
    std::shared_ptr<FiberChannel<T>> channel() const { return ch_; }

    /**
     * @brief 注册 Awaitable 关闭或析构时执行一次的清理钩子。
     *
     * 与 Qt 解耦：仅保存 std::function，不含任何 Qt 类型。
     * @param fn 清理回调（如断开信号连接）
     */
    void setOnClose(std::function<void()> fn){
        if(guard_) guard_->set(std::move(fn));
    }

    /**
     * @brief 等待一条消息（无数据时让出当前协程，不阻塞线程）
     * @return 取到数据返回 Result 值；队列关闭后返回首次终止错误，默认关闭为 no_message
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
     */
    void close(){
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭内部队列并记录终止原因，唤醒并收敛所有等待者
     * @param error 终止原因
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
     */
    std::shared_ptr<FiberChannel<int>> channel() const { return ch_; }

    /**
     * @brief 注册 Awaitable 关闭或析构时执行一次的清理钩子
     * @param fn 清理回调
     */
    void setOnClose(std::function<void()> fn){
        if(guard_) guard_->set(std::move(fn));
    }

    /**
     * @brief 等待事件发生一次（无数据时让出协程）
     * @return 事件到达返回成功 Result；队列关闭后返回首次终止错误，默认关闭为 no_message
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
     */
    void close(){
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭内部队列并记录终止原因，唤醒并收敛所有等待者
     * @param error 终止原因
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
