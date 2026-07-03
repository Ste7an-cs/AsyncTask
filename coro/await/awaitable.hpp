#ifndef AWAITABLE_HPP
#define AWAITABLE_HPP

#include <functional>
#include <memory>
#include "detail/fiberchannel.hpp"
#include "detail/result.hpp"

namespace Coro {

///
/// \brief The Awaitable class 异步等待器，生产者/消费者模型。
///     生产者通过 channel() 拿到共享队列并 push 消息，消费者 await 等待消息。
///     跨线程安全（由 FiberChannel 保证）。
///
///     该类与具体来源(Qt/std)解耦：只持有一个共享 channel 与一个不透明的
///     生命周期守卫 guard_。工厂层通过 setOnClose 注入清理逻辑(如断开信号)，
///     当最后一个 Awaitable 析构时自动执行，从而实现 "drop 即取消订阅"。
///
///     move-only，按值传递；内部 channel 为 shared_ptr，生产者只捕获 channel()
///     而不持有整个 Awaitable，避免引用环。
///
template<typename T>
class Awaitable{
    std::shared_ptr<FiberChannel<T>> ch_{std::make_shared<FiberChannel<T>>()};
    std::shared_ptr<void>            guard_{};//RAII 守卫：析构时执行 onClose 清理
public:
    Awaitable() = default;
    ~Awaitable() = default;
    Awaitable(Awaitable&& other) noexcept = default;
    Awaitable& operator=(Awaitable&& other) noexcept = default;
    Awaitable(const Awaitable&) = delete ;
    Awaitable& operator=(const Awaitable&) = delete ;

    ///
    /// \brief channel 生产者侧共享的队列。生产者只捕获它、不持有整个 Awaitable。
    ///
    std::shared_ptr<FiberChannel<T>> channel() const { return ch_; }

    ///
    /// \brief setOnClose 注册"最后一个 Awaitable 析构时执行"的清理钩子。
    ///     与 Qt 解耦：仅保存 std::function，不含任何 Qt 类型。
    ///
    void setOnClose(std::function<void()> fn){
        guard_ = std::shared_ptr<void>(nullptr, [fn = std::move(fn)](void*){ if(fn) fn(); });
    }

    ///
    /// \brief await 等待消息
    ///
    Result<T, std::error_code> await(){
        if(ch_){
            T value{};
            auto status = ch_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
        }
        return std::make_error_code(std::errc::no_message);
    }
    ///
    /// \brief await_for 等待消息，最长等待 timeout 时长
    ///
    template<typename Rep, typename Period>
    Result<T, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(ch_){
            T value{};
            auto status = ch_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
        }
        return std::make_error_code(std::errc::timed_out);
    }

    bool resolve(const T& value){
        if(ch_){
            if(ch_->is_closed()){
                return false;
            }
            return (boost::fibers::channel_op_status::success == ch_->push(value));
        }
        return false;
    }
    void close(){
        if(ch_){
            ch_->close();
        }
    }
};

///
/// \brief The Awaitable<void> class 异步等待器 void特化
///
template<>
class Awaitable<void>{
    std::shared_ptr<FiberChannel<int>> ch_{std::make_shared<FiberChannel<int>>()};
    std::shared_ptr<void>              guard_{};
public:
    Awaitable() = default;
    ~Awaitable() = default;
    Awaitable(Awaitable&& other) noexcept = default;
    Awaitable& operator=(Awaitable&& other) noexcept = default;
    Awaitable(const Awaitable&) = delete ;
    Awaitable& operator=(const Awaitable&) = delete ;

    std::shared_ptr<FiberChannel<int>> channel() const { return ch_; }

    void setOnClose(std::function<void()> fn){
        guard_ = std::shared_ptr<void>(nullptr, [fn = std::move(fn)](void*){ if(fn) fn(); });
    }

    Result<void, std::error_code> await(){
        if(ch_){
            int value;
            auto status = ch_->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
        }
        return std::make_error_code(std::errc::no_message);
    }

    template<typename Rep, typename Period>
    Result<void, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(ch_){
            int value{};
            auto status = ch_->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
        }
        return std::make_error_code(std::errc::timed_out);
    }

    bool resolve(void){
        if(ch_){
            if(ch_->is_closed()){
                return false;
            }
            /// void类型，如果队列满了，不用弹出旧的数据
            /// 因为队列不为空就表明可用，不考虑数据的时效性
            return (boost::fibers::channel_op_status::success == ch_->push(1));
        }
        return false;
    }
    void close(){
        if(ch_){
            ch_->close();
        }
    }
};

///
/// \brief await 消费一个 Awaitable：取一次消息。
///     提供左值/右值重载：await(a)(具名, 可反复取) 与 await(coro(...))(临时, 取一次) 都可用。
///
template<typename T>
Result<T> await(Awaitable<T>& a){
    return a.await();
}
template<typename T>
Result<T> await(Awaitable<T>&& a){
    return a.await();
}
///
/// \brief await 消费一个 Awaitable：带超时取一次消息。
///
template<typename T, typename Rep, typename Period>
Result<T> await(Awaitable<T>& a, const std::chrono::duration<Rep, Period>& timeout){
    return a.await_for(timeout);
}
template<typename T, typename Rep, typename Period>
Result<T> await(Awaitable<T>&& a, const std::chrono::duration<Rep, Period>& timeout){
    return a.await_for(timeout);
}

}

#endif // AWAITABLE_HPP
