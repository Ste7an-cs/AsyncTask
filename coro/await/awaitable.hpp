#ifndef AWAITABLE_HPP
#define AWAITABLE_HPP

#include "detail/fiberchannel.hpp"
#include "detail/result.hpp"

namespace Coro {

///
/// \brief The Awaitable class 异步等待器，生产者/消费者模型。
///     生产者调用resume提供消息，await等待消息
///     跨线程安全
///

template<typename T>
class Awaitable{
    std::unique_ptr<FiberChannel<T>> ch{nullptr};
public:
    Awaitable() : ch(std::make_unique<FiberChannel<T>>()){}
    ~Awaitable() = default;
    Awaitable(Awaitable&& other) = default;
    Awaitable& operator=(Awaitable&& other) = default;
    Awaitable(const Awaitable&) = delete ;
    Awaitable& operator=(const Awaitable&) = delete ;

    ///
    /// \brief await 等待消息
    /// \return
    ///
    Result<T, std::error_code> await(){
        if(ch){
            T value{};
            auto status = ch->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
        }
        return std::make_error_code(std::errc::no_message);
    }
    ///
    /// \brief await 等待消息 最长等待timeout时长
    /// \return
    ///
    template<typename Rep, typename Period>
    Result<T, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(ch){
            T value{};
            auto status = ch->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return value;
            }
        }
        return std::make_error_code(std::errc::timed_out);
    }

    bool resolve(const T& value){
        if(ch){
            if(ch->is_closed()){
                return false;
            }
            return (boost::fibers::channel_op_status::success == ch->push(value));
        }
        return false;
    }
    void close(){
        if(ch){
            ch->close();
        }
    }
};

///
/// \brief The Awaitable<void> class 异步等待器 void特化
///
template<>
class Awaitable<void>{
    std::unique_ptr<FiberChannel<int>> ch{nullptr};
public:
    Awaitable() : ch(std::make_unique<FiberChannel<int>>()){}
    ~Awaitable() = default;
    Awaitable(Awaitable&& other) = default;
    Awaitable(const Awaitable&) = delete ;
    Awaitable& operator=(const Awaitable&) = delete ;

    Result<void, std::error_code> await(){
        if(ch){
            int value;
            auto status = ch->pop(value);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
        }
        return std::make_error_code(std::errc::no_message);
    }

    template<typename Rep, typename Period>
    Result<void, std::error_code> await_for(const std::chrono::duration<Rep, Period>& timeout){
        if(ch){
            int value{};
            auto status = ch->pop_wait_for(value, timeout);
            if(status == boost::fibers::channel_op_status::success){
                return Result<void, std::error_code>();
            }
        }
        return std::make_error_code(std::errc::timed_out);
    }

    bool resolve(void){
        if(ch){
            if(ch->is_closed()){
                return false;
            }
            /// void类型，如果队列满了，不用弹出旧的数据
            /// 因为队列不为空就表明可用，不考虑数据的时效性
            return (boost::fibers::channel_op_status::success == ch->push(1));
        }
        return false;
    }
    void close(){
        if(ch){
            ch->close();
        }
    }
};
}

#endif // AWAITABLE_HPP
