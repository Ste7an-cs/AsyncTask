#ifndef FIBERCHANNEL_HPP
#define FIBERCHANNEL_HPP

#include <boost/fiber/mutex.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/exceptions.hpp>
#include <deque>

namespace Coro {

///
/// \brief The FiberChannel class 跨线程/跨协程安全的队列，可用于两个线程/协程间数据传递
///         代替boost::fibers::unbufferd_channel，原生的unbufferd_channel在非协程上pop时会crash
///
template<class T>
class FiberChannel{
    using channel_status = boost::fibers::channel_op_status;
public:
    FiberChannel() = default;
    FiberChannel(const FiberChannel& ) = delete ;
    FiberChannel& operator=(const FiberChannel&) = delete ;
    ///
    /// \brief push 添加一个元素value至队列
    /// \param value
    /// \return 成功返回success，如果channel关闭，返回closed
    ///
    channel_status push(T value){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if ( BOOST_UNLIKELY( is_closed() ) ) {
            return channel_status::closed;
        }
        queue_.push_back(std::move(value));
        cv_consumer_.notify_one();
        return channel_status::success;
    }
    ///
    /// \brief pop 获取一个元素至引用参数，如果channel中无可用值，阻塞/协程等待
    /// \param value 引用参数
    /// \return 成功返回success，如果channel关闭，返回closed
    ///
    channel_status pop(T& out){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        cv_consumer_.wait(lck, [this](){return !queue_.empty() || closed_.load();});
        if(queue_.empty()){
            return channel_status::closed;
        }
        out = std::move(queue_.front());
        queue_.pop_front();
        return channel_status::success;
    }
    ///
    /// \brief pop 获取一个元素至引用参数，如果channel中无可用值，阻塞/协程等待，最大等待时长为timeout_duration
    /// \param value 引用参数
    /// \param timeout_duration 超时时间
    /// \return 成功返回success，如果channel关闭，返回closed
    ///
    template< typename Rep, typename Period >
    channel_status pop_wait_for( T & out,
                                 std::chrono::duration< Rep, Period > const& timeout_duration){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        cv_consumer_.wait_for(lck, timeout_duration, [this](){return !queue_.empty() || closed_.load();});
        if(queue_.empty()){
            return channel_status::closed;
        }
        out = std::move(queue_.front());
        queue_.pop_front();
        return channel_status::success;
    }
    ///
    /// \brief value_pop 阻塞等待可用的值，如果channel关闭，则抛异常
    /// \return 可用的值
    ///
    T value_pop(){
        for(;;){
            std::unique_lock<boost::fibers::mutex> lck{mtx_};
            cv_consumer_.wait(lck, [this](){return !queue_.empty() || closed_.load();});
            if(queue_.empty()){
                continue;
            }
            if ( BOOST_UNLIKELY( is_closed() ) ) {
                throw boost::fibers::fiber_error{
                        std::make_error_code( std::errc::operation_not_permitted),
                        "boost fiber: channel is closed" };
            }
            T out = queue_.front();
            queue_.pop_front();
            return out;
        }
    }
    bool is_closed() const noexcept {
        return closed_.load( std::memory_order_acquire);
    }
    void close() noexcept {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        closed_.store(true);
        cv_consumer_.notify_all();
    }
private:
    boost::fibers::mutex mtx_;
    boost::fibers::condition_variable cv_consumer_;//通知消费者
    std::deque<T> queue_{};
    std::atomic_bool closed_{false};


};

}

#endif // FIBERCHANNEL_HPP
