#ifndef FIBERCHANNEL_HPP
#define FIBERCHANNEL_HPP

#include <boost/fiber/mutex.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/exceptions.hpp>
#include <deque>
#include <system_error>

namespace Coro {

/**
 * @brief 跨线程/跨协程安全的队列，可用于两个线程/协程间数据传递。
 *
 * 代替 boost::fibers::unbuffered_channel——原生的 unbuffered_channel 在
 * 非协程线程上 pop 时会 crash。内部用 fiber 版 mutex/condition_variable，
 * 等待时让出协程而非阻塞线程。
 * @tparam T 队列元素类型
 */
template<class T>
class FiberChannel{
    using channel_status = boost::fibers::channel_op_status;
public:
    /** @brief 默认构造，创建空队列 */
    FiberChannel() = default;
    /** @brief 禁止拷贝构造 */
    FiberChannel(const FiberChannel& ) = delete ;
    /** @brief 禁止拷贝赋值 */
    FiberChannel& operator=(const FiberChannel&) = delete ;
    /**
     * @brief 添加一个元素 value 至队列
     * @param value 待入队的元素
     * @return 成功返回 success；如果 channel 已关闭返回 closed
     */
    channel_status push(T value){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if ( BOOST_UNLIKELY( is_closed() ) ) {
            return channel_status::closed;
        }
        queue_.push_back(std::move(value));
        cv_consumer_.notify_one();
        return channel_status::success;
    }
    /**
     * @brief 获取一个元素至引用参数，如果 channel 中无可用值则阻塞/协程等待
     * @param out 输出参数，取到的元素
     * @return 成功返回 success；如果 channel 已关闭返回 closed
     */
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
    /**
     * @brief 获取一个元素至引用参数，无可用值则等待，最长等待 timeout_duration
     * @details 超时仅表示在给定时长内没有可用值，channel 仍可保持开放；仅当 channel
     *          已关闭且队列为空时返回 closed。若仍有排队值则返回 success 并取出该值，
     *          关闭后的终止原因由 close_error() 提供。
     * @tparam Rep 时长的计数类型
     * @tparam Period 时长的周期类型
     * @param out 输出参数，取到的元素
     * @param timeout_duration 超时时间
     * @return 成功返回 success；超时返回 timeout；channel 已关闭返回 closed
     */
    template< typename Rep, typename Period >
    channel_status pop_wait_for( T & out,
                                 std::chrono::duration< Rep, Period > const& timeout_duration){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        const bool ready = cv_consumer_.wait_for(
                    lck, timeout_duration, [this](){return !queue_.empty() || closed_.load();});
        if(!ready){
            return channel_status::timeout;
        }
        if(queue_.empty()){
            return channel_status::closed;
        }
        out = std::move(queue_.front());
        queue_.pop_front();
        return channel_status::success;
    }
    /**
     * @brief 阻塞等待可用的值，如果 channel 关闭则抛异常
     * @return 可用的值
     * @throws boost::fibers::fiber_error channel 已关闭
     */
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
    /**
     * @brief 查询 channel 是否已关闭
     * @return 已关闭返回 true
     */
    bool is_closed() const noexcept {
        return closed_.load( std::memory_order_acquire);
    }
    /**
     * @brief 关闭 channel，唤醒并收敛所有等待者
     */
    void close() noexcept {
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭 channel 并记录终止原因，唤醒并收敛所有等待者
     * @details 仅首次关闭会记录终止原因，后续 close() 调用不会覆盖已保留的错误。
     * @param error 终止原因
     */
    void close(std::error_code error) noexcept {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            return;
        }
        close_error_ = error == std::error_code{}
                ? std::make_error_code(std::errc::no_message)
                : error;
        closed_.store(true);
        cv_consumer_.notify_all();
    }
    /**
     * @brief 返回首次关闭 channel 时记录的终止原因。
     * @details 重复关闭不会改变该错误，因此消费者始终观察到首次关闭原因。
     */
    std::error_code close_error() const {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return close_error_;
    }
    /**
     * @brief 丢弃尚未被消费的值。
     * @details 该操作不改变 channel 的关闭状态，也不修改首次关闭时保留的终止错误。
     */
    void discard_pending(){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        queue_.clear();
    }
private:
    mutable boost::fibers::mutex mtx_;///< 保护队列的 fiber 互斥量
    boost::fibers::condition_variable cv_consumer_;///< 通知消费者的条件变量
    std::deque<T> queue_{};///< 底层元素队列
    std::atomic_bool closed_{false};///< 关闭标志
    std::error_code close_error_{std::make_error_code(std::errc::no_message)};///< 首次关闭时保留的终止原因


};

}

#endif // FIBERCHANNEL_HPP
