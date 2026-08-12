#ifndef FIBERCHANNEL_HPP
#define FIBERCHANNEL_HPP

#include <boost/fiber/mutex.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/exceptions.hpp>
#include <cstdint>
#include <deque>
#include <memory>
#include <system_error>
#include <vector>

namespace Coro {

template<typename T> class Awaitable;

/**
 * @brief 跨线程/跨协程安全的队列，可用于两个线程/协程间数据传递。
 *
 * 代替 boost::fibers::unbuffered_channel——原生的 unbuffered_channel 在
 * 非协程线程上 pop 时会 crash。内部用 fiber 版 mutex/condition_variable，
 * 等待时让出协程而非阻塞线程。
 * @tparam T 队列元素类型
 * @code
 * // 通常不直接使用，而是通过 Awaitable::channel() 取得（生产者只捕获 channel）
 * Coro::Awaitable<int> a;
 * auto ch = a.channel();
 * auto prod = Coro::makeTask([ch]{
 *     for(int i = 0; i < 3; i++) ch->push(i);
 *     ch->close();                                // 结束流，消费者自然收敛
 *     return 0;
 * });
 * auto cons = Coro::makeTask([&a]{
 *     while(auto v = a.await()) qDebug() << v.value();
 *     return 0;
 * });
 * @endcode
 */
template<class T>
class FiberChannel{
    using channel_status = boost::fibers::channel_op_status;
public:
    /** @brief 默认容量：超过该值 push 会丢弃队首最旧的值，见 setCapacity() */
    static constexpr std::uint32_t kDefaultCapacity = 1024;

    /** @brief 默认构造，创建空队列，初始容量为 kDefaultCapacity */
    FiberChannel() = default;
    /** @brief 禁止拷贝构造 */
    FiberChannel(const FiberChannel& ) = delete ;
    /** @brief 禁止拷贝赋值 */
    FiberChannel& operator=(const FiberChannel&) = delete ;
    /**
     * @brief 析构：源 channel 消亡而未 close 时，令镜像收敛。
     * @details 不这样做的话，订阅者的 await 会永久阻塞——镜像仍开着却再无生产者。
     *          用 connection_aborted 与正常结束的 no_message 区分，便于定位误丢源句柄。
     *          此时引用计数已归零，不存在其他持有者，自身状态无需加锁；但仍会经由
     *          mirror->close(...) 取得每个存活镜像自己的锁，因此整体并非无锁操作——
     *          释放最后一个 shared_ptr<FiberChannel> 可能因此阻塞或让出协程。
     */
    ~FiberChannel(){
        if(mirrors_){
            for(auto& weak : *mirrors_){
                if(auto mirror = weak.lock()){
                    mirror->close(std::make_error_code(std::errc::connection_aborted));
                }
            }
        }
    }
    /**
     * @brief 添加一个元素 value 至队列
     * @details 队列有容量上限（默认 kDefaultCapacity，见 setCapacity()）；push 使队列
     *          超出上限时，先丢弃队首最旧的值再追加新值，净大小停留在上限，不会阻塞
     *          生产者也不会返回失败。每个镜像按各自的容量独立丢弃，与源互不影响。
     * @param value 待入队的元素
     * @return 成功返回 success；如果 channel 已关闭返回 closed
     * @code
     * // push 在锁内自行判断关闭状态，与 close 并发安全，调用方无需先查 is_closed()
     * if(ch->push(42) != boost::fibers::channel_op_status::success){
     *     // channel 已关闭，值被丢弃
     * }
     * @endcode
     */
    channel_status push(T value){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if ( BOOST_UNLIKELY( is_closed() ) ) {
            return channel_status::closed;
        }
        if(mirrors_){
            auto& list = *mirrors_;
            for(std::size_t i = 0; i < list.size(); ){
                auto mirror = list[i].lock();
                if(!mirror){
                    // 句柄已析构：weak_ptr 失效，swap-and-pop 剔除
                    list[i] = std::move(list.back());
                    list.pop_back();
                    continue;
                }
                // 已关闭的镜像跳过投递（省掉一次加锁与一次 T 拷贝），但保留在列表中，
                // 以便 discard_pending() 仍能清掉它队列里即将悬空的值
                if(!mirror->is_closed()){
                    mirror->push(value);
                }
                ++i;
            }
        }
        queue_.push_back(std::move(value));
        // capacity_ == 0 表示无限容量（沿用旧行为）；否则丢弃队首最旧值，净大小停留在上限
        while(capacity_ != 0 && queue_.size() > capacity_){
            queue_.pop_front();
        }
        cv_consumer_.notify_one();
        return channel_status::success;
    }
    /**
     * @brief 获取一个元素至引用参数，如果 channel 中无可用值则阻塞/协程等待
     * @param out 输出参数，取到的元素
     * @return 成功返回 success；如果 channel 已关闭返回 closed
     * @code
     * int v{};
     * // 队列空时让出当前协程（不阻塞线程），有值或关闭时被唤醒
     * if(ch->pop(v) == boost::fibers::channel_op_status::success) use(v);
     * @endcode
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
     * @code
     * int v{};
     * auto st = ch->pop_wait_for(v, std::chrono::milliseconds(100));
     * if(st == boost::fibers::channel_op_status::timeout){
     *     // 仅本次等待到期，channel 仍开放，可继续等
     * }
     * @endcode
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
     * @code
     * // 框架内部一般用 pop()/pop_wait_for() 以错误码表达失败；
     * // 仅在确定 channel 不会关闭时使用本接口
     * try {
     *     int v = ch->value_pop();
     *     use(v);
     * } catch(const boost::fibers::fiber_error&) {
     *     // channel 已关闭
     * }
     * @endcode
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
     * @code
     * // 作为流式 drain 循环的终止守卫
     * while(!ch->is_closed() && socket->bytesAvailable() > 0){
     *     ch->push(socket->readAll());
     * }
     * @endcode
     */
    bool is_closed() const noexcept {
        return closed_.load( std::memory_order_acquire);
    }
    /**
     * @brief 关闭 channel，唤醒并收敛所有等待者
     * @code
     * ch->close();     // 正常终止：消费者取完余量后观察到 no_message
     * @endcode
     */
    void close() noexcept {
        close(std::make_error_code(std::errc::no_message));
    }
    /**
     * @brief 关闭 channel 并记录终止原因，唤醒并收敛所有等待者
     * @details 仅首次关闭会记录终止原因，后续 close() 调用不会覆盖已保留的错误。
     *          同时以规范化后的终止原因（空错误码替换为 no_message）递归关闭所有
     *          仍存活的镜像，否则镜像的消费者会永久挂在 await 上。镜像列表本身不清空，
     *          discard_pending() 仍需经由它触达镜像以丢弃即将悬空的排队值。
     * @param error 终止原因
     * @code
     * // 来源出错时带错误码关闭，消费者可据此区分正常结束与异常终止
     * ch->close(Coro::detail::socket_error_code(socket->error()));
     * @endcode
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
        // 终止必须传播，否则镜像的消费者会永久挂在 await 上；镜像列表本身不清空——
        // discard_pending() 之后仍可能通过它触达镜像（见 corotcpserver.hpp 的用例：
        // 消费者先 close() 再 delete server，此时仍需清掉镜像队列里即将悬空的指针）。
        if(mirrors_){
            for(auto& weak : *mirrors_){
                if(auto mirror = weak.lock()) mirror->close(close_error_);
            }
        }
    }
    /**
     * @brief 返回首次关闭 channel 时记录的终止原因。
     * @details channel 尚未关闭时返回预置的默认 no_message；关闭后返回首次
     *          记录的错误。重复关闭不会改变该错误。
     * @code
     * int v{};
     * if(ch->pop(v) == boost::fibers::channel_op_status::closed){
     *     qDebug() << ch->close_error().message().c_str();   // 终止原因
     * }
     * @endcode
     */
    std::error_code close_error() const {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return close_error_;
    }
    /**
     * @brief 丢弃尚未被消费的值。
     * @details 该操作不改变 channel 的关闭状态，也不修改首次关闭时保留的终止错误。
     * @code
     * // server 析构时清掉队列中即将悬空的 QTcpSocket*，防止消费者拿到野指针
     * QObject::connect(server, &QObject::destroyed, [ch]{ ch->discard_pending(); });
     * @endcode
     */
    void discard_pending(){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        queue_.clear();
        // 镜像里存的是同一批即将悬空的值，必须一并丢弃；push() 关闭后即早返回，
        // 因此这里是唯一还会遍历已关闭 channel 镜像列表的扇出点，顺便剔除失效槽位
        if(mirrors_){
            auto& list = *mirrors_;
            for(std::size_t i = 0; i < list.size(); ){
                auto mirror = list[i].lock();
                if(!mirror){
                    list[i] = std::move(list.back());
                    list.pop_back();
                    continue;
                }
                mirror->discard_pending();
                ++i;
            }
        }
    }
    /**
     * @brief 设置队列容量上限，超出时 push 会丢弃队首最旧的值。
     * @details 若新容量小于当前排队值数量，立即从队首丢弃直至队列大小不超过新容量；
     *          不改变关闭状态，也不修改已保留的终止错误。传 0 表示取消容量限制（旧行为）。
     *          承载了自身即代表资源的值（例如 nextConnection() 流中的 QTcpSocket*）的
     *          channel，丢弃意味着该资源永远得不到处理；这类场景应调大容量或直接传 0
     *          取消限制，而不是依赖默认的 kDefaultCapacity。
     * @param capacity 新的容量上限，0 表示无限
     * @code
     * ch->setCapacity(64);     // 收紧上限，立即丢弃多余的旧值
     * ch->setCapacity(0);      // 取消上限，恢复无界队列（谨慎使用）
     * @endcode
     */
    void setCapacity(std::uint32_t capacity){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        capacity_ = capacity;
        if(capacity_ != 0){
            while(queue_.size() > capacity_){
                queue_.pop_front();
            }
        }
    }
    /**
     * @brief 查询当前的队列容量上限
     * @return 容量上限；0 表示无限
     * @code
     * if(ch->capacity() == 0) qDebug() << "无界队列";
     * @endcode
     */
    std::uint32_t capacity() const {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return capacity_;
    }
private:
    mutable boost::fibers::mutex mtx_;///< 保护队列的 fiber 互斥量
    boost::fibers::condition_variable cv_consumer_;///< 通知消费者的条件变量
    std::deque<T> queue_{};///< 底层元素队列
    std::atomic_bool closed_{false};///< 关闭标志
    std::uint32_t capacity_{kDefaultCapacity};///< 队列容量上限，0 表示无限；紧跟 closed_ 声明以复用其后填充字节，避免 sizeof 增长
    std::error_code close_error_{std::make_error_code(std::errc::no_message)};///< 首次关闭时保留的终止原因
    std::unique_ptr<std::vector<std::weak_ptr<FiberChannel<T>>>> mirrors_;///< 镜像通道列表；无人调用 shared() 时保持空指针，不产生堆分配

    template<typename U> friend class Awaitable;

    /**
     * @brief 注册一条镜像通道，此后每次 push 都会同步投递一份副本。
     * @details 源已关闭时不注册，直接以源首次关闭时记录的终止原因关闭该镜像，
     *          避免订阅者永久挂起。仅由 Awaitable::shared() 调用；保持内部可见，
     *          公开会让调用方构造出互为镜像的环从而死锁。
     * @param mirror 接收副本的镜像通道
     */
    void addMirror(const std::shared_ptr<FiberChannel<T>>& mirror){
        if(!mirror){
            return;
        }
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            const std::error_code error = close_error_;
            lck.unlock();
            mirror->close(error);
            return;
        }
        if(!mirrors_){
            mirrors_ = std::make_unique<std::vector<std::weak_ptr<FiberChannel<T>>>>();
        }
        mirrors_->push_back(mirror);
    }
};

}

#endif // FIBERCHANNEL_HPP
