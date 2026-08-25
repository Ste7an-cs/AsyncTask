#ifndef CHANNELHUB_HPP
#define CHANNELHUB_HPP

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <system_error>
#include <vector>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include "fiberchannel.hpp"

namespace Coro {

namespace detail {

/**
 * @brief 管理一条数据流的一次性终止清理回调。
 * @details 首次显式关闭或最后一个消费者消失时执行清理。回调在互斥锁外调用，
 *          避免清理过程重入时发生死锁。
 * @note 多次调用 run() 只会执行一次清理。
 * @code
 * // ChannelHub 内部持有本守卫；工厂经 Awaitable::setOnClose 注入清理，
 * // 消费者归零时恰好执行一次
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
     * guard.set([conns]{ for(auto& c : conns) QObject::disconnect(*c); });
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
     * guard.run();    // 执行清理
     * guard.run();    // 幂等：不再重复执行
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
 * @brief 一条数据流的分发端：生产者投递一次，每个消费者队列各得一份。
 *
 * 生产者只捕获 hub（绝不捕获 Awaitable，否则成引用环），每个消费者由自己的
 * Awaitable 独占持有一条 FiberChannel 并挂到本 hub 上。源与订阅者在消费者表上
 * 没有身份差别：句柄析构即摘除，队列连同其中排队的值一起释放。
 *
 * 生产者侧的方法名与签名同 FiberChannel，因此各工厂里 `auto ch = a.channel()`
 * 之后的写法一字不变。
 *
 * @tparam T 传递的数据类型
 * @code
 * // 通常不直接使用，而是通过 Awaitable::channel() 取得
 * Coro::Awaitable<int> a;
 * auto hub = a.channel();
 * QObject::connect(obj, &Obj::valueChanged, [hub](int v){ hub->push(v); });
 * @endcode
 */
template<class T>
class ChannelHub{
    using channel_status = boost::fibers::channel_op_status;
public:
    /** @brief 默认构造，创建一个没有任何消费者的分发端 */
    ChannelHub() = default;
    /** @brief 析构：guard_ 成员析构时兜底执行一次清理 */
    ~ChannelHub() = default;
    /** @brief 禁止拷贝构造 */
    ChannelHub(const ChannelHub&) = delete;
    /** @brief 禁止拷贝赋值 */
    ChannelHub& operator=(const ChannelHub&) = delete;

    /**
     * @brief 挂载一条消费者队列，此后每次 push 都会向它投递一份。
     * @details hub 已关闭时不挂载，直接以 hub 记录的终止原因关闭该队列，避免消费者
     *          永久挂起。挂载时把 hub 当前的容量赋给该队列。
     *          本方法为 public 而非 private + friend：hub 与队列是两个不同的类型层，
     *          结构上无法互相挂载成环，因此无需收口保护。
     * @param queue 待挂载的消费者队列
     * @code
     * hub->attach(queue);      // 由 Awaitable 的构造函数调用
     * @endcode
     */
    void attach(const std::shared_ptr<FiberChannel<T>>& queue){
        if(!queue){
            return;
        }
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            const std::error_code error = close_error_;
            lck.unlock();
            queue->close(error);
            return;
        }
        queue->setCapacity(capacity_);
        consumers_.push_back(queue);
    }

    /**
     * @brief 摘除一条消费者队列；若消费者就此归零，关闭 hub 并执行一次清理。
     * @details 由 Awaitable 的析构函数调用。清理回调在锁外执行——它通常会
     *          QObject::disconnect，进而销毁生产者 lambda 并释放它持有的 hub 引用；
     *          之所以不会把 hub 自己析构掉，是因为调用方（正在析构的 Awaitable，
     *          此刻成员尚未释放）还攥着另一份引用。
     * @param queue 待摘除的队列裸指针，仅用于比对，不解引用
     * @code
     * hub->detach(queue.get());    // 由 ~Awaitable 调用
     * @endcode
     */
    void detach(const FiberChannel<T>* queue){
        bool cleanup = false;
        {
            std::unique_lock<boost::fibers::mutex> lck{mtx_};
            for(std::size_t i = 0; i < consumers_.size(); ){
                auto held = consumers_[i].lock();
                if(!held || held.get() == queue){
                    consumers_[i] = std::move(consumers_.back());
                    consumers_.pop_back();
                    continue;
                }
                ++i;
            }
            cleanup = markExhausted(std::make_error_code(std::errc::no_message));
        }
        if(cleanup){
            // guard_.run() 之后不得再触碰任何成员：回调可能是本 hub 的最后一个持有者，
            // 返回时 hub 可能已被析构。run() 内部已把回调 move 到栈上局部变量再调用。
            guard_.run();
        }
    }

    /**
     * @brief 通知 hub 某条消费者队列已被关闭，触发归零判定。
     * @details 与 detach 不同，本方法不摘除槽位——已关闭的队列要留在表里，
     *          discard_pending() 之后仍需经由它清掉即将悬空的值。归零时 hub
     *          继承传入的终止原因，使生产者查 close_error() 能拿到真正的原因。
     * @param error 消费者关闭时给出的终止原因
     * @code
     * hub->notifyClosed(error);    // 由 Awaitable::close(error) 调用
     * @endcode
     */
    void notifyClosed(std::error_code error){
        bool cleanup = false;
        {
            std::unique_lock<boost::fibers::mutex> lck{mtx_};
            cleanup = markExhausted(error);
        }
        if(cleanup){
            // guard_.run() 之后不得再触碰任何成员：回调可能是本 hub 的最后一个持有者，
            // 返回时 hub 可能已被析构。run() 内部已把回调 move 到栈上局部变量再调用。
            guard_.run();
        }
    }

    /**
     * @brief 向每一条存活且未关闭的消费者队列投递一份 value。
     * @details 失效槽位（消费者句柄已析构）在遍历中以 swap-and-pop 剔除；已关闭的
     *          队列跳过投递但保留在表中。投递延后一拍：最后一个接收者直接 move 送达，
     *          n 个消费者恰好 n-1 次拷贝。
     * @param value 待投递的元素
     * @return hub 已关闭返回 closed，否则返回 success
     * @code
     * if(hub->push(42) != boost::fibers::channel_op_status::success){
     *     // hub 已关闭，值被丢弃
     * }
     * @endcode
     */
    channel_status push(T value){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(BOOST_UNLIKELY(closed_.load())){
            return channel_status::closed;
        }
        std::shared_ptr<FiberChannel<T>> pending;
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            if(!queue->is_closed()){
                // 延后一拍：上一个接收者此刻才拿到拷贝，最后一个留到循环外 move
                if(pending){
                    pending->push(value);
                }
                pending = std::move(queue);
            }
            ++i;
        }
        if(pending){
            pending->push(std::move(value));
        }
        return channel_status::success;
    }

    /**
     * @brief 查询 hub 是否已关闭
     * @return 已关闭返回 true
     * @code
     * while(!hub->is_closed() && dev->bytesAvailable() > 0) hub->push(dev->readAll());
     * @endcode
     */
    bool is_closed() const noexcept {
        return closed_.load(std::memory_order_acquire);
    }

    /**
     * @brief 关闭整条流，唤醒并收敛所有消费者
     * @code
     * hub->close();    // 正常终止：消费者取完余量后观察到 no_message
     * @endcode
     */
    void close() noexcept {
        close(std::make_error_code(std::errc::no_message));
    }

    /**
     * @brief 关闭整条流并记录终止原因，唤醒并收敛所有消费者
     * @details 仅首次关闭记录终止原因，后续调用不覆盖。消费者表**不清空**——
     *          discard_pending() 在此之后仍需经由它触达各队列。本方法不执行清理钩子：
     *          清理由消费者归零触发（见 detach / notifyClosed），与旧实现中
     *          「生产者关 channel 不触发 guard」的语义一致。
     * @param error 终止原因
     * @code
     * hub->close(Coro::detail::socket_error_code(socket->error()));
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
        for(auto& weak : consumers_){
            if(auto queue = weak.lock()){
                queue->close(close_error_);
            }
        }
    }

    /**
     * @brief 返回首次关闭时记录的终止原因
     * @return 终止原因；尚未关闭时为预置的 no_message
     * @code
     * qDebug() << hub->close_error().message().c_str();
     * @endcode
     */
    std::error_code close_error() const {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return close_error_;
    }

    /**
     * @brief 丢弃所有消费者队列中尚未被消费的值。
     * @details 不改变关闭状态，也不修改已保留的终止错误。已关闭但尚未摘除的队列
     *          同样会被清空——来源销毁时靠这条路径清掉即将悬空的 QTcpSocket*。
     * @code
     * QObject::connect(server, &QObject::destroyed, [hub]{ hub->discard_pending(); });
     * @endcode
     */
    void discard_pending(){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            queue->discard_pending();
            ++i;
        }
    }

    /**
     * @brief 设置容量上限：级联给所有存活队列，并作为后续挂载的默认值。
     * @details 各队列按新容量立即从队首丢弃多余的值。传 0 表示无限。承载了自身即
     *          代表资源的值（如 QTcpSocket*）的流应调大容量或传 0，否则丢弃意味着
     *          该资源永远得不到处理。
     * @param capacity 新的容量上限，0 表示无限
     * @code
     * hub->setCapacity(0);     // 取消上限，恢复无界队列（谨慎使用）
     * @endcode
     */
    void setCapacity(std::uint32_t capacity){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        capacity_ = capacity;
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            queue->setCapacity(capacity);
            ++i;
        }
    }

    /**
     * @brief 查询当前的容量上限
     * @return 容量上限；0 表示无限
     * @code
     * if(hub->capacity() == 0) qDebug() << "无界队列";
     * @endcode
     */
    std::uint32_t capacity() const {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        return capacity_;
    }

    /**
     * @brief 注册消费者归零时执行一次的清理钩子
     * @param fn 清理回调（如断开信号连接）
     * @code
     * hub->setOnClose([conn]{ QObject::disconnect(*conn); });
     * @endcode
     */
    void setOnClose(std::function<void()> fn){
        guard_.set(std::move(fn));
    }

private:
    mutable boost::fibers::mutex mtx_;///< 保护消费者表与关闭状态的 fiber 互斥量
    std::vector<std::weak_ptr<FiberChannel<T>>> consumers_{};///< 消费者队列表，强引用在各 Awaitable 手里
    std::atomic_bool closed_{false};///< 关闭标志
    std::uint32_t capacity_{FiberChannel<T>::kDefaultCapacity};///< 容量上限，级联给队列并作为新挂载的默认值
    std::error_code close_error_{std::make_error_code(std::errc::no_message)};///< 首次关闭时保留的终止原因
    detail::AwaitableCloseGuard guard_;///< 消费者归零时执行一次的清理钩子；**必须声明在最后**——析构时最先销毁，此时 mtx_/consumers_ 仍存活，回调可安全重入 hub

    /**
     * @brief 判定消费者是否已归零；归零则关闭 hub 并返回 true（需持有 mtx_ 调用）。
     * @details 「归零」指表中不存在既存活又未关闭的队列。顺带以 swap-and-pop 剔除
     *          失效槽位。已关闭的 hub 不覆盖已记录的终止原因，但仍返回 true——
     *          清理钩子本身幂等，重复触发无害，而漏触发会导致 Qt 连接迟迟不断开。
     *          与另外四处 swap-and-pop 循环不同：遇到第一条「存活且未关闭」的队列
     *          即 return false 提前退出，排在它之后的失效槽位本次不会被剔除（无害，
     *          下次 push/attach/... 遍历时会顺带清掉）。五处循环刻意保持展开写、
     *          不收拢成公共 helper，此处提前返回是唯一的语义差异，故在此注明。
     * @param error 归零时采用的终止原因
     * @return 已归零返回 true
     */
    bool markExhausted(std::error_code error){
        for(std::size_t i = 0; i < consumers_.size(); ){
            auto queue = consumers_[i].lock();
            if(!queue){
                consumers_[i] = std::move(consumers_.back());
                consumers_.pop_back();
                continue;
            }
            if(!queue->is_closed()){
                return false;
            }
            ++i;
        }
        if(!closed_.load()){
            close_error_ = error == std::error_code{}
                    ? std::make_error_code(std::errc::no_message)
                    : error;
            closed_.store(true);
        }
        return true;
    }
};

}

#endif // CHANNELHUB_HPP
