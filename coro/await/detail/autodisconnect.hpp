#ifndef CORO_AUTODISCONNECT_HPP
#define CORO_AUTODISCONNECT_HPP

/**
 * @file autodisconnect.hpp
 * @brief 通用的"自动断连连接组"：按信号槽方式绑定业务逻辑，并绑定一个断连条件
 *        （智能指针失效 / 某信号 / 信号+判断函数），条件成立时整组断开。
 * @details 用于替代散落在各 socket 包装器里的 connect+登记+bind 生命周期样板。
 *          关键不变量：业务槽只捕获 channel 等数据载体，**绝不捕获整个 Awaitable**；
 *          断连条件（尤其 untilExpired）经 Awaitable::setOnClose 挂到返回句柄的收尾钩子，
 *          句柄一旦 close/析构即整组断开，从而不残留订阅、不互相截数据。
 */

#include "await/awaitable.hpp"

#include <QMetaObject>
#include <QObject>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace Coro {
namespace detail {

/**
 * @brief 一组"到条件成立即整体断开"的 Qt 连接；RAII——自身析构也断开。
 * @details 线程安全：登记与断开可并发；断开开始后新登记的连接立即断开、新清理回调
 *          立即执行；连接和回调都移出锁后调用，避免重入死锁。以 shared_ptr 持有
 *          （untilExpired/untilSignal 需要 shared_from_this）。
 */
class AutoDisconnect : public std::enable_shared_from_this<AutoDisconnect> {
    std::mutex mutex_;
    std::vector<QMetaObject::Connection> connections_;
    std::vector<std::function<void()>> cleanups_;
    bool disconnected_{false};

public:
    AutoDisconnect() = default;
    /** @brief 析构即断开全部（RAII 兜底）。 */
    ~AutoDisconnect(){ disconnectAll(); }
    AutoDisconnect(const AutoDisconnect&) = delete;
    AutoDisconnect& operator=(const AutoDisconnect&) = delete;

    /**
     * @brief 登记一条已建立的连接，随本组一并断开。
     * @param connection 待登记的连接；若本组已断开则立即断开该连接。
     */
    void add(QMetaObject::Connection connection){
        bool disconnectNow = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(disconnected_) disconnectNow = true;
            else connections_.push_back(connection);
        }
        if(disconnectNow) QObject::disconnect(connection);
    }

    /**
     * @brief 按信号槽方式绑定业务逻辑（可多条）；连接随本组断开。
     * @tparam Sender 发送者 QObject 类型。
     * @tparam Signal 成员信号指针类型。
     * @tparam Slot 可调用体（业务逻辑）。
     */
    template<typename Sender, typename Signal, typename Slot>
    void on(Sender* sender, Signal signal, Slot slot){
        add(QObject::connect(sender, signal, std::move(slot)));
    }

    /**
     * @brief 登记一个随本组清理时执行一次的回调（如停止/删除定时器）。
     * @param cleanup 清理回调；若本组已断开则立即执行。
     */
    void addCleanup(std::function<void()> cleanup){
        bool runNow = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(disconnected_) runNow = true;
            else cleanups_.push_back(std::move(cleanup));
        }
        if(runNow && cleanup) cleanup();
    }

    /**
     * @brief 断连条件：锚（Awaitable）close 或析构即整组断开。
     * @details 经 Awaitable::setOnClose 挂钩；回调只持 weak_ptr<AutoDisconnect>，
     *          **不持 Awaitable**，故不构成引用环、不延长句柄寿命。
     * @tparam T 锚的值类型。
     * @param anchor 作为生命周期锚的返回句柄。
     */
    template<typename T>
    void untilExpired(const std::shared_ptr<Awaitable<T>>& anchor){
        std::weak_ptr<AutoDisconnect> self = shared_from_this();
        anchor->setOnClose([self]{
            if(auto s = self.lock()) s->disconnectAll();
        });
    }

    /**
     * @brief 断连条件：某信号触发即整组断开。
     */
    template<typename Sender, typename Signal>
    void untilSignal(Sender* sender, Signal signal){
        std::weak_ptr<AutoDisconnect> self = shared_from_this();
        add(QObject::connect(sender, signal, [self](auto&&...){
            if(auto s = self.lock()) s->disconnectAll();
        }));
    }

    /**
     * @brief 断连条件：信号触发且 predicate 成立即整组断开。
     * @tparam Pred 形如 bool(信号参数...) 的判断函数。
     */
    template<typename Sender, typename Signal, typename Pred>
    void untilSignal(Sender* sender, Signal signal, Pred predicate){
        std::weak_ptr<AutoDisconnect> self = shared_from_this();
        add(QObject::connect(sender, signal,
                             [self, predicate = std::move(predicate)](auto&&... args){
            if(predicate(args...)){
                if(auto s = self.lock()) s->disconnectAll();
            }
        }));
    }

    /**
     * @brief 断开全部连接并运行清理，仅一次。
     * @details 连接与回调先移出锁再在锁外调用，可与登记并发。
     */
    void disconnectAll(){
        std::vector<QMetaObject::Connection> connections;
        std::vector<std::function<void()>> cleanups;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(disconnected_) return;
            disconnected_ = true;
            connections.swap(connections_);
            cleanups.swap(cleanups_);
        }
        for(const auto& connection : connections) QObject::disconnect(connection);
        for(auto& cleanup : cleanups){
            if(cleanup) cleanup();
        }
    }
};

/** @brief 创建一个 AutoDisconnect（shared_ptr，满足 shared_from_this）。 */
inline std::shared_ptr<AutoDisconnect> make_auto_disconnect(){
    return std::make_shared<AutoDisconnect>();
}

} // namespace detail
} // namespace Coro

#endif // CORO_AUTODISCONNECT_HPP
