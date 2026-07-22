#ifndef CORO_SOCKETAWAIT_HPP
#define CORO_SOCKETAWAIT_HPP

#include "await/awaitable.hpp"

#include <QCoreApplication>
#include <QAbstractSocket>
#include <QLocalSocket>
#include <QMetaObject>
#include <QObject>
#include <QPointer>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace Coro {
namespace detail {

/**
 * @brief 管理一个 socket awaitable 的信号连接和清理回调。
 * @details 注册与清理可并发进行。清理开始后新注册的连接会立即断开，新的清理
 *          回调会立即执行；连接和回调都会移出互斥锁后调用，避免重入死锁。
 */
class SocketConnectionRegistry {
public:
    /**
     * @brief 注册待清理的 Qt 信号连接。
     * @param connection 要在生命周期结束时断开的连接。
     * @details 若清理已经开始，connection 会立即在锁外断开。
     */
    void registerConnection(QMetaObject::Connection connection){
        bool disconnectImmediately = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(cleaned_){
                disconnectImmediately = true;
            }else{
                connections_.push_back(connection);
            }
        }
        if(disconnectImmediately) QObject::disconnect(connection);
    }

    /**
     * @brief 注册生命周期结束时执行的回调。
     * @param cleanup 待执行的清理回调。
     * @details 若清理已经开始，cleanup 会立即在锁外执行。
     */
    void registerCleanup(std::function<void()> cleanup){
        bool cleanupImmediately = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(cleaned_){
                cleanupImmediately = true;
            }else{
                cleanups_.push_back(std::move(cleanup));
            }
        }
        if(cleanupImmediately && cleanup) cleanup();
    }

    /**
     * @brief 仅执行一次地断开所有连接并运行所有清理回调。
     * @details 连接和回调先移出锁，再在锁外调用；可与注册操作并发。
     */
    void cleanup(){
        std::vector<QMetaObject::Connection> connections;
        std::vector<std::function<void()>> cleanups;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(cleaned_) return;
            cleaned_ = true;
            connections.swap(connections_);
            cleanups.swap(cleanups_);
        }
        for(const auto& connection : connections) QObject::disconnect(connection);
        for(auto& cleanup : cleanups){
            if(cleanup) cleanup();
        }
    }

private:
    std::mutex mutex_;
    std::vector<QMetaObject::Connection> connections_;
    std::vector<std::function<void()>> cleanups_;
    bool cleaned_{false};
};

/** @brief 共享的 socket 生命周期注册表所有权。 */
using SocketConnections = std::shared_ptr<SocketConnectionRegistry>;

/**
 * @brief 创建一个独立的 socket 生命周期注册表。
 * @return 新建的共享 socket 生命周期注册表。
 */
inline SocketConnections socket_connections(){
    return std::make_shared<SocketConnectionRegistry>();
}

/**
 * @brief 向注册表登记 Qt 信号连接。
 * @param connections 目标注册表；为空时立即断开 connection。
 * @param connection 待登记的连接。
 */
inline void register_socket_connection(const SocketConnections& connections,
                                       QMetaObject::Connection connection){
    if(connections){
        connections->registerConnection(connection);
    }else{
        QObject::disconnect(connection);
    }
}

/**
 * @brief 向注册表登记清理回调。
 * @param connections 目标注册表；为空时立即执行 cleanup。
 * @param cleanup 待登记的回调。
 */
inline void register_socket_cleanup(const SocketConnections& connections,
                                    std::function<void()> cleanup){
    if(connections){
        connections->registerCleanup(std::move(cleanup));
    }else if(cleanup){
        cleanup();
    }
}

/**
 * @brief 清理注册表中的连接和回调。
 * @param connections 待清理的注册表；为空时不做任何事。
 */
inline void cleanup_socket_connections(const SocketConnections& connections){
    if(connections) connections->cleanup();
}

/**
 * @brief 连接 QAbstractSocket 的跨 Qt 版本错误信号。
 * @tparam Function 可接收 QAbstractSocket::SocketError 的回调类型。
 * @param socket 错误信号来源。
 * @param function 接收 SocketError 的回调。
 * @return 建立的 Qt 信号连接。
 */
template<typename Function>
QMetaObject::Connection connect_socket_error(QAbstractSocket* socket,
                                             Function&& function){
#if QT_VERSION >= QT_VERSION_CHECK(5, 15, 0)
    return QObject::connect(socket, &QAbstractSocket::errorOccurred,
                            std::forward<Function>(function));
#else
    return QObject::connect(
        socket,
        static_cast<void (QAbstractSocket::*)(QAbstractSocket::SocketError)>(
            &QAbstractSocket::error),
        std::forward<Function>(function));
#endif
}

/**
 * @brief 连接 QLocalSocket 的跨 Qt 版本错误信号。
 * @tparam Function 可接收 QLocalSocket::LocalSocketError 的回调类型。
 * @param socket 错误信号来源。
 * @param function 接收 LocalSocketError 的回调。
 * @return 建立的 Qt 信号连接。
 */
template<typename Function>
QMetaObject::Connection connect_local_socket_error(QLocalSocket* socket,
                                                   Function&& function){
#if QT_VERSION >= QT_VERSION_CHECK(5, 15, 0)
    return QObject::connect(socket, &QLocalSocket::errorOccurred,
                            std::forward<Function>(function));
#else
    return QObject::connect(
        socket,
        static_cast<void (QLocalSocket::*)(QLocalSocket::LocalSocketError)>(
            &QLocalSocket::error),
        std::forward<Function>(function));
#endif
}

/**
 * @brief 生成清理共享注册表的回调。
 * @param connections 要保活并清理的注册表。
 * @return 可作为 Awaitable 关闭钩子的回调。
 */
inline std::function<void()> socket_cleanup(SocketConnections connections){
    return [connections = std::move(connections)]{
        cleanup_socket_connections(connections);
    };
}

/**
 * @brief 创建带 socket 生命周期清理钩子的共享 awaitable。
 * @tparam T 流中投递的值类型。
 * @param connections 关闭或最后一个共享 awaitable 析构时要清理的注册表。
 * @return shared_ptr awaitable；Qt 回调会强捕获它以保持等待状态有效。
 * @note 对它调用 await_for() 超时只停止该次等待，不会取消底层 Qt 操作或信号订阅。
 */
template<typename T>
std::shared_ptr<Awaitable<T>> socket_awaitable(const SocketConnections& connections){
    auto awaitable = std::make_shared<Awaitable<T>>();
    awaitable->setOnClose(socket_cleanup(connections));
    return awaitable;
}

/**
 * @brief 将 awaitable 绑定到 source 和应用对象的生命周期。
 * @tparam Object source 的 QObject 类型。
 * @tparam T awaitable 的值类型。
 * @param source 被监视的 Qt 对象；为空时立即关闭 awaitable。
 * @param awaitable 被关闭和清理的共享 awaitable。
 * @param connections 与 awaitable 一同清理的注册表。
 * @param applicationLifetime 应用生命周期对象，默认当前 QCoreApplication。
 * @details source 或 applicationLifetime 销毁、应用 aboutToQuit 时关闭 awaitable 并
 *          断开连接；回调强捕获 awaitable 和 connections 以保证清理期间有效。
 */
template<typename Object, typename T>
void bind_socket_lifecycle(QPointer<Object> source,
                           const std::shared_ptr<Awaitable<T>>& awaitable,
                           const SocketConnections& connections,
                           QPointer<QObject> applicationLifetime =
                               QPointer<QObject>(QCoreApplication::instance())){
    if(!source){
        awaitable->close();
        cleanup_socket_connections(connections);
        return;
    }

    register_socket_connection(
        connections,
        QObject::connect(source.data(), &QObject::destroyed,
                         [awaitable, connections]{
            awaitable->close();
            cleanup_socket_connections(connections);
        }));

    if(applicationLifetime){
        register_socket_connection(
            connections,
            QObject::connect(applicationLifetime.data(), &QObject::destroyed,
                             [awaitable, connections]{
                awaitable->close();
                cleanup_socket_connections(connections);
            }));

        QPointer<QCoreApplication> application =
            qobject_cast<QCoreApplication*>(applicationLifetime.data());
        if(application){
            register_socket_connection(
                connections,
                QObject::connect(application.data(), &QCoreApplication::aboutToQuit,
                                 [awaitable, connections]{
                    awaitable->close();
                    cleanup_socket_connections(connections);
                }));
        }
    }
}

} // namespace detail
} // namespace Coro

#endif // CORO_SOCKETAWAIT_HPP
