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

class SocketConnectionRegistry {
public:
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

using SocketConnections = std::shared_ptr<SocketConnectionRegistry>;

inline SocketConnections socket_connections(){
    return std::make_shared<SocketConnectionRegistry>();
}

inline void register_socket_connection(const SocketConnections& connections,
                                       QMetaObject::Connection connection){
    if(connections){
        connections->registerConnection(connection);
    }else{
        QObject::disconnect(connection);
    }
}

inline void register_socket_cleanup(const SocketConnections& connections,
                                    std::function<void()> cleanup){
    if(connections){
        connections->registerCleanup(std::move(cleanup));
    }else if(cleanup){
        cleanup();
    }
}

inline void cleanup_socket_connections(const SocketConnections& connections){
    if(connections) connections->cleanup();
}

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

inline std::function<void()> socket_cleanup(SocketConnections connections){
    return [connections = std::move(connections)]{
        cleanup_socket_connections(connections);
    };
}

template<typename T>
std::shared_ptr<Awaitable<T>> socket_awaitable(const SocketConnections& connections){
    auto awaitable = std::make_shared<Awaitable<T>>();
    awaitable->setOnClose(socket_cleanup(connections));
    return awaitable;
}

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
