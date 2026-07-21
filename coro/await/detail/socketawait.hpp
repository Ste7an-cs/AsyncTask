#ifndef CORO_SOCKETAWAIT_HPP
#define CORO_SOCKETAWAIT_HPP

#include "await/awaitable.hpp"

#include <QCoreApplication>
#include <QMetaObject>
#include <QObject>
#include <QPointer>
#include <functional>
#include <memory>
#include <mutex>
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

    void cleanup(){
        std::vector<QMetaObject::Connection> connections;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(cleaned_) return;
            cleaned_ = true;
            connections.swap(connections_);
        }
        for(const auto& connection : connections) QObject::disconnect(connection);
    }

private:
    std::mutex mutex_;
    std::vector<QMetaObject::Connection> connections_;
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

inline void cleanup_socket_connections(const SocketConnections& connections){
    if(connections) connections->cleanup();
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
