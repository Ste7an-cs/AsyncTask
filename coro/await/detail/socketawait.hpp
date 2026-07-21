#ifndef CORO_SOCKETAWAIT_HPP
#define CORO_SOCKETAWAIT_HPP

#include "await/awaitable.hpp"

#include <QCoreApplication>
#include <QMetaObject>
#include <QObject>
#include <QPointer>
#include <functional>
#include <memory>
#include <vector>

namespace Coro {
namespace detail {

using SocketConnection = std::shared_ptr<QMetaObject::Connection>;
using SocketConnections = std::shared_ptr<std::vector<SocketConnection>>;

inline SocketConnections socket_connections(){
    return std::make_shared<std::vector<SocketConnection>>();
}

inline SocketConnection socket_connection(const SocketConnections& connections){
    auto connection = std::make_shared<QMetaObject::Connection>();
    connections->push_back(connection);
    return connection;
}

inline void disconnect_socket_connections(const SocketConnections& connections){
    if(!connections) return;
    for(const auto& connection : *connections){
        if(connection) QObject::disconnect(*connection);
    }
}

inline std::function<void()> socket_cleanup(SocketConnections connections){
    return [connections = std::move(connections)]{
        disconnect_socket_connections(connections);
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
                           const SocketConnections& connections){
    if(!source){
        awaitable->close();
        disconnect_socket_connections(connections);
        return;
    }

    auto destroyed = socket_connection(connections);
    *destroyed = QObject::connect(source.data(), &QObject::destroyed,
                                  [awaitable, connections]{
        awaitable->close();
        disconnect_socket_connections(connections);
    });

    QPointer<QCoreApplication> application = QCoreApplication::instance();
    if(application){
        auto quitting = socket_connection(connections);
        *quitting = QObject::connect(application.data(), &QCoreApplication::aboutToQuit,
                                     [source, awaitable, connections]{
            if(source) awaitable->close();
            disconnect_socket_connections(connections);
        });
    }
}

} // namespace detail
} // namespace Coro

#endif // CORO_SOCKETAWAIT_HPP
