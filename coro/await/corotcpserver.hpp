#ifndef COROTCPSERVER_HPP
#define COROTCPSERVER_HPP

/**
 * @file corotcpserver.hpp
 * @brief QTcpServer 的协程包装器。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QTcpServer>
#include <QTcpSocket>
#include <QThread>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"

namespace Coro {

class CoroTcpServer{
    QPointer<QTcpServer> srv_;

    template<typename Function>
    static void onServerThread(QPointer<QTcpServer> server, Function function){
        if(!server) return;
        if(server->thread() == QThread::currentThread()){
            function(server.data());
            return;
        }
        QMetaObject::invokeMethod(
            server.data(),
            [server, function = std::move(function)]() mutable {
                if(server) function(server.data());
            },
            Qt::QueuedConnection);
    }

public:
    explicit CoroTcpServer(QTcpServer* server): srv_(server){}

    std::shared_ptr<Awaitable<QTcpSocket*>> nextConnection(){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<QTcpSocket*>(connections);
        QPointer<QTcpServer> server = srv_;

        auto drain = [awaitable](QTcpServer* current){
            while(current->hasPendingConnections()){
                awaitable->resolve(current->nextPendingConnection());
            }
        };
        if(server){
            detail::register_socket_connection(
                connections,
                QObject::connect(server.data(), &QTcpServer::newConnection,
                                 [awaitable, server]{
                    while(server && server->hasPendingConnections()){
                        awaitable->resolve(server->nextPendingConnection());
                    }
                }));
            detail::register_socket_connection(
                connections,
                QObject::connect(server.data(), &QTcpServer::acceptError,
                                 [awaitable, connections](
                                     QAbstractSocket::SocketError error){
                    awaitable->close(detail::socket_error_code(error));
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(server, awaitable, connections);
        onServerThread(server, [awaitable, connections, drain](QTcpServer* current){
            drain(current);
            if(!current->isListening()){
                awaitable->close();
                detail::cleanup_socket_connections(connections);
            }
        });
        return awaitable;
    }
};

inline CoroTcpServer coro(QTcpServer* server){
    return CoroTcpServer(server);
}

} // namespace Coro

#endif // COROTCPSERVER_HPP
