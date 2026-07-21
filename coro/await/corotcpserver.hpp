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
#include <QTimer>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"

namespace Coro {

class CoroTcpServer{
    QPointer<QTcpServer> srv_;

    template<typename Function>
    static bool onServerThread(QPointer<QTcpServer> server, Function function){
        if(!server) return false;
        if(server->thread() == QThread::currentThread()){
            function(server.data());
            return true;
        }
        return QMetaObject::invokeMethod(
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
            while(!awaitable->channel()->is_closed() &&
                  current->hasPendingConnections()){
                awaitable->resolve(current->nextPendingConnection());
            }
        };
        if(server){
            detail::register_socket_connection(
                connections,
                QObject::connect(server.data(), &QTcpServer::newConnection,
                                 [awaitable, server]{
                    while(!awaitable->channel()->is_closed() && server &&
                          server->hasPendingConnections()){
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
        auto channel = awaitable->channel();
        if(server){
            QObject::connect(server.data(), &QObject::destroyed, [channel]{
                channel->discard_pending();
            });
        }
        detail::bind_socket_lifecycle(server, awaitable, connections);
        if(!onServerThread(server, [awaitable, connections, drain, server](
                                   QTcpServer* current){
            if(awaitable->channel()->is_closed()){
                detail::cleanup_socket_connections(connections);
                return;
            }
            auto timer = new QTimer(current);
            timer->setInterval(10);
            QPointer<QTimer> timerGuard(timer);
            detail::register_socket_cleanup(connections, [timerGuard]{
                if(!timerGuard) return;
                if(timerGuard->thread() == QThread::currentThread()){
                    timerGuard->stop();
                    timerGuard->deleteLater();
                    return;
                }
                QMetaObject::invokeMethod(timerGuard.data(), [timerGuard]{
                    if(timerGuard){
                        timerGuard->stop();
                        timerGuard->deleteLater();
                    }
                }, Qt::QueuedConnection);
            });
            detail::register_socket_connection(
                connections,
                QObject::connect(timer, &QTimer::timeout,
                                 [awaitable, connections, server, timerGuard]{
                    if(awaitable->channel()->is_closed()){
                        detail::cleanup_socket_connections(connections);
                        return;
                    }
                    if(!server || !server->isListening()){
                        awaitable->close();
                        detail::cleanup_socket_connections(connections);
                    }
                }));
            timer->start();
            drain(current);
            if(!current->isListening()){
                awaitable->close();
                detail::cleanup_socket_connections(connections);
            }
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }
};

inline CoroTcpServer coro(QTcpServer* server){
    return CoroTcpServer(server);
}

} // namespace Coro

#endif // COROTCPSERVER_HPP
