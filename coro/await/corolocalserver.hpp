#ifndef COROLOCALSERVER_HPP
#define COROLOCALSERVER_HPP

/**
 * @file corolocalserver.hpp
 * @brief QLocalServer 的协程包装器。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QLocalServer>
#include <QLocalSocket>
#include <QThread>
#include <QTimer>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"

namespace Coro {

class CoroLocalServer{
    QPointer<QLocalServer> server_;

    template<typename Function>
    static bool onServerThread(QPointer<QLocalServer> server, Function function){
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
    explicit CoroLocalServer(QLocalServer* server): server_(server){}

    std::shared_ptr<Awaitable<QLocalSocket*>> nextConnection(){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<QLocalSocket*>(connections);
        QPointer<QLocalServer> server = server_;

        auto drain = [awaitable](QLocalServer* current){
            while(!awaitable->channel()->is_closed() &&
                  current->hasPendingConnections()){
                awaitable->resolve(current->nextPendingConnection());
            }
        };
        if(server){
            detail::register_socket_connection(
                connections,
                QObject::connect(server.data(), &QLocalServer::newConnection,
                                 [awaitable, server]{
                    while(!awaitable->channel()->is_closed() && server &&
                          server->hasPendingConnections()){
                        awaitable->resolve(server->nextPendingConnection());
                    }
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
                                   QLocalServer* current){
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

inline CoroLocalServer coro(QLocalServer* server){
    return CoroLocalServer(server);
}

} // namespace Coro

#endif // COROLOCALSERVER_HPP
