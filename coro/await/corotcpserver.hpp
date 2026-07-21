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

    static void stopTimer(
        const std::shared_ptr<QPointer<QTimer>>& timerState){
        if(!timerState || !*timerState) return;
        QPointer<QTimer> timer = *timerState;
        if(timer->thread() == QThread::currentThread()){
            timer->stop();
        }else{
            QMetaObject::invokeMethod(timer.data(), [timer]{
                if(timer) timer->stop();
            }, Qt::QueuedConnection);
        }
    }

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
        auto timerState = std::make_shared<QPointer<QTimer>>();

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
                                 [awaitable, connections, timerState](
                                     QAbstractSocket::SocketError error){
                    stopTimer(timerState);
                    awaitable->close(detail::socket_error_code(error));
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(server, awaitable, connections);
        onServerThread(server, [awaitable, connections, drain, server,
                                timerState](QTcpServer* current){
            if(awaitable->channel()->is_closed()) return;
            auto timer = new QTimer(current);
            timer->setInterval(10);
            *timerState = timer;
            QPointer<QTimer> timerGuard(timer);
            detail::register_socket_connection(
                connections,
                QObject::connect(timer, &QTimer::timeout,
                                 [awaitable, connections, server, timerGuard]{
                    if(awaitable->channel()->is_closed()){
                        if(timerGuard) timerGuard->stop();
                        detail::cleanup_socket_connections(connections);
                        return;
                    }
                    if(!server || !server->isListening()){
                        if(timerGuard) timerGuard->stop();
                        awaitable->close();
                        detail::cleanup_socket_connections(connections);
                    }
                }));
            timer->start();
            drain(current);
            if(!current->isListening()){
                timer->stop();
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
