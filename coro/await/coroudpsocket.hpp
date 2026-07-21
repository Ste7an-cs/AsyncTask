#ifndef COROUDPSOCKET_HPP
#define COROUDPSOCKET_HPP

/**
 * @file coroudpsocket.hpp
 * @brief QUdpSocket 的保留数据报边界协程包装器。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QNetworkDatagram>
#include <QThread>
#include <QTimer>
#include <QUdpSocket>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"

namespace Coro {

class CoroUdpSocket{
    QPointer<QUdpSocket> socket_;

    template<typename Function>
    static void onSocketThread(QPointer<QUdpSocket> socket, Function function){
        if(!socket) return;
        if(socket->thread() == QThread::currentThread()){
            function(socket.data());
            return;
        }
        QMetaObject::invokeMethod(
            socket.data(),
            [socket, function = std::move(function)]() mutable {
                if(socket) function(socket.data());
            },
            Qt::QueuedConnection);
    }

public:
    explicit CoroUdpSocket(QUdpSocket* socket): socket_(socket){}

    std::shared_ptr<Awaitable<QNetworkDatagram>> receiveDatagram(){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<QNetworkDatagram>(connections);
        QPointer<QUdpSocket> socket = socket_;

        auto drain = [awaitable](QUdpSocket* current){
            while(current->hasPendingDatagrams()){
                QNetworkDatagram datagram = current->receiveDatagram();
                if(datagram.isValid()) awaitable->resolve(datagram);
            }
        };
        if(socket){
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QIODevice::readyRead,
                                 [awaitable, socket]{
                    while(socket && socket->hasPendingDatagrams()){
                        QNetworkDatagram datagram = socket->receiveDatagram();
                        if(datagram.isValid()) awaitable->resolve(datagram);
                    }
                }));
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QAbstractSocket::errorOccurred,
                                 [awaitable, connections](
                                     QAbstractSocket::SocketError error){
                    awaitable->close(detail::socket_error_code(error));
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        onSocketThread(socket, [awaitable, connections, drain = std::move(drain),
                                socket](QUdpSocket* current){
            if(awaitable->channel()->is_closed()){
                detail::cleanup_socket_connections(connections);
                return;
            }
            auto timer = new QTimer(current);
            timer->setInterval(10);
            QPointer<QTimer> timerGuard(timer);
            auto isActive = [](const QUdpSocket* current){
                return current->state() != QAbstractSocket::UnconnectedState
                    || current->isValid();
            };
            auto wasActive = std::make_shared<bool>(isActive(current));
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
                                 [awaitable, connections, socket, wasActive,
                                  isActive]{
                    if(awaitable->channel()->is_closed()){
                        detail::cleanup_socket_connections(connections);
                        return;
                    }
                    if(!socket){
                        awaitable->close();
                        detail::cleanup_socket_connections(connections);
                        return;
                    }
                    if(isActive(socket.data())){
                        *wasActive = true;
                        return;
                    }
                    if(*wasActive){
                        awaitable->close();
                        detail::cleanup_socket_connections(connections);
                    }
                }));
            timer->start();
            drain(current);
        });
        return awaitable;
    }
};

inline CoroUdpSocket coro(QUdpSocket* socket){
    return CoroUdpSocket(socket);
}

} // namespace Coro

#endif // COROUDPSOCKET_HPP
