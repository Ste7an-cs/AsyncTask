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
#include <QUdpSocket>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"

namespace Coro {

class CoroUdpSocket{
    QPointer<QUdpSocket> socket_;

    template<typename Function>
    static bool onSocketThread(QPointer<QUdpSocket> socket, Function function){
        if(!socket) return false;
        if(socket->thread() == QThread::currentThread()){
            function(socket.data());
            return true;
        }
        return QMetaObject::invokeMethod(
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
            while(!awaitable->channel()->is_closed() &&
                  current->hasPendingDatagrams()){
                QNetworkDatagram datagram = current->receiveDatagram();
                if(datagram.isValid()) awaitable->resolve(datagram);
            }
        };
        if(socket){
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QIODevice::readyRead,
                                 [awaitable, socket]{
                    while(!awaitable->channel()->is_closed() && socket &&
                          socket->hasPendingDatagrams()){
                        QNetworkDatagram datagram = socket->receiveDatagram();
                        if(datagram.isValid()) awaitable->resolve(datagram);
                    }
                }));
            detail::register_socket_connection(
                connections,
                detail::connect_socket_error(
                    socket.data(), [awaitable, connections](
                                     QAbstractSocket::SocketError error){
                    awaitable->close(detail::socket_error_code(error));
                    detail::cleanup_socket_connections(connections);
                }));
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QAbstractSocket::stateChanged,
                                 [awaitable, connections](
                                     QAbstractSocket::SocketState state){
                    if(state == QAbstractSocket::UnconnectedState){
                        awaitable->close();
                        detail::cleanup_socket_connections(connections);
                    }
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        if(!onSocketThread(socket, [awaitable, connections, drain = std::move(drain)](
                                  QUdpSocket* current){
            if(awaitable->channel()->is_closed()){
                detail::cleanup_socket_connections(connections);
                return;
            }
            drain(current);
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }
};

inline CoroUdpSocket coro(QUdpSocket* socket){
    return CoroUdpSocket(socket);
}

} // namespace Coro

#endif // COROUDPSOCKET_HPP
