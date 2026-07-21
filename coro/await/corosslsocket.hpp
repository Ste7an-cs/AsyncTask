#ifndef COROSSLSOCKET_HPP
#define COROSSLSOCKET_HPP

/**
 * @file corosslsocket.hpp
 * @brief QSslSocket encryption handshake coroutine wrapper.
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QSslSocket>
#include <QThread>

#include "corosocket.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"

namespace Coro {

class CoroSslSocket : public CoroAbstractSocket {
    QPointer<QSslSocket> socket_;

    template<typename Function>
    static bool onSocketThread(QPointer<QSslSocket> socket, Function function){
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

    template<typename Action>
    std::shared_ptr<Awaitable<void>> waitForEncrypted(Action action){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<void>(connections);
        QPointer<QSslSocket> socket = socket_;

        if(socket){
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QSslSocket::encrypted,
                                 [awaitable, connections]{
                    awaitable->resolve();
                    awaitable->close();
                    detail::cleanup_socket_connections(connections);
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
                QObject::connect(
                    socket.data(),
                    static_cast<void (QSslSocket::*)(const QList<QSslError>&)>(
                        &QSslSocket::sslErrors),
                                 [awaitable, connections](
                                     const QList<QSslError>& errors){
                    if(!errors.isEmpty()){
                        awaitable->close(detail::ssl_error_code(errors.first().error()));
                        detail::cleanup_socket_connections(connections);
                    }
                }));
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QSslSocket::peerVerifyError,
                                 [awaitable, connections](const QSslError& error){
                    awaitable->close(detail::ssl_error_code(error.error()));
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        if(!onSocketThread(socket, [awaitable, connections, action = std::move(action)](
                                   QSslSocket* current) mutable {
            if(awaitable->channel()->is_closed()) return;
            action(current);
            if(current->isEncrypted()){
                awaitable->resolve();
                awaitable->close();
                detail::cleanup_socket_connections(connections);
            }else if(current->state() == QAbstractSocket::UnconnectedState &&
                     current->error() != QAbstractSocket::UnknownSocketError){
                awaitable->close(detail::socket_error_code(current->error()));
                detail::cleanup_socket_connections(connections);
            }
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }

public:
    explicit CoroSslSocket(QSslSocket* socket)
        : CoroAbstractSocket(socket), socket_(socket){}

    std::shared_ptr<Awaitable<void>> waitForEncrypted(){
        return waitForEncrypted([](QSslSocket*){});
    }

    std::shared_ptr<Awaitable<void>> connectToHostEncrypted(
        const QString& host, quint16 port,
        QIODevice::OpenMode mode = QIODevice::ReadWrite,
        QAbstractSocket::NetworkLayerProtocol protocol =
            QAbstractSocket::AnyIPProtocol){
        return waitForEncrypted(
            [host, port, mode, protocol](QSslSocket* socket){
                if(!socket->isEncrypted()){
                    socket->connectToHostEncrypted(host, port, mode, protocol);
                }
            });
    }
};

inline CoroSslSocket coro(QSslSocket* socket){
    return CoroSslSocket(socket);
}

} // namespace Coro

#endif // COROSSLSOCKET_HPP
