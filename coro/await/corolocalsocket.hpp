#ifndef COROLOCALSOCKET_HPP
#define COROLOCALSOCKET_HPP

/**
 * @file corolocalsocket.hpp
 * @brief QLocalSocket 的协程包装器。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QIODevice>
#include <QLocalSocket>
#include <QThread>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"

namespace Coro {

class CoroLocalSocket{
    QPointer<QLocalSocket> local_;

    template<typename Function>
    static bool onSocketThread(QPointer<QLocalSocket> socket, Function function){
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

    template<typename Signal, typename Check, typename Action>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check,
                                                   Action action,
                                                   bool peerCloseCompletes = false){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<void>(connections);
        QPointer<QLocalSocket> socket = local_;

        if(socket){
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), signal,
                                 [awaitable, connections](auto...){
                    awaitable->resolve();
                    awaitable->close();
                    detail::cleanup_socket_connections(connections);
                }));
            detail::register_socket_connection(
                connections,
                detail::connect_local_socket_error(
                    socket.data(), [awaitable, connections, socket,
                                    peerCloseCompletes](
                                     QLocalSocket::LocalSocketError error){
                    if(awaitable->channel()->is_closed()) return;
                    if(peerCloseCompletes && error == QLocalSocket::PeerClosedError){
                        if(socket && socket->state() == QLocalSocket::UnconnectedState){
                            awaitable->resolve();
                            awaitable->close();
                        }
                        return;
                    }else if(error == QLocalSocket::PeerClosedError){
                        awaitable->close();
                    }else{
                        awaitable->close(detail::local_socket_error_code(error));
                    }
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        if(!onSocketThread(socket,
                       [awaitable, connections, check = std::move(check),
                        action = std::move(action)](QLocalSocket* current) mutable {
            if(awaitable->channel()->is_closed()) return;
            action(current);
            if(check(current)){
                awaitable->resolve();
                awaitable->close();
                detail::cleanup_socket_connections(connections);
            }else if(current->state() == QLocalSocket::UnconnectedState &&
                     current->error() != QLocalSocket::UnknownSocketError){
                const auto error = current->error();
                if(error == QLocalSocket::PeerClosedError){
                    awaitable->close();
                }else{
                    awaitable->close(detail::local_socket_error_code(error));
                }
                detail::cleanup_socket_connections(connections);
            }
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }

    template<typename Signal, typename Check>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check){
        return waitForSignal(signal, std::move(check), [](QLocalSocket*){});
    }

public:
    explicit CoroLocalSocket(QLocalSocket* socket): local_(socket){}

    std::shared_ptr<Awaitable<QByteArray>> readAll(){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<QByteArray>(connections);
        QPointer<QLocalSocket> socket = local_;

        auto drain = [awaitable](QLocalSocket* current){
            if(awaitable->channel()->is_closed()) return;
            if(current->bytesAvailable() > 0){
                const QByteArray bytes = current->readAll();
                if(!bytes.isEmpty()) awaitable->resolve(bytes);
            }
        };
        if(socket){
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QIODevice::readyRead,
                                 [awaitable, socket]{
                    if(!awaitable->channel()->is_closed() && socket &&
                       socket->bytesAvailable() > 0){
                        const QByteArray bytes = socket->readAll();
                        if(!bytes.isEmpty()) awaitable->resolve(bytes);
                    }
                }));
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QLocalSocket::disconnected,
                                 [awaitable, connections, socket]{
                    if(awaitable->channel()->is_closed()) return;
                    if(socket && socket->bytesAvailable() > 0){
                        const QByteArray bytes = socket->readAll();
                        if(!bytes.isEmpty()) awaitable->resolve(bytes);
                    }
                    awaitable->close();
                    detail::cleanup_socket_connections(connections);
                }));
            detail::register_socket_connection(
                connections,
                detail::connect_local_socket_error(
                    socket.data(), [awaitable, connections, socket](
                                     QLocalSocket::LocalSocketError error){
                    if(awaitable->channel()->is_closed()) return;
                    if(socket && socket->bytesAvailable() > 0){
                        const QByteArray bytes = socket->readAll();
                        if(!bytes.isEmpty()) awaitable->resolve(bytes);
                    }
                    if(error == QLocalSocket::PeerClosedError){
                        awaitable->close();
                    }else{
                        awaitable->close(detail::local_socket_error_code(error));
                    }
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        if(!onSocketThread(socket, [awaitable, connections, drain](QLocalSocket* current){
            if(awaitable->channel()->is_closed()) return;
            drain(current);
            if(current->state() == QLocalSocket::UnconnectedState){
                const auto error = current->error();
                if(error != QLocalSocket::UnknownSocketError &&
                   error != QLocalSocket::PeerClosedError){
                    awaitable->close(detail::local_socket_error_code(error));
                }else{
                    awaitable->close();
                }
                detail::cleanup_socket_connections(connections);
            }
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }

    std::shared_ptr<Awaitable<void>> waitForReadyRead(){
        return waitForSignal(&QIODevice::readyRead, [](QLocalSocket* socket){
            return socket->bytesAvailable() > 0;
        });
    }

    std::shared_ptr<Awaitable<void>> waitForBytesWritten(){
        return waitForSignal(&QIODevice::bytesWritten, [](QLocalSocket*){
            return false;
        });
    }

    std::shared_ptr<Awaitable<void>> waitForConnected(){
        return waitForSignal(&QLocalSocket::connected, [](QLocalSocket* socket){
            return socket->state() == QLocalSocket::ConnectedState;
        });
    }

    std::shared_ptr<Awaitable<void>> waitForDisconnected(){
        return waitForSignal(
            &QLocalSocket::disconnected,
            [](QLocalSocket* socket){
                return socket->state() == QLocalSocket::UnconnectedState;
            },
            [](QLocalSocket*){}, true);
    }

    std::shared_ptr<Awaitable<void>> connectToServer(
        const QString& name,
        QIODevice::OpenMode mode = QIODevice::ReadWrite){
        return waitForSignal(
            &QLocalSocket::connected,
            [](QLocalSocket* socket){
                return socket->state() == QLocalSocket::ConnectedState;
            },
            [name, mode](QLocalSocket* socket){
                if(socket->state() != QLocalSocket::ConnectedState){
                    socket->connectToServer(name, mode);
                }
            });
    }

    std::shared_ptr<Awaitable<void>> disconnectFromServer(){
        return waitForSignal(
            &QLocalSocket::disconnected,
            [](QLocalSocket* socket){
                return socket->state() == QLocalSocket::UnconnectedState;
            },
            [](QLocalSocket* socket){
                if(socket->state() != QLocalSocket::UnconnectedState){
                    socket->disconnectFromServer();
                }
            }, true);
    }
};

inline CoroLocalSocket coro(QLocalSocket* socket){
    return CoroLocalSocket(socket);
}

} // namespace Coro

#endif // COROLOCALSOCKET_HPP
