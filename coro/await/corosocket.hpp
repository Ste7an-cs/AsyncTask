#ifndef COROSOCKET_HPP
#define COROSOCKET_HPP

/**
 * @file corosocket.hpp
 * @brief QAbstractSocket 的协程包装器。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QIODevice>
#include <QAbstractSocket>
#include <QHostAddress>
#include <QThread>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"

namespace Coro {

class CoroAbstractSocket{
    QPointer<QAbstractSocket> sock_;

    template<typename Function>
    static void onSocketThread(QPointer<QAbstractSocket> socket, Function function){
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

    template<typename Signal, typename Check, typename Action>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check,
                                                   Action action){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<void>(connections);
        QPointer<QAbstractSocket> socket = sock_;

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
                detail::connect_socket_error(
                    socket.data(), [awaitable, connections](QAbstractSocket::SocketError error){
                    awaitable->close(detail::socket_error_code(error));
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        onSocketThread(socket,
                       [awaitable, connections, check = std::move(check),
                        action = std::move(action)](
                           QAbstractSocket* current) mutable {
            action(current);
            if(check(current)){
                awaitable->resolve();
                awaitable->close();
                detail::cleanup_socket_connections(connections);
            }else if(current->state() == QAbstractSocket::UnconnectedState &&
                     current->error() != QAbstractSocket::UnknownSocketError){
                awaitable->close(detail::socket_error_code(current->error()));
                detail::cleanup_socket_connections(connections);
            }
        });
        return awaitable;
    }

    template<typename Signal, typename Check>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check){
        return waitForSignal(signal, std::move(check), [](QAbstractSocket*){});
    }

public:
    explicit CoroAbstractSocket(QAbstractSocket* socket): sock_(socket){}

    std::shared_ptr<Awaitable<QByteArray>> readAll(){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<QByteArray>(connections);
        QPointer<QAbstractSocket> socket = sock_;

        auto drain = [awaitable](QAbstractSocket* current){
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
                    if(socket && socket->bytesAvailable() > 0){
                        const QByteArray bytes = socket->readAll();
                        if(!bytes.isEmpty()) awaitable->resolve(bytes);
                    }
                }));
            detail::register_socket_connection(
                connections,
                QObject::connect(socket.data(), &QAbstractSocket::disconnected,
                                 [awaitable, connections, socket]{
                    if(socket && socket->bytesAvailable() > 0){
                        const QByteArray bytes = socket->readAll();
                        if(!bytes.isEmpty()) awaitable->resolve(bytes);
                    }
                    awaitable->close();
                    detail::cleanup_socket_connections(connections);
                }));
            detail::register_socket_connection(
                connections,
                detail::connect_socket_error(
                    socket.data(), [awaitable, connections, socket](
                                     QAbstractSocket::SocketError error){
                    if(socket && socket->bytesAvailable() > 0){
                        const QByteArray bytes = socket->readAll();
                        if(!bytes.isEmpty()) awaitable->resolve(bytes);
                    }
                    if(error == QAbstractSocket::RemoteHostClosedError){
                        awaitable->close();
                    }else{
                        awaitable->close(detail::socket_error_code(error));
                    }
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        onSocketThread(socket, [awaitable, connections, drain](QAbstractSocket* current){
            drain(current);
            if(current->state() == QAbstractSocket::UnconnectedState){
                const auto error = current->error();
                if(error != QAbstractSocket::UnknownSocketError &&
                   error != QAbstractSocket::RemoteHostClosedError){
                    awaitable->close(detail::socket_error_code(error));
                }else{
                    awaitable->close();
                }
                detail::cleanup_socket_connections(connections);
            }
        });
        return awaitable;
    }

    std::shared_ptr<Awaitable<void>> waitForReadyRead(){
        return waitForSignal(&QIODevice::readyRead, [](QAbstractSocket* socket){
            return socket->bytesAvailable() > 0;
        });
    }

    std::shared_ptr<Awaitable<void>> waitForBytesWritten(){
        return waitForSignal(&QIODevice::bytesWritten, [](QAbstractSocket*){
            return false;
        });
    }

    std::shared_ptr<Awaitable<void>> waitForConnected(){
        return waitForSignal(&QAbstractSocket::connected, [](QAbstractSocket* socket){
            return socket->state() == QAbstractSocket::ConnectedState;
        });
    }

    std::shared_ptr<Awaitable<void>> waitForDisconnected(){
        return waitForSignal(&QAbstractSocket::disconnected, [](QAbstractSocket* socket){
            return socket->state() == QAbstractSocket::UnconnectedState;
        });
    }

    std::shared_ptr<Awaitable<void>> connectToHost(
        const QString& host, quint16 port,
        QIODevice::OpenMode mode = QIODevice::ReadWrite){
        return waitForSignal(
            &QAbstractSocket::connected,
            [](QAbstractSocket* socket){
                return socket->state() == QAbstractSocket::ConnectedState;
            },
            [host, port, mode](QAbstractSocket* socket){
                if(socket->state() != QAbstractSocket::ConnectedState){
                    socket->connectToHost(host, port, mode);
                }
            });
    }

    std::shared_ptr<Awaitable<void>> connectToHost(
        const QHostAddress& address, quint16 port,
        QIODevice::OpenMode mode = QIODevice::ReadWrite){
        return waitForSignal(
            &QAbstractSocket::connected,
            [](QAbstractSocket* socket){
                return socket->state() == QAbstractSocket::ConnectedState;
            },
            [address, port, mode](QAbstractSocket* socket){
                if(socket->state() != QAbstractSocket::ConnectedState){
                    socket->connectToHost(address, port, mode);
                }
            });
    }

    std::shared_ptr<Awaitable<void>> disconnectFromHost(){
        auto awaitable = waitForDisconnected();
        QPointer<QAbstractSocket> socket = sock_;
        onSocketThread(socket, [](QAbstractSocket* current){
            if(current->state() != QAbstractSocket::UnconnectedState){
                current->disconnectFromHost();
            }
        });
        return awaitable;
    }
};

inline CoroAbstractSocket coro(QAbstractSocket* socket){
    return CoroAbstractSocket(socket);
}

} // namespace Coro

#endif // COROSOCKET_HPP
