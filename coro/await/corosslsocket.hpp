#ifndef COROSSLSOCKET_HPP
#define COROSSLSOCKET_HPP

/**
 * @file corosslsocket.hpp
 * @brief QSslSocket 加密握手的协程包装器。
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

/**
 * @brief QSslSocket 的非拥有 TLS 协程包装器。
 * @details 不取得传入 Qt 对象的所有权。所有 TLS 操作都在所属线程直接执行或投递到该
 *          线程；Qt 回调强捕获返回的 shared Awaitable。await_for() 超时不会取消正在
 *          进行的连接、握手或 signal 订阅。
 */
class CoroSslSocket : public CoroAbstractSocket {
    QPointer<QSslSocket> socket_;

    /**
     * @brief 在 SSL socket 所属线程执行或排队执行函数。
     * @param socket 非拥有的 socket 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；socket 已销毁或投递失败时为 false。
     */
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

    /**
     * @brief 等待当前 TLS 握手加密完成的内部辅助函数。
     * @details action 在对象线程执行。传输错误以 qt.socket category 结束，证书验证和
     *          握手错误以 qt.ssl category 结束；回调强捕获 shared awaitable。
     */
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
    /**
     * @brief 用现有 QSslSocket 创建非拥有包装器。
     * @param socket 源对象，可为空；包装器不会删除它。
     */
    explicit CoroSslSocket(QSslSocket* socket)
        : CoroAbstractSocket(socket), socket_(socket){}

    /**
     * @brief 仅等待当前已发起的 TLS 握手完成。
     * @return socket 已加密时成功；传输错误使用 qt.socket category，证书或握手错误
     *         使用 qt.ssl category。
     */
    std::shared_ptr<Awaitable<void>> waitForEncrypted(){
        return waitForEncrypted([](QSslSocket*){});
    }

    /**
     * @brief 在对象线程发起到主机的 TLS 连接和握手。
     * @param host 目标主机名。
     * @param port 目标端口。
     * @param mode 打开模式。
     * @param protocol 网络层协议。
     * @return 连接并完成握手、socket 加密后成功；传输错误使用 qt.socket category，
     *         证书或握手错误使用 qt.ssl category。
     */
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

/**
 * @brief 创建 QSslSocket 的非拥有协程包装器。
 * @param socket 源对象，可为空。
 * @return 即使 socket 为空也返回可安全关闭的 wrapper，且不会取得对象所有权。
 */
inline CoroSslSocket coro(QSslSocket* socket){
    return CoroSslSocket(socket);
}

} // namespace Coro

#endif // COROSSLSOCKET_HPP
