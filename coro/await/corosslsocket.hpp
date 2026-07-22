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
 *
 * 源 QObject 销毁或应用结束时，已返回的 awaitable 以默认 no_message 正常关闭；
 * 已排队的成功事件会先于终止结果被消费。消费者显式调用返回 Awaitable 的
 * close() 或 close(error) 时，首次关闭错误被保留，已排队事件仍先消费，
 * 注册的 Qt 信号连接和 cleanup 仅清理一次。errorOccurred（包括握手期间的
 * 对端关闭）使用 qt.socket category，sslErrors 和 peerVerifyError 使用
 * qt.ssl category；继承的非 TLS 操作遵循 CoroAbstractSocket 的终止契约。
 */
class CoroSslSocket : public CoroAbstractSocket {
    QPointer<QSslSocket> socket_;

    /**
     * @brief 在 SSL socket 所属线程执行或排队执行函数。
     * @tparam Function 可用 QSslSocket* 调用的函数类型。
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
     * @details action 在对象线程执行，encrypted 信号或 action 后的同步检查可成功完成
     *          等待。首次终止信号决定错误 category：errorOccurred 使用 qt.socket，
     *          sslErrors 或 peerVerifyError 使用 qt.ssl；部分握手失败可能先通过
     *          errorOccurred 报告，因此不保证所有握手失败都属于 qt.ssl。回调强捕获
     *          shared awaitable。
     * @tparam Action 可用 QSslSocket* 调用的握手动作函数类型。
     * @param action 在 SSL socket 所属线程执行一次的握手动作。
     * @return socket 已加密时成功，否则携带首次终止信号所确定错误的共享 awaitable。
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
     * @return socket 已加密时成功；首次终止的 errorOccurred 使用 qt.socket category，
     *         sslErrors 或 peerVerifyError 使用 qt.ssl category。部分握手失败可能属于
     *         qt.socket。
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
     * @return 连接并完成握手、socket 加密后成功；首次终止的 errorOccurred 使用
     *         qt.socket category，sslErrors 或 peerVerifyError 使用 qt.ssl category。
     *         部分握手失败可能属于 qt.socket。
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
 * @return 不取得对象所有权的 wrapper；socket 为空时，之后调用 TLS 或继承操作
 *         会返回立即以默认 no_message 正常关闭的 Awaitable。
 */
inline CoroSslSocket coro(QSslSocket* socket){
    return CoroSslSocket(socket);
}

} // namespace Coro

#endif // COROSSLSOCKET_HPP
