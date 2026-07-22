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

/**
 * @brief QAbstractSocket 的非拥有协程包装器。
 * @details 不取得传入 Qt 对象的所有权。所有触及 socket 的操作都在其所属线程直接执行，
 *          或投递到该线程执行。公开工厂返回 shared Awaitable，Qt 回调强捕获该对象以
 *          保持等待状态有效；对返回值调用 await_for() 超时不会取消底层操作或订阅。
 */
class CoroAbstractSocket{
    QPointer<QAbstractSocket> sock_;

    /**
     * @brief 在 socket 所属线程执行或排队执行函数。
     * @tparam Function 可用 QAbstractSocket* 调用的函数类型。
     * @param socket 非拥有的 socket 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；socket 已销毁或投递失败时为 false。
     */
    template<typename Function>
    static bool onSocketThread(QPointer<QAbstractSocket> socket, Function function){
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
     * @brief 等待信号并可在 socket 线程发起动作的内部辅助函数。
     * @details 目标信号到达时直接 resolve 并关闭 awaitable；check 不约束信号回调，
     *          仅在 action 执行后提供同步完成的 fast path。Qt 错误以 qt.socket category
     *          关闭 awaitable；回调强捕获 shared awaitable，source 或应用销毁时关闭它。
     * @tparam Signal 可传给 QObject::connect() 的目标信号类型。
     * @tparam Check 可用 QAbstractSocket* 调用并返回完成状态的检查函数类型。
     * @tparam Action 可用 QAbstractSocket* 调用的动作函数类型。
     * @param signal 到达时直接完成等待的目标信号。
     * @param check action 执行后用于同步 fast path 的完成状态检查函数。
     * @param action 在 socket 所属线程执行一次的动作。
     * @param peerCloseCompletes 远端关闭是否应作为正常完成处理。
     * @return 目标信号或同步 fast path 完成时成功，否则携带终止原因的共享 awaitable。
     */
    template<typename Signal, typename Check, typename Action>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check,
                                                   Action action,
                                                   bool peerCloseCompletes = false){
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
                    socket.data(), [awaitable, connections, socket,
                                    peerCloseCompletes](QAbstractSocket::SocketError error){
                    if(awaitable->channel()->is_closed()) return;
                    if(peerCloseCompletes &&
                       error == QAbstractSocket::RemoteHostClosedError){
                        if(socket && socket->state() == QAbstractSocket::UnconnectedState){
                            awaitable->resolve();
                            awaitable->close();
                        }
                        return;
                    }
                    awaitable->close(detail::socket_error_code(error));
                    detail::cleanup_socket_connections(connections);
                }));
        }
        detail::bind_socket_lifecycle(socket, awaitable, connections);
        if(!onSocketThread(socket,
                       [awaitable, connections, check = std::move(check),
                        action = std::move(action)](
                           QAbstractSocket* current) mutable {
            if(awaitable->channel()->is_closed()) return;
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
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }

    /**
     * @brief 使用空动作等待信号的 waitForSignal() 简化重载。
     * @tparam Signal 可传给 QObject::connect() 的目标信号类型。
     * @tparam Check 可用 QAbstractSocket* 调用并返回完成状态的检查函数类型。
     * @param signal 到达时直接完成等待的目标信号。
     * @param check 空动作执行后用于同步 fast path 的完成状态检查函数。
     * @return 目标信号或同步 fast path 完成时成功，否则携带终止原因的共享 awaitable。
     */
    template<typename Signal, typename Check>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check){
        return waitForSignal(signal, std::move(check), [](QAbstractSocket*){});
    }

public:
    /**
     * @brief 用现有 QAbstractSocket 创建非拥有包装器。
     * @param socket 源对象，可为空；包装器不会删除它。
     */
    explicit CoroAbstractSocket(QAbstractSocket* socket): sock_(socket){}

    /**
     * @brief 创建持续读取字节块的流式 awaitable。
     * @return 每个值都是非空的当前可读字节块，直到 socket 关闭；普通远端关闭正常结束，
     *         其他传输错误以 qt.socket category 结束。
     * @note await_for() 超时不停止读取流，也不取消 Qt 信号订阅。
     */
    std::shared_ptr<Awaitable<QByteArray>> readAll(){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<QByteArray>(connections);
        QPointer<QAbstractSocket> socket = sock_;

        auto drain = [awaitable](QAbstractSocket* current){
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
                QObject::connect(socket.data(), &QAbstractSocket::disconnected,
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
                detail::connect_socket_error(
                    socket.data(), [awaitable, connections, socket](
                                     QAbstractSocket::SocketError error){
                    if(awaitable->channel()->is_closed()) return;
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
        if(!onSocketThread(socket, [awaitable, connections, drain](QAbstractSocket* current){
            if(awaitable->channel()->is_closed()) return;
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
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }

    /**
     * @brief 等待至少一个可读字节。
     * @return bytesAvailable() 大于零时成功；传输错误以 qt.socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForReadyRead(){
        return waitForSignal(&QIODevice::readyRead, [](QAbstractSocket* socket){
            return socket->bytesAvailable() > 0;
        });
    }

    /**
     * @brief 等待 Qt 发出 bytesWritten 信号。
     * @return 写入信号发生时成功；传输错误以 qt.socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForBytesWritten(){
        return waitForSignal(&QIODevice::bytesWritten, [](QAbstractSocket*){
            return false;
        });
    }

    /**
     * @brief 等待 socket 进入 ConnectedState。
     * @return 已连接时成功；连接失败以 qt.socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForConnected(){
        return waitForSignal(&QAbstractSocket::connected, [](QAbstractSocket* socket){
            return socket->state() == QAbstractSocket::ConnectedState;
        });
    }

    /**
     * @brief 等待 socket 进入 UnconnectedState。
     * @return 已断开时成功，包含远端正常关闭；其他传输错误以 qt.socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForDisconnected(){
        return waitForSignal(
            &QAbstractSocket::disconnected,
            [](QAbstractSocket* socket){
                return socket->state() == QAbstractSocket::UnconnectedState;
            },
            [](QAbstractSocket*){}, true);
    }

    /**
     * @brief 在对象线程发起到主机名的连接并等待成功。
     * @param host 目标主机名。
     * @param port 目标端口。
     * @param mode 打开模式。
     * @return ConnectedState 时成功；连接错误以 qt.socket category 结束。
     */
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

    /**
     * @brief 在对象线程发起到地址的连接并等待成功。
     * @param address 目标网络地址。
     * @param port 目标端口。
     * @param mode 打开模式。
     * @return ConnectedState 时成功；连接错误以 qt.socket category 结束。
     */
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

    /**
     * @brief 在对象线程请求断开并等待 UnconnectedState。
     * @return 已断开或远端正常关闭时成功；其他传输错误以 qt.socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> disconnectFromHost(){
        return waitForSignal(
            &QAbstractSocket::disconnected,
            [](QAbstractSocket* socket){
                return socket->state() == QAbstractSocket::UnconnectedState;
            },
            [](QAbstractSocket* socket){
                if(socket->state() != QAbstractSocket::UnconnectedState){
                    socket->disconnectFromHost();
                }
            }, true);
    }
};

/**
 * @brief 创建 QAbstractSocket 的非拥有协程包装器。
 * @param socket 源对象，可为空。
 * @return 即使 socket 为空也返回可安全关闭的 wrapper，且不会取得对象所有权。
 */
inline CoroAbstractSocket coro(QAbstractSocket* socket){
    return CoroAbstractSocket(socket);
}

} // namespace Coro

#endif // COROSOCKET_HPP
