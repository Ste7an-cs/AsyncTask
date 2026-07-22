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

/**
 * @brief QLocalSocket 的非拥有协程包装器。
 * @details 不取得传入 Qt 对象的所有权。所有触及 socket 的操作都在其所属线程直接执行，
 *          或投递到该线程执行。公开工厂返回 shared Awaitable，Qt 回调强捕获该对象以
 *          保持等待状态有效；对返回值调用 await_for() 超时不会取消底层操作或订阅。
 */
class CoroLocalSocket{
    QPointer<QLocalSocket> local_;

    /**
     * @brief 在本地 socket 所属线程执行或排队执行函数。
     * @tparam Function 可用 QLocalSocket* 调用的函数类型。
     * @param socket 非拥有的 socket 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；socket 已销毁或投递失败时为 false。
     */
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

    /**
     * @brief 等待本地 socket 信号并可在对象线程发起动作的内部辅助函数。
     * @details 目标信号到达时直接 resolve 并关闭 awaitable；check 不约束信号回调，
     *          仅在 action 执行后提供同步完成的 fast path。PeerClosedError 正常关闭
     *          awaitable，其他 Qt 错误以 qt.local_socket category 关闭它；回调强捕获
     *          shared awaitable，source 或应用销毁时关闭它。
     * @tparam Signal 可传给 QObject::connect() 的目标信号类型。
     * @tparam Check 可用 QLocalSocket* 调用并返回完成状态的检查函数类型。
     * @tparam Action 可用 QLocalSocket* 调用的动作函数类型。
     * @param signal 到达时直接完成等待的目标信号。
     * @param check action 执行后用于同步 fast path 的完成状态检查函数。
     * @param action 在 socket 所属线程执行一次的动作。
     * @param peerCloseCompletes 对端关闭是否应作为成功完成处理。
     * @return 目标信号或同步 fast path 完成时成功，否则携带终止原因的共享 awaitable。
     */
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

    /**
     * @brief 使用空动作等待本地 socket 信号的简化重载。
     * @tparam Signal 可传给 QObject::connect() 的目标信号类型。
     * @tparam Check 可用 QLocalSocket* 调用并返回完成状态的检查函数类型。
     * @param signal 到达时直接完成等待的目标信号。
     * @param check 空动作执行后用于同步 fast path 的完成状态检查函数。
     * @return 目标信号或同步 fast path 完成时成功，否则携带终止原因的共享 awaitable。
     */
    template<typename Signal, typename Check>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check){
        return waitForSignal(signal, std::move(check), [](QLocalSocket*){});
    }

public:
    /**
     * @brief 用现有 QLocalSocket 创建非拥有包装器。
     * @param socket 源对象，可为空；包装器不会删除它。
     */
    explicit CoroLocalSocket(QLocalSocket* socket): local_(socket){}

    /**
     * @brief 创建持续读取字节块的流式 awaitable。
     * @return 每个值都是非空的当前可读字节块，直到 socket 关闭；PeerClosedError 正常
     *         结束流，其他本地 socket 错误以 qt.local_socket category 结束。
     * @note await_for() 超时不停止读取流，也不取消 Qt 信号订阅。
     */
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

    /**
     * @brief 等待至少一个可读字节。
     * @return bytesAvailable() 大于零时成功；PeerClosedError 正常关闭等待，其他本地
     *         socket 错误以 qt.local_socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForReadyRead(){
        return waitForSignal(&QIODevice::readyRead, [](QLocalSocket* socket){
            return socket->bytesAvailable() > 0;
        });
    }

    /**
     * @brief 等待 Qt 发出 bytesWritten 信号。
     * @return 写入信号发生时成功；PeerClosedError 正常关闭等待，其他本地 socket 错误
     *         以 qt.local_socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForBytesWritten(){
        return waitForSignal(&QIODevice::bytesWritten, [](QLocalSocket*){
            return false;
        });
    }

    /**
     * @brief 等待本地 socket 进入 ConnectedState。
     * @return 已连接时成功；PeerClosedError 正常关闭等待，其他连接错误以
     *         qt.local_socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForConnected(){
        return waitForSignal(&QLocalSocket::connected, [](QLocalSocket* socket){
            return socket->state() == QLocalSocket::ConnectedState;
        });
    }

    /**
     * @brief 等待本地 socket 进入 UnconnectedState。
     * @return 已断开或 PeerClosedError 表示对端正常关闭时成功；其他错误以
     *         qt.local_socket category 结束。
     */
    std::shared_ptr<Awaitable<void>> waitForDisconnected(){
        return waitForSignal(
            &QLocalSocket::disconnected,
            [](QLocalSocket* socket){
                return socket->state() == QLocalSocket::UnconnectedState;
            },
            [](QLocalSocket*){}, true);
    }

    /**
     * @brief 在对象线程发起到本地 server 的连接并等待成功。
     * @param name Qt 本地 server 名称。
     * @param mode 打开模式。
     * @return ConnectedState 时成功；PeerClosedError 正常关闭等待，其他连接错误以
     *         qt.local_socket category 结束。
     */
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

    /**
     * @brief 在对象线程请求从本地 server 断开并等待 UnconnectedState。
     * @return 已断开或 PeerClosedError 表示对端正常关闭时成功；其他错误以
     *         qt.local_socket category 结束。
     */
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

/**
 * @brief 创建 QLocalSocket 的非拥有协程包装器。
 * @param socket 源对象，可为空。
 * @return 即使 socket 为空也返回可安全关闭的 wrapper，且不会取得对象所有权。
 */
inline CoroLocalSocket coro(QLocalSocket* socket){
    return CoroLocalSocket(socket);
}

} // namespace Coro

#endif // COROLOCALSOCKET_HPP
