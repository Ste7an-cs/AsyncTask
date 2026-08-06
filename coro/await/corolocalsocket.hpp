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

#include <QCoreApplication>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"
#include "detail/autodisconnect.hpp"

namespace Coro {

/**
 * @brief QLocalSocket 的非拥有协程包装器。
 * @details 不取得传入 Qt 对象的所有权。所有触及 socket 的操作都在其所属线程直接执行，
 *          或投递到该线程执行。公开工厂返回 shared Awaitable，Qt 回调强捕获该对象以
 *          保持等待状态有效；对返回值调用 await_for() 超时不会取消底层操作或订阅。
 *
 * 源 QObject 销毁或应用结束时，已返回的 awaitable 以默认 no_message 正常关闭；
 * 消费者仍会先取完已排队的值，随后才观察到该终止结果。消费者也可在返回的
 * Awaitable 上显式调用 close() 或 close(error)；首次关闭决定终止错误，已排队值
 * 仍先被消费，且注册的 Qt 信号连接和 cleanup 仅清理一次。Qt 本地 socket 错误
 * 使用 qt.local_socket category。readAll() 遇到 PeerClosedError 正常结束；
 * waitForDisconnected() 和 disconnectFromServer() 把对端关闭作为断开成功，
 * 其他等待或连接操作则正常关闭而不产生成功事件。
 * @code
 * using namespace std::chrono_literals;
 * QLocalSocket sock;
 * // 与 TCP 版用法一致，仅错误 category 为 qt.local_socket
 * if(Coro::await_for(Coro::coro(&sock).connectToServer(QStringLiteral("my-service")), 2s)){
 *     auto reply = Coro::coro(&sock).readAll();
 *     sock.write("ping");
 *     auto data = Coro::await_for(reply, 2s);
 * }
 * @endcode
 */
class CoroLocalSocket{
    QPointer<QLocalSocket> local_;

    /**
     * @brief 在本地 socket 所属线程执行或排队执行函数。
     * @tparam Function 可用 QLocalSocket* 调用的函数类型。
     * @param socket 非拥有的 socket 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；socket 已销毁或投递失败时为 false。
     * @code
     * // 内部使用：保证 QLocalSocket 操作都在其所属线程执行
     * onSocketThread(socket, [name](QLocalSocket* s){
     *     if(s->state() != QLocalSocket::ConnectedState) s->connectToServer(name);
     * });
     * @endcode
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
     * @code
     * // 内部使用：一次性等待的统一实现。connectToServer 即由它组合而成
     * return waitForSignal(
     *     &QLocalSocket::connected,
     *     [](QLocalSocket* s){ return s->state() == QLocalSocket::ConnectedState; },
     *     [name](QLocalSocket* s){ s->connectToServer(name); });
     * @endcode
     */
    template<typename Signal, typename Check, typename Action>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check,
                                                   Action action,
                                                   bool peerCloseCompletes = false){
        auto awaitable = std::make_shared<Awaitable<void>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QLocalSocket> socket = local_;

        auto succeed = [channel, scope]{
            channel->push(1);          // resolve void
            channel->close();
            scope->disconnectAll();
        };
        auto closeStop = [channel, scope]{
            channel->close();
            scope->disconnectAll();
        };
        if(socket){
            scope->on(socket.data(), signal, [succeed](auto...){ succeed(); });
            scope->add(detail::connect_local_socket_error(
                socket.data(), [channel, scope, socket, peerCloseCompletes, succeed](
                                 QLocalSocket::LocalSocketError error){
                if(channel->is_closed()) return;
                if(peerCloseCompletes && error == QLocalSocket::PeerClosedError){
                    if(socket && socket->state() == QLocalSocket::UnconnectedState){
                        succeed();
                    }
                    return;
                }else if(error == QLocalSocket::PeerClosedError){
                    channel->close();
                }else{
                    channel->close(detail::local_socket_error_code(error));
                }
                scope->disconnectAll();
            }));
            scope->on(socket.data(), &QObject::destroyed, closeStop);
            if(auto app = QCoreApplication::instance()){
                scope->on(app, &QObject::destroyed, closeStop);
                scope->on(app, &QCoreApplication::aboutToQuit, closeStop);
            }
        }
        scope->untilExpired(awaitable);
        if(!onSocketThread(socket,
                       [channel, scope, succeed, check = std::move(check),
                        action = std::move(action)](QLocalSocket* current) mutable {
            if(channel->is_closed()) return;
            action(current);
            if(check(current)){
                succeed();
            }else if(current->state() == QLocalSocket::UnconnectedState &&
                     current->error() != QLocalSocket::UnknownSocketError){
                const auto error = current->error();
                if(error == QLocalSocket::PeerClosedError){
                    channel->close();
                }else{
                    channel->close(detail::local_socket_error_code(error));
                }
                scope->disconnectAll();
            }
        })){
            channel->close();
            scope->disconnectAll();
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
     * @code
     * // 内部使用：无需发起动作、只等信号的场景
     * return waitForSignal(&QIODevice::readyRead,
     *                      [](QLocalSocket* s){ return s->bytesAvailable() > 0; });
     * @endcode
     */
    template<typename Signal, typename Check>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check){
        return waitForSignal(signal, std::move(check), [](QLocalSocket*){});
    }

public:
    /**
     * @brief 用现有 QLocalSocket 创建非拥有包装器。
     * @param socket 源对象，可为空；包装器不会删除它。
     * @code
     * // 一般用工厂 coro(sock)；包装器不取得所有权
     * QLocalSocket sock;
     * Coro::CoroLocalSocket w(&sock);
     * Coro::await(w.connectToServer(QStringLiteral("my-service")));
     * @endcode
     */
    explicit CoroLocalSocket(QLocalSocket* socket): local_(socket){}

    /**
     * @brief 创建持续读取字节块的流式 awaitable。
     * @return 每个值都是非空的当前可读字节块，直到 socket 关闭；PeerClosedError 正常
     *         结束流，其他本地 socket 错误以 qt.local_socket category 结束。
     * @note await_for() 超时不停止读取流，也不取消 Qt 信号订阅。
     * @code
     * using namespace std::chrono_literals;
     * // 建一次、反复取（订阅随句柄析构自动取消）
     * auto stream = Coro::coro(&sock).readAll();
     * while(auto chunk = Coro::await_for(stream, 2s)) append(chunk.value());
     *
     * // 或流式遍历
     * for(const QByteArray& c : Coro::generate(Coro::coro(&sock).readAll())) append(c);
     * @endcode
     */
    std::shared_ptr<Awaitable<QByteArray>> readAll(){
        auto awaitable = std::make_shared<Awaitable<QByteArray>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QLocalSocket> socket = local_;

        auto drain = [channel](QLocalSocket* current){
            if(current->bytesAvailable() > 0){
                const QByteArray bytes = current->readAll();
                if(!bytes.isEmpty()) channel->push(bytes);
            }
        };
        auto closeStop = [channel, scope]{
            channel->close();
            scope->disconnectAll();
        };
        if(socket){
            scope->on(socket.data(), &QIODevice::readyRead, [channel, socket]{
                if(socket && !channel->is_closed() && socket->bytesAvailable() > 0){
                    const QByteArray bytes = socket->readAll();
                    if(!bytes.isEmpty()) channel->push(bytes);
                }
            });
            scope->on(socket.data(), &QLocalSocket::disconnected,
                      [channel, socket, scope, drain]{
                if(channel->is_closed()) return;
                if(socket) drain(socket.data());
                channel->close();
                scope->disconnectAll();
            });
            scope->add(detail::connect_local_socket_error(
                socket.data(), [channel, socket, scope, drain](
                                 QLocalSocket::LocalSocketError error){
                if(channel->is_closed()) return;
                if(socket) drain(socket.data());
                if(error == QLocalSocket::PeerClosedError){
                    channel->close();
                }else{
                    channel->close(detail::local_socket_error_code(error));
                }
                scope->disconnectAll();
            }));
            scope->on(socket.data(), &QObject::destroyed, closeStop);
            if(auto app = QCoreApplication::instance()){
                scope->on(app, &QObject::destroyed, closeStop);
                scope->on(app, &QCoreApplication::aboutToQuit, closeStop);
            }
        }
        scope->untilExpired(awaitable);
        if(!onSocketThread(socket, [channel, scope, drain](QLocalSocket* current){
            if(channel->is_closed()) return;
            drain(current);
            if(current->state() == QLocalSocket::UnconnectedState){
                const auto error = current->error();
                if(error != QLocalSocket::UnknownSocketError &&
                   error != QLocalSocket::PeerClosedError){
                    channel->close(detail::local_socket_error_code(error));
                }else{
                    channel->close();
                }
                scope->disconnectAll();
            }
        })){
            channel->close();
            scope->disconnectAll();
        }
        return awaitable;
    }

    /**
     * @brief 等待至少一个可读字节。
     * @return bytesAvailable() 大于零时成功；PeerClosedError 正常关闭等待，其他本地
     *         socket 错误以 qt.local_socket category 结束。
     * @code
     * if(Coro::await(Coro::coro(&sock).waitForReadyRead())){
     *     QByteArray head = sock.read(4);
     * }
     * @endcode
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
     * @code
     * using namespace std::chrono_literals;
     * // 先建等待器再 write，避免漏掉快速到达的 bytesWritten
     * auto written = Coro::coro(&sock).waitForBytesWritten();
     * sock.write(payload);
     * Coro::await_for(written, 2s);
     * @endcode
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
     * @code
     * using namespace std::chrono_literals;
     * // 连接由外部发起，这里只等待其完成
     * sock.connectToServer(QStringLiteral("my-service"));
     * Coro::await_for(Coro::coro(&sock).waitForConnected(), 2s);
     * @endcode
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
     * @code
     * // 等待对端主动断开
     * Coro::await(Coro::coro(&sock).waitForDisconnected());
     * @endcode
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
     * @code
     * using namespace std::chrono_literals;
     * // 发起连接并等待完成（已连接则立即成功）
     * auto ok = Coro::await_for(
     *     Coro::coro(&sock).connectToServer(QStringLiteral("my-service")), 2s);
     * if(!ok) qWarning() << ok.error().message().c_str();
     * @endcode
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
     * @code
     * using namespace std::chrono_literals;
     * // 主动断开并等待完成后再释放 socket
     * Coro::await_for(Coro::coro(&sock).disconnectFromServer(), 2s);
     * @endcode
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
 * @return 不取得对象所有权的 wrapper；socket 为空时，之后调用操作会返回
 *         立即以默认 no_message 正常关闭的 Awaitable。
 * @code
 * using namespace std::chrono_literals;
 * QLocalSocket sock;
 * Coro::await_for(Coro::coro(&sock).connectToServer(QStringLiteral("my-service")), 2s);
 * @endcode
 */
inline CoroLocalSocket coro(QLocalSocket* socket){
    return CoroLocalSocket(socket);
}

} // namespace Coro

#endif // COROLOCALSOCKET_HPP
