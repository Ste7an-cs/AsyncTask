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

#include <QCoreApplication>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"
#include "detail/autodisconnect.hpp"

namespace Coro {

/**
 * @brief QAbstractSocket 的非拥有协程包装器。
 * @details 不取得传入 Qt 对象的所有权。所有触及 socket 的操作都在其所属线程直接执行，
 *          或投递到该线程执行。公开工厂返回 shared Awaitable，Qt 回调强捕获该对象以
 *          保持等待状态有效；对返回值调用 await_for() 超时不会取消底层操作或订阅。
 *
 * 源 QObject 销毁或应用结束时，已返回的 awaitable 以默认 no_message 正常关闭；
 * 消费者仍会先取完已排队的值，随后才观察到该终止结果。消费者也可在返回的
 * Awaitable 上显式调用 close() 或 close(error)；首次关闭决定终止错误，已排队值
 * 仍先被消费，且注册的 Qt 信号连接和 cleanup 仅清理一次。Qt 传输错误使用
 * qt.socket category。readAll() 中的 RemoteHostClosedError 以及断开等待中的
 * 普通远端关闭正常结束；其他等待或连接操作不会把该错误统一当作成功。
 * @code
 * using namespace std::chrono_literals;
 * QTcpSocket sock;
 * // 连接 -> 发送 -> 读回包，全部顺序书写，等待期间不阻塞线程
 * if(Coro::await_for(Coro::coro(&sock).connectToHost(host, port), 2s)){
 *     auto reply = Coro::coro(&sock).readAll();   // 流式等待器：建一次反复用
 *     sock.write("ping");
 *     auto data = Coro::await_for(reply, 2s);
 *     if(data) qDebug() << data.value();
 * }
 * @endcode
 */
class CoroAbstractSocket{
    QPointer<QAbstractSocket> sock_;

    /**
     * @brief 在 socket 所属线程执行或排队执行函数。
     * @tparam Function 可用 QAbstractSocket* 调用的函数类型。
     * @param socket 非拥有的 socket 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；socket 已销毁或投递失败时为 false。
     * @code
     * // 内部使用：保证所有 QObject 操作都发生在 socket 所属线程
     * onSocketThread(socket, [](QAbstractSocket* s){
     *     if(s->state() != QAbstractSocket::ConnectedState) s->connectToHost(host, port);
     * });
     * @endcode
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
     * @code
     * // 内部使用：一次性等待的统一实现。例如 connectToHost 即由它组合而成——
     * // action 发起连接，check 提供"已连接则立即成功"的同步快路径
     * return waitForSignal(
     *     &QAbstractSocket::connected,
     *     [](QAbstractSocket* s){ return s->state() == QAbstractSocket::ConnectedState; },
     *     [host, port](QAbstractSocket* s){ s->connectToHost(host, port); });
     * @endcode
     */
    template<typename Signal, typename Check, typename Action>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check,
                                                   Action action,
                                                   bool peerCloseCompletes = false){
        auto awaitable = std::make_shared<Awaitable<void>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QAbstractSocket> socket = sock_;

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
            scope->add(detail::connect_socket_error(
                socket.data(), [channel, scope, socket, peerCloseCompletes, succeed](
                                 QAbstractSocket::SocketError error){
                if(channel->is_closed()) return;
                if(peerCloseCompletes &&
                   error == QAbstractSocket::RemoteHostClosedError){
                    if(socket && socket->state() == QAbstractSocket::UnconnectedState){
                        succeed();
                    }
                    return;
                }
                channel->close(detail::socket_error_code(error));
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
                        action = std::move(action)](
                           QAbstractSocket* current) mutable {
            if(channel->is_closed()) return;
            action(current);
            if(check(current)){
                succeed();
            }else if(current->state() == QAbstractSocket::UnconnectedState &&
                     current->error() != QAbstractSocket::UnknownSocketError){
                channel->close(detail::socket_error_code(current->error()));
                scope->disconnectAll();
            }
        })){
            channel->close();
            scope->disconnectAll();
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
     * @code
     * // 内部使用：无需发起动作、只等信号的场景
     * return waitForSignal(&QIODevice::readyRead,
     *                      [](QAbstractSocket* s){ return s->bytesAvailable() > 0; });
     * @endcode
     */
    template<typename Signal, typename Check>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check){
        return waitForSignal(signal, std::move(check), [](QAbstractSocket*){});
    }

public:
    /**
     * @brief 用现有 QAbstractSocket 创建非拥有包装器。
     * @param socket 源对象，可为空；包装器不会删除它。
     * @code
     * // 一般用工厂 coro(sock)；包装器不取得所有权，socket 生命周期由调用方负责
     * QTcpSocket sock;
     * Coro::CoroAbstractSocket w(&sock);
     * Coro::await(w.waitForConnected());
     * @endcode
     */
    explicit CoroAbstractSocket(QAbstractSocket* socket): sock_(socket){}

    /**
     * @brief 创建持续读取字节块的流式 awaitable。
     * @return 每个值都是非空的当前可读字节块，直到 socket 关闭；普通远端关闭正常结束，
     *         其他传输错误以 qt.socket category 结束。
     * @note await_for() 超时不停止读取流，也不取消 Qt 信号订阅。
     * @code
     * using namespace std::chrono_literals;
     * // 推荐：建一次，反复取（订阅随句柄析构自动取消）
     * auto stream = Coro::coro(sock).readAll();
     * while(auto chunk = Coro::await_for(stream, 2s)){
     *     append(chunk.value());
     * }
     *
     * // 或直接流式遍历，socket 关闭时迭代自然结束
     * for(const QByteArray& c : Coro::generate(Coro::coro(sock).readAll())) append(c);
     * @endcode
     */
    std::shared_ptr<Awaitable<QByteArray>> readAll(){
        auto awaitable = std::make_shared<Awaitable<QByteArray>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QAbstractSocket> socket = sock_;

        // 业务槽只捕获 channel（+scope 用于终止时整组断开），绝不捕获 awaitable。
        auto drain = [channel](QAbstractSocket* current){
            if(current->bytesAvailable() > 0){
                const QByteArray bytes = current->readAll();
                if(!bytes.isEmpty()) channel->push(bytes);   // push 锁内自判 closed
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
            scope->on(socket.data(), &QAbstractSocket::disconnected,
                      [channel, socket, scope, drain]{
                if(channel->is_closed()) return;
                if(socket) drain(socket.data());
                channel->close();
                scope->disconnectAll();
            });
            scope->add(detail::connect_socket_error(
                socket.data(), [channel, socket, scope, drain](
                                 QAbstractSocket::SocketError error){
                if(channel->is_closed()) return;
                if(socket) drain(socket.data());
                if(error == QAbstractSocket::RemoteHostClosedError){
                    channel->close();
                }else{
                    channel->close(detail::socket_error_code(error));
                }
                scope->disconnectAll();
            }));
            scope->on(socket.data(), &QObject::destroyed, closeStop);
            if(auto app = QCoreApplication::instance()){
                scope->on(app, &QObject::destroyed, closeStop);
                scope->on(app, &QCoreApplication::aboutToQuit, closeStop);
            }
        }
        scope->untilExpired(awaitable);   // 句柄 close/析构 → 整组断开（修复核心）
        if(!onSocketThread(socket, [channel, scope, drain](QAbstractSocket* current){
            if(channel->is_closed()) return;
            drain(current);
            if(current->state() == QAbstractSocket::UnconnectedState){
                const auto error = current->error();
                if(error != QAbstractSocket::UnknownSocketError &&
                   error != QAbstractSocket::RemoteHostClosedError){
                    channel->close(detail::socket_error_code(error));
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
     * @return bytesAvailable() 大于零时成功；传输错误以 qt.socket category 结束。
     * @code
     * // 等到有数据后自行控制读取方式（与 readAll 的流式消费二选一）
     * if(Coro::await(Coro::coro(sock).waitForReadyRead())){
     *     QByteArray head = sock->read(4);
     * }
     * @endcode
     */
    std::shared_ptr<Awaitable<void>> waitForReadyRead(){
        return waitForSignal(&QIODevice::readyRead, [](QAbstractSocket* socket){
            return socket->bytesAvailable() > 0;
        });
    }

    /**
     * @brief 等待 Qt 发出 bytesWritten 信号。
     * @return 写入信号发生时成功；传输错误以 qt.socket category 结束。
     * @code
     * using namespace std::chrono_literals;
     * // 必须先建等待器再 write，否则可能漏掉快速到达的 bytesWritten
     * auto written = Coro::coro(sock).waitForBytesWritten();
     * sock->write(payload);
     * if(!Coro::await_for(written, 2s)) qWarning() << "flush timeout";
     * @endcode
     */
    std::shared_ptr<Awaitable<void>> waitForBytesWritten(){
        return waitForSignal(&QIODevice::bytesWritten, [](QAbstractSocket*){
            return false;
        });
    }

    /**
     * @brief 等待 socket 进入 ConnectedState。
     * @return 已连接时成功；连接失败以 qt.socket category 结束。
     * @code
     * using namespace std::chrono_literals;
     * // 连接由外部发起，这里只等待其完成
     * sock->connectToHost(host, port);
     * if(!Coro::await_for(Coro::coro(sock).waitForConnected(), 2s)) handleTimeout();
     * @endcode
     */
    std::shared_ptr<Awaitable<void>> waitForConnected(){
        return waitForSignal(&QAbstractSocket::connected, [](QAbstractSocket* socket){
            return socket->state() == QAbstractSocket::ConnectedState;
        });
    }

    /**
     * @brief 等待 socket 进入 UnconnectedState。
     * @return 已断开时成功，包含远端正常关闭；其他传输错误以 qt.socket category 结束。
     * @code
     * // 等待对端主动断开（远端正常关闭视为成功）
     * Coro::await(Coro::coro(sock).waitForDisconnected());
     * @endcode
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
     * @code
     * using namespace std::chrono_literals;
     * // 发起连接并等待完成（已连接则立即成功）
     * auto ok = Coro::await_for(
     *     Coro::coro(sock).connectToHost(QStringLiteral("example.com"), 80), 2s);
     * if(!ok) qWarning() << ok.error().message().c_str();
     * @endcode
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
     * @code
     * using namespace std::chrono_literals;
     * // 直接用地址连接，省去主机名解析
     * auto ok = Coro::await_for(
     *     Coro::coro(sock).connectToHost(QHostAddress::LocalHost, 8080), 2s);
     * @endcode
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
     * @code
     * using namespace std::chrono_literals;
     * // 主动断开并等待完成后再释放 socket
     * Coro::await_for(Coro::coro(sock).disconnectFromHost(), 2s);
     * @endcode
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
 * @return 不取得对象所有权的 wrapper；socket 为空时，之后调用操作会返回
 *         立即以默认 no_message 正常关闭的 Awaitable。
 * @code
 * using namespace std::chrono_literals;
 * QTcpSocket sock;
 * // 统一入口：方法名与 Qt 同名，返回值交给 await/await_for/generate 消费
 * Coro::await_for(Coro::coro(&sock).connectToHost(QHostAddress::LocalHost, 8080), 2s);
 * @endcode
 */
inline CoroAbstractSocket coro(QAbstractSocket* socket){
    return CoroAbstractSocket(socket);
}

} // namespace Coro

#endif // COROSOCKET_HPP
