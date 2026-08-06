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

#include <QCoreApplication>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"
#include "detail/autodisconnect.hpp"

namespace Coro {

/**
 * @brief QUdpSocket 的非拥有协程包装器。
 * @details 不取得传入 Qt 对象的所有权。所有 socket 操作都在所属线程直接执行或投递到
 *          该线程；Qt 回调强捕获返回的 shared Awaitable。await_for() 超时不会取消
 *          数据报接收流或 signal 订阅。
 *
 * 源 QObject 销毁、应用结束或 socket 进入 UnconnectedState 时，数据报流以默认
 * no_message 正常关闭；消费者仍会先取完已排队的数据报，随后才观察到
 * 该终止结果。消费者显式调用返回 Awaitable 的 close() 或 close(error) 时，
 * 首次关闭决定终止错误，已排队数据报仍先消费，且注册的 Qt 信号连接和
 * cleanup 仅清理一次。Qt 传输错误使用 qt.socket category 终止流。
 * @code
 * QUdpSocket sock;
 * sock.bind(QHostAddress::LocalHost, 9000);
 * Coro::makeTask([&sock]{
 *     // 每个元素是一个完整数据报，保留发送者地址/端口等 metadata
 *     for(const QNetworkDatagram& dg :
 *             Coro::generate(Coro::coro(&sock).receiveDatagram())){
 *         qDebug() << dg.senderAddress() << dg.data();
 *     }
 *     return 0;
 * });
 * @endcode
 */
class CoroUdpSocket{
    QPointer<QUdpSocket> socket_;

    /**
     * @brief 在 UDP socket 所属线程执行或排队执行函数。
     * @tparam Function 可用 QUdpSocket* 调用的函数类型。
     * @param socket 非拥有的 socket 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；socket 已销毁或投递失败时为 false。
     * @code
     * // 内部使用：保证 QUdpSocket 操作都在其所属线程执行
     * onSocketThread(socket, [](QUdpSocket* s){
     *     while(s->hasPendingDatagrams()) take(s->receiveDatagram());
     * });
     * @endcode
     */
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
    /**
     * @brief 用现有 QUdpSocket 创建非拥有包装器。
     * @param socket 源对象，可为空；包装器不会删除它。
     * @code
     * // 一般用工厂 coro(sock)；包装器不取得所有权
     * QUdpSocket sock;
     * sock.bind(QHostAddress::LocalHost, 9000);
     * Coro::CoroUdpSocket w(&sock);
     * auto dg = Coro::await(w.receiveDatagram());
     * @endcode
     */
    explicit CoroUdpSocket(QUdpSocket* socket): socket_(socket){}

    /**
     * @brief 创建保留数据报边界的接收流。
     * @return 每个值对应一个完整 QNetworkDatagram，并保留发送端地址、端口等元数据；
     *         初始检查发现 UnconnectedState 或后续 stateChanged 进入该状态时正常关闭流，
     *         socket 销毁时也正常关闭；传输错误使用 qt.socket category 终止流。
     * @code
     * using namespace std::chrono_literals;
     * // 流式接收：数据报边界不会被合并或拆分
     * for(const QNetworkDatagram& dg :
     *         Coro::generate(Coro::coro(&sock).receiveDatagram())){
     *     reply(dg.senderAddress(), dg.senderPort(), dg.data());
     * }
     *
     * // 或带超时收一个
     * auto one = Coro::await_for(Coro::coro(&sock).receiveDatagram(), 1s);
     * @endcode
     */
    std::shared_ptr<Awaitable<QNetworkDatagram>> receiveDatagram(){
        auto awaitable = std::make_shared<Awaitable<QNetworkDatagram>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QUdpSocket> socket = socket_;

        auto drain = [channel](QUdpSocket* current){
            while(!channel->is_closed() && current->hasPendingDatagrams()){
                QNetworkDatagram datagram = current->receiveDatagram();
                if(datagram.isValid()) channel->push(datagram);
            }
        };
        auto closeStop = [channel, scope]{
            channel->close();
            scope->disconnectAll();
        };
        if(socket){
            scope->on(socket.data(), &QIODevice::readyRead, [channel, socket, drain]{
                if(socket) drain(socket.data());
            });
            scope->add(detail::connect_socket_error(
                socket.data(), [channel, scope](QAbstractSocket::SocketError error){
                if(channel->is_closed()) return;
                channel->close(detail::socket_error_code(error));
                scope->disconnectAll();
            }));
            scope->on(socket.data(), &QAbstractSocket::stateChanged,
                      [scope, closeStop](QAbstractSocket::SocketState state){
                if(state == QAbstractSocket::UnconnectedState) closeStop();
            });
            scope->on(socket.data(), &QObject::destroyed, closeStop);
            if(auto app = QCoreApplication::instance()){
                scope->on(app, &QObject::destroyed, closeStop);
                scope->on(app, &QCoreApplication::aboutToQuit, closeStop);
            }
        }
        scope->untilExpired(awaitable);
        if(!onSocketThread(socket, [channel, scope, drain](QUdpSocket* current){
            if(channel->is_closed()) return;
            if(current->state() == QAbstractSocket::UnconnectedState){
                channel->close();
                scope->disconnectAll();
                return;
            }
            drain(current);
        })){
            channel->close();
            scope->disconnectAll();
        }
        return awaitable;
    }
};

/**
 * @brief 创建 QUdpSocket 的非拥有协程包装器。
 * @param socket 源对象，可为空。
 * @return 不取得对象所有权的 wrapper；socket 为空时，之后调用 receiveDatagram()
 *         会返回立即以默认 no_message 正常关闭的 Awaitable。
 * @code
 * QUdpSocket sock;
 * sock.bind(QHostAddress::LocalHost, 9000);
 * for(const QNetworkDatagram& dg :
 *         Coro::generate(Coro::coro(&sock).receiveDatagram())){
 *     qDebug() << dg.data();
 * }
 * @endcode
 */
inline CoroUdpSocket coro(QUdpSocket* socket){
    return CoroUdpSocket(socket);
}

} // namespace Coro

#endif // COROUDPSOCKET_HPP
