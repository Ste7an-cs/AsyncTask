#ifndef COROLOCALSERVER_HPP
#define COROLOCALSERVER_HPP

/**
 * @file corolocalserver.hpp
 * @brief QLocalServer 的协程包装器。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QLocalServer>
#include <QLocalSocket>
#include <QThread>
#include <QTimer>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"

namespace Coro {

/**
 * @brief QLocalServer 的非拥有协程包装器。
 * @details 不取得 server 或其接受 socket 的所有权；所有 server 操作都在所属线程直接
 *          执行或投递到该线程。返回的 shared Awaitable 会被 Qt 回调强捕获，
 *          await_for() 超时不会取消连接流或 signal 订阅。
 *
 * server 停止监听时，连接流以默认 no_message 正常关闭，已排队指针仍先
 * 被消费；源 QObject 销毁时也正常关闭，但会先丢弃不能再安全返回的已排队
 * 原始指针。消费者显式调用返回 Awaitable 的 close() 或 close(error) 时，
 * 已排队指针仍先消费，随后返回首次终止错误；注册的 Qt 信号连接和 cleanup
 * 仅清理一次。该包装器未将 QLocalServer::serverError() 转换为终止错误。
 */
class CoroLocalServer{
    QPointer<QLocalServer> server_;

    /**
     * @brief 在 server 所属线程执行或排队执行函数。
     * @tparam Function 可用 QLocalServer* 调用的函数类型。
     * @param server 非拥有的 server 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；server 已销毁或投递失败时为 false。
     */
    template<typename Function>
    static bool onServerThread(QPointer<QLocalServer> server, Function function){
        if(!server) return false;
        if(server->thread() == QThread::currentThread()){
            function(server.data());
            return true;
        }
        return QMetaObject::invokeMethod(
            server.data(),
            [server, function = std::move(function)]() mutable {
                if(server) function(server.data());
            },
            Qt::QueuedConnection);
    }

public:
    /**
     * @brief 用现有 QLocalServer 创建非拥有包装器。
     * @param server 源 server，可为空；包装器不会删除它。
     */
    explicit CoroLocalServer(QLocalServer* server): server_(server){}

    /**
     * @brief 创建连续产生本地连接的流式 awaitable。
     * @return 先排空当前 pending 队列，随后持续投递新连接；server 关闭或销毁时流结束。
     * @details 每个 QLocalSocket* 仍由 Qt server 管理，消费者不得让它超过 server 生命周期，
     *          并必须遵守其线程亲和性。10 ms 定时器仅检测未发停止信号的 close()，不是
     *          连接超时；server 析构时会丢弃尚未消费的 queued 原始指针。
     */
    std::shared_ptr<Awaitable<QLocalSocket*>> nextConnection(){
        auto connections = detail::socket_connections();
        auto awaitable = detail::socket_awaitable<QLocalSocket*>(connections);
        QPointer<QLocalServer> server = server_;

        auto drain = [awaitable](QLocalServer* current){
            while(!awaitable->channel()->is_closed() &&
                  current->hasPendingConnections()){
                awaitable->resolve(current->nextPendingConnection());
            }
        };
        if(server){
            detail::register_socket_connection(
                connections,
                QObject::connect(server.data(), &QLocalServer::newConnection,
                                 [awaitable, server]{
                    while(!awaitable->channel()->is_closed() && server &&
                          server->hasPendingConnections()){
                        awaitable->resolve(server->nextPendingConnection());
                    }
                }));
        }
        auto channel = awaitable->channel();
        if(server){
            QObject::connect(server.data(), &QObject::destroyed, [channel]{
                channel->discard_pending();
            });
        }
        detail::bind_socket_lifecycle(server, awaitable, connections);
        if(!onServerThread(server, [awaitable, connections, drain, server](
                                   QLocalServer* current){
            if(awaitable->channel()->is_closed()){
                detail::cleanup_socket_connections(connections);
                return;
            }
            auto timer = new QTimer(current);
            timer->setInterval(10);
            QPointer<QTimer> timerGuard(timer);
            detail::register_socket_cleanup(connections, [timerGuard]{
                if(!timerGuard) return;
                if(timerGuard->thread() == QThread::currentThread()){
                    timerGuard->stop();
                    timerGuard->deleteLater();
                    return;
                }
                QMetaObject::invokeMethod(timerGuard.data(), [timerGuard]{
                    if(timerGuard){
                        timerGuard->stop();
                        timerGuard->deleteLater();
                    }
                }, Qt::QueuedConnection);
            });
            detail::register_socket_connection(
                connections,
                QObject::connect(timer, &QTimer::timeout,
                                 [awaitable, connections, server, timerGuard]{
                    if(awaitable->channel()->is_closed()){
                        detail::cleanup_socket_connections(connections);
                        return;
                    }
                    if(!server || !server->isListening()){
                        awaitable->close();
                        detail::cleanup_socket_connections(connections);
                    }
                }));
            timer->start();
            drain(current);
            if(!current->isListening()){
                awaitable->close();
                detail::cleanup_socket_connections(connections);
            }
        })){
            awaitable->close();
            detail::cleanup_socket_connections(connections);
        }
        return awaitable;
    }
};

/**
 * @brief 创建 QLocalServer 的非拥有协程包装器。
 * @param server 源 server，可为空。
 * @return 不取得对象所有权的 wrapper；server 为空时，之后调用 nextConnection()
 *         会返回立即以默认 no_message 正常关闭的 Awaitable。
 */
inline CoroLocalServer coro(QLocalServer* server){
    return CoroLocalServer(server);
}

} // namespace Coro

#endif // COROLOCALSERVER_HPP
