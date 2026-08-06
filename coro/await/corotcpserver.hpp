#ifndef COROTCPSERVER_HPP
#define COROTCPSERVER_HPP

/**
 * @file corotcpserver.hpp
 * @brief QTcpServer 的协程包装器。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QTcpServer>
#include <QTcpSocket>
#include <QThread>
#include <QTimer>

#include <QCoreApplication>

#include "awaitable.hpp"
#include "detail/socketawait.hpp"
#include "detail/socketerror.hpp"
#include "detail/autodisconnect.hpp"

namespace Coro {

/**
 * @brief QTcpServer 的非拥有协程包装器。
 * @details 不取得 server 或其接受 socket 的所有权；所有 server 操作都在所属线程直接
 *          执行或投递到该线程。返回的 shared Awaitable 会被 Qt 回调强捕获，
 *          await_for() 超时不会取消连接流或 signal 订阅。
 *
 * server 停止监听时，连接流以默认 no_message 正常关闭，已排队指针仍先
 * 被消费。applicationLifetime 发出 aboutToQuit 或销毁时也以默认 no_message
 * 正常关闭并仅清理一次注册的 Qt 信号连接和 cleanup；该路径不丢弃已排队
 * 连接，消费者会先取完它们，队列耗尽后才观察到 no_message。源 server QObject
 * 销毁时同样正常关闭，但会先通过 discard_pending() 丢弃不能再安全返回的
 * 由 server 拥有的原始指针，防止悬空。消费者显式调用返回 Awaitable 的 close() 或
 * close(error) 时，
 * 已排队指针仍先消费，随后返回首次终止错误；注册的 Qt 信号连接和 cleanup
 * 仅清理一次。acceptError 使用 qt.socket category 终止流。
 * @code
 * QTcpServer server;
 * server.listen(QHostAddress::LocalHost, 0);
 * Coro::makeTask([&server]{
 *     // 像遍历容器一样接受连接；server 停止监听或销毁时迭代自然结束
 *     for(QTcpSocket* peer : Coro::generate(Coro::coro(&server).nextConnection())){
 *         handle(peer);      // peer 仍归 server 所有，勿超出其生命周期使用
 *     }
 *     return 0;
 * });
 * @endcode
 */
class CoroTcpServer{
    QPointer<QTcpServer> srv_;

    /**
     * @brief 在 server 所属线程执行或排队执行函数。
     * @tparam Function 可用 QTcpServer* 调用的函数类型。
     * @param server 非拥有的 server 守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；server 已销毁或投递失败时为 false。
     * @code
     * // 内部使用：保证 QTcpServer 操作都在其所属线程执行
     * onServerThread(server, [](QTcpServer* s){
     *     while(s->hasPendingConnections()) accept(s->nextPendingConnection());
     * });
     * @endcode
     */
    template<typename Function>
    static bool onServerThread(QPointer<QTcpServer> server, Function function){
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
     * @brief 用现有 QTcpServer 创建非拥有包装器。
     * @param server 源 server，可为空；包装器不会删除它。
     * @code
     * // 一般用工厂 coro(server)；包装器不取得所有权
     * QTcpServer server;
     * server.listen(QHostAddress::LocalHost, 0);
     * Coro::CoroTcpServer w(&server);
     * for(QTcpSocket* peer : Coro::generate(w.nextConnection())) handle(peer);
     * @endcode
     */
    explicit CoroTcpServer(QTcpServer* server): srv_(server){}

    /**
     * @brief 创建连续产生新连接的流式 awaitable。
     * @return 先排空当前 pending 队列，随后持续投递新连接；accept 错误以 qt.socket
     *         category 结束，server 关闭或销毁时流结束。
     * @details 每个 QTcpSocket* 仍由 Qt server 管理，消费者不得让它超过 server 生命周期，
     *          并必须遵守其线程亲和性。10 ms 定时器仅检测未发停止信号的 close()，不是
     *          连接超时；server 析构时会丢弃尚未消费的 queued 原始指针。
     * @code
     * using namespace std::chrono_literals;
     * // 流式接受连接（推荐）
     * for(QTcpSocket* peer : Coro::generate(Coro::coro(&server).nextConnection())){
     *     Coro::makeTask([peer]{ serve(peer); return 0; });
     * }
     *
     * // 或只接受一个连接
     * auto incoming = Coro::coro(&server).nextConnection();
     * auto first = Coro::await_for(incoming, 5s);
     * if(first) serve(first.value());
     * @endcode
     */
    std::shared_ptr<Awaitable<QTcpSocket*>> nextConnection(){
        auto awaitable = std::make_shared<Awaitable<QTcpSocket*>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QTcpServer> server = srv_;

        auto drain = [channel](QTcpServer* current){
            while(!channel->is_closed() && current->hasPendingConnections()){
                channel->push(current->nextPendingConnection());
            }
        };
        auto closeStop = [channel, scope]{
            channel->close();
            scope->disconnectAll();
        };
        if(server){
            // 独立 raw 连接(不入 scope)：server 析构时丢弃悬空的排队指针。必须独立于
            // scope，因为消费者可能先 close() 掉 awaitable(触发整组断开)再 delete server，
            // 此时仍需清掉已排队但即将悬空的 QTcpSocket*。随 server 析构自动移除、只捕 channel。
            QObject::connect(server.data(), &QObject::destroyed, [channel]{
                channel->discard_pending();
            });
            scope->on(server.data(), &QTcpServer::newConnection,
                      [channel, server, drain]{
                if(server) drain(server.data());
            });
            scope->on(server.data(), &QTcpServer::acceptError,
                      [channel, scope](QAbstractSocket::SocketError error){
                if(channel->is_closed()) return;
                channel->close(detail::socket_error_code(error));
                scope->disconnectAll();
            });
            scope->on(server.data(), &QObject::destroyed, closeStop);
            if(auto app = QCoreApplication::instance()){
                scope->on(app, &QObject::destroyed, closeStop);
                scope->on(app, &QCoreApplication::aboutToQuit, closeStop);
            }
        }
        scope->untilExpired(awaitable);
        if(!onServerThread(server, [channel, scope, drain, server](QTcpServer* current){
            if(channel->is_closed()) return;
            // 10 ms 定时器仅用于检测未发停止信号的 close()，不是连接超时。
            auto timer = new QTimer(current);
            timer->setInterval(10);
            QPointer<QTimer> timerGuard(timer);
            scope->addCleanup([timerGuard]{
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
            scope->on(timer, &QTimer::timeout, [channel, scope, server]{
                if(channel->is_closed()) return;
                if(!server || !server->isListening()){
                    channel->close();
                    scope->disconnectAll();
                }
            });
            timer->start();
            drain(current);
            if(!current->isListening()){
                channel->close();
                scope->disconnectAll();
            }
        })){
            channel->close();
            scope->disconnectAll();
        }
        return awaitable;
    }
};

/**
 * @brief 创建 QTcpServer 的非拥有协程包装器。
 * @param server 源 server，可为空。
 * @return 不取得对象所有权的 wrapper；server 为空时，之后调用 nextConnection()
 *         会返回立即以默认 no_message 正常关闭的 Awaitable。
 * @code
 * QTcpServer server;
 * server.listen(QHostAddress::LocalHost, 0);
 * for(QTcpSocket* peer : Coro::generate(Coro::coro(&server).nextConnection())){
 *     handle(peer);
 * }
 * @endcode
 */
inline CoroTcpServer coro(QTcpServer* server){
    return CoroTcpServer(server);
}

} // namespace Coro

#endif // COROTCPSERVER_HPP
