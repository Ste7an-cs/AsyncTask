/// @brief TCP Socket Awaitable 的 ping-pong 示例。
///
/// @details 服务端绑定临时 loopback 端口并回显一次 ping；客户端随后连接已释放
/// 的端口，以稳定验证连接被拒绝。所有外部等待均有时限，且每个 Result 都会检查，
/// 因此既可在 CI 中运行，也可直接从终端运行。
#include <QCoreApplication>
#include <QDebug>
#include <QHostAddress>
#include <QNetworkProxy>
#include <QTcpServer>
#include <QTcpSocket>

#include <atomic>
#include <chrono>
#include <memory>
#include <system_error>

#include "await/coro.hpp"
#include "task/fiberapplication.h"
#include "task/fibertask.h"

using namespace Coro;
using namespace std::chrono_literals;

namespace {

constexpr auto kTimeout = 2s;

/// @brief 记录操作失败并返回失败状态。
/// @details 调用方必须检查对应的 Result，并把错误转换为示例的最终退出状态。
bool reportFailure(const char* operation, const std::error_code& error)
{
    qCritical() << operation << "failed:" << QString::fromStdString(error.message());
    return false;
}

} // namespace

/// @brief 运行有界的 TCP 回显、关闭和连接拒绝验证。
/// @details 主任务保留其结果直到事件循环退出，使所有 Result 检查共同决定进程状态。
int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();

    /// @details Coro::exec() 始终返回零，故在 quit() 解除阻塞前保存任务结果；原子
    /// 变量也为未来将任务绑定到工作线程的情形保持正确性。
    std::atomic_int exitCode{1};
    /// @brief 创建服务端、运行客户端成功路径，并汇总所有检查结果。
    makeTask([&exitCode]{
        auto server = std::make_unique<QTcpServer>();
        if(!server->listen(QHostAddress::LocalHost, 0)){
            qCritical() << "[server] listen failed:" << server->errorString();
            exitCode.store(1, std::memory_order_release);
            quit();
            return 1;
        }
        const quint16 port = server->serverPort();
        qDebug() << "[server] listening on" << port;

        /// @details Socket 包装方法返回 shared_ptr<Awaitable<T>>。流及其 Qt 回调会
        /// 在服务端关闭前强持有该句柄，源关闭时生成器迭代结束。
        auto incoming = coro(server.get()).nextConnection();
        /// @brief 接收连接，读取 ping 并回显其原始内容。
        /// @details 每个读取、写入完成的 Result 都必须检查；失败连接会显式中止并释放。
        auto serverTask = makeTask([incoming]{
            bool ok = true;
            for(QTcpSocket* peer : generate(incoming)){
                auto request = await_for(coro(peer).readAll(), kTimeout);
                if(!request){
                    ok = reportFailure("[server] read", request.error()) && ok;
                    peer->abort();
                    peer->deleteLater();
                    continue;
                }

                /// @details 必须在 write() 前创建等待器，避免快速到达的 bytesWritten
                /// 信号被遗漏。
                auto written = coro(peer).waitForBytesWritten();
                const qint64 count = peer->write(request.value());
                if(count != request.value().size()){
                    qCritical() << "[server] write failed:" << peer->errorString();
                    ok = false;
                }else{
                    const auto flushed = await_for(written, kTimeout);
                    if(!flushed) ok = reportFailure("[server] bytesWritten", flushed.error()) && ok;
                }
                peer->disconnectFromHost();
                peer->deleteLater();
            }
            qDebug() << "[server] connection stream closed";
            return ok;
        });

        bool ok = true;
        /// @brief 连接本地服务端并验证一次 ping-pong 成功路径。
        /// @details 连接、写入完成、读取回显和断开连接的每个 Result 都会单独检查。
        QTcpSocket client;
        client.setProxy(QNetworkProxy::NoProxy);
        const auto connected = await_for(
            coro(&client).connectToHost(QHostAddress::LocalHost, port), kTimeout);
        if(!connected){
            ok = reportFailure("[client] connect", connected.error()) && ok;
        }else{
            auto response = coro(&client).readAll();
            auto written = coro(&client).waitForBytesWritten();
            const QByteArray ping("ping");
            const qint64 count = client.write(ping);
            if(count != ping.size()){
                qCritical() << "[client] write failed:" << client.errorString();
                ok = false;
            }else{
                const auto flushed = await_for(written, kTimeout);
                if(!flushed) ok = reportFailure("[client] bytesWritten", flushed.error()) && ok;

                const auto echoed = await_for(response, kTimeout);
                if(!echoed){
                    ok = reportFailure("[client] read", echoed.error()) && ok;
                }else if(echoed.value() != ping){
                    qCritical() << "[client] unexpected response:" << echoed.value();
                    ok = false;
                }else{
                    qDebug() << "[client] got" << echoed.value();
                }
            }

            const auto disconnected = await_for(coro(&client).disconnectFromHost(), kTimeout);
            if(!disconnected) ok = reportFailure("[client] disconnect", disconnected.error()) && ok;
        }

        /// @brief 使用已释放端口稳定触发本地连接被拒绝。
        /// @details 临时监听器先保留端口再关闭，避免依赖公共主机；连接 Result 必须是
        /// qt.socket 的 ConnectionRefusedError。
        QTcpServer portProbe;
        if(!portProbe.listen(QHostAddress::LocalHost, 0)){
            qCritical() << "[refused] probe listen failed:" << portProbe.errorString();
            ok = false;
        }else{
            const quint16 refusedPort = portProbe.serverPort();
            portProbe.close();
            QTcpSocket refused;
            refused.setProxy(QNetworkProxy::NoProxy);
            const auto refusal = await_for(
                coro(&refused).connectToHost(QHostAddress::LocalHost, refusedPort), kTimeout);
            if(refusal){
                qCritical() << "[refused] connection unexpectedly succeeded";
                ok = false;
            }else if(QString::fromLatin1(refusal.error().category().name()) != "qt.socket" ||
                     refusal.error().value() != QAbstractSocket::ConnectionRefusedError){
                ok = reportFailure("[refused] expected connection refusal", refusal.error()) && ok;
            }else{
                qDebug() << "[refused]" << QString::fromStdString(refusal.error().message());
            }
        }

        /// @details await_for 若超时，仅以 timed_out 结束本次等待，不会取消 Awaitable，
        /// 也不会关闭 socket；因此仍需显式关闭服务端，以终止读取流。
        server->close();
        const auto serverResult = serverTask.get();
        if(!serverResult || !serverResult.value()){
            qCritical() << "[server] task failed";
            ok = false;
        }

        qDebug() << (ok ? "socket ping-pong passed" : "socket ping-pong failed");
        exitCode.store(ok ? 0 : 1, std::memory_order_release);
        quit();
        return ok ? 0 : 1;
    });

    exec();
    return exitCode.load(std::memory_order_acquire);
}
