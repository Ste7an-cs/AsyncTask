/// TCP socket Awaitable example.
///
/// The server binds an ephemeral loopback port, echoes one ping, then the
/// client demonstrates a deterministic refused connection on a released port.
/// Every external wait is bounded and every Result is checked, so this example
/// is safe to run in CI as well as from a terminal.
#include <QCoreApplication>
#include <QDebug>
#include <QHostAddress>
#include <QNetworkProxy>
#include <QTcpServer>
#include <QTcpSocket>

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

bool reportFailure(const char* operation, const std::error_code& error)
{
    qCritical() << operation << "failed:" << QString::fromStdString(error.message());
    return false;
}

} // namespace

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();

    makeTask([]{
        auto server = std::make_unique<QTcpServer>();
        if(!server->listen(QHostAddress::LocalHost, 0)){
            qCritical() << "[server] listen failed:" << server->errorString();
            quit();
            return 1;
        }
        const quint16 port = server->serverPort();
        qDebug() << "[server] listening on" << port;

        // Socket wrapper methods return shared_ptr<Awaitable<T>>. The stream
        // and its Qt callbacks keep this handle strongly alive until the
        // server is closed; generator iteration ends when that source closes.
        auto incoming = coro(server.get()).nextConnection();
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

                // Create the waiter before write() so a fast bytesWritten
                // signal cannot be missed.
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

        // Closing a temporary listener gives a locally reserved, now-refused
        // port. This tests the Qt error path without relying on a public host.
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

        // await_for only ends this wait with timed_out. It neither cancels the
        // Awaitable nor closes the socket, so source shutdown remains explicit.
        server->close();
        const auto serverResult = serverTask.get();
        if(!serverResult || !serverResult.value()){
            qCritical() << "[server] task failed";
            ok = false;
        }

        qDebug() << (ok ? "socket ping-pong passed" : "socket ping-pong failed");
        quit();
        return ok ? 0 : 1;
    });

    return exec();
}
