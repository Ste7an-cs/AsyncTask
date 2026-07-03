///
/// socket ping-pong 示例：TCP 服务端/客户端。
///   - 服务端：generate(coro(server).nextConnection()) 持续接受连接，
///     每个连接一个协程，用 generate(coro(sock).readAll()) 流式读取并回显。
///   - 客户端：coro(client).waitForConnected() 等连接，之后收发若干轮。
///
#include <QCoreApplication>
#include <QTcpServer>
#include <QTcpSocket>
#include <QHostAddress>
#include <QDebug>
#include <boost/fiber/all.hpp>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/coro.hpp"

using namespace Coro;

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();

    makeTask([]{
        QTcpServer* server = new QTcpServer();

        // 服务端协程
        auto task_server = makeTask([server]{
            server->listen(QHostAddress::LocalHost, 40088);
            for(QTcpSocket* sock : generate(coro(server).nextConnection())){
                makeTask([sock]{
                    for(const QByteArray& msg : generate(coro(sock).readAll())){
                        sock->write(msg);              // 回显
                        boost::this_fiber::yield();
                    }
                    qDebug() << "[server] connection closed";
                });
                boost::this_fiber::yield();
            }
        });

        // 客户端协程
        auto task_client = makeTask([]{
            QTcpSocket* client = new QTcpSocket();
            client->connectToHost(QHostAddress::LocalHost, 40088);
            await(coro(client).waitForConnected());
            for(int k = 0; k < 3; k++){
                client->write(QByteArray("ping ") + QByteArray::number(k));
                auto r = await(coro(client).readAll());
                if(r) qDebug() << "[client] got" << r.value();
                boost::this_fiber::yield();
            }
            client->close();
            client->deleteLater();
        });

        task_client.get();
        Coro::sleep(1);
        server->close();
        delete server;          // 触发 nextConnection 收尾，服务端协程结束
        task_server.get();

        quit();
        return 0;
    });

    return exec();
}
