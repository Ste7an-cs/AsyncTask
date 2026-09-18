/**
 * @file tst_testlatency.cpp
 * @brief Qt 系 I/O 的往返时延回归断言。
 * @details 单进程内 TCP 回环请求-响应，度量往返最小值。阈值宽松，只为守住
 *          issue #6 修复后不再退回「每跳等一个固定事件分发间隔」的地板，
 *          不追求精确基准（精确基准见 example/latency_pingpong）。
 */
#include <QtTest>
#include <QCoreApplication>
#include <QElapsedTimer>
#include <QHostAddress>
#include <QNetworkProxy>
#include <QTcpServer>
#include <QTcpSocket>

#include <atomic>
#include <chrono>
#include <limits>

#include "await/coro.hpp"
#include "task/fiberapplication.h"
#include "task/fibertask.h"

using namespace std::chrono_literals;

class test_latency : public QObject
{
    Q_OBJECT
private slots:
    void initTestCase();
    void test_case_loopback_rtt();
};

void test_latency::initTestCase()
{
    // 环境里若有 SOCKS5 代理，Qt 的 bind 会失败，用例会以无关原因挂掉。
    QNetworkProxy::setApplicationProxy(QNetworkProxy::NoProxy);
}

void test_latency::test_case_loopback_rtt()
{
    Coro::installFiberApplication();

    QTcpServer server;
    QVERIFY2(server.listen(QHostAddress::LocalHost, 0),
             qPrintable(server.errorString()));
    const quint16 port = server.serverPort();

    constexpr int kSamples = 200;
    const QByteArray payload(16, 'x');

    std::atomic_llong minNs{std::numeric_limits<long long>::max()};
    std::atomic_int  completed{0};

    // 服务端：把收到的每一块原样回显。flush() 避免等写通知器多绕一轮事件循环。
    auto incoming = Coro::coro(&server).nextConnection();
    Coro::makeTask([incoming]{
        for(QTcpSocket* peer : Coro::generate(incoming)){
            for(const QByteArray& chunk : Coro::generate(Coro::coro(peer).readAll())){
                peer->write(chunk);
                peer->flush();
            }
        }
        return 0;
    });

    // 客户端：连上之后做 kSamples 次请求-响应，记录最小往返。
    Coro::makeTask([port, payload, &minNs, &completed]{
        QTcpSocket sock;
        sock.setProxy(QNetworkProxy::NoProxy);
        sock.connectToHost(QHostAddress::LocalHost, port);
        if(!Coro::await_for(Coro::coro(&sock).waitForConnected(), 2s)){
            qCritical() << "connect failed:" << sock.errorString();
            Coro::quit();
            return 1;
        }

        auto stream = Coro::coro(&sock).readAll();
        for(int i = 0; i < kSamples; ++i){
            QElapsedTimer timer;
            timer.start();
            sock.write(payload);
            sock.flush();
            auto echo = Coro::await_for(stream, 2s);
            if(!echo){
                qCritical() << "echo timeout at sample" << i;
                Coro::quit();
                return 1;
            }
            const long long ns = timer.nsecsElapsed();
            long long prev = minNs.load(std::memory_order_relaxed);
            while(ns < prev
                  && !minNs.compare_exchange_weak(prev, ns,
                                                  std::memory_order_relaxed)){}
            completed.fetch_add(1, std::memory_order_relaxed);
        }
        sock.disconnectFromHost();
        Coro::quit();
        return 0;
    });

    Coro::exec();

    QCOMPARE(completed.load(), kSamples);
    const double minUs = minNs.load() / 1000.0;
    qInfo() << "loopback RTT min =" << minUs << "us over" << kSamples << "samples";
    // 修复前约 3128us。阈值 1000us 宽松到不会被普通抖动触发，
    // 又足以在「固定分发间隔」回归时立刻报警。
    QVERIFY2(minUs < 1000.0,
             qPrintable(QString("往返 min = %1 us，超过 1000 us 的地板阈值")
                            .arg(minUs)));
}

QTEST_GUILESS_MAIN(test_latency)

#include "tst_testlatency.moc"
