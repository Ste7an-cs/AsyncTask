/**
 * @file main.cpp
 * @brief TCP 回环请求-响应的时延基准。
 * @details 单进程内起服务端与客户端，度量往返时延的 min / mean / stdev，并打印
 *          进程 CPU 时间。口径对齐 issue #6 中 Transport 的基准工具，用于核对
 *          「每跳固定分发间隔」是否已消除。与 test/testlatency 的区别：那个是
 *          带阈值的回归断言，这个是给人看数字的。
 */
#include <QCoreApplication>
#include <QDebug>
#include <QElapsedTimer>
#include <QHostAddress>
#include <QNetworkProxy>
#include <QTcpServer>
#include <QTcpSocket>

#include <algorithm>
#include <cmath>
#include <vector>

#include <sys/resource.h>

#include "await/coro.hpp"
#include "task/fiberapplication.h"
#include "task/fibertask.h"

using namespace Coro;
using namespace std::chrono_literals;

namespace {
constexpr int kSamples  = 300;
constexpr int kPayload  = 16;

/// @brief 打印一组往返时延的统计量（单位微秒）
void report(std::vector<double>& us)
{
    if(us.empty()){
        qCritical() << "no samples";
        return;
    }
    std::sort(us.begin(), us.end());
    double sum = 0.0;
    for(double v : us) sum += v;
    const double mean = sum / us.size();
    double sq = 0.0;
    for(double v : us) sq += (v - mean) * (v - mean);
    const double stdev = std::sqrt(sq / us.size());

    rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    const double cpu = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6
                     + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;

    qInfo().nospace() << "samples=" << (int)us.size()
                      << " payload=" << kPayload << "B";
    qInfo().nospace() << "RTT min=" << us.front() << "us"
                      << " mean=" << mean << "us"
                      << " p50=" << us[us.size() / 2] << "us"
                      << " max=" << us.back() << "us"
                      << " stdev=" << stdev;
    qInfo().nospace() << "CPU total=" << cpu << "s";
}
} // namespace

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    QNetworkProxy::setApplicationProxy(QNetworkProxy::NoProxy);
    installFiberApplication();

    QTcpServer server;
    if(!server.listen(QHostAddress::LocalHost, 0)){
        qCritical() << "listen failed:" << server.errorString();
        return 1;
    }
    const quint16 port = server.serverPort();

    auto incoming = coro(&server).nextConnection();
    makeTask([incoming]{
        for(QTcpSocket* peer : generate(incoming)){
            for(const QByteArray& chunk : generate(coro(peer).readAll())){
                peer->write(chunk);
                peer->flush();
            }
        }
        return 0;
    });

    makeTask([port]{
        QTcpSocket sock;
        sock.setProxy(QNetworkProxy::NoProxy);
        sock.connectToHost(QHostAddress::LocalHost, port);
        if(!await_for(coro(&sock).waitForConnected(), 2s)){
            qCritical() << "connect failed:" << sock.errorString();
            quit();
            return 1;
        }

        const QByteArray payload(kPayload, 'x');
        auto stream = coro(&sock).readAll();
        std::vector<double> us;
        us.reserve(kSamples);
        for(int i = 0; i < kSamples; ++i){
            QElapsedTimer timer;
            timer.start();
            sock.write(payload);
            sock.flush();

            // readAll() 只交付「已到达」的字节；回显若被拆成多个 TCP 分段，
            // 单次 await 可能只拿到残片。累积到完整 payload 长度再停表，
            // 否则会记下虚低的往返值，还会把剩余分段留给下一轮误判为新样本。
            // 做法与 test/testlatency 的测量循环一致。
            QByteArray received;
            while(received.size() < payload.size()){
                auto echo = await_for(stream, 2s);
                if(!echo){
                    qCritical() << "echo timeout at sample" << i;
                    quit();
                    return 1;
                }
                received += echo.value();
            }
            if(received != payload){
                qCritical() << "echo mismatch at sample" << i
                            << "got" << received << "expected" << payload;
                quit();
                return 1;
            }
            us.push_back(timer.nsecsElapsed() / 1000.0);
        }
        report(us);
        sock.disconnectFromHost();
        quit();
        return 0;
    });

    return exec();
}
