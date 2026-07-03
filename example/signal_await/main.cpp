///
/// 协程信号等待示例：用 coro(obj, &信号) + await 像同步代码一样等待 Qt 信号。
///   - 无参信号   -> Awaitable<void>
///   - 多参信号   -> Awaitable<tuple<...>>
///   - coro<T...> -> 只取指定参数
///
#include <QCoreApplication>
#include <QTimer>
#include <QDebug>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/corosignal.hpp"   // 只需信号等待，可单独引入（无需 socket/网络）
#include "await/awaitable.hpp"

using namespace Coro;

class Sensor : public QObject
{
    Q_OBJECT
public:
    void start(){
        QTimer::singleShot(300, this, [this]{ emit ready(); });
        QTimer::singleShot(600, this, [this]{ emit reading(42, QStringLiteral("ok")); });
    }
signals:
    void ready();
    void reading(int value, QString status);
};

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();

    Sensor* sensor = new Sensor();

    makeTask([sensor]{
        // 等无参信号
        await(coro(sensor, &Sensor::ready));
        qDebug() << "ready";

        // 等多参信号，得到 tuple<int, QString>
        auto r = await(coro(sensor, &Sensor::reading));
        if(r) qDebug() << "reading tuple:" << std::get<0>(r.value()) << std::get<1>(r.value());

        sensor->deleteLater();
        quit();
        return 0;
    });

    sensor->start();
    return exec();
}

#include "main.moc"
