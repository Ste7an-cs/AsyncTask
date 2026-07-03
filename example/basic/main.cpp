///
/// AsyncTask 基础示例：
///   - 用 makeTask 启动协程任务，链式 then / on_finally
///   - 用 coro(信号) + await 像同步代码一样等待 Qt 信号（期间不阻塞线程）
///   - 用 Coro::exec()/quit() 驱动与退出
///
#include <QCoreApplication>
#include <QTimer>
#include <QDebug>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/coro.hpp"

using namespace Coro;

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();               // 安装调度器 + 工作线程池

    // 1) 结构化并发：链式任务
    makeTask([]{
        qDebug() << "step1";
        return 10;
    }, Priority::Normal, Affinity::sticky())
    .then([](int v){
        qDebug() << "step2, got" << v;
        return v + 1;
    });

    // 2) 等待一个 Qt 信号：像同步代码一样顺序书写
    makeTask([]{
        QTimer* timer = new QTimer();
        timer->start(500);
        await(coro(timer, &QTimer::timeout));   // 让出协程，等待 timeout，不阻塞线程
        qDebug() << "timer timeout fired";
        timer->deleteLater();

        quit();                                  // 收尾并退出
        return 0;
    });

    return exec();                               // 用 Coro::exec() 驱动（不是 app.exec()）
}
