///
/// generator 流式示例：
///   - 生产者协程用 yield 持续推送序列；消费者用 for 迭代（input-iterator）。
///   - 也可把任意 Awaitable 用 generate(a) 当数据流迭代（见注释）。
///
#include <QCoreApplication>
#include <QDebug>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "detail/asyncdefine.h"
#include "await/generator.hpp"
// #include "await/corosignal.hpp"   // 若要把信号当流：generate(coro(obj, &sig))

using namespace Coro;

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();

    makeTask([]{
        // 生产者：每 100ms 产出一个平方数，暂停期间不阻塞线程
        Generator<int> squares([](auto yield){
            for(int i = 0; i < 6; i++){
                msleep(100);
                yield(i * i);
            }
        });

        // 消费者：像遍历容器一样迭代协程数据流
        for(int v : squares){
            qDebug() << "got" << v;
        }
        qDebug() << "stream finished";

        // 把信号当流的写法（示意）：
        //   for(auto v : generate(coro(obj, &Obj::valueChanged))) { ... }

        quit();
        return 0;
    });

    return exec();
}
