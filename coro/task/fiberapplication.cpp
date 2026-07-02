#include "fiberapplication.h"
#include "executor/fiberpool.h"
#include "executor/scheduler/qtlocalfiberscheduler.h"

Coro::FiberApplication *Coro::FiberApplication::instance()
{
    static FiberApplication app;
    return &app;
}

int Coro::FiberApplication::exec()
{
    block.wait();
    return 0;
}

void Coro::FiberApplication::quit()
{
    block.close();
    Coro::FibersPool::instance().close();
    QCoreApplication::exit();
}

Coro::FiberApplication::FiberApplication(): QObject(nullptr)
{
    boost::fibers::use_scheduling_algorithm<Coro::QtLocalFiberScheduler>();
    Coro::FibersPool::instance();
    QObject::connect(QCoreApplication::instance(), &QCoreApplication::aboutToQuit, this, [this](){
        block.close();
        Coro::FibersPool::instance().close();
    });
}

int Coro::exec()
{
    return FiberApplication::instance()->exec();
}

void Coro::installFiberApplication()
{
    FiberApplication::instance();
}

void Coro::quit()
{
    FiberApplication::instance()->quit();
}
