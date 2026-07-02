#include "qtfiberthread.h"
#include "scheduler/qtlocalfiberscheduler.h"
#include "scheduler/fiberproperty.h"

Coro::QtFiberThread::QtFiberThread(QObject *parent):QThread(parent)
{
}

Coro::QtFiberThread::~QtFiberThread()
{
    block.close();
    QThread::quit();
    while(!isFinished()){
        QThread::msleep(1);
    }
}

void Coro::QtFiberThread::quit()
{
    block.close();
    QThread::quit();
}

void Coro::QtFiberThread::run()
{
    boost::fibers::use_scheduling_algorithm<QtLocalFiberScheduler>();
    block.wait();
    return;
}
