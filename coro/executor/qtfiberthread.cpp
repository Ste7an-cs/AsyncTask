#include "qtfiberthread.h"
#include "scheduler/qtlocalfiberscheduler.h"
#include "scheduler/fiberproperty.h"

/**
 * @brief 构造
 * @param parent 父对象
 */
Coro::QtFiberThread::QtFiberThread(QObject *parent):QThread(parent)
{
}

/**
 * @brief 析构：解除阻塞、请求退出并等待线程真正结束
 */
Coro::QtFiberThread::~QtFiberThread()
{
    block.close();
    QThread::quit();
    while(!isFinished()){
        QThread::msleep(1);
    }
}

/**
 * @brief 退出线程：解除阻塞并请求 QThread 退出
 */
void Coro::QtFiberThread::quit()
{
    block.close();
    QThread::quit();
}

/**
 * @brief 线程主体：安装 QtLocalFiberScheduler 后 block.wait() 挂起待命
 */
void Coro::QtFiberThread::run()
{
    boost::fibers::use_scheduling_algorithm<QtLocalFiberScheduler>();
    block.wait();
    return;
}
