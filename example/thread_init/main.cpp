///
/// 协程初始化 + 创建协程线程示例：
///   - installFiberApplication()：主线程安装调度器 + 启动全局工作线程池（协程在其上并发调度）。
///   - QtFiberThread：创建一个专用的“协程线程”（可运行协程的 QThread）。
///   - launch_properties + Affinity：以 Shared 亲和创建协程，观察它们被多个线程取用执行。
///
#include <QCoreApplication>
#include <QThread>
#include <QDebug>
#include <atomic>
#include <sstream>
#include <boost/fiber/all.hpp>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "executor/qtfiberthread.h"
#include "detail/asyncdefine.h"

using namespace Coro;

static QString tid(){ std::ostringstream o; o << std::this_thread::get_id(); return QString::fromStdString(o.str()); }

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);

    // 1) 协程初始化：安装主线程调度器 + 启动全局工作线程池
    installFiberApplication();

    // 2) 创建一个专用的协程线程（也会参与 Shared 协程的调度）
    QtFiberThread* worker = new QtFiberThread();
    worker->start();
    QThread::msleep(50);

    makeTask([]{
        std::atomic_int done{0};
        // 3) 创建 8 个 Shared 协程：由工作线程池 + QtFiberThread 并发取用执行
        for(int i = 0; i < 8; i++){
            boost::fibers::fiber fb(launch_properties([i, &done]{
                msleep(100);
                qDebug() << "shared fiber" << i << "ran on thread" << tid();
                done.fetch_add(1);
            }, Priority::High, Affinity::shared()));
            fb.detach();
        }
        // 等它们跑完（当前协程让出，不阻塞线程）
        while(done.load() < 8){ msleep(20); }
        qDebug() << "all shared fibers done; main-side task on thread" << tid();

        quit();
        return 0;
    });

    int rc = exec();
    worker->quit();
    delete worker;
    return rc;
}
