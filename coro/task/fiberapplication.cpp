#include "fiberapplication.h"
#include "executor/fiberpool.h"
#include "executor/scheduler/qtlocalfiberscheduler.h"
#include "executor/scheduler/qtfiberscheduler.h"
#include "executor/scheduler/fibertaskqueue.h"
#include <QEventLoop>
#include <QElapsedTimer>
#include <QThread>
#include <boost/fiber/operations.hpp>

namespace {
/**
 * @brief 关机排空：应用仍存活时反复驱动 Qt 事件与主线程协程调度。
 *
 * 让被 aboutToQuit 唤醒的协程跑完并处理其投递的 deleteLater。否则残留的挂起
 * 协程会在 QCoreApplication 析构后被 boost.fiber 收尾流程唤醒，届时访问已销毁
 * 的事件系统而崩溃，或因协程未终结导致线程无法退出而卡死。
 * @param maxMs 最长排空时间（兜底上限）
 */
void drainUntilIdle(int maxMs = 3000){
    QElapsedTimer timer;
    timer.start();
    int idle = 0;
    while(timer.elapsed() < maxMs && idle < 5){
        QCoreApplication::processEvents(QEventLoop::AllEvents, 2);
        QCoreApplication::sendPostedEvents(nullptr, QEvent::DeferredDelete);
        boost::this_fiber::yield();
        idle = (Coro::FiberGlobalQueue::instance()->size() > 0) ? 0 : idle + 1;
    }
}
}

/**
 * @brief 获取全局单例
 * @return 单例指针
 */
Coro::FiberApplication *Coro::FiberApplication::instance()
{
    static FiberApplication app;
    return &app;
}

/**
 * @brief 主循环：主线程挂起于协程调度器
 * @return 退出码
 */
int Coro::FiberApplication::exec()
{
    block.wait();
    return 0;
}

/**
 * @brief 安全退出：广播 aboutToQuit → 排空 → 停线程池 → 退出
 *
 * 允许在任意线程调用：非主线程调用时先投递回主线程再执行。
 * 收尾必须在主线程完成——signalExit() 一置位主线程的泵协程就停摆，而
 * drainUntilIdle() 里的 processEvents/sendPostedEvents 只作用于调用线程的
 * 事件队列。若就地在工作线程收尾，主线程上 pending 的 deleteLater 无人冲刷、
 * 投递给主线程对象的 aboutToQuit 槽也永远不会被执行（进程仍以 0 退出，
 * 问题被静默吞掉）。此处投递时尚未 signalExit()，主线程泵仍在跑，投递必达。
 */
void Coro::FiberApplication::quit()
{
    QCoreApplication* app = QCoreApplication::instance();
    if(app != nullptr && QThread::currentThread() != app->thread()){
        QMetaObject::invokeMethod(app, []{ Coro::quit(); }, Qt::QueuedConnection);
        return;
    }

    QMetaObject::invokeMethod(app, "aboutToQuit", Qt::DirectConnection);
    QtFiberScheduler::signalExit();
    drainUntilIdle();
    block.close();
    Coro::FibersPool::instance().close();
    QCoreApplication::exit();
}

/**
 * @brief 构造：主线程安装本地调度器并启动工作线程池
 */
Coro::FiberApplication::FiberApplication(): QObject(nullptr)
{
    boost::fibers::use_scheduling_algorithm<Coro::QtLocalFiberScheduler>();
    Coro::FibersPool::instance();
    // 关机统一走 Coro::quit()（触发 aboutToQuit → 排空 → 停池 → 退出），
    // 故此处不再在 aboutToQuit 里直接 close 线程池，避免抢在排空之前把池关掉、
    // 导致仍在途的 worker 协程被中途抛弃。
}

/**
 * @brief 进入主循环（等价于 FiberApplication::exec）
 * @return 退出码
 */
int Coro::exec()
{
    return FiberApplication::instance()->exec();
}

/**
 * @brief 在主线程安装协程应用
 */
void Coro::installFiberApplication()
{
    FiberApplication::instance();
}

/**
 * @brief 触发安全退出（等价于 FiberApplication::quit）
 */
void Coro::quit()
{
    FiberApplication::instance()->quit();
}
