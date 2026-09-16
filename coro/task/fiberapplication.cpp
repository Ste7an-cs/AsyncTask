#include "fiberapplication.h"
#include "executor/fiberpool.h"
#include "executor/scheduler/qtlocalfiberscheduler.h"
#include "executor/scheduler/qtfiberscheduler.h"
#include "executor/scheduler/fibertaskqueue.h"
#include "detail/asyncdefine.h"
#include <QAbstractEventDispatcher>
#include <QEventLoop>
#include <QElapsedTimer>
#include <QThread>
#include <thread>
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
 * @brief 主循环：主线程挂起于协程调度器，Qt 主循环跑在绑定主线程的协程上
 *
 * 必须真正进入 QCoreApplication::exec()：只有它会置位 in_exec 并把 QEventLoop
 * 压进 threadData->eventLoops。缺了这两样，Qt 在源头就不产生退出事件——
 * QApplicationPrivate::emitLastWindowClosed() 的第一句就是 if(in_exec)，所以
 * 关窗既不 post QEvent::Quit 也不 emit lastWindowClosed；而 QCoreApplication::
 * exit() 只遍历 eventLoops，栈为空时是空操作。GUI 程序因此关窗后退不出去。
 *
 * 代价是 exec() 会把线程闷在 poll() 里。aboutToBlock 钩子负责在 Qt 每次准备
 * 阻塞前把线程还给 boost.fiber：先按预算排空已到达的事件（排空过程中穿插
 * yield，避免事件风暴饿死协程），再让出一个时间片，最后 wakeUp() 使随后的
 * poll() 立即返回。这段等价于原泵协程的 processEvents(AllEvents)+msleep(1)，
 * 只是搬到了 Qt 的阻塞点上。
 * @return 退出码（QCoreApplication::exec() 的返回值）
 */
int Coro::FiberApplication::exec()
{
    QCoreApplication* app = QCoreApplication::instance();
    if(app == nullptr){
        return -1;
    }
    if(quit_requested_){
        // quit() 在 exec() 之前就已调用过，收尾已在那里就地完成，不再起主循环
        return exit_code_;
    }

    QAbstractEventDispatcher* disp = QAbstractEventDispatcher::instance();
    block_conn_ = QObject::connect(disp, &QAbstractEventDispatcher::aboutToBlock,
                                   this, [disp]{
        // 排空已到达的事件。不带 WaitForMoreEvents，canWait 为 false，
        // 因此不会递归触发 aboutToBlock。
        int budget = 0;
        while(++budget < kEventDrainBudget && disp->processEvents(QEventLoop::AllEvents)){
            boost::this_fiber::yield();
        }
        Coro::msleep(kFiberSliceMs);   // 这一片时间归 boost.fiber
        disp->wakeUp();                // 令随后的 poll() 立即返回
    });

    // 停掉 QtFiberScheduler 的 processEvents 泵：事件改由 exec() 分发，
    // 两个循环并存会重复迭代同一个事件源。
    FiberScheduler::stopCurrentThreadPump();

    in_exec_ = true;

    Coro::launch_properties([this, app]{
        // 与 quit() 抢跑的窗口：quit() 见到 in_exec_ 已置位，只发了
        // QCoreApplication::exit()，而 Qt5 的 exec() 入口会先把 quitNow 清掉，
        // 那次退出请求就丢了。改为预投递一个退出事件，让 exec() 进去就返回。
        // 不能干脆跳过 exec()——那样 aboutToQuit 就没有发出者了。
        // 此处到 exec() 之间没有让出点，本线程其它协程插不进来。
        if(quit_requested_){
            QMetaObject::invokeMethod(app, []{ QCoreApplication::exit(0); }, Qt::QueuedConnection);
        }
        exit_code_ = QCoreApplication::exec();
        shutdown();
    }, Priority::Normal, Affinity::fixed(std::this_thread::get_id()), "qt-loop").detach();

    block.wait();
    return exit_code_;
}

/**
 * @brief 安全退出
 *
 * 允许在任意线程调用：非主线程调用时先投递回主线程再执行。收尾必须在主线程
 * 完成——signalExit() 一置位主线程的常驻协程就停摆，而 drainUntilIdle()
 * 里的 processEvents/sendPostedEvents 只作用于调用线程的事件队列。若就地在
 * 工作线程收尾，主线程上 pending 的 deleteLater 无人冲刷、投递给主线程对象的
 * aboutToQuit 槽也永远不会被执行（进程仍以 0 退出，问题被静默吞掉）。
 *
 * 两条路径：
 *  - 已进入 Coro::exec()：只让 Qt 主循环返回，收尾交给 shutdown()。关窗、
 *    qApp->quit()、平台退出请求也都汇合到这里，aboutToQuit 由 Qt 原生发出。
 *  - 未进入 Coro::exec()（例如 QTest 驱动只调 installFiberApplication()）：
 *    没有「exec() 返回」这一刻，Qt 不会发 aboutToQuit，只能由本函数代发并
 *    就地收尾，语义与引入原生退出路径之前保持一致。
 */
void Coro::FiberApplication::quit()
{
    QCoreApplication* app = QCoreApplication::instance();
    if(app == nullptr){
        return;
    }
    if(QThread::currentThread() != app->thread()){
        QMetaObject::invokeMethod(app, []{ Coro::quit(); }, Qt::QueuedConnection);
        return;
    }
    if(quit_requested_){
        return;                                 // 幂等：收尾只做一次
    }
    quit_requested_ = true;

    if(in_exec_){
        QCoreApplication::exit(0);
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
 * @brief 收尾（运行在 qt-loop 协程上，QCoreApplication::exec() 返回后）
 *
 * 顺序不可调换：block.close() 必须是最后一句。它唤醒主纤程后 exec() 即返回、
 * main() 随即析构 QCoreApplication，排在其后的任何清理都可能撞上已销毁的对象。
 * 本函数返回后 qt-loop 协程即结束，主纤程才会被调度到。
 */
void Coro::FiberApplication::shutdown()
{
    in_exec_ = false;
    quit_requested_ = true;             // 此后再调 quit() 一律无视

    QObject::disconnect(block_conn_);   // 收尾期间不再让出/唤醒
    QtFiberScheduler::signalExit();     // 各工作线程的泵协程自行退出
    drainUntilIdle();                   // 排空被 aboutToQuit 唤醒的协程及其 deleteLater
    Coro::FibersPool::instance().close();
    block.close();                      // 必须最后
}

/**
 * @brief 构造：主线程安装本地调度器并启动工作线程池
 */
Coro::FiberApplication::FiberApplication(): QObject(nullptr)
{
    boost::fibers::use_scheduling_algorithm<Coro::QtLocalFiberScheduler>();
    Coro::FibersPool::instance();
    // 关机统一走「退出 Qt 主循环 → shutdown()」（aboutToQuit 由 Qt 原生发出，
    // 随后排空 → 停池 → 解除阻塞），故此处不再在 aboutToQuit 里直接 close
    // 线程池，避免抢在排空之前把池关掉、导致仍在途的 worker 协程被中途抛弃。
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
