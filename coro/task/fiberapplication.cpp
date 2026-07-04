#include "fiberapplication.h"
#include "executor/fiberpool.h"
#include "executor/scheduler/qtlocalfiberscheduler.h"
#include "executor/scheduler/fibertaskqueue.h"
#include <QEventLoop>
#include <QElapsedTimer>
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
 */
void Coro::FiberApplication::quit()
{
    // ① 主动触发真正的 QCoreApplication::aboutToQuit。
    //    本框架不跑 QCoreApplication::exec()（Coro::exec 实为 block.wait，Qt 事件靠
    //    QtFiberScheduler::suspend_until 里的 processEvents 驱动），因此 exit()/quit()
    //    不会发出 aboutToQuit，需在此主动触发。这样每个 awaitable 早已 connect 的
    //    close 绑定被点亮，唤醒所有仍阻塞在 await() 里的协程。
    QMetaObject::invokeMethod(QCoreApplication::instance(), "aboutToQuit", Qt::DirectConnection);
    // ② 排空：应用仍存活时把被唤醒的协程及其 deleteLater 事件跑完。
    drainUntilIdle();
    // ③ 停线程池、退出。
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
