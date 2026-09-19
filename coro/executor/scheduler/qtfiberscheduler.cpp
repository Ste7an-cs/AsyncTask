#include "qtfiberscheduler.h"
#include <QCoreApplication>
#include <QDebug>
#include <QThread>
#include <algorithm>
#include <chrono>
#include <thread>
#include "detail/asyncdefine.h"
#include <boost/fiber/all.hpp>
#include <boost/fiber/context.hpp>

thread_local Coro::QtFiberScheduler* Coro::QtFiberScheduler::t_self_{ nullptr };

Coro::QtFiberScheduler::QtFiberScheduler(void):FiberScheduler()
{
    t_self_ = this;
    deadline_timer_.setSingleShot(true);
    /// @details 事件分发器必须在 eventloop 成员构造之后取：QEventLoop 的构造会
    /// 为本线程 ensureEventDispatcher()，在那之前 instance() 可能是空。
    disp_ = QAbstractEventDispatcher::instance();
    if(disp_ != nullptr){
        /// @details 本调度器由 boost.fiber 的 thread_local scheduler 持有，活得比
        /// 事件分发器久：主线程的分发器是 QCoreApplication 的子对象，~QCoreApplication
        /// 就把它删了，而本对象要到进程退出阶段的 thread_local 析构才走。缓存裸指针
        /// 必须挂到 destroyed 上及时失效，否则收尾期的 notify() 就是 use-after-free。
        /// 连接强制 DirectConnection：接收方正在析构，排队投递永远等不到执行。
        disp_conn_ = QObject::connect(disp_, &QObject::destroyed, disp_,
                                      [this]{ detachDispatcher(); },
                                      Qt::DirectConnection);
        /// @details 再挂一道 qApp：主线程的分发器是 QCoreApplication 的子对象，
        /// 由 ~QObject 的 deleteChildren() 删除，而那时分发器的派生析构已经跑完 ——
        /// destroyed 挂得太晚。qApp 的 destroyed 发在 ~QObject 的开头、deleteChildren
        /// 之前，此刻分发器对象还完好，因此主线程这条路**没有 use-after-free 窗口**。
        /// 但不是「零窗口」：~QCoreApplication 自己的函数体在 emit destroyed 之前就
        /// 调过 eventDispatcher->closingDown() 并清掉了 threadData->eventDispatcher，
        /// 落在这段区间里的跨线程 wakeUp() 戳的是一个 Qt 已经关停的分发器。该调用在
        /// Linux/Qt 5.15 上是 g_main_context_wakeup 或往 eventfd 写一字节，对象内存
        /// 尚在、不涉及 Qt 状态机，所以只是白戳一下，安全且无副作用。
        /// 只有主线程调度器挂这道，免得工作线程去连一个别的线程的对象。
        QCoreApplication* app = QCoreApplication::instance();
        if(app != nullptr && app->thread() == QThread::currentThread()){
            app_conn_ = QObject::connect(app, &QObject::destroyed, app,
                                         [this]{ detachDispatcher(); },
                                         Qt::DirectConnection);
        }
        // wakeUp() 是 Qt 明确保证线程安全的少数函数之一；指针取用见 wakeDispatcher()。
        FiberScheduler::registerWaker(this, [this]{ wakeDispatcher(); });
    }else{
        /// @details 取不到分发器意味着本线程既不登记唤醒回调、notify() 里戳分发器
        /// 也永远是空操作：远端就绪捅不破本线程的 poll()，事件泵只能靠保险丝
        /// （maxEventBlockMs）到点自醒，表现为「莫名其妙的固定延迟」。这条路没有
        /// 自动恢复的机会，必须让人看见。
        qWarning("QtFiberScheduler: 本线程取不到 QAbstractEventDispatcher，"
                 "跨线程唤醒将失效，事件分发退化为按 maxEventBlockMs 轮询。");
    }
    /// @details 两个钩子都是无捕获的静态函数，每个线程装调度器时都会写一次同样的
    /// 值；用原子函数指针存放，重复写与 stopCurrentThreadPump() /
    /// detachCurrentThreadDispatcher() 的读都不构成竞争。
    FiberScheduler::local_unpark_hook_.store(&QtFiberScheduler::unparkLocal,
                                            std::memory_order_release);
    FiberScheduler::local_detach_hook_.store(&QtFiberScheduler::detachLocal,
                                            std::memory_order_release);
}

Coro::QtFiberScheduler::~QtFiberScheduler(void)
{
    detachDispatcher();
    /// @details 不能无条件清空：boost.fiber 在同线程二次安装调度器时，先构造新
    /// 实例（此时 t_self_ 已指向新对象），再析构旧实例。若这里无条件置空，析构
    /// 旧实例会把刚装好的 t_self_ 抹掉，导致新实例活着的这段时间里
    /// detachCurrentThreadDispatcher() / parkUntilIdle() / unparkLocal() 全部
    /// 静默退化为空操作（实测对 boost 1.89 成立；testexecutor 里主线程二次装
    /// 调度器正是这个场景）。只清自己名下的那一份。
    if(t_self_ == this){
        t_self_ = nullptr;
    }
}

/**
 * @brief 戳醒本线程的 Qt 事件分发器
 */
void Coro::QtFiberScheduler::wakeDispatcher(void) noexcept
{
    std::lock_guard<std::mutex> guard(disp_mtx_);
    if(disp_ != nullptr){
        disp_->wakeUp();
    }
}

/**
 * @brief 与本线程的事件分发器解绑
 */
void Coro::QtFiberScheduler::detachDispatcher(void) noexcept
{
    /// @details 先注销唤醒回调：unregisterWaker 要拿 waker_mtx_，而 wakeAllBlocked()
    /// 整轮回调都持着它，所以它返回时在途的那一轮已经跑完，之后不会再有新回调。
    FiberScheduler::unregisterWaker(this);
    /// @details 再拿 disp_mtx_ 置空：等 notify() 里在途的 wakeUp() 收尾。两步不嵌套，
    /// 否则会与回调路径的 waker_mtx_ → disp_mtx_ 锁序相反而死锁。
    std::lock_guard<std::mutex> guard(disp_mtx_);
    if(disp_ != nullptr){
        QObject::disconnect(disp_conn_);
        QObject::disconnect(app_conn_);
        disp_conn_ = QMetaObject::Connection{};
        app_conn_  = QMetaObject::Connection{};
        disp_ = nullptr;
    }
}

/**
 * @brief 与本线程的事件分发器解绑（线程收尾时经基类钩子调用）
 */
void Coro::QtFiberScheduler::detachLocal(void)
{
    QtFiberScheduler* self = t_self_;
    if(self == nullptr){
        return;         ///< 本线程装的是非 Qt 调度器，无事可做
    }
    self->detachDispatcher();
}

/**
 * @brief 挂起调用协程，返回可安全阻塞至的时刻
 */
std::chrono::steady_clock::time_point Coro::QtFiberScheduler::parkUntilIdle(void)
{
    QtFiberScheduler* self = t_self_;
    if(self == nullptr){
        return std::chrono::steady_clock::now();
    }
    /// @details Qt 持有者只有一席。撞上说明有两个协程都想当持有者，属实现错误。
    /// 这里不能用 Q_ASSERT：release 构建里它整条编译掉，第二个 park 会覆盖
    /// qt_waker_，而 create_waker() 同时作废了前一个协程的 epoch —— 那个协程
    /// 从此叫不醒，表现为一次没有任何提示的挂死。宁可拒绝第二个 park。
    if(self->parked_.load(std::memory_order_acquire)){
        qWarning("QtFiberScheduler::parkUntilIdle: 本线程已有协程持有 Qt 事件循环，"
                 "拒绝重复挂起（重复挂起会让先到的协程永远醒不来）。");
        return std::chrono::steady_clock::now();
    }

    boost::fibers::context* ctx = boost::fibers::context::active();
    self->qt_waker_ = ctx->create_waker();
    self->next_deadline_ = std::chrono::steady_clock::now();
    self->parked_.store(true, std::memory_order_release);
    /// @details store 与 suspend 之间没有让出点（同线程、无抢占），而 suspend_until
    /// 只在本协程挂起之后才拿得到控制权，故不会丢唤醒。
    ctx->suspend();
    return self->next_deadline_;
}

/**
 * @brief 解除本线程 Qt 持有者协程的挂起
 */
void Coro::QtFiberScheduler::unparkLocal(void)
{
    QtFiberScheduler* self = t_self_;
    if(self == nullptr){
        return;
    }
    if(self->parked_.exchange(false, std::memory_order_acq_rel)){
        self->next_deadline_ = std::chrono::steady_clock::now();
        self->qt_waker_.wake();
    }
}

/**
 * @brief 唤醒挂起的调度器线程，并戳破本线程可能正在进行的 poll()
 */
void Coro::QtFiberScheduler::notify(void) noexcept
{
    FiberScheduler::notify();
    wakeDispatcher();
}

void Coro::QtFiberScheduler::suspend_until(const std::chrono::steady_clock::time_point &time_point) noexcept
{
    /// @details 有 Qt 持有者挂在 parkUntilIdle() 上时，本回调是唯一知道「最近一个
    /// 协程截止时刻」的地方。交棒给它、立即返回，真正的阻塞发生在它自己的协程栈
    /// 上 —— 那里执行 Qt 槽才是合法的；本回调跑在 dispatcher context，切栈会崩。
    if(parked_.exchange(false, std::memory_order_acq_rel)){
        next_deadline_ = time_point;
        qt_waker_.wake();       // 只入队，不切栈
        return;
    }

    bool startedPump = false;
    std::call_once(pump_once_, [this, &startedPump]{
        if(FiberScheduler::s_exit_.load(std::memory_order_acquire)
           || FiberScheduler::t_stop_.load(std::memory_order_acquire)){
            return;             // 已在退出，不必再起泵
        }
        startedPump = true;
        boost::fibers::fiber(launch_properties([this]{
            pumpLoop();
        }, Priority::High, Affinity::fixed(std::this_thread::get_id()))).detach();
    });
    if(startedPump) return;
    FiberScheduler::suspend_until(time_point);
}

/**
 * @brief 常驻事件泵协程主体：排空 → 交出线程 → 按需阻塞在 poll()
 * @details 不再有固定分发间隔。无就绪协程时阻塞在 Qt 的 poll() 里，fd 就绪即刻
 *          返回；空闲时线程真正休眠而非轮询。阻塞上限由 Coro::maxEventBlockMs()
 *          兜底 —— 本泵**不参与**跨线程唤醒广播，那把保险丝因此是远端就绪唯一的
 *          兜底手段，原委见头文件里的长注释。
 */
void Coro::QtFiberScheduler::pumpLoop(void)
{
    auto stopping = [this]{
        return FiberScheduler::s_exit_.load(std::memory_order_acquire)
            || FiberScheduler::t_stop_.load(std::memory_order_acquire);
    };

    while(!stopping()){
        eventloop.processEvents(QEventLoop::AllEvents);   // 槽在本协程栈上跑
        if(stopping()) break;

        const auto tp = parkUntilIdle();                  // 交出线程，等交棒
        if(stopping()) break;

        /// @details 交棒之后、本协程真正被调度之前，可能又有协程被远端投递进来。
        /// 此时不能再去睡，否则手上有活却阻塞在 poll() 里。
        /// **不可用 has_ready_fibers()** —— main_queue_ 里常驻一个 pinned 的
        /// dispatcher context 使其恒为真，泵会一次都不阻塞、退化成满速忙转。
        if(hasReadyWork() || stopping()){
            continue;
        }

        const auto now = std::chrono::steady_clock::now();
        long long ms = 0;
        if(tp > now){
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp - now).count();
        }
        ms = std::min<long long>(ms, Coro::maxEventBlockMs());
        if(ms <= 0){
            continue;
        }

        deadline_timer_.start(static_cast<int>(ms));
        eventloop.processEvents(QEventLoop::AllEvents | QEventLoop::WaitForMoreEvents);
        deadline_timer_.stop();
    }
}
