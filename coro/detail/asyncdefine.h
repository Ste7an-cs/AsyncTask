#ifndef ASYNCDEFINE_H
#define ASYNCDEFINE_H
#include <boost/fiber/all.hpp>
#include <executor/scheduler/fiberproperty.h>

namespace Coro {

using boost::fibers::fiber;
using boost::fibers::future;
using boost::fibers::promise;
using boost::fibers::mutex;
using boost::fibers::condition_variable;
using boost::fibers::condition_variable_any;
using namespace boost::this_fiber;

/**
 * @brief 当前协程休眠指定毫秒数（休眠期让出线程，不阻塞线程）
 * @param mescs 休眠的毫秒数
 * @code
 * Coro::makeTask([]{
 *     Coro::msleep(100);      // 让出线程 100ms，同线程其它协程可继续运行
 *     return 0;               // 切勿用 QThread::msleep —— 那会阻塞整个线程
 * });
 * @endcode
 */
void msleep(unsigned long mescs);

/**
 * @brief 当前协程休眠指定秒数（休眠期让出线程，不阻塞线程）
 * @param secs 休眠的秒数
 * @code
 * Coro::makeTask([]{
 *     Coro::sleep(1);         // 休眠 1 秒，期间线程可调度其它协程
 *     return 0;
 * });
 * @endcode
 */
void sleep(unsigned long secs);

/**
 * @brief 设置事件阻塞的安全上限（毫秒）
 * @details 这**不是**事件分发间隔 —— 在跑 QtFiberScheduler 事件泵的工作线程上，
 *          事件与协程都是按需唤醒的，fd 一就绪 poll() 立刻返回，没有间隔可言。
 *          默认 10ms，传入非正值夹到 1。
 *          主线程同理：进入 Coro::exec() 后泵是停掉的（fiberapplication.cpp
 *          中的 stopCurrentThreadPump()），阻塞上限改由 aboutToBlock 钩子按同
 *          一条规则算出，本旋钮对它**同样生效**。
 * @warning 它是**新投递的 Shared / Sticky 协程的跨线程拾取上界**，而不只是一道
 *          保险丝（已归属本线程的协程走 boost 的 remote_ready_queue_ + notify()，
 *          会被立即唤醒，不受本上限影响）：事件泵有意
 *          不参与跨线程唤醒广播（原委见 QtFiberScheduler::pumpLoop() 的注释，
 *          那条路实测会把 CPU 打到 10 倍），所以一个已经睡在 poll() 里的工作
 *          线程**不会**被远端投递的 Shared 协程叫醒，要睡满本上限才自醒。
 *          对跨线程投递延迟敏感的程序应调小它（代价是空闲时更费电）。
 * @code
 * Coro::setMaxEventBlockMs(50);   // 更省电，但跨线程拾取最坏要等 50ms
 * Coro::setMaxEventBlockMs(2);    // 跨线程更跟手，空闲唤醒更频繁
 * @endcode
 * @param ms 上限毫秒数
 */
void setMaxEventBlockMs(int ms);
/**
 * @brief 读取事件阻塞的安全上限（毫秒）
 * @return 当前上限
 */
int maxEventBlockMs(void);

/**
 * @brief 以指定调度属性启动一个协程（带调度属性启动协程的统一低层入口）。
 *
 * 以 MetaContext(pri, affine, name) 作为 fiber 属性创建 boost fiber 执行 func。
 * @tparam Fn 可调用体类型
 * @param func 协程执行的可调用体
 * @param pri 协程优先级
 * @param affine 协程线程亲和
 * @param name 协程名称（可选，便于调试）
 * @return 创建的 boost fiber
 * @code
 * // 比 makeTask 更低层：可直接指定优先级与线程亲和，但不返回 FiberTask 句柄
 * auto fb = Coro::launch_properties([]{
 *     Coro::msleep(100);      // 在任意空闲工作线程上运行
 * }, Coro::Priority::High, Coro::Affinity::shared(), "worker-fiber");
 * fb.detach();                // 交由调度器管理；需要取结果请改用 makeTask
 * @endcode
 */
template< typename Fn >
fiber launch_properties( Fn && func, Coro::Priority pri, Coro::Affinity affine, std::string name="") {
    boost::fibers::fiber_properties * meta = new Coro::MetaContext(pri, affine, name, nullptr);
    boost::fibers::fiber fiber(meta, func);
    return fiber;
}

}

#endif // ASYNCDEFINE_H
