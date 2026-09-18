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
 * @details 这**不是**事件分发间隔 —— 分发已改为按需唤醒，没有间隔了。它是
 *          跨线程唤醒广播万一失效时的保险丝：让症状退化成「慢」而不是「死锁」。
 *          默认 10ms。传入非正值会被夹到 1。
 * @code
 * Coro::setMaxEventBlockMs(50);   // 更省电，但唤醒漏路径更难察觉
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
