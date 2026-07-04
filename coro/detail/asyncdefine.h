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
 */
void msleep(unsigned long mescs);

/**
 * @brief 当前协程休眠指定秒数（休眠期让出线程，不阻塞线程）
 * @param secs 休眠的秒数
 */
void sleep(unsigned long secs);

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
 */
template< typename Fn >
fiber launch_properties( Fn && func, Coro::Priority pri, Coro::Affinity affine, std::string name="") {
    boost::fibers::fiber_properties * meta = new Coro::MetaContext(pri, affine, name, nullptr);
    boost::fibers::fiber fiber(meta, func);
    return fiber;
}

}

#endif // ASYNCDEFINE_H
