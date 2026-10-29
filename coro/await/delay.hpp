#ifndef DELAY_HPP
#define DELAY_HPP

#include <chrono>
#include <system_error>

#include "awaitable.hpp"

namespace Coro {

/**
 * @brief 可被 FiberTask::cancel 提前中止的协程延时
 * @details 内部使用一条无生产者的 Awaitable<void>，通过 await_for(duration) 挂起：
 *          - 正常到期：await_for 返回 timed_out，本函数映射为成功；
 *          - 任务 cancel：当前句柄消费者队列以 interrupted 关闭，本函数返回该错误；
 *          - duration <= 0：立即成功，不挂起。
 *          只响应 task.cancel；Coro::quit()/线程结束不会提前唤醒，仍会等满 duration。
 *          不创建定时线程、不依赖 Qt、不修改 FiberChannel/ChannelHub/await_for。
 * @tparam Rep 时长的计数类型
 * @tparam Period 时长的周期类型
 * @param duration 最长等待时长
 * @return 正常到期成功；取消时返回 std::errc::interrupted
 * @code
 * using namespace std::chrono_literals;
 * auto r = Coro::delay_for(100ms);
 * if(!r && r.error() == std::make_error_code(std::errc::interrupted)){
 *     // 任务被 cancel
 * }
 * @endcode
 */
template<class Rep, class Period>
Result<void> delay_for(const std::chrono::duration<Rep, Period>& duration)
{
    using duration_type = std::chrono::duration<Rep, Period>;
    if(duration <= duration_type::zero()){
        return Result<void>();
    }

    Awaitable<void> a;                  // 无生产者：只等超时或被 task cancel 关闭
    auto r = a.await_for(duration);
    if(r){
        return r;
    }
    if(r.error() == std::make_error_code(std::errc::timed_out)){
        return Result<void>();          // 正常睡满
    }
    return r;                            // interrupted / 其它错误
}

} // namespace Coro

#endif // DELAY_HPP
