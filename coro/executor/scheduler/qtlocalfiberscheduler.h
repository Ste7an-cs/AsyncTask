#ifndef QTLOCALFIBERSCHEDULER_H
#define QTLOCALFIBERSCHEDULER_H

#include "qtfiberscheduler.h"

namespace Coro {

/**
 * @brief 用于非全局的 QThread 和 Qt 主线程的调度器。
 * @code
 * // 主线程 / QtFiberThread 上安装：只取 Fixed 与 Shared 协程，
 * // 不把新的 Sticky 协程就地绑定到宿主线程
 * boost::fibers::use_scheduling_algorithm<Coro::QtLocalFiberScheduler>();
 * @endcode
 *
 * 该调度器只分配 Fixed 和 Shared 任务（不把新 Sticky 就地绑定到宿主线程），
 * 适合已有事件循环归属的宿主线程。
 */
class QtLocalFiberScheduler : public QtFiberScheduler
{
public:
    /** @brief 构造 */
    QtLocalFiberScheduler(void);
    /** @brief 析构 */
    ~QtLocalFiberScheduler(void) override;
    /**
     * @brief 获取下一个可运行的 Fixed 或 Shared 协程
     * @code
     * // 由 boost.fiber 调度循环自动回调，使用方不直接调用。
     * // 取用顺序：本线程 Fixed -> Shared -> 本线程主队列
     * @endcode
     * @return 下一个可运行的 fiber 上下文；无则返回 nullptr
     */
    boost::fibers::context * pick_next() noexcept override;

    /**
     * @brief 判断当前调度器线程中是否有可用的协程（Fixed 或 Shared）
     * @code
     * // 由调度器内部调用，决定是否进入 suspend_until 阻塞
     * @endcode
     * @return 有则返回 true
     */
    bool has_ready_fibers(void) const noexcept override;
};

}

#endif // QTLOCALFIBERSCHEDULER_H
