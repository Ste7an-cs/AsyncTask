#ifndef QTLOCALFIBERSCHEDULER_H
#define QTLOCALFIBERSCHEDULER_H

#include "qtfiberscheduler.h"

namespace Coro {

/**
 * @brief 用于非全局的 QThread 和 Qt 主线程的调度器。
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
     * @return 下一个可运行的 fiber 上下文；无则返回 nullptr
     */
    boost::fibers::context * pick_next() noexcept override;

    /**
     * @brief 判断当前调度器线程中是否有可用的协程（Fixed 或 Shared）
     * @return 有则返回 true
     */
    bool has_ready_fibers(void) const noexcept override;
};

}

#endif // QTLOCALFIBERSCHEDULER_H
