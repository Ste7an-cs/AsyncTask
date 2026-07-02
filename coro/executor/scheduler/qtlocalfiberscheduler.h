#ifndef QTLOCALFIBERSCHEDULER_H
#define QTLOCALFIBERSCHEDULER_H

#include "qtfiberscheduler.h"

namespace Coro {

///
/// \brief The QtLocalFiberScheduler class
/// 用于非全局的QThread和Qt mainthread的调度器，这个调度器只分配Fixed和shared任务
///
class QtLocalFiberScheduler : public QtFiberScheduler
{
public:
    QtLocalFiberScheduler(void);
    ~QtLocalFiberScheduler(void) override;
    ///
    /// \brief pick_next 协程调度器获取下一个可运行的Fixed或shared 任务
    /// \return 被唤醒的fiber 上下文
    ///
    boost::fibers::context * pick_next() noexcept override;

    ///
    /// \brief has_ready_fibers 判断当前调度器线程中是否有可用的协程
    /// \return
    ///
    bool has_ready_fibers(void) const noexcept override;
};

}

#endif // QTLOCALFIBERSCHEDULER_H
