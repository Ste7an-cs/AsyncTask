#ifndef QTFIBERSCHEDULER_H
#define QTFIBERSCHEDULER_H

#include "fiberscheduler.h"
#include <QEventLoop>

namespace Coro {

///
/// \brief The QtFiberScheduler class
/// 支持QT事件循环的调度器
///
class QtFiberScheduler : public FiberScheduler
{
public:
    QtFiberScheduler(void);
    ~QtFiberScheduler(void) override;

    ///
    /// \brief suspend_until    没有可运行的协程时，休眠线程
    /// \param time_point       下一个唤醒的时刻
    ///
    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept override;
    void notify(void) noexcept override;
protected:
    QEventLoop loop;
    std::atomic_bool interrupt_flg{false};
    boost::fibers::fiber event_fiber_;
    QEventLoop eventloop;
};

}

#endif // QTFIBERSCHEDULER_H
