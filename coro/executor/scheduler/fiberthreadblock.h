#ifndef FIBERTHREADBLOCK_H
#define FIBERTHREADBLOCK_H
#include <mutex>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/condition_variable.hpp>

namespace Coro {

///
/// \brief The FiberThreadBlock class 用于阻止用户创建的thread退出
///     thread将阻塞在wait处，但Fiber可以正常调度，取消阻塞时调用close
///
///
class FiberThreadBlock
{
public:
    FiberThreadBlock();
    void wait();
    void close();
    bool isClosed();
protected:
    void schedulerWait();
private:
    bool is_closed_{false};
    boost::fibers::mutex mtx_;
    boost::fibers::condition_variable cond_;
};
}
#endif // FIBERTHREADBLOCK_H
