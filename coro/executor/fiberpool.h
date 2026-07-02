#ifndef FIBERPOOL_H
#define FIBERPOOL_H

#include <thread>
#include <vector>
#include <functional>
#include "scheduler/fiberthreadblock.h"

namespace Coro {

///
/// \brief The FibersPool class 全局共享的协程池
///
class FibersPool{
    using TaskType = std::function<void(void)>;
public:
    static FibersPool& instance(void);
    void close(void);//main Thread 结束前需手动close，避免堆栈损坏
    FibersPool(FibersPool&& pool) = delete ;
    FibersPool(const FibersPool& pool) = delete ;
    FibersPool& operator=(FibersPool&& pool) = delete ;
    FibersPool& operator=(const FibersPool& pool) = delete ;
private:
    explicit FibersPool(const int work_num);
    ~FibersPool(void);
    void worker(void);
    int worker_num;
    std::vector<std::thread> tds;
    FiberThreadBlock block;
};

}

#endif // FIBERPOOL_H
