#include "fiberpool.h"
#ifdef __linux__
#include "pthread.h"
#endif
#include <QEventLoop>
#include <boost/fiber/all.hpp>
#include "scheduler/fiberthreadblock.h"
#ifdef ASYNC_HAS_QTCORE
#include "scheduler/qtfiberscheduler.h"
#else
#include "scheduler/fiberscheduler.h"
#endif
Coro::FibersPool &Coro::FibersPool::instance()
{
    static FibersPool fibers(std::max(std::thread::hardware_concurrency(), 2U));
    return fibers;
}

void Coro::FibersPool::close()
{
    block.close();
}

Coro::FibersPool::FibersPool(const int work_num)
{
    for(int i=0; i<work_num; i++){
        std::thread th(&FibersPool::worker, this);
#ifdef __linux__
        pthread_setname_np(th.native_handle(), QString("fiberspool%1").arg(i).toLatin1().data());
//        ///
//        /// 实测，标准内核下，FIFO或RR调度器的稳定度不如默认的CFS
//        ///
//        struct sched_param sp{};
//        sp.sched_priority = 20;
//        pthread_setschedparam(th.native_handle(), SCHED_FIFO | SCHED_RESET_ON_FORK, &sp);//设为FIFO调度器，且创建的子线程不继承该优先级
#endif
        tds.emplace_back(std::move(th));
    }
}

Coro::FibersPool::~FibersPool()
{
    for(auto& td:tds){
        if(td.joinable()){
            td.join();
        }
    }
}

void Coro::FibersPool::worker()
{
#ifdef ASYNC_HAS_QTCORE
    boost::fibers::use_scheduling_algorithm< Coro::QtFiberScheduler >();
#else
    boost::fibers::use_scheduling_algorithm< Coro::FiberScheduler >();
#endif
    block.wait();
}
