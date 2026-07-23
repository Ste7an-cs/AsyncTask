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

/**
 * @brief 获取全局单例，首次调用按 max(硬件并发, 2) 创建工作线程
 * @return 单例引用
 */
Coro::FibersPool &Coro::FibersPool::instance()
{
    static FibersPool fibers(std::max(std::thread::hardware_concurrency(), 2U));
    return fibers;
}

/**
 * @brief 关闭线程池：唤醒各工作线程并 join 至其真正退出。
 *
 * 必须在此 join：worker() 返回前，工作线程的调度器仍会在 pick_next() 中访问
 * FiberGlobalQueue 单例。若不 join 就返回，主线程会继续跑到 main() 结束、触发
 * 静态析构——而 FiberGlobalQueue 比 FibersPool 后构造、先析构，于是尚未退出的
 * 工作线程会访问已销毁的队列 → SIGSEGV。join 保证所有工作线程在返回前彻底停下。
 */
void Coro::FibersPool::close()
{
    block.close();
    for(auto& td:tds){
        if(td.joinable()){
            td.join();
        }
    }
}

/**
 * @brief 构造：创建 work_num 个工作线程并设置线程名
 * @param work_num 工作线程数量
 */
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

/**
 * @brief 析构：join 所有工作线程
 */
Coro::FibersPool::~FibersPool()
{
    for(auto& td:tds){
        if(td.joinable()){
            td.join();
        }
    }
}

/**
 * @brief 工作线程主体：安装调度算法后 block.wait() 挂起（线程不退出、可调度协程）
 */
void Coro::FibersPool::worker()
{
#ifdef ASYNC_HAS_QTCORE
    boost::fibers::use_scheduling_algorithm< Coro::QtFiberScheduler >();
#else
    boost::fibers::use_scheduling_algorithm< Coro::FiberScheduler >();
#endif
    block.wait();
}
