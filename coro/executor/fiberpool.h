#ifndef FIBERPOOL_H
#define FIBERPOOL_H

#include <thread>
#include <vector>
#include <functional>
#include "scheduler/fiberthreadblock.h"

namespace Coro {

/**
 * @brief 全局共享的协程池（工作线程池，单例）。
 *
 * 首次使用时创建 N 个工作线程，各安装调度器并挂起待命（线程不退出但可调度
 * 协程）；close() 唤醒各线程使其退出。
 */
class FibersPool{
    using TaskType = std::function<void(void)>;
public:
    /**
     * @brief 获取全局单例
     * @return 单例引用
     */
    static FibersPool& instance(void);
    /**
     * @brief 关闭线程池，唤醒并结束各工作线程。
     * @note main Thread 结束前需手动 close，避免堆栈损坏。
     */
    void close(void);
    /** @brief 禁止移动构造 */
    FibersPool(FibersPool&& pool) = delete ;
    /** @brief 禁止拷贝构造 */
    FibersPool(const FibersPool& pool) = delete ;
    /** @brief 禁止移动赋值 */
    FibersPool& operator=(FibersPool&& pool) = delete ;
    /** @brief 禁止拷贝赋值 */
    FibersPool& operator=(const FibersPool& pool) = delete ;
private:
    /**
     * @brief 构造，创建 work_num 个工作线程
     * @param work_num 工作线程数量
     */
    explicit FibersPool(const int work_num);
    /** @brief 析构，join 所有工作线程 */
    ~FibersPool(void);
    /** @brief 工作线程主体：安装调度器并挂起待命 */
    void worker(void);
    int worker_num;///< 工作线程数量
    std::vector<std::thread> tds;///< 工作线程集合
    FiberThreadBlock block;///< 阻止工作线程退出的阻塞基元
};

}

#endif // FIBERPOOL_H
