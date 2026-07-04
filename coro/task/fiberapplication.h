#ifndef FIBERAPPLICATION_H
#define FIBERAPPLICATION_H
#include <QObject>
#include <QCoreApplication>
#include "executor/scheduler/fiberthreadblock.h"
namespace Coro {

/**
 * @brief 应用集成与生命周期（单例）。
 *
 * 在主线程安装本地调度器并启动工作线程池；exec() 让主线程留在协程调度器中，
 * quit() 走安全退出流程。
 */
class FiberApplication : QObject{
    Q_OBJECT
public:
    /**
     * @brief 获取全局单例
     * @return 单例指针
     */
    static FiberApplication* instance();
    /**
     * @brief 主循环：主线程挂起于协程调度器（协程与 Qt 事件都能推进）
     * @return 退出码
     */
    int exec();
    /**
     * @brief 安全退出：广播 aboutToQuit → 排空在途协程与事件 → 停线程池 → 退出
     */
    void quit();
protected:
    /** @brief 构造：主线程安装本地调度器并启动线程池 */
    FiberApplication();
    Coro::FiberThreadBlock block;///< 阻止主线程退出的阻塞基元
};

/**
 * @brief 在主线程安装协程应用（安装本地调度器并启动线程池）
 */
void installFiberApplication();
/**
 * @brief 进入主循环（等价于 FiberApplication::exec）
 * @return 退出码
 */
int exec();
/**
 * @brief 触发安全退出（等价于 FiberApplication::quit）
 */
void quit();

}

#endif // FIBERAPPLICATION_H
