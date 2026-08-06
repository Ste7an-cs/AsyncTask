#ifndef FIBERAPPLICATION_H
#define FIBERAPPLICATION_H
#include <QObject>
#include <QCoreApplication>
#include "executor/scheduler/fiberthreadblock.h"
namespace Coro {

/**
 * @brief 应用集成与生命周期（单例）。
 * @code
 * int main(int argc, char* argv[]) {
 *     QCoreApplication app(argc, argv);
 *     Coro::installFiberApplication();     // 安装调度器 + 启动工作线程池
 *     Coro::makeTask([]{ work(); Coro::quit(); return 0; });
 *     return Coro::exec();                 // 用 Coro::exec() 而非 app.exec()
 * }
 * @endcode
 *
 * 在主线程安装本地调度器并启动工作线程池；exec() 让主线程留在协程调度器中，
 * quit() 走安全退出流程。
 */
class FiberApplication : QObject{
    Q_OBJECT
public:
    /**
     * @brief 获取全局单例
     * @code
     * auto* app = Coro::FiberApplication::instance();
     * @endcode
     * @return 单例指针
     */
    static FiberApplication* instance();
    /**
     * @brief 主循环：主线程挂起于协程调度器（协程与 Qt 事件都能推进）
     * @code
     * // 等价于自由函数 Coro::exec()；与 QCoreApplication::exec() 互斥
     * return Coro::FiberApplication::instance()->exec();
     * @endcode
     * @return 退出码
     */
    int exec();
    /**
     * @brief 安全退出：广播 aboutToQuit → 排空在途协程与事件 → 停线程池 → 退出
     * @code
     * // 等价于自由函数 Coro::quit()；可在任意协程或槽中调用
     * Coro::FiberApplication::instance()->quit();
     * @endcode
     */
    void quit();
protected:
    /** @brief 构造：主线程安装本地调度器并启动线程池 */
    FiberApplication();
    Coro::FiberThreadBlock block;///< 阻止主线程退出的阻塞基元
};

/**
 * @brief 在主线程安装协程应用（安装本地调度器并启动线程池）
 * @code
 * QCoreApplication app(argc, argv);
 * Coro::installFiberApplication();     // 必须在 Coro::exec() 之前调用
 * @endcode
 */
void installFiberApplication();
/**
 * @brief 进入主循环（等价于 FiberApplication::exec）
 * @code
 * return Coro::exec();     // 取代 app.exec()：协程与 Qt 事件都由它驱动
 * @endcode
 * @return 退出码
 */
int exec();
/**
 * @brief 触发安全退出（等价于 FiberApplication::quit）
 * @code
 * Coro::makeTask([job]{
 *     job.get();          // 先等其它任务完成（让出式）
 *     Coro::quit();       // 再安全退出：唤醒挂起协程、排空在途任务后收尾
 *     return 0;
 * });
 * @endcode
 */
void quit();

}

#endif // FIBERAPPLICATION_H
