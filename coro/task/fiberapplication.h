#ifndef FIBERAPPLICATION_H
#define FIBERAPPLICATION_H
#include <QObject>
#include <QCoreApplication>
#include <QTimer>
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
 * 同时把 Qt 主循环跑在一个绑定主线程的协程上，quit() 走安全退出流程。
 *
 * GUI 程序同样适用：窗口关闭、qApp->quit() 与 Coro::quit() 汇合到同一条退出
 * 路径，无需特殊处理。
 * @code
 * int main(int argc, char* argv[]) {
 *     QApplication app(argc, argv);
 *     Coro::installFiberApplication();
 *     MainWindow w; w.show();
 *     return Coro::exec();                 // 关闭窗口即正常退出
 * }
 * @endcode
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
     * @brief 主循环：主线程挂起于协程调度器，Qt 主循环跑在绑定主线程的协程上
     * @code
     * // 等价于自由函数 Coro::exec()；内部会进入 QCoreApplication::exec()，
     * // 因此不要再自行调用 app.exec()
     * return Coro::FiberApplication::instance()->exec();
     * @endcode
     * @return 退出码（QCoreApplication::exec() 的返回值）
     */
    int exec();
    /**
     * @brief 安全退出：退出 Qt 主循环，收尾由 exec() 返回后的 shutdown() 承担
     *
     * 可在任意线程调用：非主线程调用时自动投递回主线程执行，收尾语义一致。
     * 未调用过 Coro::exec() 时（如 QTest 驱动）退化为就地收尾并代发
     * aboutToQuit。重复调用是幂等的。
     * @code
     * // 等价于自由函数 Coro::quit()；可在任意协程、槽或线程中调用
     * Coro::FiberApplication::instance()->quit();
     * @endcode
     * @warning 不可在 POSIX 信号处理函数中调用（非 async-signal-safe，会在
     *          调度器上下文中切栈而崩溃）；见 example/signal_quit。
     */
    void quit();
protected:
    /** @brief 构造：主线程安装本地调度器并启动线程池 */
    FiberApplication();
    /**
     * @brief 收尾：停各线程泵 → 排空在途协程与事件 → 停线程池 → 解除主线程阻塞
     *
     * 运行在 qt-loop 协程上，由 QCoreApplication::exec() 返回后调用；此时 Qt
     * 已自行 emit aboutToQuit 并冲刷过 DeferredDelete。
     */
    void shutdown();

    Coro::FiberThreadBlock block;///< 阻止主线程退出的阻塞基元
    QMetaObject::Connection block_conn_;///< aboutToBlock 钩子，收尾时断开
    int  exit_code_{0};///< QCoreApplication::exec() 的返回码
    bool in_exec_{false};///< 已进入 Coro::exec()，退出走 Qt 原生路径
    bool quit_requested_{false};///< 已请求或已完成退出（quit() 的幂等闸门）
    QTimer deadline_timer_{};///< 单次定时器，给随后的 poll() 设上限
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
 *
 * 线程安全：在工作线程或任意非主线程调用时会自动投递回主线程执行。
 * @code
 * Coro::makeTask([job]{
 *     job.get();          // 先等其它任务完成（让出式）
 *     Coro::quit();       // 再安全退出：唤醒挂起协程、排空在途任务后收尾
 *     return 0;
 * });
 * @endcode
 * @warning 不可在 POSIX 信号处理函数中调用；正确做法见 example/signal_quit。
 */
void quit();

}

#endif // FIBERAPPLICATION_H
