#ifndef QTFIBERTHREAD_H
#define QTFIBERTHREAD_H
#include <QThread>
#include "scheduler/fiberthreadblock.h"

namespace Coro {

/**
 * @brief 支持 fiber 的 QThread。
 *
 * run() 中安装 QtLocalFiberScheduler 并挂起，成为可承载 Fixed 协程的宿主线程。
 */
class QtFiberThread : public QThread
{
public:
    /**
     * @brief 构造
     * @param parent 父对象
     */
    QtFiberThread(QObject *parent = nullptr);
    /** @brief 析构：关闭阻塞、退出并等待线程结束 */
    ~QtFiberThread();
public slots:
    /** @brief 退出线程：解除阻塞并请求 QThread 退出 */
    void quit();
protected:
    /** @brief 线程主体：安装本地调度器并挂起待命 */
    void run(void) override;
protected:
    FiberThreadBlock block;///< 阻止线程退出的阻塞基元
};

}

#endif // QTFIBERTHREAD_H
