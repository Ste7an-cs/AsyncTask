///
/// Ctrl-C（SIGINT/SIGTERM）优雅退出示例（POSIX / Linux）。
///
/// 三条必须遵守的规则：
///   1. 绝不在信号处理函数里直接调用 Coro::quit()。
///      信号是异步打断，handler 直接跑在被打断线程的栈上，而主线程此刻通常
///      正停在 FiberScheduler::suspend_until 的 pthread_cond_timedwait 里，
///      也就是跑在 boost.fiber 的 dispatcher context 上。Coro::quit() 内部的
///      drainUntilIdle() 会 boost::this_fiber::yield()，等于在 handler 帧还
///      活着的时候从 dispatcher 换栈、递归重入调度器 —— 直接 SIGSEGV。
///      （quit() 里的 emit aboutToQuit / processEvents / fiber mutex / 收线程
///        同样都不是 async-signal-safe。）
///      正确做法：handler 里只做 write() 这类 async-signal-safe 的动作，把
///      真正的退出动作交回普通上下文执行（self-pipe + QSocketNotifier）。
///
///   2. 触发退出用 Coro::quit()，不要用 qApp->quit()。
///      本框架用 Coro::exec() 取代 QCoreApplication::exec()，Qt 自己那套
///      「exec() 返回时 emit aboutToQuit」的收尾路径根本不会执行，
///      qApp->quit() 只会让程序挂死。
///
///   3. 不要把 Coro::quit() 挂到 QCoreApplication::aboutToQuit 上。
///      aboutToQuit 是 quit() 的输出而不是输入 —— Coro::quit() 的第一句就是
///      广播 aboutToQuit（各 channel 靠它收敛），挂回去就成了无限递归，
///      协程栈几百层就溢出崩溃。aboutToQuit 槽里只做清理（存配置、关文件）。
///
/// 另外：协程必须响应 channel 关闭（await 返回 false）并返回。aboutToQuit 只
/// 广播一次，若协程无视它继续 await 一个新 channel，就会永久挂起，导致
/// boost.fiber 的 ~scheduler 卡死 —— 表现为「不崩了，但退不出去」。
///
#include <QCoreApplication>
#include <QSocketNotifier>
#include <QTimer>
#include <QDebug>

#include <csignal>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/coro.hpp"

using namespace Coro;

/// self-pipe：g_sig_fd[0] 供 handler 写，g_sig_fd[1] 交给 QSocketNotifier 读
static int g_sig_fd[2] = {-1, -1};

/**
 * @brief 信号处理函数：只做 async-signal-safe 的动作
 *
 * 唯一允许的动作是往 self-pipe 写一个字节。绝不在这里碰 Qt、协程或任何锁。
 */
extern "C" void onTermSignal(int sig)
{
    const char byte = static_cast<char>(sig);
    const ssize_t ignored = ::write(g_sig_fd[0], &byte, 1);   // async-signal-safe
    (void)ignored;
}

/**
 * @brief 安装 SIGINT/SIGTERM 的 self-pipe 转发
 * @param app 用于挂 QSocketNotifier 的父对象
 */
static void installSignalHandler(QCoreApplication* app)
{
    if(::socketpair(AF_UNIX, SOCK_STREAM, 0, g_sig_fd) != 0){
        qWarning() << "socketpair failed:" << ::strerror(errno);
        return;
    }

    auto* notifier = new QSocketNotifier(g_sig_fd[1], QSocketNotifier::Read, app);
    QObject::connect(notifier, &QSocketNotifier::activated, notifier, [notifier]{
        notifier->setEnabled(false);                          // 只处理一次，避免重入
        char sig = 0;
        const ssize_t ignored = ::read(g_sig_fd[1], &sig, 1);
        (void)ignored;
        qDebug() << "caught signal" << int(sig) << "-> Coro::quit()";
        quit();                                               // 此处已是普通槽上下文，安全
    });

    ::signal(SIGINT,  onTermSignal);
    ::signal(SIGTERM, onTermSignal);
}

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();

    installSignalHandler(&app);

    // aboutToQuit 只做清理，绝不在这里调 quit()
    QObject::connect(&app, &QCoreApplication::aboutToQuit, []{
        qDebug() << "aboutToQuit: 保存配置、关闭文件……";
    });

    // 一个长期运行的协程：靠 await 的返回值感知「应用要退出了」
    makeTask([]{
        QTimer* timer = new QTimer();
        timer->start(500);

        qDebug() << "working... 按 Ctrl-C 退出";
        for(;;){
            auto tick = coro(timer, &QTimer::timeout);
            if(!await(tick)){                 // aboutToQuit 已 close 掉 channel
                qDebug() << "channel closed -> 协程收尾并返回";
                break;                        // 必须 break，否则协程永久挂起、进程退不出去
            }
            qDebug() << "tick";
        }

        timer->deleteLater();
        return 0;
    });

    return exec();                            // 用 Coro::exec()，不是 app.exec()
}
