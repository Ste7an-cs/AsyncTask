///
/// GUI 程序退出示例：关闭窗口即退出。
///
/// 要点：Coro::exec() 内部真正进入了 QCoreApplication::exec()，因此
///   - QApplicationPrivate::in_exec 被置位，关闭最后一个窗口才会 emit
///     lastWindowClosed 并 post QEvent::Quit（该函数第一句就是 if(in_exec)）；
///   - QEventLoop 被压进 threadData->eventLoops，QCoreApplication::exit()
///     才有循环可退（栈为空时它是空操作）。
/// 于是「关窗」「qApp->quit()」「Coro::quit()」「平台退出请求」汇合到同一条
/// 路径：让 app.exec() 返回 → Qt 原生 emit aboutToQuit → 框架排空在途协程
/// 与 deleteLater → 停线程池 → Coro::exec() 返回。
///
/// GUI 侧无需为协程做任何特殊处理，照常写 QApplication + 窗口即可。
///
/// 无头运行（CI / 无 X11）：
///   QT_QPA_PLATFORM=offscreen CORO_AUTOCLOSE_MS=1500 ./gui_quit
/// CORO_AUTOCLOSE_MS 仅用于自动化验证，模拟用户在指定毫秒后关闭窗口。
///
#include <QApplication>
#include <QLabel>
#include <QPushButton>
#include <QTimer>
#include <QVBoxLayout>
#include <QWidget>
#include <QDebug>

#include <thread>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/coro.hpp"

using namespace Coro;

int main(int argc, char* argv[])
{
    QApplication app(argc, argv);
    installFiberApplication();               // 安装调度器 + 工作线程池

    QWidget window;
    window.setWindowTitle("AsyncTask GUI quit");
    auto* label  = new QLabel("tick: 0", &window);
    auto* button = new QPushButton("quit()", &window);
    auto* layout = new QVBoxLayout(&window);
    layout->addWidget(label);
    layout->addWidget(button);

    // 按钮走 Coro::quit()；直接连 qApp->quit() 效果完全相同
    QObject::connect(button, &QPushButton::clicked, []{ quit(); });

    // aboutToQuit 由 Qt 在主循环返回时原生发出，只做清理
    QObject::connect(&app, &QCoreApplication::aboutToQuit, []{
        qDebug() << "aboutToQuit: 保存配置、关闭文件……";
    });

    // 协程与 GUI 并存：await 期间不阻塞主线程，界面照常响应。
    // 要碰控件就必须把协程钉在 GUI 线程上（Affinity::fixed），
    // 否则它会被调度到工作线程，跨线程操作 QWidget 属于未定义行为。
    makeTask([label]{
        QTimer timer;
        timer.start(200);

        for(int tick = 1; ; ++tick){
            auto tock = coro(&timer, &QTimer::timeout);
            if(!await(tock)){                // aboutToQuit 已 close 掉 channel
                qDebug() << "channel closed -> 协程收尾并返回";
                break;                       // 必须 break，否则协程永久挂起、进程退不出去
            }
            label->setText(QString("tick: %1").arg(tick));
        }
        return 0;
    }, Priority::Normal, Affinity::fixed(std::this_thread::get_id()));

    window.show();

    // 仅供无头自动化：到点自动关窗，等价于用户点右上角的叉
    const QByteArray autoclose = qgetenv("CORO_AUTOCLOSE_MS");
    if(!autoclose.isEmpty()){
        QTimer::singleShot(autoclose.toInt(), &window, [&window]{
            qDebug() << "auto close window";
            window.close();                  // 最后一个窗口关闭 -> post QEvent::Quit
        });
    }

    return exec();                           // 用 Coro::exec()，关窗即正常退出
}
