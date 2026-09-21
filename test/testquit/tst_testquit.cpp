///
/// Coro::quit() 调用上下文的回归测试。
///
/// quit() 会终结进程，一个进程只能测一次，因此本程序是「自举双模式」的：
///   - 带 --helper=<mode> 启动时，扮演被测程序，在指定上下文里调用 quit()，
///     并把可观测的收尾标记打到 stdout；
///   - 不带参数启动时，是 QtTest 用例，用 QProcess 把自己按各种 mode 拉起来，
///     断言退出码与收尾标记。
///
/// 被断言的收尾语义（quit() 必须完成的事）：
///   DRAIN_OK        主线程上 pending 的 deleteLater 被冲刷（析构真的跑了）
///   SLOT_MAIN       带 context 的 aboutToQuit 槽被触发，且跑在主线程
///   COROUTINE_DONE  在途协程因 channel 关闭而收敛返回
/// 三者缺一，就说明收尾被静默跳过。
///
/// 除 Coro::quit() 外，还覆盖 Qt 自己的退出入口——qApp->quit()、关闭最后一个
/// 窗口、QCoreApplication::exit(code)。Coro::exec() 内部真正进入了
/// QCoreApplication::exec()，这些入口必须与 Coro::quit() 收敛到同一条收尾路径，
/// 且退出码要如实传回。另有 noexec 一档：不调用 Coro::exec() 的用法（QTest 驱动
/// 就是这样）下，quit() 仍须就地完成全部收尾。
///
#include <QtTest>
#include <QApplication>
#include <QProcess>
#include <QTimer>
#include <QThread>
#include <QWidget>

#include <memory>
#include <thread>
#include <cstdio>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/coro.hpp"

using namespace Coro;

// ---------------------------------------------------------------- helper 侧

namespace {

/// 主线程 id，用于判断槽跑在哪个线程
std::thread::id g_main_thread_id;

/// 输出一个标记（用 stdout 而非 qDebug，避免和 Qt 日志重定向纠缠）
void mark(const char* tag)
{
    std::printf("%s\n", tag);
    std::fflush(stdout);
}

/// 探针：退出时投递 deleteLater，析构跑到了才说明 drain 生效
class DrainProbe : public QObject
{
    Q_OBJECT
public:
    ~DrainProbe() override { mark("DRAIN_OK"); }
};

/**
 * @brief 被测程序主体：在 mode 指定的上下文里触发退出
 * @param mode main / pool / thread / slot / posted / qapp / window / exitcode / noexec
 * @return 进程退出码
 */
int runHelper(const QString& mode, int argc, char* argv[])
{
    // window 档要真窗口；无头环境下走 offscreen 平台插件
    const bool gui = (mode == QLatin1String("window"));
    if(gui && qEnvironmentVariableIsEmpty("QT_QPA_PLATFORM")){
        qputenv("QT_QPA_PLATFORM", "offscreen");
    }
    std::unique_ptr<QCoreApplication> owner(
                gui ? static_cast<QCoreApplication*>(new QApplication(argc, argv))
                    : new QCoreApplication(argc, argv));
    QCoreApplication& app = *owner;

    installFiberApplication();
    g_main_thread_id = std::this_thread::get_id();

    // 带 context 的 connect：跨线程 emit 时会退化成 QueuedConnection
    QObject::connect(&app, &QCoreApplication::aboutToQuit, &app, []{
        mark(std::this_thread::get_id() == g_main_thread_id ? "SLOT_MAIN" : "SLOT_OTHER");
    });

    // 一个在途协程：靠 await 返回 false 感知退出，并留下待冲刷的 deleteLater
    makeTask([]{
        QTimer* timer = new QTimer();
        timer->start(50);
        for(;;){
            auto tick = coro(timer, &QTimer::timeout);
            if(!await(tick)) break;
        }
        timer->deleteLater();
        (new DrainProbe())->deleteLater();
        mark("COROUTINE_DONE");
        return 0;
    });

    if(mode == "main"){
        // 主线程协程（makeTask 默认 Affinity::fixed(当前线程)）
        makeTask([]{ Coro::sleep(1); quit(); return 0; });
    }else if(mode == "pool"){
        // 线程池工作线程上的协程
        makeTask([]{ Coro::sleep(1); quit(); return 0; },
                 Priority::Normal, Affinity::sticky());
    }else if(mode == "thread"){
        // 完全在框架之外的普通线程
        static std::thread th([]{
            std::this_thread::sleep_for(std::chrono::seconds(1));
            quit();
        });
        th.detach();
    }else if(mode == "slot"){
        // 主线程的 Qt 槽
        QTimer::singleShot(1000, &app, []{ quit(); });
    }else if(mode == "posted"){
        // 普通线程 -> 手工投递回主线程（历史上唯一安全的跨线程写法）
        static std::thread th([]{
            std::this_thread::sleep_for(std::chrono::seconds(1));
            QMetaObject::invokeMethod(QCoreApplication::instance(),
                                      []{ quit(); }, Qt::QueuedConnection);
        });
        th.detach();
    }else if(mode == "qapp"){
        // Qt 自己的退出入口，必须与 Coro::quit() 等价
        QTimer::singleShot(1000, &app, []{ QCoreApplication::quit(); });
    }else if(mode == "window"){
        // 关闭最后一个窗口——本框架最初的缺陷就在这里：Coro::exec() 不进
        // QCoreApplication::exec() 时 in_exec 为假，emitLastWindowClosed()
        // 第一句就被短路，关窗既不 post QEvent::Quit 也不 emit lastWindowClosed
        QWidget* w = new QWidget();
        w->setAttribute(Qt::WA_DeleteOnClose);
        w->show();
        QTimer::singleShot(1000, w, [w]{ w->close(); });
    }else if(mode == "exitcode"){
        // 退出码要从 QCoreApplication::exec() 如实传回 Coro::exec()
        QTimer::singleShot(1000, &app, []{ QCoreApplication::exit(3); });
    }else if(mode == "noexec"){
        // 不进入 Coro::exec() 的用法（QTest 驱动即如此）：没有「exec() 返回」
        // 这一刻，quit() 必须就地代发 aboutToQuit 并完成收尾
        makeTask([]{
            Coro::sleep(1);
            quit();
            return 0;
        });
        Coro::msleep(3000);                   // 让出主纤程，驱动上面的协程
        return 0;
    }else{
        std::fprintf(stderr, "unknown helper mode: %s\n", qPrintable(mode));
        return 2;
    }

    return exec();
}

} // namespace

// ------------------------------------------------------------------ 测试侧

class TestQuit : public QObject
{
    Q_OBJECT

private slots:
    void quit_completes_shutdown_data();
    void quit_completes_shutdown();

private:
    void runMode(const QString& mode, QByteArray* out, int* exitCode);
};

/**
 * @brief 按 mode 拉起 helper 子进程
 * @param mode helper 模式
 * @param out 合并后的 stdout+stderr
 * @param exitCode 子进程退出码；超时置 -1
 */
void TestQuit::runMode(const QString& mode, QByteArray* out, int* exitCode)
{
    QProcess proc;
    proc.setProcessChannelMode(QProcess::MergedChannels);
    if(mode == QLatin1String("window")){
        // GUI 档只在这一档关掉 LeakSanitizer：fontconfig 和平台插件自身有
        // 常驻分配，退出时被报成泄漏、把子进程退出码顶成 1，与被测语义无关。
        QProcessEnvironment env = QProcessEnvironment::systemEnvironment();
        const QString prev = env.value(QStringLiteral("ASAN_OPTIONS"));
        env.insert(QStringLiteral("ASAN_OPTIONS"),
                   prev.isEmpty() ? QStringLiteral("detect_leaks=0")
                                  : prev + QStringLiteral(":detect_leaks=0"));
        proc.setProcessEnvironment(env);
    }
    proc.start(QCoreApplication::applicationFilePath(),
               QStringList() << QStringLiteral("--helper=%1").arg(mode));
    QVERIFY2(proc.waitForStarted(5000), "helper 子进程启动失败");

    if(!proc.waitForFinished(15000)){
        proc.kill();
        proc.waitForFinished(3000);
        *out = proc.readAll();
        *exitCode = -1;                       // 视为挂死
        return;
    }
    *out = proc.readAll();
    *exitCode = (proc.exitStatus() == QProcess::NormalExit) ? proc.exitCode() : -2;
}

void TestQuit::quit_completes_shutdown_data()
{
    QTest::addColumn<QString>("mode");
    QTest::addColumn<int>("expectedExit");
    QTest::newRow("main thread coroutine")  << "main"     << 0;
    QTest::newRow("pool thread coroutine")  << "pool"     << 0;
    QTest::newRow("plain std::thread")      << "thread"   << 0;
    QTest::newRow("qt slot on main thread") << "slot"     << 0;
    QTest::newRow("posted back to main")    << "posted"   << 0;
    QTest::newRow("qApp->quit()")           << "qapp"     << 0;
    QTest::newRow("last window closed")     << "window"   << 0;
    QTest::newRow("exit code propagated")   << "exitcode" << 3;
    QTest::newRow("no Coro::exec()")        << "noexec"   << 0;
}

/**
 * @brief 无论从哪个入口触发，退出都必须完整收尾且退出码如实传回
 */
void TestQuit::quit_completes_shutdown()
{
    QFETCH(QString, mode);
    QFETCH(int, expectedExit);

    QByteArray out;
    int exitCode = 0;
    runMode(mode, &out, &exitCode);

    // 无头环境缺 offscreen 插件时，GUI 档没法测，跳过而不是误报失败
    if(mode == QLatin1String("window") && exitCode != expectedExit
            && out.contains("platform plugin")){
        QSKIP("no usable Qt platform plugin for the GUI case");
    }

    QVERIFY2(exitCode != -1, qPrintable(QStringLiteral("mode=%1 挂死未退出\n%2")
                                        .arg(mode, QString::fromUtf8(out))));
    QVERIFY2(exitCode == expectedExit,
             qPrintable(QStringLiteral("mode=%1 退出码=%2（期望 %3）\n%4")
                        .arg(mode).arg(exitCode).arg(expectedExit)
                        .arg(QString::fromUtf8(out))));

    QVERIFY2(out.contains("COROUTINE_DONE"),
             qPrintable(QStringLiteral("mode=%1 在途协程未收敛\n%2").arg(mode, QString::fromUtf8(out))));
    QVERIFY2(out.contains("DRAIN_OK"),
             qPrintable(QStringLiteral("mode=%1 deleteLater 未被冲刷\n%2").arg(mode, QString::fromUtf8(out))));
    QVERIFY2(out.contains("SLOT_MAIN"),
             qPrintable(QStringLiteral("mode=%1 aboutToQuit 槽未在主线程触发\n%2").arg(mode, QString::fromUtf8(out))));
}

int main(int argc, char* argv[])
{
    const QLatin1String prefix("--helper=");
    if(argc > 1 && QLatin1String(argv[1]).startsWith(prefix)){
        return runHelper(QString::fromLatin1(argv[1]).mid(prefix.size()), argc, argv);
    }

    QCoreApplication app(argc, argv);
    TestQuit tc;
    return QTest::qExec(&tc, argc, argv);
}

#include "tst_testquit.moc"
