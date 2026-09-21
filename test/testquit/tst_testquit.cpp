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
#include <QtTest>
#include <QProcess>
#include <QTimer>
#include <QThread>

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
 * @brief 被测程序主体：在 mode 指定的上下文里调用 Coro::quit()
 * @param mode main / pool / thread / slot / posted
 * @return 进程退出码
 */
int runHelper(const QString& mode, int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
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
    QTest::newRow("main thread coroutine")  << "main";
    QTest::newRow("pool thread coroutine")  << "pool";
    QTest::newRow("plain std::thread")      << "thread";
    QTest::newRow("qt slot on main thread") << "slot";
    QTest::newRow("posted back to main")    << "posted";
}

/**
 * @brief 无论从哪个上下文调用，quit() 都必须完整收尾
 */
void TestQuit::quit_completes_shutdown()
{
    QFETCH(QString, mode);

    QByteArray out;
    int exitCode = 0;
    runMode(mode, &out, &exitCode);

    QVERIFY2(exitCode != -1, qPrintable(QStringLiteral("mode=%1 挂死未退出\n%2")
                                        .arg(mode, QString::fromUtf8(out))));
    QVERIFY2(exitCode == 0, qPrintable(QStringLiteral("mode=%1 退出码=%2\n%3")
                                       .arg(mode).arg(exitCode).arg(QString::fromUtf8(out))));

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
