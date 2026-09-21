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
/// 还有一档 starve：它测的不是退出，而是**主线程 Qt 事件循环不得被协程积压饿死**。
/// 之所以也放在本文件，是因为这条路径只存在于 Coro::exec() 里（aboutToBlock →
/// parkUntilIdle 的交棒），而 exec() 一个进程只进得去一次、返回即收尾，没法和
/// test_scheduler 里的其它用例共处一个进程；本文件的「自举子进程 + 15s 硬超时」
/// 正好是这种一次性主循环场景的现成夹具，回归时报失败而不是把整条测试队列拖死。
///
#include <QtTest>
#include <QApplication>
#include <QProcess>
#include <QTimer>
#include <QThread>
#include <QWidget>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <cstdio>

#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/coro.hpp"
#include "detail/asyncdefine.h"

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

// ------------------------------------------------- starve 档：主线程饥饿守卫

/// starve 档的观测量，全部是原子量：写在主线程的 Qt 槽里，读在退出前
struct StarveStat
{
    std::atomic_int      ticks{ 0 };          ///< QTimer 总触发次数
    std::atomic_int      ticks_in_backlog{ 0 };///< 积压窗口内的触发次数
    std::atomic_llong    max_gap_ms{ 0 };     ///< 积压窗口内两次分发的最大间隔
    std::atomic_llong    sleep_ms{ -1 };      ///< 主线程协程 msleep(200) 的实测耗时
};

/**
 * @brief starve 档被测程序：主线程背着协程积压，Qt 事件循环必须照常分发
 *
 * 场景就是 GUI 程序的「卡死」现场：kBacklogFibers 个 Shared 协程持续忙转
 * kBacklogMs 毫秒。主线程的 pick_next() 会去全局队列里偷 Shared 协程，于是
 * 本线程**永远不会进入空闲态**，suspend_until() 一次都不会被回调 —— 而
 * parkUntilIdle() 挂起的 qt-loop 协程正是靠它交棒才醒得过来。没有饥饿守卫
 * 时，这段时间里主线程一个 Qt 事件都不分发：窗口不重绘、点不动、关不掉。
 *
 * 断言用的可观测量是一个 50ms 的主线程 QTimer：积压期间它触发了几次、两次
 * 触发之间最长隔了多久。另外记一个诊断量 sleep_ms —— 主线程 Fixed 协程
 * msleep(200) 的实测耗时，用来区分「Qt 被饿死」和「整个主线程协程时基被饿死」。
 * @return 退出码
 */
int runStarveHelper(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    installFiberApplication();

    constexpr int kBacklogFibers = 256;   ///< 足够深，保证全局队列一刻不空
    constexpr int kBacklogMs     = 1500;  ///< 积压持续时长
    constexpr int kSpinUs        = 100;   ///< 每个协程两次让出之间的忙转时长
    constexpr int kTimerMs       = 50;    ///< 主线程 QTimer 周期
    constexpr int kQuitMs        = 2600;  ///< 积压结束后再留出余量才退出

    auto stat = std::make_shared<StarveStat>();

    /// 主线程的 Qt 定时器：它能不能响，就是「Qt 有没有被饿死」的判据
    auto* timer = new QTimer(&app);
    // 计时起点在 exec() 之前一刻设定，见下面 last_tick 的赋值
    auto last_tick = std::make_shared<std::chrono::steady_clock::time_point>();
    auto backlog_end = std::make_shared<std::chrono::steady_clock::time_point>();
    QObject::connect(timer, &QTimer::timeout, &app, [stat, last_tick, backlog_end]{
        const auto now = std::chrono::steady_clock::now();
        const auto gap = std::chrono::duration_cast<std::chrono::milliseconds>(now - *last_tick).count();
        stat->ticks.fetch_add(1);
        /// 只统计「起点落在积压窗口内」的那些间隔：积压结束之后 Qt 自然恢复，
        /// 那段的间隔说明不了问题。
        if(*last_tick < *backlog_end){
            if(gap > stat->max_gap_ms.load()){
                stat->max_gap_ms.store(gap);
            }
            if(now <= *backlog_end){
                stat->ticks_in_backlog.fetch_add(1);
            }
        }
        *last_tick = now;
    });

    /// 诊断协程：主线程 Fixed，睡 200ms 看实际睡了多久
    makeTask([stat]{
        const auto t0 = std::chrono::steady_clock::now();
        Coro::msleep(200);
        stat->sleep_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t0).count());
        return 0;
    });

    /// 退出闸：积压结束后 Qt 必定恢复，这个单次定时器到点收尾
    QTimer::singleShot(kQuitMs, &app, [stat]{
        std::printf("STARVE_TICKS=%d\n",          stat->ticks.load());
        std::printf("STARVE_TICKS_BACKLOG=%d\n",  stat->ticks_in_backlog.load());
        std::printf("STARVE_MAXGAP_MS=%lld\n",    stat->max_gap_ms.load());
        std::printf("STARVE_SLEEP_MS=%lld\n",     stat->sleep_ms.load());
        std::fflush(stdout);
        quit();
    });

    const auto t0 = std::chrono::steady_clock::now();
    *last_tick   = t0;
    *backlog_end = t0 + std::chrono::milliseconds(kBacklogMs);

    /// 积压本体：Shared 协程忙转 + 让出，让全局共享队列一刻不空。
    /// 它们到点自行收敛，所以即便守卫失效本进程也能正常退出（报数字而不是挂死）。
    const auto deadline = *backlog_end;
    for(int i = 0; i < kBacklogFibers; ++i){
        Coro::launch_properties([deadline, spin_us = kSpinUs]{
            while(std::chrono::steady_clock::now() < deadline){
                const auto spin_end = std::chrono::steady_clock::now()
                                    + std::chrono::microseconds(spin_us);
                while(std::chrono::steady_clock::now() < spin_end){}
                boost::this_fiber::yield();
            }
        }, Priority::Normal, Affinity::shared(), "backlog").detach();
    }

    timer->start(kTimerMs);
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
    void qt_loop_survives_coroutine_backlog();

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

/**
 * @brief 从 helper 输出里抓一个 KEY=数字 标记
 * @param out helper 的合并输出
 * @param key 标记名
 * @return 取到的数值；没抓到返回 -1
 */
static long long markValue(const QByteArray& out, const char* key)
{
    const QRegularExpression re(QStringLiteral("%1=(-?\\d+)").arg(QLatin1String(key)));
    const auto m = re.match(QString::fromUtf8(out));
    return m.hasMatch() ? m.captured(1).toLongLong() : -1;
}

/**
 * @brief 主线程背着协程积压时，Qt 事件循环仍须被分发（饥饿守卫回归）
 *
 * qt-loop 协程挂在 parkUntilIdle() 上，只有「本线程调度器空闲 → suspend_until()
 * 回调」时才会被交棒唤醒。而主线程的 pick_next() 会去偷全局队列里的 Shared 协程，
 * 只要还有协程排队，主线程就永远不空闲 —— Qt 于是一个事件都不分发，GUI 表现为
 * 无限期冻结。守卫的职责是把最坏分发间隔压回 ~100ms：卡顿可见，但窗口还能点、
 * 还能关。
 *
 * @note 这**不是** Task 5 引入的回归。把本用例原样编进 Task 5 之前的提交
 *       （6532132，钩子里还是 `Coro::msleep(1)` 固定时间片）实测：同样
 *       0 次分发、最大间隔 1501ms，与修复前完全一致。原因是 `msleep()` 的唤醒
 *       也要靠 dispatcher context，而它在积压下同样被饿死（见下面的 sleep 诊断量）。
 *       固定时间片提供的「至少每 1ms 分发一次」从来就不成立。
 *
 * 判据取两条：积压窗口内 50ms 定时器至少响了若干次，且两次分发的最大间隔有上界。
 * 二者缺一都抓不住回归——只看次数，一次长冻结加一串密集触发也能蒙混过关；
 * 只看间隔，一次都不响时反而没有间隔可测。
 */
void TestQuit::qt_loop_survives_coroutine_backlog()
{
    QByteArray out;
    int exitCode = 0;
    runMode(QStringLiteral("starve"), &out, &exitCode);

    QVERIFY2(exitCode != -1, qPrintable(QStringLiteral("starve 档挂死未退出\n%1")
                                        .arg(QString::fromUtf8(out))));
    QVERIFY2(exitCode == 0, qPrintable(QStringLiteral("starve 档退出码=%1（期望 0）\n%2")
                                       .arg(exitCode).arg(QString::fromUtf8(out))));

    const long long ticks   = markValue(out, "STARVE_TICKS_BACKLOG");
    const long long maxGap  = markValue(out, "STARVE_MAXGAP_MS");
    const long long sleepMs = markValue(out, "STARVE_SLEEP_MS");
    qInfo() << "积压窗口内 Qt 分发次数" << ticks
            << "最大分发间隔(ms)" << maxGap
            << "（诊断）主线程协程 msleep(200) 实测(ms)" << sleepMs;

    /// 守卫周期 100ms，积压窗口 1500ms：理论上够响 15 次。下限取 5 是给忙机器
    /// 留的余量——真出回归时这个数会是 0 或 1，离 5 远得很，不会擦边。
    QVERIFY2(ticks >= 5, qPrintable(QStringLiteral("积压期间 Qt 只分发了 %1 次，主循环被饿死\n%2")
                                    .arg(ticks).arg(QString::fromUtf8(out))));
    /// 上限取 500ms：守卫是 100ms 一次，留 5 倍余量给 WSL2 + ASan 的调度抖动。
    /// 冻结回归时这个数会是整个积压窗口的长度（~1500ms），同样不会擦边。
    QVERIFY2(maxGap >= 0 && maxGap <= 500,
             qPrintable(QStringLiteral("积压期间最大分发间隔 %1ms，超过 500ms 的可用性底线\n%2")
                        .arg(maxGap).arg(QString::fromUtf8(out))));
}

int main(int argc, char* argv[])
{
    const QLatin1String prefix("--helper=");
    if(argc > 1 && QLatin1String(argv[1]).startsWith(prefix)){
        const QString mode = QString::fromLatin1(argv[1]).mid(prefix.size());
        // starve 档测的是主循环不被饿死而不是退出语义，另起一套被测程序
        if(mode == QLatin1String("starve")){
            return runStarveHelper(argc, argv);
        }
        return runHelper(mode, argc, argv);
    }

    QCoreApplication app(argc, argv);
    TestQuit tc;
    return QTest::qExec(&tc, argc, argv);
}

#include "tst_testquit.moc"
