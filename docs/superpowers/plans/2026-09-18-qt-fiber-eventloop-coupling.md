# Qt 事件循环与 Fiber 调度器按需耦合 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 去掉 Qt 事件泵的 1ms 固定间隔，让 Qt fd 就绪到协程被唤醒之间不再有任何固定睡眠，同时空闲时线程真正睡在 `poll()` 里。

**Architecture:** 一个线程只能有一个阻塞点。本方案让 Qt 阻塞在 `poll()`，由 fiber 调度器通过 `suspend_until(tp)` 把「最近一个协程截止时刻」交棒给持有 Qt 循环的那个**协程**（工作线程是泵协程，主线程是 `Coro::exec()` 的 `qt-loop` 协程在 `aboutToBlock` 中）。交棒用 boost.fiber 的 `waker` 做，无锁、不切栈，因此在 dispatcher context 上执行安全。泵长时间阻塞会让跨线程投来的协程叫不醒，故另建一张限流的唤醒登记表。

**Tech Stack:** C++17、Qt 5.15.3（qmake）、boost.fiber 1.89（装在 `/usr/local`）、QtTest。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-09-18-qt-fiber-eventloop-coupling-design.md`，本计划不得偏离其第 2 节的目标与非目标。
- **不得修改 `/usr/local` 下的任何文件，不得重新编译或重新安装 boost。** boost 1.89 已就位，`AsyncTask.pri` 硬编码依赖它；曾有改动把该安装弄坏过。
- **跑任何测试前先 `unset all_proxy ALL_PROXY`。** 环境里的 SOCKS5 代理会让 Qt 的 bind 失败，导致 socket 用例假阳性失败。
- 不修复 `FiberGlobalQueue` 的亲和错配语义（协程迁移到别的线程时可能进不了对应线程的就绪队列）。本次只解决「线程叫不醒」。
- 不做 Qt 事件与协程的统一调度（自研 `QAbstractEventDispatcher`）。已评估，代价过大。
- 平台范围：Linux / Qt 5.15。不为 Windows、macOS 做适配。
- 注释语言跟随现有代码：中文 Doxygen 风格（`@brief` / `@code` / `@param` / `@return`）。
- 构建方式：qmake 影子构建。本计划统一用 `/tmp/atbuild/<名字>` 作为构建目录。
- 每个任务结束时提交，提交信息用中文，正文说明「为什么」而非「改了什么」。

## 构建与测试速查

```bash
# 构建并运行单个测试（以 testquit 为例）
unset all_proxy ALL_PROXY
mkdir -p /tmp/atbuild/testquit && cd /tmp/atbuild/testquit
qmake /home/david/zpj/Framework-dev/AsyncTask/test/testquit/testquit.pro
make -j8
./testquit
```

现有测试目录：`test/testProfile`、`test/test_scheduler`、`test/testexecutor`、`test/testfiberawait`、`test/testfibertask`、`test/testquit`。新测试必须加进 `test/TestFiber.pro` 的 `SUBDIRS`。

---

### Task 1: 时延回归断言（TDD 驱动）

先写下整个改造的验收标准。这个测试在当前代码上**必须失败**（往返 min ≈ 3ms），改造完成后必须通过。

**Files:**
- Create: `test/testlatency/testlatency.pro`
- Create: `test/testlatency/tst_testlatency.cpp`
- Modify: `test/TestFiber.pro`

**Interfaces:**
- Consumes: 现有公开 API `Coro::installFiberApplication()`、`Coro::makeTask()`、`Coro::exec()`、`Coro::quit()`、`Coro::coro()`、`Coro::await_for()`、`Coro::generate()`
- Produces: 可执行的 `testlatency`，后续每个任务都要重跑它

- [ ] **Step 1: 建测试工程文件**

创建 `test/testlatency/testlatency.pro`。注意**不开 ASan** —— 本测试测的是时延，ASan 会让数字失去意义（其他测试开 ASan 是为了查内存错误，目的不同）。

```pro
QT += testlib network
QT -= gui

CONFIG += qt console warn_on depend_includepath testcase
CONFIG -= app_bundle

TEMPLATE = app
QMAKE_CXXFLAGS += -std=c++17
# 本测试度量时延，刻意不开 sanitizer：插桩会让往返时间失去参考价值。
include($$PWD/../../AsyncTask.pri)

SOURCES += tst_testlatency.cpp
```

- [ ] **Step 2: 写失败的测试**

创建 `test/testlatency/tst_testlatency.cpp`：

```cpp
/**
 * @file tst_testlatency.cpp
 * @brief Qt 系 I/O 的往返时延回归断言。
 * @details 单进程内 TCP 回环请求-响应，度量往返最小值。阈值宽松，只为守住
 *          issue #6 修复后不再退回「每跳等一个固定事件分发间隔」的地板，
 *          不追求精确基准（精确基准见 example/latency_pingpong）。
 */
#include <QtTest>
#include <QCoreApplication>
#include <QElapsedTimer>
#include <QHostAddress>
#include <QNetworkProxy>
#include <QTcpServer>
#include <QTcpSocket>

#include <atomic>
#include <chrono>
#include <limits>

#include "await/coro.hpp"
#include "task/fiberapplication.h"
#include "task/fibertask.h"

using namespace std::chrono_literals;

class test_latency : public QObject
{
    Q_OBJECT
private slots:
    void initTestCase();
    void test_case_loopback_rtt();
};

void test_latency::initTestCase()
{
    // 环境里若有 SOCKS5 代理，Qt 的 bind 会失败，用例会以无关原因挂掉。
    QNetworkProxy::setApplicationProxy(QNetworkProxy::NoProxy);
}

void test_latency::test_case_loopback_rtt()
{
    Coro::installFiberApplication();

    QTcpServer server;
    QVERIFY2(server.listen(QHostAddress::LocalHost, 0),
             qPrintable(server.errorString()));
    const quint16 port = server.serverPort();

    constexpr int kSamples = 200;
    const QByteArray payload(16, 'x');

    std::atomic_llong minNs{std::numeric_limits<long long>::max()};
    std::atomic_int  completed{0};

    // 服务端：把收到的每一块原样回显。flush() 避免等写通知器多绕一轮事件循环。
    auto incoming = Coro::coro(&server).nextConnection();
    Coro::makeTask([incoming]{
        for(QTcpSocket* peer : Coro::generate(incoming)){
            for(const QByteArray& chunk : Coro::generate(Coro::coro(peer).readAll())){
                peer->write(chunk);
                peer->flush();
            }
        }
        return 0;
    });

    // 客户端：连上之后做 kSamples 次请求-响应，记录最小往返。
    Coro::makeTask([port, payload, &minNs, &completed]{
        QTcpSocket sock;
        sock.setProxy(QNetworkProxy::NoProxy);
        sock.connectToHost(QHostAddress::LocalHost, port);
        if(!Coro::await_for(Coro::coro(&sock).waitForConnected(), 2s)){
            qCritical() << "connect failed:" << sock.errorString();
            Coro::quit();
            return 1;
        }

        auto stream = Coro::coro(&sock).readAll();
        for(int i = 0; i < kSamples; ++i){
            QElapsedTimer timer;
            timer.start();
            sock.write(payload);
            sock.flush();
            auto echo = Coro::await_for(stream, 2s);
            if(!echo){
                qCritical() << "echo timeout at sample" << i;
                Coro::quit();
                return 1;
            }
            const long long ns = timer.nsecsElapsed();
            long long prev = minNs.load(std::memory_order_relaxed);
            while(ns < prev
                  && !minNs.compare_exchange_weak(prev, ns,
                                                  std::memory_order_relaxed)){}
            completed.fetch_add(1, std::memory_order_relaxed);
        }
        sock.disconnectFromHost();
        Coro::quit();
        return 0;
    });

    Coro::exec();

    QCOMPARE(completed.load(), kSamples);
    const double minUs = minNs.load() / 1000.0;
    qInfo() << "loopback RTT min =" << minUs << "us over" << kSamples << "samples";
    // 修复前约 3128us。阈值 1000us 宽松到不会被普通抖动触发，
    // 又足以在「固定分发间隔」回归时立刻报警。
    QVERIFY2(minUs < 1000.0,
             qPrintable(QString("往返 min = %1 us，超过 1000 us 的地板阈值")
                            .arg(minUs)));
}

QTEST_GUILESS_MAIN(test_latency)

#include "tst_testlatency.moc"
```

- [ ] **Step 3: 挂进 SUBDIRS**

修改 `test/TestFiber.pro`，在 `SUBDIRS` 末尾加一行：

```pro
TEMPLATE = subdirs

SUBDIRS += \
    testProfile \
    test_scheduler/testscheduler.pro \
    testexecutor \
    testfiberawait \
    testfibertask \
    testlatency \
    testquit
```

- [ ] **Step 4: 运行，确认它以「时延超标」失败**

```bash
unset all_proxy ALL_PROXY
mkdir -p /tmp/atbuild/testlatency && cd /tmp/atbuild/testlatency
qmake /home/david/zpj/Framework-dev/AsyncTask/test/testlatency/testlatency.pro && make -j8
./testlatency
```

预期：编译通过、连接与回显成功、`completed == 200`，最后 `QVERIFY2` 失败并打印
`往返 min = 3xxx us，超过 1000 us 的地板阈值`。

**若失败原因不是时延超标（连接失败、超时、崩溃），必须先修到「只剩时延这一条失败」再往下走** —— 否则后续任务无法用它判断是否修好。

- [ ] **Step 5: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add test/testlatency test/TestFiber.pro
git commit -m "test: 增加 Qt 系 I/O 往返时延的回归断言

当前必然失败（往返 min 约 3ms），作为 issue #6 改造的验收标准：
固定事件分发间隔一旦回归，这条断言立刻报警。"
```

---

### Task 2: 唤醒登记表与限流广播

泵一旦可以长时间阻塞在 `poll()` 里，跨线程投来的协程就必须有办法把线程叫醒。先把这套机制建在**与 Qt 无关的基类**上。

**Files:**
- Modify: `coro/executor/scheduler/fiberscheduler.h`
- Modify: `coro/executor/scheduler/fiberscheduler.cpp:46-62`（`awakened`）、`:30-41`（`signalExit` / `stopCurrentThreadPump`）
- Test: `test/test_scheduler/tst_testscheduler.cpp`

**Interfaces:**
- Produces（后续任务依赖这些精确签名）：
  - `using FiberScheduler::WakeFn = std::function<void()>;`
  - `static void FiberScheduler::registerWaker(void* key, WakeFn wake);`
  - `static void FiberScheduler::unregisterWaker(void* key);`
  - `static void FiberScheduler::wakeAllBlocked(void);`
  - `static void FiberScheduler::enterBlocked(void);`
  - `static void FiberScheduler::leaveBlocked(void);`
  - `static int  FiberScheduler::blockedCount(void);`（仅供测试断言）

- [ ] **Step 1: 写失败的测试**

在 `test/test_scheduler/tst_testscheduler.cpp` 的类里加一个 slot 声明 `void test_case_waker_registry();`，并加上实现：

```cpp
void test_scheduler::test_case_waker_registry()
{
    int calls = 0;
    int key = 0;
    Coro::FiberScheduler::registerWaker(&key, [&calls]{ ++calls; });

    // 没有线程睡在 poll 里时不得广播：避免惊群，这正是本机制的限流闸。
    QCOMPARE(Coro::FiberScheduler::blockedCount(), 0);
    Coro::FiberScheduler::wakeAllBlocked();
    QCOMPARE(calls, 0);

    // 有线程阻塞时才真正广播。
    Coro::FiberScheduler::enterBlocked();
    QCOMPARE(Coro::FiberScheduler::blockedCount(), 1);
    Coro::FiberScheduler::wakeAllBlocked();
    QCOMPARE(calls, 1);

    Coro::FiberScheduler::leaveBlocked();
    QCOMPARE(Coro::FiberScheduler::blockedCount(), 0);

    // 注销之后不得再被调用。
    Coro::FiberScheduler::enterBlocked();
    Coro::FiberScheduler::unregisterWaker(&key);
    Coro::FiberScheduler::wakeAllBlocked();
    QCOMPARE(calls, 1);
    Coro::FiberScheduler::leaveBlocked();
}
```

若该文件尚未包含调度器头文件，在顶部加 `#include "executor/scheduler/fiberscheduler.h"`。

- [ ] **Step 2: 运行，确认编译失败**

```bash
unset all_proxy ALL_PROXY
mkdir -p /tmp/atbuild/test_scheduler && cd /tmp/atbuild/test_scheduler
qmake /home/david/zpj/Framework-dev/AsyncTask/test/test_scheduler/testscheduler.pro && make -j8
```

预期：FAIL，`'registerWaker' is not a member of 'Coro::FiberScheduler'`。

- [ ] **Step 3: 在头文件中声明**

修改 `coro/executor/scheduler/fiberscheduler.h`。顶部补 include：

```cpp
#include <functional>
#include <unordered_map>
```

在 `// —— 调度器退出接口 ——` 区块之后、`protected://全局` 之前插入：

```cpp
    // —— 阻塞唤醒登记表 ——
    /// @brief 唤醒回调类型（Qt 实现注入 QAbstractEventDispatcher::wakeUp）
    using WakeFn = std::function<void()>;
    /**
     * @brief 登记本线程的唤醒回调
     * @code
     * // QtFiberScheduler 构造时注入 Qt 的线程安全唤醒函数
     * FiberScheduler::registerWaker(this, [disp]{ disp->wakeUp(); });
     * @endcode
     * @param key 登记键（用调度器实例地址），注销时用同一个键
     * @param wake 可跨线程调用的唤醒回调
     */
    static void registerWaker(void* key, WakeFn wake);
    /**
     * @brief 注销唤醒回调（调度器析构时调用）
     * @param key 登记时用的键
     */
    static void unregisterWaker(void* key);
    /**
     * @brief 叫醒所有阻塞中的线程
     * @details 仅在确有线程阻塞时才遍历登记表。无条件广播会造成惊群 ——
     *          每次协程就绪都叫醒全部工作线程，正是本次要消除的 CPU 浪费。
     */
    static void wakeAllBlocked(void);
    /** @brief 进入阻塞前调用（阻塞线程计数 +1） */
    static void enterBlocked(void);
    /** @brief 离开阻塞后调用（阻塞线程计数 -1） */
    static void leaveBlocked(void);
    /** @brief 当前阻塞中的线程数（供测试断言） */
    static int  blockedCount(void);
```

在 `protected:` 的静态成员区（`static thread_local std::atomic_bool t_stop_;` 之后）加：

```cpp
    static std::mutex                        waker_mtx_;   ///< 保护登记表
    static std::unordered_map<void*, WakeFn> wakers_;      ///< 各线程的唤醒回调
    static std::atomic_int                   s_blocked_count_;///< 阻塞中的线程数
```

- [ ] **Step 4: 实现**

修改 `coro/executor/scheduler/fiberscheduler.cpp`。在文件顶部的静态成员定义区（`thread_local std::atomic_bool FiberScheduler::t_stop_{ false };` 之后）加：

```cpp
std::mutex FiberScheduler::waker_mtx_{};
std::unordered_map<void*, FiberScheduler::WakeFn> FiberScheduler::wakers_{};
std::atomic_int FiberScheduler::s_blocked_count_{ 0 };
```

在 `stopCurrentThreadPump` 之后追加：

```cpp
/**
 * @brief 登记本线程的唤醒回调
 */
void FiberScheduler::registerWaker(void* key, WakeFn wake)
{
    std::lock_guard<std::mutex> guard(waker_mtx_);
    wakers_[key] = std::move(wake);
}

/**
 * @brief 注销唤醒回调
 */
void FiberScheduler::unregisterWaker(void* key)
{
    std::lock_guard<std::mutex> guard(waker_mtx_);
    wakers_.erase(key);
}

/**
 * @brief 叫醒所有阻塞中的线程（无人阻塞时直接返回，避免惊群）
 */
void FiberScheduler::wakeAllBlocked(void)
{
    if(s_blocked_count_.load(std::memory_order_acquire) <= 0){
        return;
    }
    std::lock_guard<std::mutex> guard(waker_mtx_);
    for(auto& entry : wakers_){
        entry.second();
    }
}

/**
 * @brief 进入阻塞前计数 +1
 */
void FiberScheduler::enterBlocked(void)
{
    s_blocked_count_.fetch_add(1, std::memory_order_release);
}

/**
 * @brief 离开阻塞后计数 -1
 */
void FiberScheduler::leaveBlocked(void)
{
    s_blocked_count_.fetch_sub(1, std::memory_order_release);
}

/**
 * @brief 当前阻塞中的线程数
 */
int FiberScheduler::blockedCount(void)
{
    return s_blocked_count_.load(std::memory_order_acquire);
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
cd /tmp/atbuild/test_scheduler && make -j8 && ./testscheduler
```

预期：`test_case_waker_registry` PASS，其余用例不受影响。

- [ ] **Step 6: 在 `awakened()` 里接上广播**

修改 `coro/executor/scheduler/fiberscheduler.cpp` 的 `awakened`（原 `:46-62`）。关键点：**锁必须收进作用域，广播在释放 `global_mtx` 之后**。持着全局锁回调进 Qt 是锁序风险。

```cpp
void Coro::FiberScheduler::awakened(boost::fibers::context *ctx, Coro::MetaContext &props) noexcept
{
    // fiber自身的事件，必须绑定至scheduler所在的线程，这里缓存至main_queue_
    if ( ctx->is_context( boost::fibers::type::pinned_context) ) {
        main_queue_.push(ctx);
        return;
    }
    // 根据协程的属性分配队列
    ctx->detach();
    bool need_wake{ false };
    {
        std::lock_guard<std::mutex> guard(global_mtx);
        // 目标就是本线程时无需叫人；此判断必须在锁内完成——一旦解锁，另一线程可能
        // 立刻从队列取走并跑完这个协程，props 引用的属性对象随之被释放，
        // 解锁后再访问 props 就是 use-after-free。
        need_wake = !(props.affinity() == Affinity::fixed(std::this_thread::get_id()));
        FiberGlobalQueue::instance()->emplace_back(props);
    }
    /// @details 广播必须在释放 global_mtx 之后，避免持全局锁回调进 Qt 造成锁序问题。
    if(need_wake){
        wakeAllBlocked();
    }
}
```

**⚠️ 此处曾是计划缺陷（Task 2 实施时发现并订正）**：原稿把 `props.affinity()` 读在
解锁之后。`emplace_back` 一旦发布该协程，另一线程可以立刻取走、跑完并释放其属性
对象，解锁后再读 `props` 就是 use-after-free（ASan 实证）。判断必须在锁内完成。

两处易踩的坑，已核实过：`MetaContext::affinity_` 是**私有**成员，只能走公开访问器
`affinity()`（`fiberproperty.h:329`）；`Affinity` 只定义了 `operator==`（`:104`），**没有
`operator!=`**，所以只能写成 `!(a == b)`。

- [ ] **Step 7: 让退出路径也广播**

同文件，改 `signalExit` 与 `stopCurrentThreadPump`：

```cpp
void FiberScheduler::signalExit(void)
{
    s_exit_.store(true, std::memory_order_release);
    wakeAllBlocked();   ///< 否则各线程会一直睡在 poll() 里，关机挂死
}

void FiberScheduler::stopCurrentThreadPump(void)
{
    t_stop_.store(true, std::memory_order_release);
    wakeAllBlocked();
}
```

- [ ] **Step 8: 跑全量测试确认无回归**

```bash
unset all_proxy ALL_PROXY
for t in test_scheduler testexecutor testfibertask testProfile testquit; do
  d=/tmp/atbuild/$t; mkdir -p $d && cd $d
  p=/home/david/zpj/Framework-dev/AsyncTask/test/$t
  [ -f $p/testscheduler.pro ] && pro=$p/testscheduler.pro || pro=$p/$t.pro
  qmake $pro >/dev/null && make -j8 >/dev/null 2>&1 && ./$(basename $pro .pro) > /tmp/atbuild/$t.log 2>&1
  echo "$t => $?"
done
```

预期：全部 `=> 0`。此时 `testlatency` 仍应失败（机制还没接上）。

- [ ] **Step 9: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/executor/scheduler/fiberscheduler.h coro/executor/scheduler/fiberscheduler.cpp test/test_scheduler
git commit -m "feat: 调度器增加限流的跨线程唤醒登记表

事件泵即将可以长时间阻塞在 poll() 里，跨线程投来的协程必须有办法把
目标线程叫醒，否则会被饿死。登记表存回调而非 Qt 类型，基类因此不必
依赖 Qt。只在确有线程阻塞、且目标不是本线程时才广播，避免惊群。"
```

---

### Task 3: 交棒原语 `parkUntilIdle()`

本任务是整个改造的核心。已用独立 spike 验证过 boost.fiber 的 `waker` 交棒可行（`suspend_until` 能拿到 sleeper 的截止时刻，在 dispatcher context 上 `wake()` 不切栈）。

**Files:**
- Modify: `coro/executor/scheduler/qtfiberscheduler.h`
- Modify: `coro/executor/scheduler/qtfiberscheduler.cpp`
- Modify: `coro/detail/asyncdefine.h`、`coro/detail/asyncdefine.cpp`
- Test: `test/test_scheduler/tst_testscheduler.cpp`

**Interfaces:**
- Consumes: Task 2 的 `registerWaker` / `unregisterWaker` / `enterBlocked` / `leaveBlocked`
- Produces（后续任务依赖）：
  - `static std::chrono::steady_clock::time_point QtFiberScheduler::parkUntilIdle(void);`
  - `void Coro::setMaxEventBlockMs(int ms);`
  - `int  Coro::maxEventBlockMs(void);`

- [ ] **Step 1: 写失败的测试**

在 `test/test_scheduler/tst_testscheduler.cpp` 加 slot 声明 `void test_case_max_event_block_ms();` 与实现：

```cpp
void test_scheduler::test_case_max_event_block_ms()
{
    QCOMPARE(Coro::maxEventBlockMs(), 10);       // 默认 10ms 保险丝
    Coro::setMaxEventBlockMs(25);
    QCOMPARE(Coro::maxEventBlockMs(), 25);
    Coro::setMaxEventBlockMs(0);                 // 非正值应被夹到 1
    QCOMPARE(Coro::maxEventBlockMs(), 1);
    Coro::setMaxEventBlockMs(10);                // 复原，免得影响别的用例
}
```

顶部需要 `#include "detail/asyncdefine.h"`（多半已包含）。

- [ ] **Step 2: 运行，确认编译失败**

```bash
cd /tmp/atbuild/test_scheduler && make -j8
```

预期：FAIL，`'maxEventBlockMs' is not a member of 'Coro'`。

- [ ] **Step 3: 加配置接口**

`coro/detail/asyncdefine.h` 中 `void msleep(unsigned long mescs);` 附近追加：

```cpp
/**
 * @brief 设置事件阻塞的安全上限（毫秒）
 * @details 这**不是**事件分发间隔 —— 分发已改为按需唤醒，没有间隔了。它是
 *          跨线程唤醒广播万一失效时的保险丝：让症状退化成「慢」而不是「死锁」。
 *          默认 10ms。传入非正值会被夹到 1。
 * @code
 * Coro::setMaxEventBlockMs(50);   // 更省电，但唤醒漏路径更难察觉
 * @endcode
 * @param ms 上限毫秒数
 */
void setMaxEventBlockMs(int ms);
/**
 * @brief 读取事件阻塞的安全上限（毫秒）
 * @return 当前上限
 */
int maxEventBlockMs(void);
```

`coro/detail/asyncdefine.cpp` 追加（顶部需 `#include <atomic>` 与 `#include <algorithm>`）：

```cpp
namespace {
std::atomic_int g_max_event_block_ms{ 10 };
}

/**
 * @brief 设置事件阻塞的安全上限
 */
void Coro::setMaxEventBlockMs(int ms){
    g_max_event_block_ms.store(std::max(1, ms), std::memory_order_release);
}

/**
 * @brief 读取事件阻塞的安全上限
 */
int Coro::maxEventBlockMs(void){
    return g_max_event_block_ms.load(std::memory_order_acquire);
}
```

- [ ] **Step 4: 运行，确认配置测试通过**

```bash
cd /tmp/atbuild/test_scheduler && make -j8 && ./testscheduler
```

预期：`test_case_max_event_block_ms` PASS。

- [ ] **Step 5: 声明交棒原语**

改 `coro/executor/scheduler/qtfiberscheduler.h`。补 include：

```cpp
#include <QTimer>
#include <QAbstractEventDispatcher>
#include <boost/fiber/waker.hpp>
#include <atomic>
#include <chrono>
```

在 `public:` 区，`suspend_until` 声明之后加：

```cpp
    /**
     * @brief 挂起调用协程，直到本线程再无就绪协程；返回可安全阻塞至的时刻
     * @details 由**持有 Qt 事件循环的那个协程**调用：工作线程是泵协程，主线程是
     *          Coro::exec() 的 qt-loop 协程在 aboutToBlock 中。挂起之后调度器才
     *          会回调 suspend_until，那里是唯一能拿到「最近一个协程截止时刻」的
     *          地方，它把该时刻交棒回来并唤醒本协程。
     * @code
     * auto tp = Coro::QtFiberScheduler::parkUntilIdle();
     * int ms = 可阻塞毫秒数(tp);
     * if(ms > 0) eventloop.processEvents(QEventLoop::AllEvents
     *                                  | QEventLoop::WaitForMoreEvents);
     * @endcode
     * @return 可安全阻塞至的时刻；无任何定时协程时为 time_point::max()
     */
    static std::chrono::steady_clock::time_point parkUntilIdle(void);

    /**
     * @brief 唤醒挂起的调度器线程，并戳破本线程可能正在进行的 poll()
     * @param 无
     */
    void notify(void) noexcept override;

    /**
     * @brief 解除本线程 Qt 持有者协程的挂起（交出 Qt 持有权前调用）
     * @details Coro::exec() 会先 stopCurrentThreadPump() 停掉泵协程、再让
     *          qt-loop 协程接手。若此刻泵正挂在 parkUntilIdle() 上，不先把它放
     *          出来，Qt 持有者这个唯一席位就会被一个永远不会醒的协程占死。
     */
    static void unparkLocal(void);
```

`protected:` 区把 `int pump_interval_ms_{ 1 };` **删掉**，换成：

```cpp
    static thread_local QtFiberScheduler* t_self_;///< 本线程的调度器实例
    boost::fibers::waker  qt_waker_{};            ///< 挂起中的 Qt 持有者协程
    std::atomic_bool      parked_{ false };       ///< Qt 持有者是否正挂起
    std::chrono::steady_clock::time_point next_deadline_{};///< 交棒过来的截止时刻
    QAbstractEventDispatcher* disp_{ nullptr };   ///< 本线程的事件分发器
    QTimer                deadline_timer_{};      ///< 单次定时器，用于打断 poll()
```

- [ ] **Step 6: 实现交棒**

改 `coro/executor/scheduler/qtfiberscheduler.cpp`。补 include：

```cpp
#include <boost/fiber/context.hpp>
#include "detail/asyncdefine.h"
```

文件顶部加静态成员定义：

```cpp
thread_local Coro::QtFiberScheduler* Coro::QtFiberScheduler::t_self_{ nullptr };
```

构造/析构改为：

```cpp
Coro::QtFiberScheduler::QtFiberScheduler(void):FiberScheduler()
{
    t_self_ = this;
    deadline_timer_.setSingleShot(true);
    /// @details 事件分发器必须在 eventloop 成员构造之后取：QEventLoop 的构造会
    /// 为本线程 ensureEventDispatcher()，在那之前 instance() 可能是空。
    disp_ = QAbstractEventDispatcher::instance();
    if(disp_ != nullptr){
        QAbstractEventDispatcher* disp = disp_;
        // wakeUp() 是 Qt 明确保证线程安全的少数函数之一。
        FiberScheduler::registerWaker(this, [disp]{ disp->wakeUp(); });
    }
}

Coro::QtFiberScheduler::~QtFiberScheduler(void)
{
    FiberScheduler::unregisterWaker(this);
    t_self_ = nullptr;
}
```

追加交棒原语：

```cpp
/**
 * @brief 挂起调用协程，返回可安全阻塞至的时刻
 */
std::chrono::steady_clock::time_point Coro::QtFiberScheduler::parkUntilIdle(void)
{
    QtFiberScheduler* self = t_self_;
    if(self == nullptr){
        return std::chrono::steady_clock::now();
    }
    /// @details Qt 持有者只有一席。撞上说明有两个协程都想当持有者，属实现错误。
    Q_ASSERT(!self->parked_.load(std::memory_order_acquire));

    boost::fibers::context* ctx = boost::fibers::context::active();
    self->qt_waker_ = ctx->create_waker();
    self->next_deadline_ = std::chrono::steady_clock::now();
    self->parked_.store(true, std::memory_order_release);
    /// @details store 与 suspend 之间没有让出点（同线程、无抢占），而 suspend_until
    /// 只在本协程挂起之后才拿得到控制权，故不会丢唤醒。
    ctx->suspend();
    return self->next_deadline_;
}

/**
 * @brief 解除本线程 Qt 持有者协程的挂起
 */
void Coro::QtFiberScheduler::unparkLocal(void)
{
    QtFiberScheduler* self = t_self_;
    if(self == nullptr){
        return;
    }
    if(self->parked_.exchange(false, std::memory_order_acq_rel)){
        self->next_deadline_ = std::chrono::steady_clock::now();
        self->qt_waker_.wake();
    }
}

/**
 * @brief 唤醒挂起的调度器线程，并戳破本线程可能正在进行的 poll()
 */
void Coro::QtFiberScheduler::notify(void) noexcept
{
    FiberScheduler::notify();
    if(disp_ != nullptr){
        disp_->wakeUp();
    }
}
```

`suspend_until` 改为交棒（泵循环下一个任务再改，本任务只动交棒部分）：

```cpp
void Coro::QtFiberScheduler::suspend_until(const std::chrono::steady_clock::time_point &time_point) noexcept
{
    /// @details 有 Qt 持有者挂在 parkUntilIdle() 上时，本回调是唯一知道「最近一个
    /// 协程截止时刻」的地方。交棒给它、立即返回，真正的阻塞发生在它自己的协程栈
    /// 上 —— 那里执行 Qt 槽才是合法的；本回调跑在 dispatcher context，切栈会崩。
    if(parked_.exchange(false, std::memory_order_acq_rel)){
        next_deadline_ = time_point;
        qt_waker_.wake();       // 只入队，不切栈
        return;
    }

    bool startedPump = false;
    std::call_once(pump_once_, [this, &startedPump]{
        if(FiberScheduler::s_exit_.load(std::memory_order_acquire)
           || FiberScheduler::t_stop_.load(std::memory_order_acquire)){
            return;             // 已在退出，不必再起泵
        }
        startedPump = true;
        boost::fibers::fiber(launch_properties([this]{
            pumpLoop();
        }, Priority::High, Affinity::fixed(std::this_thread::get_id()))).detach();
    });
    if(startedPump) return;
    FiberScheduler::suspend_until(time_point);
}
```

在 `qtfiberscheduler.h` 的 `protected:` 加 `void pumpLoop(void);` 声明，`.cpp` 里先给出与现状等价的实现（下一个任务重写）：

```cpp
/**
 * @brief 常驻事件泵协程主体
 */
void Coro::QtFiberScheduler::pumpLoop(void)
{
    while (!FiberScheduler::s_exit_.load(std::memory_order_acquire)
           && !FiberScheduler::t_stop_.load(std::memory_order_acquire)) {
        eventloop.processEvents(QEventLoop::AllEvents);
        Coro::msleep(1);
    }
}
```

- [ ] **Step 7: 让 `stopCurrentThreadPump` 放出挂起的持有者**

改 `coro/executor/scheduler/fiberscheduler.cpp` 的 `stopCurrentThreadPump`，加一个虚钩子避免基类依赖 Qt。在 `fiberscheduler.h` 的 protected 静态区加：

```cpp
    /// @brief 交出 Qt 持有权前解除本线程挂起协程的钩子（Qt 实现注入）
    static std::function<void()> local_unpark_hook_;
```

`fiberscheduler.cpp` 定义 `std::function<void()> FiberScheduler::local_unpark_hook_{};`，并改：

```cpp
void FiberScheduler::stopCurrentThreadPump(void)
{
    t_stop_.store(true, std::memory_order_release);
    if(local_unpark_hook_) local_unpark_hook_();   ///< 放出挂在 park 上的泵协程
    wakeAllBlocked();
}
```

在 `QtFiberScheduler` 构造函数末尾注入：

```cpp
    FiberScheduler::local_unpark_hook_ = []{ QtFiberScheduler::unparkLocal(); };
```

- [ ] **Step 8: 构建并跑全量测试**

```bash
unset all_proxy ALL_PROXY
for t in test_scheduler testexecutor testfibertask testProfile testfiberawait testquit; do
  d=/tmp/atbuild/$t; mkdir -p $d && cd $d
  p=/home/david/zpj/Framework-dev/AsyncTask/test/$t
  [ -f $p/testscheduler.pro ] && pro=$p/testscheduler.pro || pro=$p/$t.pro
  qmake $pro >/dev/null && make -j8 >/dev/null 2>&1 && ./$(basename $pro .pro) > /tmp/atbuild/$t.log 2>&1
  echo "$t => $?"
done
```

预期：全部 `=> 0`。本任务只是把机制铺好、行为仍与改造前等价，`testlatency` 仍失败。

- [ ] **Step 9: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/executor/scheduler/qtfiberscheduler.h coro/executor/scheduler/qtfiberscheduler.cpp \
        coro/executor/scheduler/fiberscheduler.h coro/executor/scheduler/fiberscheduler.cpp \
        coro/detail/asyncdefine.h coro/detail/asyncdefine.cpp test/test_scheduler
git commit -m "feat: suspend_until 改为把最近截止时刻交棒给 Qt 持有者协程

最近一个协程的截止时刻只存在于 boost.fiber 的 sleep_queue 里，唯一出口
是 suspend_until 回调；但该回调跑在 dispatcher context，在那里执行会挂起
的用户代码会切栈崩溃。改为只交棒不睡觉：真正的阻塞留给持有者协程自己的
栈，Qt 槽因此仍可 await。用 waker 而非 fiber 条件变量，后者的 notify_one
要拿 fiber mutex，理论上能在 dispatcher context 里挂起调用者。

本次只铺机制，泵循环行为未变。"
```

---

### Task 4: 工作线程泵循环改为按需阻塞

**Files:**
- Modify: `coro/executor/scheduler/qtfiberscheduler.cpp`（`pumpLoop`）

**Interfaces:**
- Consumes: Task 3 的 `parkUntilIdle()`、`Coro::maxEventBlockMs()`；Task 2 的 `enterBlocked()` / `leaveBlocked()`

- [ ] **Step 1: 重写泵循环**

把 Task 3 里那个过渡版 `pumpLoop` 替换为：

```cpp
/**
 * @brief 常驻事件泵协程主体：排空 → 交出线程 → 按需阻塞在 poll()
 * @details 不再有固定分发间隔。无就绪协程时阻塞在 Qt 的 poll() 里，fd 就绪即刻
 *          返回；空闲时线程真正休眠而非轮询。阻塞上限由 Coro::maxEventBlockMs()
 *          兜底，防唤醒广播万一失效时死锁。
 */
void Coro::QtFiberScheduler::pumpLoop(void)
{
    auto stopping = [this]{
        return FiberScheduler::s_exit_.load(std::memory_order_acquire)
            || FiberScheduler::t_stop_.load(std::memory_order_acquire);
    };

    while(!stopping()){
        eventloop.processEvents(QEventLoop::AllEvents);   // 槽在本协程栈上跑
        if(stopping()) break;

        const auto tp = parkUntilIdle();                  // 交出线程，等交棒
        if(stopping()) break;

        /// @details 顺序不可颠倒：必须**先**计数、**后**复查就绪队列。若反过来，
        /// 复查到入队之间有个窗口 —— 别的线程此刻投递协程并调 wakeAllBlocked()，
        /// 采样到 s_blocked_count_ 为 0 便跳过广播，这次唤醒就丢了，只能靠
        /// maxEventBlockMs 兜底（症状是偶尔慢 10ms，极难定位）。先计数则该窗口内
        /// 的广播一定会打到我们身上：Qt 的 wakeUp() 是粘性的，随后的 poll() 立即返回。
        FiberScheduler::enterBlocked();

        /// @details 交棒之后、本协程真正被调度之前，可能又有协程被远端投递进来。
        /// 此时不能再去睡，否则手上有活却阻塞在 poll() 里。
        if(has_ready_fibers() || stopping()){
            FiberScheduler::leaveBlocked();
            continue;
        }

        const auto now = std::chrono::steady_clock::now();
        long long ms = 0;
        if(tp > now){
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp - now).count();
        }
        ms = std::min<long long>(ms, Coro::maxEventBlockMs());
        if(ms <= 0){
            FiberScheduler::leaveBlocked();
            continue;
        }

        deadline_timer_.start(static_cast<int>(ms));
        eventloop.processEvents(QEventLoop::AllEvents | QEventLoop::WaitForMoreEvents);
        FiberScheduler::leaveBlocked();
        deadline_timer_.stop();
    }
}
```

顶部若缺 `#include <algorithm>` 则补上。

**每一条 `continue` 路径都必须配对 `leaveBlocked()`** —— 漏掉一条，计数就会单调上涨，
`wakeAllBlocked()` 的限流闸从此永远打开，退化成无条件惊群广播。

**⚠️ 此处曾是计划缺陷（Task 2 评审提出，Task 4 派发前订正）**：原稿是先
`has_ready_fibers()` 复查、后 `enterBlocked()`，存在上述丢唤醒窗口。

- [ ] **Step 2: 跑时延测试，确认工作线程路径已改善**

```bash
unset all_proxy ALL_PROXY
cd /tmp/atbuild/testlatency && make -j8 && ./testlatency
```

预期：`loopback RTT min` 相比 Task 1 记录的约 3128us 有明显下降。**本步骤不要求断言通过** —— `testlatency` 走 `Coro::exec()`，socket 多半落在主线程，那条路径要到 Task 5 才改。记录本次数字用于对照。

- [ ] **Step 3: 跑全量测试**

```bash
unset all_proxy ALL_PROXY
for t in test_scheduler testexecutor testfibertask testProfile testfiberawait testquit; do
  d=/tmp/atbuild/$t; mkdir -p $d && cd $d
  p=/home/david/zpj/Framework-dev/AsyncTask/test/$t
  [ -f $p/testscheduler.pro ] && pro=$p/testscheduler.pro || pro=$p/$t.pro
  qmake $pro >/dev/null && make -j8 >/dev/null 2>&1 && timeout 300 ./$(basename $pro .pro) > /tmp/atbuild/$t.log 2>&1
  echo "$t => $?"
done
```

预期：全部 `=> 0`。**特别留意 `testquit`** —— 阻塞语义变了，关机挂死会表现为 `timeout` 返回 124。若挂死，先查唤醒路径是否有遗漏（Task 2 Step 7 的三条广播）。

- [ ] **Step 4: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/executor/scheduler/qtfiberscheduler.cpp
git commit -m "feat: 工作线程事件泵改为按需阻塞，去掉 1ms 固定间隔

原来每轮 processEvents 后固定 msleep(1)，给一切经 Qt 分发的 I/O 压出一条
每跳 1ms 的硬地板。改为无就绪协程时直接阻塞在 Qt 的 poll() 上，fd 就绪即
返回，空闲时线程真正休眠。阻塞上限由 maxEventBlockMs() 兜底。"
```

---

### Task 5: 主线程 `aboutToBlock` 改为按需阻塞

`Coro::exec()` 不能改用泵协程 —— `QCoreApplication::exec()` 是 GUI 关窗退出的前提（见 `fiberapplication.cpp:47-59` 的注释）。但跑 `exec()` 的 `qt-loop` 协程在 `aboutToBlock` 槽里就是「Qt 持有者」，用同一个交棒原语。

**Files:**
- Modify: `coro/task/fiberapplication.h:79-88`
- Modify: `coro/task/fiberapplication.cpp:62-107`

**Interfaces:**
- Consumes: Task 3 的 `QtFiberScheduler::parkUntilIdle()`、`Coro::maxEventBlockMs()`；Task 2 的 `enterBlocked()` / `leaveBlocked()`

- [ ] **Step 1: 头文件换成员**

改 `coro/task/fiberapplication.h`。补 `#include <QTimer>`。把

```cpp
    /// 单次 aboutToBlock 内最多排空的事件轮数（Qt 与协程的线程时间分配旋钮）
    static constexpr int kEventDrainBudget = 64;
    /// 每轮让给 boost.fiber 的时间片（ms），等同原泵协程的 msleep(1)
    static constexpr int kFiberSliceMs = 1;
```

**整段删除**，在 `bool quit_requested_{false};` 之后加：

```cpp
    QTimer deadline_timer_{};///< 单次定时器，给随后的 poll() 设上限
```

两个常量失去意义：`parkUntilIdle()` 天然实现了「协程要跑多久就跑多久，跑完才让 Qt 阻塞」，不再需要靠预算和时间片拍脑袋分配。

- [ ] **Step 2: 改写 `aboutToBlock` 钩子**

改 `coro/task/fiberapplication.cpp`。补 include：

```cpp
#include <QTimer>
#include <algorithm>
#include <chrono>
```

把 `exec()` 里的连接（原 `:74-84`）替换为：

```cpp
    deadline_timer_.setSingleShot(true);
    block_conn_ = QObject::connect(disp, &QAbstractEventDispatcher::aboutToBlock,
                                   this, [this, disp]{
        /// @details 这里跑在 qt-loop 协程栈上。挂起自己，把线程交给 boost.fiber；
        /// 调度器无事可做时会通过 suspend_until 把最近的协程截止时刻交棒回来。
        const auto tp = Coro::QtFiberScheduler::parkUntilIdle();

        const auto now = std::chrono::steady_clock::now();
        long long ms = 0;
        if(tp > now){
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp - now).count();
        }
        ms = std::min<long long>(ms, Coro::maxEventBlockMs());
        if(ms > 0){
            deadline_timer_.start(static_cast<int>(ms));  // 让随后的 poll() 至多睡 ms
        }else{
            disp->wakeUp();                               // 立刻回来继续跑协程
        }
    });
```

同时更新 `exec()` 上方的 Doxygen 注释：把「这段等价于原泵协程的 processEvents(AllEvents)+msleep(1)，只是搬到了 Qt 的阻塞点上」改成：

```
 * 代价是 exec() 会把线程闷在 poll() 里。aboutToBlock 钩子负责在 Qt 每次准备
 * 阻塞前把线程还给 boost.fiber：挂起 qt-loop 协程，等调度器把最近的协程截止
 * 时刻交棒回来，据此给随后的 poll() 设一个上限。没有固定时间片 —— 协程要跑
 * 多久就跑多久，跑完 Qt 才阻塞，且 fd 就绪即刻返回。
```

- [ ] **Step 3: 阻塞计数**

`aboutToBlock` 返回后 Qt 才真正 poll，钩子内部没法包住阻塞区间。改用 `awake` 信号配对。在 `exec()` 里 `block_conn_` 之后再连一个：

```cpp
    // aboutToBlock 之后 Qt 才真正 poll，awake 则在 poll 返回后发出。
    // 用这一对信号把「本线程睡在 poll 里」的区间标出来，供唤醒广播限流。
    awake_conn_ = QObject::connect(disp, &QAbstractEventDispatcher::awake, this, []{
        Coro::FiberScheduler::leaveBlocked();
    });
```

并在上一 Step 的 `aboutToBlock` lambda 末尾（设完定时器之后）加 `Coro::FiberScheduler::enterBlocked();`。

`fiberapplication.h` 加成员 `QMetaObject::Connection awake_conn_;///< awake 钩子，收尾时断开`，`shutdown()` 里 `QObject::disconnect(block_conn_);` 之后加 `QObject::disconnect(awake_conn_);`。

> 注意：Qt 的 `awake` 在 `processEvents` 开头也会发出（不止 poll 返回后），因此计数可能短暂为负。`wakeAllBlocked()` 用 `<= 0` 判断，负值只会让广播被跳过一次，不影响正确性 —— 有 `maxEventBlockMs` 兜底。若实现中发现计数漂移影响判断，改为在 `enterBlocked()` 前先检查是否已计数（用一个 `thread_local bool`）。

- [ ] **Step 4: 跑时延测试，确认断言通过**

```bash
unset all_proxy ALL_PROXY
cd /tmp/atbuild/testlatency && make -j8 && ./testlatency
```

预期：**PASS**，`loopback RTT min` 打印一个远低于 1000us 的数字（参考量级：几十到几百 us）。

这是整个改造的验收点。若仍不过，按以下顺序排查：
1. `readAll()` 的写路径是否真的 `flush()` 了（测试里已有）
2. `maxEventBlockMs` 是否被误当成分发间隔使用
3. 唤醒协程是否被弹进全局队列后由别的线程捞起（属 spec 第 2 节的非目标，若确认是这条，记录数字并在 Step 6 的提交信息里说明，不在本次修复）

- [ ] **Step 5: 跑全量测试 + GUI 手工验证**

```bash
unset all_proxy ALL_PROXY
for t in test_scheduler testexecutor testfibertask testProfile testfiberawait testquit testlatency; do
  d=/tmp/atbuild/$t; mkdir -p $d && cd $d
  p=/home/david/zpj/Framework-dev/AsyncTask/test/$t
  [ -f $p/testscheduler.pro ] && pro=$p/testscheduler.pro || pro=$p/$t.pro
  qmake $pro >/dev/null && make -j8 >/dev/null 2>&1 && timeout 300 ./$(basename $pro .pro) > /tmp/atbuild/$t.log 2>&1
  echo "$t => $?"
done
```

预期：全部 `=> 0`。

示例程序（每个都必须自行退出，不得挂死）：

```bash
unset all_proxy ALL_PROXY
for e in basic generator socket_pingpong signal_await thread_init; do
  d=/tmp/atbuild/ex_$e; mkdir -p $d && cd $d
  qmake /home/david/zpj/Framework-dev/AsyncTask/example/$e/*.pro >/dev/null && make -j8 >/dev/null 2>&1
  timeout 60 ./$(ls -t | grep -v '\.' | head -1) >/dev/null 2>&1; echo "$e => $?"
done
```

`example/gui_quit` 需要图形环境，由人工在有显示的环境下运行并**关窗**，确认进程退出码为 0。若当前环境无显示，在提交信息里注明该项未验证。

- [ ] **Step 6: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add coro/task/fiberapplication.h coro/task/fiberapplication.cpp
git commit -m "feat: 主线程 aboutToBlock 改为按需阻塞，删掉两个启发式常量

kFiberSliceMs(1ms) 只是把泵的固定间隔换了个地方，地板没变；
kEventDrainBudget(64) 是靠预算硬分配 Qt 与协程的线程时间。改用交棒之后
两者都失去意义：协程要跑多久就跑多久，跑完 Qt 才阻塞，fd 就绪即刻返回。

至此 issue #6 的往返时延回归断言通过。"
```

---

### Task 6: 基准示例与文档收尾

**Files:**
- Create: `example/latency_pingpong/latency_pingpong.pro`
- Create: `example/latency_pingpong/main.cpp`
- Modify: `coro/executor/scheduler/qtfiberscheduler.h`（类注释）
- Modify: `ReadMe.md`

**Interfaces:**
- Consumes: 全部已完成的改造

- [ ] **Step 1: 建基准工程**

创建 `example/latency_pingpong/latency_pingpong.pro`：

```pro
QT += core network
QT -= gui

CONFIG += console c++17
CONFIG -= app_bundle

TEMPLATE = app

QMAKE_CXXFLAGS += -std=c++17

include($$PWD/../../AsyncTask.pri)

SOURCES += main.cpp
```

- [ ] **Step 2: 写基准程序**

创建 `example/latency_pingpong/main.cpp`：

```cpp
/**
 * @file main.cpp
 * @brief TCP 回环请求-响应的时延基准。
 * @details 单进程内起服务端与客户端，度量往返时延的 min / mean / stdev，并打印
 *          进程 CPU 时间。口径对齐 issue #6 中 Transport 的基准工具，用于核对
 *          「每跳固定分发间隔」是否已消除。与 test/testlatency 的区别：那个是
 *          带阈值的回归断言，这个是给人看数字的。
 */
#include <QCoreApplication>
#include <QDebug>
#include <QElapsedTimer>
#include <QHostAddress>
#include <QNetworkProxy>
#include <QTcpServer>
#include <QTcpSocket>

#include <algorithm>
#include <cmath>
#include <vector>

#include <sys/resource.h>

#include "await/coro.hpp"
#include "task/fiberapplication.h"
#include "task/fibertask.h"

using namespace Coro;
using namespace std::chrono_literals;

namespace {
constexpr int kSamples  = 300;
constexpr int kPayload  = 16;

/// @brief 打印一组往返时延的统计量（单位微秒）
void report(std::vector<double>& us)
{
    if(us.empty()){
        qCritical() << "no samples";
        return;
    }
    std::sort(us.begin(), us.end());
    double sum = 0.0;
    for(double v : us) sum += v;
    const double mean = sum / us.size();
    double sq = 0.0;
    for(double v : us) sq += (v - mean) * (v - mean);
    const double stdev = std::sqrt(sq / us.size());

    rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    const double cpu = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6
                     + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;

    qInfo().nospace() << "samples=" << (int)us.size()
                      << " payload=" << kPayload << "B";
    qInfo().nospace() << "RTT min=" << us.front() << "us"
                      << " mean=" << mean << "us"
                      << " p50=" << us[us.size() / 2] << "us"
                      << " max=" << us.back() << "us"
                      << " stdev=" << stdev;
    qInfo().nospace() << "CPU total=" << cpu << "s";
}
} // namespace

int main(int argc, char* argv[])
{
    QCoreApplication app(argc, argv);
    QNetworkProxy::setApplicationProxy(QNetworkProxy::NoProxy);
    installFiberApplication();

    QTcpServer server;
    if(!server.listen(QHostAddress::LocalHost, 0)){
        qCritical() << "listen failed:" << server.errorString();
        return 1;
    }
    const quint16 port = server.serverPort();

    auto incoming = coro(&server).nextConnection();
    makeTask([incoming]{
        for(QTcpSocket* peer : generate(incoming)){
            for(const QByteArray& chunk : generate(coro(peer).readAll())){
                peer->write(chunk);
                peer->flush();
            }
        }
        return 0;
    });

    makeTask([port]{
        QTcpSocket sock;
        sock.setProxy(QNetworkProxy::NoProxy);
        sock.connectToHost(QHostAddress::LocalHost, port);
        if(!await_for(coro(&sock).waitForConnected(), 2s)){
            qCritical() << "connect failed:" << sock.errorString();
            quit();
            return 1;
        }

        const QByteArray payload(kPayload, 'x');
        auto stream = coro(&sock).readAll();
        std::vector<double> us;
        us.reserve(kSamples);
        for(int i = 0; i < kSamples; ++i){
            QElapsedTimer timer;
            timer.start();
            sock.write(payload);
            sock.flush();
            if(!await_for(stream, 2s)){
                qCritical() << "echo timeout at sample" << i;
                quit();
                return 1;
            }
            us.push_back(timer.nsecsElapsed() / 1000.0);
        }
        report(us);
        sock.disconnectFromHost();
        quit();
        return 0;
    });

    return exec();
}
```

- [ ] **Step 3: 构建并运行基准**

```bash
unset all_proxy ALL_PROXY
mkdir -p /tmp/atbuild/ex_latency && cd /tmp/atbuild/ex_latency
qmake /home/david/zpj/Framework-dev/AsyncTask/example/latency_pingpong/latency_pingpong.pro
make -j8 && timeout 120 ./latency_pingpong; echo "EXIT=$?"
```

预期：`EXIT=0`，打印 min / mean / p50 / max / stdev 与 CPU 总时间。把这组数字记下来，Step 5 的提交信息要用。

- [ ] **Step 4: 更新文档**

改 `coro/executor/scheduler/qtfiberscheduler.h` 的类注释。原文说「首次 suspend_until 用 std::call_once 创建一个绑定当前线程的常驻"事件协程"持续分发 Qt 事件」，改为：

```cpp
/**
 * @brief 支持 Qt 事件循环的调度器。
 *
 * 首次 suspend_until 用 std::call_once 创建一个绑定当前线程的常驻"事件泵协程"。
 * 泵协程排空 Qt 事件后调 parkUntilIdle() 交出线程；调度器无就绪协程时通过
 * suspend_until 把最近的协程截止时刻交棒回来，泵据此阻塞在 Qt 的 poll() 上。
 * 没有固定分发间隔：fd 就绪即刻返回，空闲时线程真正休眠。阻塞上限由
 * Coro::maxEventBlockMs() 兜底，防跨线程唤醒广播失效时死锁。
 *
 * Qt 槽运行在泵协程的栈上，因此槽内可以 Coro::await()。
 *
 * 退出流程：Coro::quit() 调 signalExit() 设全局退出标志并广播唤醒，各线程的泵
 * 协程醒来后自行退出，从而 ~scheduler 无需等待无限协程即可干净析构。
 * @code
 * // 工作线程上安装本调度器：既能调度协程，也能分发 Qt 事件
 * boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
 * @endcode
 */
```

改 `ReadMe.md`：现有小节是 `### 3.4 Socket Awaitable` 与 `### 3.5 常见问题与排除方法`
（`ReadMe.md:124` 与 `:173`）。把 FAQ 那节的标题改成 `### 3.6 常见问题与排除方法`，
并在它之前插入新的一节：

```markdown
### 3.5 事件分发与时延

Qt 事件分发与协程调度按需耦合，没有固定的轮询间隔：无就绪协程时线程阻塞在 Qt 的
`poll()` 上，fd 就绪即刻返回；空闲时线程真正休眠而非轮询。

`Coro::setMaxEventBlockMs(int)` 设置阻塞上限（默认 10ms）。它**不是**事件分发间隔
——那个已经不存在了——而是跨线程唤醒广播万一失效时的保险丝。绝大多数场景无需调整。

回环时延可用 `example/latency_pingpong` 实测；`test/testlatency` 是对应的回归断言。
```

- [ ] **Step 5: 提交**

```bash
cd /home/david/zpj/Framework-dev/AsyncTask
git add example/latency_pingpong coro/executor/scheduler/qtfiberscheduler.h ReadMe.md
git commit -m "docs: 增加回环时延基准示例，更新调度器文档

基准与 issue #6 中 Transport 工具口径一致，便于核对每跳固定分发间隔已消除。
调度器类注释原来描述的是「持续分发 + 固定间隔」，已与实现不符。"
```

- [ ] **Step 6: 全量回归终检**

```bash
unset all_proxy ALL_PROXY
for t in test_scheduler testexecutor testfibertask testProfile testfiberawait testquit testlatency; do
  d=/tmp/atbuild/$t; mkdir -p $d && cd $d
  p=/home/david/zpj/Framework-dev/AsyncTask/test/$t
  [ -f $p/testscheduler.pro ] && pro=$p/testscheduler.pro || pro=$p/$t.pro
  qmake $pro >/dev/null && make -j8 >/dev/null 2>&1 && timeout 300 ./$(basename $pro .pro) > /tmp/atbuild/$t.log 2>&1
  echo "$t => $?"
done
grep -rn "pump_interval_ms_\|kFiberSliceMs\|kEventDrainBudget" coro/ || echo "旧旋钮已清除"
```

预期：全部 `=> 0`，且 `旧旋钮已清除`。

---

## 完成后

在 issue #6 下回复：说明 `pump_interval_ms_` 不是变可配而是**消失**（优先级 1 被优先级 2 吸收），附上 `example/latency_pingpong` 改造前后的数字，并说明 spec 第 2 节列为非目标的两项（全局队列亲和错配、Qt/协程统一调度）未在本次处理及其原因。
