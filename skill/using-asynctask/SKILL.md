---
name: using-asynctask
description: 当编写、生成或修改使用 AsyncTask 协程框架（命名空间 `Coro`，基于 boost.fiber）的 C++ 代码时使用 —— 涉及 makeTask/then/get 任务链、用 coro()/await()/generate() 等待 Qt 信号/socket/QIODevice/future、Awaitable/Generator/Result，或 installFiberApplication/exec/quit 生命周期。遇到 AsyncTask 常见问题（主线程协程不执行、进程无法退出、deleteLater 延迟、运行时改线程亲和无效）时也适用。
---

# AsyncTask（Coro）使用指南

## 概述

AsyncTask 是基于 boost.fiber 的**有栈协程**框架，面向 Qt/C++17。等待中的协程会**让出线程而非阻塞线程**，因此可以用同步风格的顺序代码书写异步逻辑，同时让大量协程在少量线程上并发运行。

三条核心规则（违反其一是绝大多数 bug 的根源）：

1. **用 `Coro::exec()` 驱动主循环，不要用 `QCoreApplication::exec()`。** 协程调度器即主循环，Qt 事件由每个安装 `QtFiberScheduler` 的线程上的**常驻泵协程**（首次空闲时懒启动、在 worker 上下文持续 `processEvents`）分发；二者在同一线程互斥。
2. **用 `Coro::quit()` 退出** —— 设置全局退出标志 → 各线程泵协程自行终止 → 唤醒挂起协程 → 排空在途任务 → 退出。**`block.wait()` 返回后会自动停止本线程的泵协程**，因此 `QtFiberThread` 等单独退出也安全。不调用 `quit()` 会导致退出时卡死或崩溃。
3. **所有 API 都在命名空间 `Coro` 下。** 先 `using namespace Coro;`。

## 何时使用

- 编写任何调用 `makeTask`、`coro`、`await`、`generate`、`Coro::exec/quit`、`Awaitable`、`Generator`、`Result` 的代码。
- 需要把 Qt 信号 / socket / `QIODevice` / `QFuture` / `std::future` 当成同步调用来等待。
- 搭建 AsyncTask 程序或创建专用协程线程。
- 排障：主线程协程不执行、进程无法退出、`deleteLater` 不生效、改线程亲和无效。

不适用：非协程的普通 Qt 代码，或“是否选用本框架”的选型决策（该决策已定）。

## 环境配置

**`.pro` 文件**（Qt 工程；`QT` 含 `core`/`network` 时自动启用对应 Qt 能力）：
```pro
QT += core network            # socket / tcpserver 的 coro() 包装器需要 network
CONFIG += console c++17
include($$PWD/path/to/AsyncTask.pri)   # 按实际相对路径修改
SOURCES += main.cpp
```

**头文件**（按需引入）：
```cpp
#include "task/fiberapplication.h"   // installFiberApplication / exec / quit
#include "task/fibertask.h"          // makeTask / FiberTask / Priority / Affinity
#include "await/coro.hpp"            // 伞头：一次性引入所有来源的 coro()/await()/generate()
// 或按来源单独引入：await/corosignal.hpp、corosocket.hpp、corotcpserver.hpp、
//    corolocalsocket.hpp、corolocalserver.hpp、coroudpsocket.hpp、corosslsocket.hpp、
//    coroiodevice.hpp、corofuture.hpp、await/generator.hpp
#include "detail/asyncdefine.h"      // sleep / msleep / launch_properties
#include "executor/qtfiberthread.h"  // QtFiberThread（专用协程线程）
using namespace Coro;
```

## 程序骨架（固定为这个形状）

```cpp
int main(int argc, char* argv[]) {
    QCoreApplication app(argc, argv);
    installFiberApplication();      // 主线程安装调度器 + 启动工作线程池

    makeTask([]{
        // ... 协程逻辑；这里的 await/sleep 会让出线程 ...
        quit();                     // 完成后安全退出
        return 0;
    });

    return exec();                  // Coro::exec() —— 不是 app.exec()
}
```

## 速查表

| 需求 | 接口 |
|---|---|
| 启动协程任务 | `auto t = makeTask(fn, pri=Priority::Normal, affine=Affinity::fixed(当前线程));` |
| 链式后继 | `t.then([](Prev v){ return ...; })` |
| 任务链结束回调 | `t.on_finally([]{ ... })` |
| 取消任务链 | `t.cancel();`（只令尚未开始的节点短路） |
| 取结果（让出式） | `Result<T> r = t.get();` |
| 读取 Result | `if (r) use(r.value());` / `r.value_or(def)` / `r.has_value()` / `r.error()` |
| 优先级 | `Priority::Low / Normal / High` |
| 线程亲和 | `Affinity::shared()` / `sticky()` / `fixed(threadId)` |
| 等待信号 | `auto r = await(coro(obj, &Obj::sig));` |
| 等待并指定类型 | `await(coro<int>(obj, &Obj::sig));` |
| 带超时等待 | `await_for(coro(...), std::chrono::milliseconds(500))` |
| TCP socket / iodevice | `await_for(coro(sock).connectToHost(host, port), 2s);` `await(coro(dev).readAll())` |
| 接受连接 | `for (QTcpSocket* s : generate(coro(server).nextConnection())) {...}` |
| 等待 future | `await(coro(std::move(fut)));` |
| 把任意 Awaitable 当流 | `for (auto v : generate(coro(...))) {...}` |
| 协程内让出 / 休眠 | `boost::this_fiber::yield();` `sleep(1);` `msleep(100);` |
| 专用协程线程 | `auto* w = new QtFiberThread(); w->start(); ... w->quit();` |
| 底层创建协程 | `auto fb = launch_properties(fn, pri, affine); fb.detach();` |

信号参数目数 → 结果类型：无参 → `Awaitable<void>`；单参 → `Awaitable<Value>`；多参 → `Awaitable<tuple<...>>`。等待无参(void)信号返回 `Result<void>` —— 可当 bool 用（`if (await(coro(obj,&sig)))`），也可忽略返回值作“触发即继续”。

## Socket Awaitable Contract

**所有 socket 包装器方法返回 `std::shared_ptr<Awaitable<T>>`，不是按值 Awaitable。**
Qt 回调和调用者强持有同一 handle；可直接传给 `await`、`await_for`、`generate`。
按值 move-only `Awaitable<T>` 及其既有消费 API 仍用于通用/手工 Awaitable，不适用于
socket 方法的返回值。空 shared handle 的等待结果为 `invalid_argument`。

```cpp
auto connected = await_for(coro(&socket).connectToHost(host, port), 2s);
if(!connected) qWarning() << connected.error().message();
```

- `close()`：正常终止；已排队数据先被消费，最终 `Result.error()` 是
  `std::errc::no_message`。
- `close(std::error_code)`：记录第一个终止错误；TCP/UDP 为 `qt.socket`，本地 socket
  为 `qt.localsocket`，SSL 为 `qt.ssl`，保留 Qt 枚举值。
- `await_for(...)` 的 `std::errc::timed_out` 仅结束本次等待；**不取消 Awaitable，
  不断开订阅，也不关闭/取消底层 socket 操作**。停止来源必须显式调用 Qt 的
  `disconnectFromHost`、`disconnectFromServer`、`close` 等操作。
- **QObject 方法只能在对象所属线程执行。** 不要从任意协程线程直接调用
  socket/server；对象跨线程时使用 queued invocation。包装器自身将发起动作调度到
  所属线程，但对象必须在等待完成前存活。

| 包装器 | 完整方法库存 |
|---|---|
| `CoroAbstractSocket` / `QTcpSocket` | `readAll`、`waitForReadyRead`、`waitForBytesWritten`、`waitForConnected`、`waitForDisconnected`、两个 `connectToHost` 重载（`QString` / `QHostAddress`）、`disconnectFromHost` |
| `CoroTcpServer` | `nextConnection` → `Awaitable<QTcpSocket*>` 流 |
| `CoroLocalSocket` | `readAll`、`waitForReadyRead`、`waitForBytesWritten`、`waitForConnected`、`waitForDisconnected`、`connectToServer`、`disconnectFromServer` |
| `CoroLocalServer` | `nextConnection` → `Awaitable<QLocalSocket*>` 流 |
| `CoroUdpSocket` | `receiveDatagram` → `Awaitable<QNetworkDatagram>` 流；每个元素保留一个 UDP datagram 的边界、payload、发送者/目标地址和端口 metadata |
| `CoroSslSocket` | 继承 TCP 方法；`waitForEncrypted`、`connectToHostEncrypted` |

`QSslSocket` 的握手、证书和 peer verification 失败产生 `qt.ssl` 错误；框架**从不**
自动调用 `ignoreSslErrors()`，应用必须明确实现自己的证书策略。`nextConnection` 和
`receiveDatagram` 传给 `generate(shared_ptr)` 后，来源正常关闭会使迭代自然结束。

**跨协程协调退出：** `quit()` 应在工作真正完成后才调用。若要先等其它协程/任务结束，**捕获其 `FiberTask` 并调用 `.get()`**（让出而非阻塞）再 `quit()`：
```cpp
auto job = makeTask([]{ /* 后台工作 */ return 1; });
makeTask([job]{ job.get(); /* 现在安全了 */ quit(); return 0; });  // 先 join 再 quit
```

## 常用范式

**任务链（结构化并发）**
```cpp
auto task = makeTask([]{ return 10; }, Priority::Normal, Affinity::sticky())
    .then([](int v){ return v + 1; })       // 前驱结果作为入参
    .on_finally([]{ qDebug() << "chain done"; });
Result<int> r = task.get();                 // 让出直到就绪，不阻塞线程
```

**等待 Qt 信号（同步风格）**
```cpp
makeTask([]{
    QTimer* timer = new QTimer();
    timer->start(500);
    await(coro(timer, &QTimer::timeout));   // 让出协程；线程保持空闲可用
    timer->deleteLater();
    quit();
    return 0;
});
```

**socket 收发 / 流式读取**
```cpp
QTcpSocket* c = new QTcpSocket();
auto connected = await_for(coro(c).connectToHost(QHostAddress::LocalHost, 40088), 2s);
if(connected){
    auto data = await_for(coro(c).readAll(), 2s);
    auto written = coro(c).waitForBytesWritten();
    if(c->write("ping") == 4 && await_for(written, 2s) && data) { /* use data.value() */ }
}
// 流式：for (const QByteArray& msg : generate(coro(c).readAll())) { ... }
```

**Generator（生产者/消费者数据流）**
```cpp
Generator<int> squares([](auto yield){
    for (int i = 0; i < 6; i++){ msleep(100); yield(i * i); }  // 暂停期间让出线程
});
for (int v : squares) qDebug() << v;
```

**用 Awaitable 通道做生产者/消费者**
```cpp
Awaitable<int> a;
auto prod = makeTask([ch = a.channel()]{ for(int i=0;i<10;i++) ch->push(i); ch->close(); });
auto cons = makeTask([&a]{ while (auto v = a.await()) { /* v.value() */ } });
```

**专用协程线程 + Shared 协程**
```cpp
installFiberApplication();
QtFiberThread* worker = new QtFiberThread();
worker->start();                            // 也会参与 Shared 协程的调度
makeTask([]{
    auto fb = launch_properties([]{ msleep(100); /* 在某工作线程上运行 */ },
                                Priority::High, Affinity::shared());
    fb.detach();
    quit(); return 0;
});
int rc = exec();
worker->quit(); delete worker;
```

## 调度与退出机制（简要）

- **工作原理**：`QtFiberScheduler` 重写 `suspend_until`（无就绪协程时调用）：首次进入用 `std::call_once` 创建一个常驻泵协程（固定当前线程、worker 上下文、detach），它循环 `processEvents(AllEvents)` 分发 Qt 事件；`suspend_until` 自身委托基类 `FiberScheduler` 做真正的 cv 阻塞——因此空闲时线程真正睡眠、不满核空转。
- **退出**：`Coro::quit()` 设全局退出标志 `FiberScheduler::s_exit_` → 各线程泵协程醒来看到标志、自行退出；`FiberThreadBlock::wait()` 返回前自动调 `FiberScheduler::stopCurrentThreadPump()` 设 `thread_local` 退出标志并短暂让出，确保 `~scheduler` 不挂死。`QtFiberThread::quit/析构 → block.close() → block.wait() 返回 → 自动停泵 → ~scheduler 干净` —— 无额外调用。
- **泵在 worker 上下文**：泵协程不是调度器 dispatcher，Qt 回调跑在 worker 协程上 → 回调里**可以安全做协程阻塞**(`await`/`msleep`/`get`)；不会在 dispatcher 上下文 yield 而崩溃。
- **QTimer / socket 在 worker 上可用**：`QtFiberScheduler` 构造时创建 `QEventLoop` 成员，为本线程创建 Qt 事件派发器 → QTimer/socket 等 Qt 对象在工作线程上也能工作（泵协程负责分发）。

## 关键规则与常见问题

| 现象 | 原因与排除 |
|---|---|
| 主线程协程从不执行 | 用了 `QCoreApplication::exec()` 驱动主循环。改用 `Coro::exec()`（协程调度器即主循环并泵 Qt 事件；二者同线程互斥）。 |
| 进程无法退出 / 退出崩溃 | 没调用 `Coro::quit()`。它会唤醒挂起协程、排空在途任务再退出。detached 协程勿在 `QCoreApplication` 析构后仍访问 Qt 对象。 |
| 运行中 `deleteLater` 迟迟不生效 | 协程调度期间 Qt 不派发 `DeferredDelete`，通常到 `quit()` 才处理。需要即时释放请用普通 `delete`。 |
| 运行中改 `Affinity` 但协程未迁移线程 | 不受支持。请在创建协程时用 `Affinity` 指定线程归属，勿运行中反复 `setAffinity` 迁移。 |
| 找不到 `coro(socket)` / `coro(server)` | 缺 `QT += network`，或未引入对应头（`await/corosocket.hpp` 等）或伞头 `await/coro.hpp`。 |
| 链接报 ASan 运行时不兼容 | 测试 `.pro` 中保留 `-static-libasan`，去掉 `LIBS += -lasan`。 |
| qmake 找不到 boost | 未按 ReadMe §2.2 将 boost 安装到 `/usr/local`，或未 `include(AsyncTask.pri)`。 |

## 常见错误

- 用 `app.exec()` 而非 `exec()`（Coro）。始终用后者。
- 忘记 `quit()` —— 程序退出时卡死。
- 在协程内做阻塞调用（`QThread::sleep`、`waitForXxx`、`future.get()`）：这些会阻塞整个线程。请改用 `sleep`/`msleep`/`this_fiber::yield` 与 `await(coro(...))`。
- 在生产者 lambda 中捕获整个 `Awaitable` —— 应捕获 `a.channel()`（一个 `shared_ptr`），以避免引用环。
- 按值拷贝通用 `Awaitable` —— 它是 move-only；请移动它。socket 方法返回的是
  `shared_ptr<Awaitable<T>>`，可直接交给 `await`/`await_for`/`generate`。

## 参考

更深入的设计/行为：`ReadMe.md` §3、`doc/需求规格说明.md`（做什么与为什么）、`doc/软件设计说明.md`（怎么实现，含图）、可运行的 `example/`（basic、signal_await、socket_pingpong、generator、thread_init）。
