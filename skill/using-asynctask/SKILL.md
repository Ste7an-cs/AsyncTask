---
name: using-asynctask
description: Use when writing, generating, or modifying C++ code that uses the AsyncTask coroutine framework (namespace `Coro`, boost.fiber based) — makeTask/then/get task chains, awaiting Qt signals/sockets/QIODevice/futures with coro()/await()/generate(), Awaitable/Generator/Result, or the installFiberApplication/exec/quit lifecycle. Also when hitting AsyncTask pitfalls (main-thread coroutines not running, process won't exit, deleteLater deferred, runtime affinity change ignored).
---

# Using AsyncTask (Coro)

## Overview

AsyncTask is a **stackful coroutine** framework (built on boost.fiber) for Qt/C++17. A coroutine that waits **yields its thread instead of blocking it**, so you write async logic as straight-line synchronous-style code while many coroutines run concurrently on a few threads.

Core rules (violating any of these is the usual cause of bugs):

1. **Drive the app with `Coro::exec()`, NOT `QCoreApplication::exec()`.** The fiber scheduler is the main loop; it pumps Qt events itself. The two are mutually exclusive on the same thread.
2. **Shut down with `Coro::quit()`** — it wakes suspended coroutines, drains in-flight work, then exits. Skipping it causes hang-on-exit or exit crashes.
3. **Everything lives in namespace `Coro`.** Do `using namespace Coro;`.

## When to use

- Writing any code that calls `makeTask`, `coro`, `await`, `generate`, `Coro::exec/quit`, `Awaitable`, `Generator`, `Result`.
- Awaiting a Qt signal / socket / `QIODevice` / `QFuture` / `std::future` as if it were synchronous.
- Setting up an AsyncTask program or a dedicated coroutine thread.
- Debugging: main-thread coroutines don't run, process won't exit, `deleteLater` never fires, affinity change ignored.

Not for: non-coroutine Qt code, or picking the framework itself (that decision is already made).

## Setup

**`.pro` file** (Qt project — Qt bits auto-enable when `QT` has `core`/`network`):
```pro
QT += core network            # network needed for socket/tcpserver coro() wrappers
CONFIG += console c++17
include($$PWD/path/to/AsyncTask.pri)   # adjust relative path
SOURCES += main.cpp
```

**Includes** (only what you use):
```cpp
#include "task/fiberapplication.h"   // installFiberApplication / exec / quit
#include "task/fibertask.h"          // makeTask / FiberTask / Priority / Affinity
#include "await/coro.hpp"            // umbrella: coro()/await()/generate() for all sources
// or a specific source header: await/corosignal.hpp, corosocket.hpp, corotcpserver.hpp,
//    coroiodevice.hpp, corolocalsocket.hpp, corofuture.hpp, await/generator.hpp
#include "detail/asyncdefine.h"      // sleep / msleep / launch_properties
#include "executor/qtfiberthread.h"  // QtFiberThread (dedicated coroutine thread)
using namespace Coro;
```

## Program skeleton (always this shape)

```cpp
int main(int argc, char* argv[]) {
    QCoreApplication app(argc, argv);
    installFiberApplication();      // install scheduler on main thread + start worker pool

    makeTask([]{
        // ... your coroutine logic; await/sleep here yield the thread ...
        quit();                     // safe shutdown when done
        return 0;
    });

    return exec();                  // Coro::exec() — NOT app.exec()
}
```

## Quick reference

| Need | API |
|---|---|
| Start a coroutine task | `auto t = makeTask(fn, pri=Priority::Normal, affine=Affinity::fixed(current thread));` |
| Chain after result | `t.then([](Prev v){ return ...; })` |
| End-of-chain callback | `t.on_finally([]{ ... })` |
| Cancel a chain | `t.cancel();` (only short-circuits not-yet-started nodes) |
| Get result (yields) | `Result<T> r = t.get();` |
| Read a Result | `if (r) use(r.value());` / `r.value_or(def)` / `r.has_value()` / `r.error()` |
| Priority | `Priority::Low / Normal / High` |
| Affinity | `Affinity::shared()` / `sticky()` / `fixed(threadId)` |
| Await a signal | `auto r = await(coro(obj, &Obj::sig));` |
| Await, force types | `await(coro<int>(obj, &Obj::sig));` |
| Await w/ timeout | `await(coro(...), std::chrono::milliseconds(500))` |
| Socket / iodevice | `await(coro(sock).waitForConnected());` `await(coro(dev).readAll())` |
| Accept connections | `for (QTcpSocket* s : generate(coro(server).nextConnection())) {...}` |
| Await a future | `await(coro(std::move(fut)));` |
| Stream any Awaitable | `for (auto v : generate(coro(...))) {...}` |
| Yield / sleep in coroutine | `boost::this_fiber::yield();` `sleep(1);` `msleep(100);` |
| Dedicated coroutine thread | `auto* w = new QtFiberThread(); w->start(); ... w->quit();` |
| Low-level fiber | `auto fb = launch_properties(fn, pri, affine); fb.detach();` |

Signal arity → result type: no args → `Awaitable<void>`; one arg → `Awaitable<Value>`; many → `Awaitable<tuple<...>>`.

## Recipes

**Task chain (structured concurrency)**
```cpp
auto task = makeTask([]{ return 10; }, Priority::Normal, Affinity::sticky())
    .then([](int v){ return v + 1; })       // prev result is the argument
    .on_finally([]{ qDebug() << "chain done"; });
Result<int> r = task.get();                 // yields until ready, doesn't block thread
```

**Await a Qt signal (synchronous-style)**
```cpp
makeTask([]{
    QTimer* timer = new QTimer();
    timer->start(500);
    await(coro(timer, &QTimer::timeout));   // yields; thread stays free
    timer->deleteLater();
    quit();
    return 0;
});
```

**Socket ping / stream read**
```cpp
QTcpSocket* c = new QTcpSocket();
c->connectToHost(QHostAddress::LocalHost, 40088);
await(coro(c).waitForConnected());
c->write("ping");
QByteArray data = await(coro(c).readAll()).value_or(QByteArray());
// stream: for (const QByteArray& msg : generate(coro(c).readAll())) { ... }
```

**Generator (producer/consumer stream)**
```cpp
Generator<int> squares([](auto yield){
    for (int i = 0; i < 6; i++){ msleep(100); yield(i * i); }  // pause yields thread
});
for (int v : squares) qDebug() << v;
```

**Producer/consumer via an Awaitable channel**
```cpp
Awaitable<int> a;
auto prod = makeTask([ch = a.channel()]{ for(int i=0;i<10;i++) ch->push(i); ch->close(); });
auto cons = makeTask([&a]{ while (auto v = a.await()) { /* v.value() */ } });
```

**Dedicated coroutine thread + Shared coroutines**
```cpp
installFiberApplication();
QtFiberThread* worker = new QtFiberThread();
worker->start();                            // also schedules Shared coroutines
makeTask([]{
    auto fb = launch_properties([]{ msleep(100); /* runs on some worker */ },
                                Priority::High, Affinity::shared());
    fb.detach();
    quit(); return 0;
});
int rc = exec();
worker->quit(); delete worker;
```

## Critical rules & pitfalls

| Symptom | Cause & fix |
|---|---|
| Main-thread coroutines never run | You drove the loop with `QCoreApplication::exec()`. Use `Coro::exec()` (fiber scheduler is the main loop and pumps Qt events; the two are mutually exclusive per thread). |
| Process won't exit / crashes on exit | You didn't call `Coro::quit()`. It wakes suspended coroutines, drains in-flight tasks, then exits. Don't let detached coroutines touch Qt objects after `QCoreApplication` is destroyed. |
| `deleteLater` never fires mid-run | During coroutine scheduling Qt doesn't dispatch `DeferredDelete` until `quit()`. For immediate release use plain `delete`. |
| Changed `Affinity` at runtime but coroutine didn't migrate | Not supported. Set thread affinity at creation via `Affinity`; do not repeatedly re-affinitize a running coroutine. |
| `coro(socket)` / `coro(server)` not found | Missing `QT += network`, or include the specific header (`await/corosocket.hpp` etc.) or the umbrella `await/coro.hpp`. |
| Link error: incompatible ASan runtimes | In test `.pro`, keep `-static-libasan`, remove `LIBS += -lasan`. |
| qmake can't find boost | Install boost to `/usr/local` (see ReadMe §2.2) and `include(AsyncTask.pri)`. |

## Common mistakes

- Calling `app.exec()` instead of `exec()` (Coro). Always the latter.
- Forgetting `quit()` — the program hangs at shutdown.
- Blocking calls inside a coroutine (`QThread::sleep`, `waitForXxx`, `future.get()`): these block the whole thread. Use `sleep`/`msleep`/`this_fiber::yield` and `await(coro(...))` instead.
- Capturing the whole `Awaitable` in a producer lambda — capture `a.channel()` (a `shared_ptr`) instead, to avoid a reference cycle.
- Passing an `Awaitable` by copy — it is move-only; move it, or hand it straight to `await`/`generate`.

## Reference

Deeper design/behavior: `ReadMe.md` §3, `doc/需求规格说明.md` (what & why), `doc/软件设计说明.md` (how it works, with diagrams), runnable `example/` (basic, signal_await, socket_pingpong, generator, thread_init).
