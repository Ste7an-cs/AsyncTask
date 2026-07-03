# await 接口统一化设计

日期：2026-07-03
状态：待评审

## 1. 背景与目标

当前 await 层是**函数式**接口：每个阻塞等待各有一个函数，且分成 `awaitXxx`（阻塞取一次，返回 `Result<T>`）与 `generateXxx`（返回 `Generator<T>`）两套，命名分散、使用方式不统一。例如：

- 信号：`Coro::await(obj, sig)` / `Coro::generate(obj, sig)`
- IODevice：`awaitReadAll(dev)` / `awaitForReadAll(dev, ms)` / `generateReadAll(dev)` / `awaitForReadyRead` / `awaitForBytesWritten`
- Socket：`awaitReadAll` / `awaitForConnected` / `awaitConnectToHost` / `awaitForNewConnection` / `generateNewConnection` …
- Future：`await(std::future)` / `await(QFuture)`

**目标**：把"等待什么"（生产者，来源）与"怎么消费"（取一次 / 带超时 / 当流迭代）解耦。异步操作统一**返回一个 `Awaitable<T>` 对象**，消费方用对称的自由函数 `await(a)` / `await(a, timeout)` / `generate(a)` 处理。工厂采用 qcoro 式的**单一重载入口 `coro(...)` + 镜像原 API 方法名**的包装器，避免"每个操作一个独特名字"。

## 2. 参考：qcoro 的做法

qcoro（Qt 的 C++20 协程库）用**一个重载入口 `qCoro(...)` + 复用 Qt 对象自身方法名**解决命名分散：

```cpp
co_await qCoro(sender, &Sender::valueChanged);      // 等信号
co_await qCoro(socket).waitForConnected(5s);        // 镜像原方法名
QByteArray d = co_await qCoro(device).readAll();
auto r = co_await qCoro(future);
```

本框架绝大多数等待本质是"等一个 Qt 信号"，可塌缩成 `coro(sender, signal)` 一个入口；对象操作则用 `coro(obj).mirroredMethod()`。差别在于本框架是**有栈协程（boost.fiber）**，没有 `co_await`，消费是阻塞式的 `await(a)`（让出 fiber）。

## 3. 设计总览

```
来源(生产者)                    统一载体            消费(对称自由函数)
─────────────                  ─────────           ────────────────────
coro(obj, &T::sig)  ┐
coro(dev).readAll() ├─────►  Awaitable<T>  ─────►  await(a)          // 取一次
coro(sock).waitForConnected() ┘  (按值/move)       await(a, 1s)      // 带超时
coro(server).nextConnection()                      generate(a)       // 当流迭代
coro(future)
```

- `Awaitable<T>`：唯一载体，**与 Qt 完全解耦**，move-only 按值传递，析构即取消订阅（RAII）。
- `coro(...)`：唯一工厂入口（重载），返回 `Awaitable<T>`（信号/future）或包装器对象（IODevice/Socket/Server），包装器方法返回 `Awaitable<T>`。所有 Qt 依赖集中在此层。
- `await` / `generate`：唯一消费入口，参数都是 `Awaitable`，形态对称。

## 4. `Awaitable<T>` 增强（`coro/await/awaitable.hpp`）

```cpp
template<typename T>
class Awaitable {
    std::shared_ptr<FiberChannel<T>> ch_;     // 与生产者共享: 生产者只捕获 ch_, 不持有整个 Awaitable
    std::shared_ptr<void>            guard_;   // RAII 守卫: 最后一个 Awaitable 析构时运行清理(如 disconnect)
public:
    Awaitable();                              // 建 channel
    Awaitable(Awaitable&&) noexcept = default;
    Awaitable& operator=(Awaitable&&) noexcept = default;
    Awaitable(const Awaitable&) = delete;     // move-only, 单一所有权
    ~Awaitable();                             // 触发 guard_ 清理 + close channel

    // —— 生产者侧(任意来源, 无 Qt 依赖) ——
    std::shared_ptr<FiberChannel<T>> channel() const { return ch_; }
    bool resolve(const T& v);                 // = ch_->push(v)
    void close();                             // = ch_->close()
    // 设置"析构即执行"的清理钩子(Qt 层把 disconnect 塞进来); 内部包成 shared_ptr<void>
    void setOnClose(std::function<void()> fn);

    // —— 消费侧 ——
    Result<T> await();
    template<class R,class P> Result<T> await_for(std::chrono::duration<R,P> d);
};
```

- **Qt 解耦**：`Awaitable` 只知道"有个 channel"和"析构时跑一个不透明清理钩子"，不含 `QMetaObject::Connection`。`setOnClose(fn)` 内部：`guard_ = std::shared_ptr<void>(nullptr, [fn](void*){ fn(); });`（或等价的持有器），`guard_` 只被 `Awaitable` 持有 → 使用方 drop → `fn()` 执行 disconnect。
- **无引用环**：生产者 lambda 只捕获 `ch_`（不持有 `guard_`/整个 Awaitable）；断连后 lambda 被 QObject 销毁，`ch_` 引用自然释放。
- `void` 特化保持现有语义。

## 5. 消费接口（自由函数）

`await` 放在 `awaitable.hpp`，`generate` 放在 `generator.hpp`（新增重载）：

```cpp
// 同时支持具名左值(可反复 await 做流式) 与 临时右值(await(coro(...)) 一次性)
template<class T> Result<T> await(Awaitable<T>& a){ return a.await(); }
template<class T> Result<T> await(Awaitable<T>&& a){ return a.await(); }
template<class T, class Rep, class Period>
Result<T> await(Awaitable<T>& a, std::chrono::duration<Rep,Period> d){ return a.await_for(d); }
template<class T, class Rep, class Period>
Result<T> await(Awaitable<T>&& a, std::chrono::duration<Rep,Period> d){ return a.await_for(d); }

template<class T> Generator<T> generate(Awaitable<T> a);   // 按值接管所有权, 循环 await + yield 直到 close
```

- `await` 提供左值/右值两组重载：`await(coro(...))`（临时对象，取一次）和 `Awaitable<T> a = coro(...); await(a); await(a);`（具名对象，反复取）都能编译。临时对象在整表达式内存活，`await` 在其析构前完成。
- `generate(Awaitable<T> a)` 以值接管 Awaitable（move 进生成器）：`generate(coro(...))` 直接可用；具名左值需 `generate(std::move(a))`（语义上 generate 独占该 Awaitable）。生成器 fiber 内循环 `a.await()`：有值 `yield`，出错/关闭则结束，Awaitable 生命周期与生成器绑定。

## 6. `coro(...)` 工厂与包装器（`coro/await/coro.hpp`，仅此层依赖 Qt）

```cpp
namespace Coro {
// (A) 信号: 直接返回 Awaitable(信号即天然的 awaitable/流)
template<class Obj, class Sig> auto coro(Obj* obj, Sig sig) -> Awaitable</*打包的信号参数*/>;

// (B) 对象: 返回镜像原 API 名的包装器, 方法返回 Awaitable
class CoroIODevice {                 // coro(QIODevice*)
    Awaitable<QByteArray> readAll();          // 基于 readyRead + dev->readAll()
    Awaitable<void>       waitForReadyRead();
    Awaitable<void>       waitForBytesWritten();
};
class CoroAbstractSocket : public CoroIODevice { // coro(QAbstractSocket*)
    Awaitable<void> connectToHost(host, port, ...);
    Awaitable<void> waitForConnected();
    Awaitable<void> waitForDisconnected();
};
class CoroTcpServer {                 // coro(QTcpServer*)
    Awaitable<QTcpSocket*> nextConnection();  // 原 awaitForNewConnection/generateNewConnection
};
class CoroLocalSocket { Awaitable<void> connectToServer(...); };

CoroIODevice        coro(QIODevice* dev);
CoroAbstractSocket  coro(QAbstractSocket* s);
CoroTcpServer       coro(QTcpServer* srv);
CoroLocalSocket     coro(QLocalSocket* s);

// (C) future
template<class T> Awaitable<T> coro(std::future<T>&& f);
template<class T> Awaitable<T> coro(const QFuture<T>& f);   // ASYNC_HAS_QTCORE
}
```

工厂内部统一套路（以信号为例）：
```cpp
template<class Obj,class Sig> auto coro(Obj* obj, Sig sig){
    using R = /* packed args, 复用现有 detail::signal_args/pack_result */;
    Awaitable<R> a;
    auto ch = a.channel();                                   // 生产者只捕获 ch
    auto conn = QObject::connect(obj, sig, [ch](auto... args){ ch->push(pack(args...)); });
    a.setOnClose([conn]{ QObject::disconnect(conn); });      // 析构即断连
    return a;                                                // 按值返回(move)
}
```

## 7. 旧接口 → 新接口 迁移映射

| 旧（函数式） | 新（统一） |
|---|---|
| `await(obj, sig)` | `await(coro(obj, sig))` |
| `await<Types...>(obj, sig)` | `await(coro<Types...>(obj, sig))`（保留类型指定重载） |
| `generate(obj, sig)` | `generate(coro(obj, sig))` |
| `awaitReadAll(dev)` | `await(coro(dev).readAll())` |
| `awaitForReadAll(dev, ms)` | `await(coro(dev).readAll(), ms)` |
| `generateReadAll(dev)` | `generate(coro(dev).readAll())` |
| `awaitForReadyRead(dev, ms)` | `await(coro(dev).waitForReadyRead(), ms)` |
| `awaitForConnected(sock, ms)` | `await(coro(sock).waitForConnected(), ms)` |
| `awaitConnectToHost(sock, ...)` | `await(coro(sock).connectToHost(...), ms)` |
| `awaitForNewConnection(srv, ms)` | `await(coro(srv).nextConnection(), ms)` |
| `generateNewConnection(srv)` | `generate(coro(srv).nextConnection())` |
| `await(std::future&&)` | `await(coro(std::move(fut)))` |
| `await(QFuture)` | `await(coro(fut))` |

旧的实现函数（`await_single_impl`/`await_impl` 及各 `awaitXxx`）**改名下沉为包装器方法的内部实现**，公开 API 只保留 `coro / await / generate`。

## 8. 文件与依赖分层

- `coro/await/awaitable.hpp`：`Awaitable<T>` 增强 + `await(a)` / `await(a,timeout)`。**无 Qt 依赖**。
- `coro/await/generator.hpp`：新增 `generate(Awaitable<T>)` 重载。**无 Qt 依赖**。
- `coro/await/coro.hpp`（新增）：`coro(...)` 入口 + 各包装器 + 信号参数打包（复用现有 `detail` 模板）。**Qt 依赖集中于此**，按 `ASYNC_HAS_QTCORE` / `contains(QT, network)` 条件编译。
- 移除/合并：`signalawait.hpp` 的公开 `await/generate`、`socketawait.{hpp,cpp}`、`iodeviceawait.{hpp,cpp}`、`futureawait.hpp` 的公开函数——逻辑迁入 `coro.hpp`（signal 打包模板可留在 `detail` 或 coro.hpp）。
- `AsyncTask.pri`：登记 `coro.hpp`，移除已合并文件。

## 9. 实现阶段（每步单独提交、可编译验证）

1. **P1**：`Awaitable` 增强（shared channel + `setOnClose` + move-only + 析构 RAII）+ `await(a)` / `await(a,timeout)` + `generate(Awaitable)`。附最小单元验证（纯 fiber，无 Qt）。
2. **P2**：`coro.hpp` 信号入口 `coro(obj,sig)`（含类型指定重载）+ future 重载。
3. **P3**：IODevice / Socket / TcpServer / LocalSocket 包装器（迁移 socketawait/iodeviceawait 逻辑）。
4. **P4**：迁移 `testfiberawait` 到新 API；跑通（/usr/local boost + ASan）11 passed / 0 failed、退出码 0。
5. **P5**：清理旧文件、更新 `AsyncTask.pri`、补文档/示例。

## 10. 测试迁移

`testfiberawait` 的用例按第 7 节映射改写（`await(obj,sig)`→`await(coro(obj,sig))` 等）。验收：编译通过、11 passed / 0 failed、退出码 0（沿用已修的关机排空逻辑）。

## 11. 风险与取舍

- **兼容性**：本次**直接改公开 API**（旧名不保留薄封装），需同步改测试与文档；范围可控，仓库内无其它使用方。
- **`coro(obj,sig)` 与 `coro(obj)` 重载消歧**：签名不同（是否带 signal 实参），可正常重载；必要时用 SFINAE 约束第二实参为成员函数指针。
- **信号参数打包**：复用现有 `detail::signal_args` / `pack_result`（无参→`void`，单参→`T`，多参→`tuple`）。
- **生命周期**：`Awaitable` move-only + `guard_` 仅由 Awaitable 持有，确保"drop 即断连"；生产者持 `ch_` 不成环。跨线程安全由 `FiberChannel` 保证。
