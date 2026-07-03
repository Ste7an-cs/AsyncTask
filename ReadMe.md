# 软件使用说明
## 1.软件简述
### 1.1基本信息
Async是一个用户态协程的异步框架，提供一套基于`boost::fiber`异步编程接口。

用户可以将函数封装为可被暂停和回复的协程函数。当协程被暂停，线程不会被阻塞，调用方可继续执行其他的代码，协程将等待条件满足后被唤醒。这使得异步的代码可以像同步代码一样编写，从而更易被阅读和理解。

### 1.2 开发环境
gcc>=9.0,需支持C++17的特性。

若使用Qt，Qt>=5.12

boost>=1.89.0

### 1.3 工程结构
```
---AsyncTask        项目工程文件夹
|---3dParty         第三方库文件夹
|---coro            协程框架代码
|---doc             相关文档
|---skill           当前项目的Skill
|---example         使用例程
|---test            测试用例
|   AsyncTask.pri   工程配置文件
|   ReadMe.md       使用说明
```
### 1.4 第三方依赖清单
| 库名称 | 组件名 |
| ---- | ---- |
| Boost | fiber |
| Boost | context |
| Boost | thread |
| Boost | chrono |
## 2 安装说明
### 2.1 安装环境
gcc g++ qmake boost Qt

### 2.2 安装步骤
1. 进入3dParty/boost路径，打开终端；
2. 使用管理员权限编译并安装boost，可执行命令` sudo bash install.sh`,boost库将安装至`/usr/local/boost`路径下；
3. 使用库时，可在工程配置文件中添加`include($$PWD/../AsyncTask.pri)`，将`AsyncTask.pri`加入工程中（`include(xxx/AsyncTask.pri)`需根据实际路径修改）

AsyncTask会根据项目配置自动使能部分功能，例如在Qt项目中才会启动Qt相关的协程接口和支持Qt时间循环的调度器。在使能Qt network时，才会启用network相关的协程接口。
更完整的说明见 `doc/需求文档.md` 与 `doc/架构设计.md`，示例见 `example/`。

## 3 使用说明

### 3.1 命名格式
- 命名空间：`Coro`。建议 `using namespace Coro;` 后直接使用 `makeTask` / `coro` / `await` / `generate`。
- 类型（类/结构体/枚举）：`PascalCase`，如 `FiberTask`、`Awaitable`、`Generator`、`Result`、`MetaContext`、`CoroTcpServer`；枚举为 `enum class`，取值 `PascalCase`（`Priority::High`、`AffinityMode::Shared`）。
- 私有成员：尾下划线，如 `ch_`、`priority_`、`affinity_`。
- 头文件与来源类型对应：`await/coro*.hpp` 按 Qt 对象类型分文件，可按需引入或用伞头 `await/coro.hpp` 一次性引入。

### 3.2 运行示例

最小示例（Qt 项目，`main.cpp`）：

```cpp
#include <QCoreApplication>
#include "task/fiberapplication.h"
#include "task/fibertask.h"
#include "await/coro.hpp"
using namespace Coro;

int main(int argc, char* argv[]) {
    QCoreApplication app(argc, argv);
    installFiberApplication();               // 安装调度器 + 线程池

    makeTask([]{
        QTimer timer; timer.start(500);
        // 像同步代码一样“等待”一个信号，期间不阻塞线程
        await(coro(&timer, &QTimer::timeout));
        qDebug() << "timeout fired";
        quit();                              // 收尾并退出
        return 0;
    });

    return exec();                           // 用 Coro::exec() 驱动（非 app.exec()）
}
```

工程配置（`.pro`）中加入：`include(路径/AsyncTask.pri)`。

### 3.3 使用详解

**创建协程任务与结构化并发**
```cpp
auto task = makeTask([]{ /* ... */ return 10; },
                     Priority::Normal, Affinity::sticky())
    .then([](int v){ return v + 1; })        // 链式后继，前驱结果为入参
    .on_finally([]{ /* 任务链结束回调 */ });
Result<int> r = task.get();                  // 等待结果
// task.cancel();                            // 需要时取消任务链
```

**优先级与线程亲和**
```cpp
Affinity::shared();                 // 任意可用线程
Affinity::sticky();                 // 首次执行线程绑定
Affinity::fixed(std::this_thread::get_id()); // 指定线程
```

**统一等待接口 coro() / await() / generate()**
```cpp
// 信号
auto r  = await(coro(obj, &Obj::valueChanged));       // 单参 -> Awaitable<Value>
auto r2 = await(coro<int>(obj, &Obj::twoArgsSignal)); // 指定所需类型
// socket / iodevice（镜像原 Qt 方法名）
await(coro(socket).waitForConnected());
QByteArray data = await(coro(dev).readAll()).value_or(QByteArray());
// 流式：把 Awaitable 当数据流迭代
for (QTcpSocket* s : generate(coro(server).nextConnection())) { /* ... */ }
// future
auto v = await(coro(std::move(fut)));
```

**Awaitable 直接使用（生产者/消费者）**
```cpp
Awaitable<int> a;
auto prod = makeTask([ch = a.channel()]{ for(int i=0;i<10;i++) ch->push(i); ch->close(); });
auto cons = makeTask([&a]{ while(auto v = a.await()) { /* 消费 v.value() */ } });
```

### 3.4 常见问题与排除方法

| 现象 | 原因与排除 |
|---|---|
| 主线程协程不执行 | 用 `QCoreApplication::exec()` 驱动了主循环。应改用 `Coro::exec()`（本框架用 fiber 调度器作主循环，Qt 事件由调度器泵；二者同线程互斥）。 |
| 链接报 `Your application is linked against incompatible ASan runtimes` | 测试 `.pro` 里 `-static-libasan` 与 `LIBS += -lasan` 冲突，去掉 `-lasan`（保留 `-static-libasan`）。 |
| 程序结束不退出 / 退出崩溃 | 确保调用 `Coro::quit()` 收尾（会唤醒挂起协程、排空在途任务再退出）。detached 协程勿在 `QCoreApplication` 析构后仍访问 Qt 对象。 |
| `deleteLater` 迟迟不生效 | 调度期 Qt 不派发 `DeferredDelete`（见 `doc/需求文档.md` LIM-2），通常到 `quit()` 才处理；需要即时释放可显式 `delete`。 |
| 运行中改线程亲和后未迁移到目标线程 | 不受支持（LIM-1）。线程归属请在协程创建时用 `Affinity` 指定，勿运行中反复 `setAffinity` 迁移。 |
| qmake 报找不到 boost | 未按 2.2 安装 boost 到 `/usr/local`，或未在 `.pro` 中 `include(AsyncTask.pri)`。 |

