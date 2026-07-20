# example 新接口与失败路径示例设计

## 1. 目标

更新 `example/` 下现有五个示例，使它们统一使用 AsyncTask 当前公开接口：

- `coro(...)` 将 Qt 信号、IODevice、socket 和服务器对象转换为等待来源；
- `await(...)` 执行无超时的单次等待；
- `await_for(...)` 执行带超时的单次等待；
- `generate(...)` 将持续产生数据的 `Awaitable` 转换为可迭代数据流；
- `Result<T>` 显式区分成功与错误；
- `installFiberApplication()`、`exec()` 和 `quit()` 管理应用生命周期。

每个示例都应是可以独立阅读、编译和运行的模板。注释不仅描述代码做了什么，还应解释等待为何不阻塞线程、对象由谁释放、fiber 为何需要 `detach()`，以及失败后如何安全退出。

## 2. 范围

只修改以下五个示例及必要的工程配置：

- `example/basic`
- `example/signal_await`
- `example/generator`
- `example/socket_pingpong`
- `example/thread_init`

不修改框架实现、测试实现、README、需求文档或公开接口。本次不保留旧式 `awaitXxx`、`generateXxx` 或 `await(a, timeout)` 写法。

## 3. 示例职责

### 3.1 basic

作为最小应用骨架，演示：

- 安装协程调度环境并以 `Coro::exec()` 驱动主循环；
- 使用 `makeTask()` 和 `then()` 编排任务；
- 使用 `await(coro(timer, &QTimer::timeout))` 等待信号；
- 使用 `await_for(...)` 构造一次确定会超时的等待；
- 检查 `Result`，分别记录成功、超时和其它错误；
- 所有演示完成后统一调用 `quit()`。

### 3.2 signal_await

集中展示 Qt 信号适配：

- 无参信号映射为 `Result<void>`；
- 多参信号映射为 `Result<std::tuple<...>>`；
- `coro<T...>` 只选取所需的信号参数；
- 使用短超时演示 `await_for` 失败；
- 销毁信号来源对象，使正在等待的 `Awaitable` 收敛，并检查关闭结果。

对象销毁场景不得留下捕获悬空指针的协程。示例用明确的任务等待或严格的生命周期顺序保证安全。

### 3.3 generator

展示两种流：

- 直接构造 `Generator<int>`，由生产者调用 `yield`；
- 创建 `Awaitable<int>`，由生产者通过 `channel()` 投递数据，再由 `generate(std::move(a))` 接管并迭代。

注释解释 `Awaitable` 是 move-only、`generate` 接管所有权、channel 关闭后循环自然结束。错误/关闭路径通过流结束后的日志说明，不把正常关闭误报为业务失败。

### 3.4 socket_pingpong

保留本机 TCP ping-pong 场景，并补齐：

- 检查 `QTcpServer::listen()` 返回值；
- 客户端使用 `coro(client).connectToHost(...)` 与 `await_for(...)` 等待连接；
- 显式构造一个连接到未监听端口的客户端，演示连接超时或连接失败；
- 每轮读取都检查 `Result<QByteArray>`；
- 写入失败、读取失败、连接关闭都输出可诊断信息；
- 服务端、连接处理任务和客户端任务在 `quit()` 前完成协调收尾。

失败路径不得导致示例永久等待。每个可能等待外部事件的单次操作都使用有限超时，持续流通过关闭来源对象结束。

### 3.5 thread_init

展示调度和线程所有权：

- 主线程调用 `installFiberApplication()`；
- 创建并启动 `QtFiberThread`；
- 用 `launch_properties(..., Affinity::shared())` 创建底层 fiber；
- 明确说明返回的 fiber 仍由调用者负责 `detach()` 或 `join()`；
- 用 `FiberTask::get()`/`Result` 协调并检查工作结果；
- `exec()` 返回后停止并销毁专用线程。

## 4. 注释规范

每个 `main.cpp` 包含以下层次的注释：

1. 文件头：示例目的、涉及接口、预期输出和关键限制；
2. 初始化：解释 `QCoreApplication`、`installFiberApplication()` 和 `exec()` 的关系；
3. 任务与亲和：说明任务运行位置和等待时的让出语义；
4. 等待与结果：说明 `Result` 成功值、超时和关闭/其它错误；
5. 生命周期：说明 Qt 对象、Awaitable、Generator、FiberTask 和裸 fiber 的所有权；
6. 退出：说明为何必须在在途任务收敛后调用 `quit()`。

注释使用中文，接口名、类型名和错误码保留英文。避免逐字翻译语句本身，优先解释容易误用的原因和约束。

## 5. 错误处理原则

- 不对 `Result::value()` 做未检查访问；先使用 `if (result)` 或 `has_value()`；
- 超时使用 `result.error() == std::make_error_code(std::errc::timed_out)` 判断；
- 其它错误输出 `error().message()`；
- Qt 自身的立即失败接口（例如 `listen()`、`write()`）检查其返回值和 `errorString()`；
- 失败路径仍需关闭对象、结束生成器/服务端任务，并最终进入 `quit()`；
- 示例不依赖无限等待来证明失败，所有故意失败的等待都有明确时间上限。

当前框架实现尚不能在 `await_for` 中可靠区分“来源关闭”和“超时”。示例只把明确由时间上限触发的结果解释为超时；来源关闭场景使用无超时 `await()` 检查 `no_message`，避免注释超出实际能力。

## 6. 验证

完成修改后执行：

1. 分别用各目录的 `.pro` 文件在独立临时构建目录运行 `qmake` 和 `make`；
2. 运行五个示例并设置外部总超时，防止失败路径造成永久挂起；
3. 核对成功路径、故意超时、连接失败和正常退出日志；
4. 搜索 `example/`，确认不存在旧式等待接口；
5. 检查 Git diff，确保未修改范围外文件（本设计文档除外）。

验收标准是五个示例全部编译成功、在限定时间内退出，并且注释与实际接口和错误语义一致。
