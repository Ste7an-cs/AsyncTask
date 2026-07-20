# Socket Awaitable 接口补全设计

## 1. 目标

补全 AsyncTask 的 Qt socket 协程接口，使 TCP、本地 socket、SSL 和 UDP 都能通过统一的 `coro(...).method()` 工厂产生智能指针管理的 `Awaitable`，并由 `await()`、`await_for()` 或 `generate()` 消费。

本次同时补齐错误传播和终止语义：成功值由 `resolve()` 传递；正常关闭、失败关闭和消费端超时必须可区分；Qt 信号回调强捕获 `std::shared_ptr<Awaitable<T>>`，不引入额外 Resolver 类型。

## 2. 范围

本次修改包括：

- `FiberChannel` 的正常关闭、带错误关闭和超时状态；
- `Awaitable` 的智能指针消费重载；
- `QAbstractSocket/QTcpSocket`；
- `QTcpServer`；
- `QLocalSocket/QLocalServer`；
- `QSslSocket`；
- `QUdpSocket/QNetworkDatagram`；
- 对应构建配置、测试、文档和 socket 示例。

本次不实现 `QFile` 协程包装器，不扩展 `QNetworkReply`、DBus、Widgets 或进程接口。

## 3. 所有权模型

socket 包装器的方法返回：

```cpp
std::shared_ptr<Awaitable<T>>
```

Qt 信号回调强捕获该 shared pointer：

```cpp
auto a = std::make_shared<Awaitable<QByteArray>>();
QObject::connect(socket, &QIODevice::readyRead, [a, socket] {
    a->resolve(socket->readAll());
});
return a;
```

约束如下：

- `resolve(value)` 只传递成功值；
- 调用方丢弃自己的 shared pointer 不会取消订阅；
- Awaitable 至少存活至连接被断开或来源 QObject 被销毁；
- 来源销毁、应用退出和显式断连必须释放 Qt 回调持有的引用；
- 不捕获栈上 Awaitable 引用，不引入 Resolver/Producer 句柄；
- 生命周期辅助设施必须保存并断开所有建立的连接，不能遗漏关闭/错误连接。

## 4. 通道与错误模型

### 4.1 FiberChannel

`FiberChannel<T>` 增加终止错误状态：

```cpp
void close();
void close(std::error_code error);
std::error_code close_error() const;
```

语义：

- `close()` 使用 `std::errc::no_message` 表示正常无数据结束；
- `close(error)` 保存失败原因并唤醒等待者；
- 第一次关闭决定终态，后续关闭不覆盖原始原因；
- 已入队的数据可以先被消费，队列耗尽后才返回终止错误；
- `pop_wait_for()` 必须区分 `success`、`timeout` 和 `closed`，不得把关闭误报为超时。

### 4.2 Awaitable

`Awaitable<T>` 和 `Awaitable<void>` 增加：

```cpp
void close(std::error_code error);
```

已有 `resolve()` 保持成功值投递职责，不承担错误传递。

消费结果：

- 取到值：成功 `Result<T>`；
- 消费端达到时限：`std::errc::timed_out`；
- 来源正常关闭/销毁：`std::errc::no_message`；
- socket 失败：对应 Qt socket error code；
- `await_for()` 超时不关闭 Awaitable，也不取消底层 socket 操作，之后仍可再次等待。

### 4.3 Qt 错误类别

新增 Qt socket 错误转换设施，至少覆盖：

- `QAbstractSocket::SocketError`；
- `QLocalSocket::LocalSocketError`；
- SSL 握手与证书错误。

错误以独立 `std::error_category` 保留 Qt 枚举数值和可读消息。包装器在 `errorOccurred()`、`sslErrors()` 或 `peerVerifyError()` 到达时以错误关闭等待器。

## 5. 智能指针消费接口

保留所有现有按值接口，并新增：

```cpp
template<class T>
Result<T> await(const std::shared_ptr<Awaitable<T>>& a);

template<class T, class Rep, class Period>
Result<T> await_for(const std::shared_ptr<Awaitable<T>>& a,
                    const std::chrono::duration<Rep, Period>& timeout);

template<class T>
Generator<T> generate(std::shared_ptr<Awaitable<T>> a);
```

因此常见调用形式不改变：

```cpp
await(coro(socket).readAll());
await_for(coro(socket).waitForConnected(), std::chrono::seconds(3));
generate(coro(server).nextConnection());
```

空 shared pointer 返回 `invalid_argument`，不得解引用崩溃。

## 6. 包装器接口

### 6.1 QAbstractSocket / QTcpSocket

`CoroAbstractSocket` 提供：

```cpp
readAll();
waitForReadyRead();
waitForBytesWritten();
waitForConnected();
waitForDisconnected();
connectToHost(QString, quint16, QIODevice::OpenMode);
connectToHost(QHostAddress, quint16, QIODevice::OpenMode);
disconnectFromHost();
```

`QTcpSocket` 直接复用此包装器。方法建立完成、错误、来源销毁和应用退出连接后，再检查当前 socket 状态和缓冲数据，消除 check-then-connect 竞态。

`readAll()` 作为流使用时，在每次 `readyRead()` 中读取当前缓冲数据；正常远端关闭使流正常结束，非正常错误保留错误终态。

### 6.2 QTcpServer

保留：

```cpp
nextConnection();
```

返回 `std::shared_ptr<Awaitable<QTcpSocket*>>`。每次 `newConnection()` 必须取完当前 pending connection；`acceptError()` 以错误关闭；server 正常关闭或销毁使流正常结束。

### 6.3 QLocalSocket

`CoroLocalSocket` 提供：

```cpp
readAll();
waitForReadyRead();
waitForBytesWritten();
waitForConnected();
waitForDisconnected();
connectToServer(QString, QIODevice::OpenMode);
disconnectFromServer();
```

行为与 `CoroAbstractSocket` 对齐，错误使用 `QLocalSocket::LocalSocketError` 类别。

### 6.4 QLocalServer

新增 `CoroLocalServer`：

```cpp
nextConnection();
```

返回 `std::shared_ptr<Awaitable<QLocalSocket*>>`；一次信号取完 pending connections。Qt 5.15 的 `QLocalServer` 没有与 `QTcpServer::acceptError()` 对应的运行期错误信号，因此 `listen()` 的同步失败由调用方立即检查 `serverError()/errorString()`，连接流仅在 server 关闭、销毁或应用退出时正常结束，不伪造异步 accept error。

### 6.5 QSslSocket

新增 `CoroSslSocket`，复用 `QAbstractSocket` 通用等待，并增加：

```cpp
waitForEncrypted();
connectToHostEncrypted(host, port, mode, protocol);
```

`encrypted()` 为成功终态；`sslErrors()`、`peerVerifyError()` 和 socket error 为失败终态。若创建等待器时 socket 已加密，立即 resolve。

### 6.6 QUdpSocket

新增 `CoroUdpSocket`：

```cpp
receiveDatagram();
```

返回 `std::shared_ptr<Awaitable<QNetworkDatagram>>`。每次 `readyRead()` 中循环调用 `receiveDatagram()` 直至没有 pending datagram，确保：

- 每个 UDP 数据报独立投递，不合并包边界；
- 保留发送方/目标地址、端口和可用元数据；
- 等待器建立前已到达的数据报被立即取出；
- socket 错误以失败终止流。

发送继续使用 `QUdpSocket::writeDatagram()`；写完成等待复用 `waitForBytesWritten()`。不增加 Awaitable 化的 `writeDatagram()`，避免多个并发发送与 `bytesWritten()` 无法可靠一一配对。

## 7. 内部结构

新增或调整：

- `await/detail/socketerror.hpp`：Qt socket 错误 category 与转换；
- `await/detail/socketawait.hpp`：共享的智能 Awaitable 创建、连接登记、终态和立即状态检查辅助逻辑；
- `await/corolocalserver.hpp`；
- `await/corosslsocket.hpp`；
- `await/coroudpsocket.hpp`；
- `await/coro.hpp` 伞头；
- `AsyncTask.pri` 头文件清单。

辅助模块不得隐藏对象线程亲和要求：Qt 对象的方法在其所属线程调用；跨线程触发动作时使用 Qt queued invocation，不能直接从任意 worker 操作 socket。

## 8. 测试

### 8.1 通道与 Awaitable

- 正常关闭返回 `no_message`；
- 带错误关闭返回原始错误；
- 第一个终态生效；
- 队列已有值时先消费值，再得到终止错误；
- `await_for()` 区分 timeout 与 closed；
- 一次 timeout 后仍可继续 resolve 和 await；
- shared pointer 为空时返回 `invalid_argument`。

### 8.2 Socket

- TCP：连接成功、连接拒绝、读写、远端正常关闭、accept error；
- Local socket：连接、读写、断开、local server 接受连接；
- UDP：连续两个数据报保持边界，发送方地址/端口正确；
- SSL：使用仓库内仅供测试的 PEM 证书/私钥夹具验证本地成功握手，并以明文服务端验证确定性的握手错误；测试不依赖系统证书或外部网络；
- 生命周期：调用方释放引用后回调仍持有 Awaitable；来源销毁和连接清理后引用释放；
- 流式生成器在来源关闭后结束，不永久等待。

测试使用动态端口或唯一 local server 名称，避免固定端口冲突。所有可能等待外部事件的测试设置有限超时。

## 9. 文档与示例

同步更新：

- `ReadMe.md`；
- `doc/架构设计.md`；
- `doc/软件设计说明.md`；
- `skill/using-asynctask/SKILL.md`；
- `example/socket_pingpong`。

文档说明智能指针强捕获语义、错误关闭、timeout 不取消底层操作、UDP 数据报边界及 QObject 线程亲和约束。

## 10. 验收

- 新旧 Awaitable 消费接口均能编译；
- 相关 QtTest 全部通过；
- TCP、local socket、UDP 和 SSL 错误路径在限定时间内收敛；
- 示例可编译运行并正常退出；
- 文档、Skill、伞头和 qmake 清单与实现一致；
- `git diff --check` 无格式错误；
- 未混入 QFile 或其它后续接口实现。
