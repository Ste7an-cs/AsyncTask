# Qt 阻塞接口的 Awaitable 化候选

本报告面向 Qt 5.15 和 AsyncTask 当前的有栈协程模型。判断原则是：阻塞接口背后若已有 Qt 完成/错误信号，就适合直接转换为 `Awaitable`；若没有事件通知，只能把阻塞工作卸载到线程池，或改用 fiber 原语。

## 第一优先级

### QProcess

Qt 明确把 `waitForStarted()`、`waitForReadyRead()`、`waitForBytesWritten()` 和 `waitForFinished()` 列为同步进程 API，并警告在主线程调用可能冻结界面。对应信号包括 `started()`、`readyRead*()`、`bytesWritten()`、`finished(exitCode, exitStatus)` 和 `errorOccurred(error)`。

建议接口：

- `coro(process).start(program, args) -> Awaitable<void>`
- `coro(process).waitForStarted() -> Awaitable<void>`
- `coro(process).readStdout() -> Awaitable<QByteArray>`
- `coro(process).readStderr() -> Awaitable<QByteArray>`
- `coro(process).waitForFinished() -> Awaitable<ProcessResult>`

必须让完成信号与 `errorOccurred()` 竞速，并在进程已启动/已结束时立即投递，避免先检查后连接的竞态。

来源：[Qt 5.15 QProcess](https://doc.qt.io/archives/qt-5.15/qprocess.html)

### QLocalServer

`waitForNewConnection()` 可由 `newConnection()` 驱动，适合与现有 `CoroTcpServer::nextConnection()` 对称实现：

- `coro(localServer).nextConnection() -> Awaitable<QLocalSocket*>`

来源：[Qt 5.15 QLocalServer](https://doc.qt.io/archives/qt-5.15/qlocalserver.html)

### QSslSocket

除继承的 socket 等待外，`waitForEncrypted()` 可映射到 `encrypted()`；失败必须同时监听 `sslErrors(...)`、`peerVerifyError(...)` 和 socket 的 `errorOccurred(...)`。

- `coro(sslSocket).waitForEncrypted() -> Awaitable<void>`
- `coro(sslSocket).connectToHostEncrypted(...) -> Awaitable<void>`

来源：[Qt 5.15 QSslSocket](https://doc.qt.io/archives/qt-5.15/qsslsocket.html)

## 第二优先级

### QThread

`QThread::wait()` 会阻塞调用线程，而 `finished()` 可用于事件式完成通知：

- `coro(thread).waitForFinished() -> Awaitable<void>`

这只代表收到 Qt 的 finished 通知。销毁线程对象前仍需遵守 Qt 的线程生命周期约束；取消 Awaitable 不等于终止线程，应由调用方先 `requestInterruption()`/`quit()`。

来源：[Qt 5.15 QThread](https://doc.qt.io/archives/qt-5.15/qthread.html)

### QDBusPendingCall

`QDBusPendingCall::waitForFinished()` 可由 `QDBusPendingCallWatcher::finished()` 替代。结果应保留 DBus error，而不是只返回 void：

- `coro(pendingCall) -> Awaitable<QDBusMessage>` 或类型化 DBus Result

来源：[Qt 5.15 QDBusPendingCallWatcher](https://doc.qt.io/archives/qt-5.15/qdbuspendingcallwatcher.html)

### QDialog 及派生对话框

`QDialog::exec()` 启动嵌套事件循环。更合适的模型是调用 `open()`，等待 `finished(int)`：

- `coro(dialog).open() -> Awaitable<int>`

必须说明 `hide()`/`setVisible(false)` 不一定发出 `finished()`，并处理对象销毁以关闭 channel。

来源：[Qt 5.15 QDialog](https://doc.qt.io/archives/qt-5.15/qdialog.html)

## 第三优先级：本来就是异步 API，但值得统一

- `QNetworkReply`：等待 `finished()`，流式读取 `readyRead()`，错误取 `error()`；取消映射到 `abort()`。[Qt 5.15 QNetworkReply](https://doc.qt.io/archives/qt-5.15/qnetworkreply.html)
- `QDnsLookup`：等待 `finished()`，结果包含 DNS error 和 records。[Qt 5.15 QDnsLookup](https://doc.qt.io/archives/qt-5.15/qdnslookup.html)
- `QHostInfo::lookupHost()`：把回调转换为 `Awaitable<QHostInfo>`，取消时调用 `abortHostLookup()`。[Qt 5.15 QHostInfo](https://doc.qt.io/archives/qt-5.15/qhostinfo.html)
- `QSerialPort`：复用 `QIODevice` 的 readyRead/bytesWritten 模型，同时监听 `errorOccurred()`。[Qt 5.15 QSerialPort](https://doc.qt.io/archives/qt-5.15/qserialport.html)
- `QTimer`：把单次 timeout 转换为 Awaitable，可作为 `Coro::sleep/msleep` 之外、绑定 Qt 对象线程的定时等待。[Qt 5.15 QTimer](https://doc.qt.io/archives/qt-5.15/qtimer.html)

## 不应直接做成信号型 Awaitable

- `QFile`/`QSaveFile` 的磁盘读写：没有异步完成信号，应卸载到工作线程或线程池。
- `QMutex::lock()`、`QSemaphore::acquire()`、`QWaitCondition::wait()`：没有 QObject 信号，应使用 boost.fiber 对应原语或 `FiberChannel`。
- `QThreadPool::waitForDone()`：没有逐任务完成信号；应包装每个任务的 future/channel，而不是等待整个池。
- `QProcess::execute()`：应改写为 `QProcess::start()` 加进程 Awaitable，而不是在另一个 fiber 中直接调用同步 execute。
- `QEventLoop::exec()`/`QCoreApplication::exec()`：不应包装；AsyncTask 已要求以 `Coro::exec()` 驱动主循环。

## 公共实现约束

1. 成功、错误、对象销毁、应用退出和超时是竞速关系，只允许第一个终态关闭并写入 channel。
2. 建立信号连接后立即检查当前状态，处理信号早于连接发生的情况。
3. `await_for` 的超时默认只取消本次订阅，不应擅自终止进程、线程、网络请求或对话框；破坏性取消需要显式 API。
4. QObject 的读取、关闭和销毁必须遵守其线程亲和；跨线程回调使用 queued invocation。
5. 流式来源使用 `generate()`；一次性完成使用 `await()`/`await_for()`。
