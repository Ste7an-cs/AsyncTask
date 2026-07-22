# Socket 新增代码中文 Doxygen 注释实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 原始目标是为 `786f05a..67b71a7` 中新增或实质修改的 C++ 代码补充详细、准确的中文 Doxygen 注释，且不改变运行行为。后续审查中用户另行批准了两项有限的行为补全，因此最终系列不是纯注释差异。

**Architecture:** 按“基础 await 机制、socket 包装器、示例与测试”三个独立文件组实施。公开接口记录所有权、线程亲和、结果和失败语义；内部实现记录生命周期、强捕获、并发清理及错误传播原因；示例和测试仅解释关键辅助结构和验证目标。

**Tech Stack:** C++17、Qt 5/6、Boost.Fiber、Doxygen、qmake、Qt Test。

## Global Constraints

- 所有新增代码注释均使用中文，Doxygen 命令保留标准英文形式。
- 使用 `/** ... */`、`///` 或 `///<`；正文遵循 Google C++ 注释习惯，写完整句子并避免复述代码。
- 原始注释工作不修改 API、ABI、控制流、表达式、空白布局或运行行为；仅下文“用户后续批准的行为补全”两项作为例外。
- 不修改证书、qmake 工程文件、既有 Markdown 用户文档或 `docs/research/`。
- `await_for` 超时只结束本次等待，不关闭或取消底层订阅。
- socket wrapper 不拥有 Qt 源对象；跨线程操作投递到源对象的线程。
- socket 回调强捕获 `std::shared_ptr<Awaitable<T>>`，终止时执行幂等清理。

## 用户后续批准的行为补全

- UDP 接收流在初始状态已是 `UnconnectedState` 时，必须在 drain 数据报之前以默认 `no_message` 正常关闭；同时增加了专门回归测试。该测试使完整 Qt test suite 的预期数量从 56 增加为 57。
- Socket ping-pong 示例先执行一次 20 ms 的预期超时，然后复用同一个仍开放的 Awaitable 发送 `ping` 并继续接收回显，用于证明 `await_for()` 超时不会取消流。
- 上述两项保留为实施历史；基线 `46ee146` 之后的最终 Doxygen 审查修复仍只能修改 C++ 注释以及本设计/计划 Markdown，不得再改 C++ 行为 token。

---

### Task 1: Awaitable、Channel、Generator 与调度器注释

**Files:**
- Modify: `coro/await/awaitable.hpp`
- Modify: `coro/await/generator.hpp`
- Modify: `coro/detail/fiberchannel.hpp`
- Modify: `coro/executor/scheduler/qtfiberscheduler.cpp`

**Interfaces:**
- Consumes: `AwaitableCloseGuard`, `Awaitable<T>`, `FiberChannel<T>`, `generate(std::shared_ptr<Awaitable<T>>)` 和 `QtFiberScheduler::suspend_until()` 的现有实现。
- Produces: 上述类型和函数的中文 Doxygen 生命周期、终止错误、轮询取消及首次调度语义说明。

- [ ] **Step 1: 补充关闭守卫和 Awaitable 契约**

  为 `AwaitableCloseGuard`、`set()`、`run()`、`Awaitable<T>` 与 `Awaitable<void>` 的相关接口补充下列信息：

  ```cpp
  /**
   * @brief 管理 Awaitable 的一次性终止清理回调。
   * @details 首次显式关闭或最后一个共享守卫析构时执行清理。回调在互斥锁外调用，
   *          避免清理过程重入时发生死锁。
   * @note 多次调用 run() 只会执行一次清理。
   */
  ```

  `setOnClose()` 还需说明替换旧回调时旧回调会在锁外立即执行；`close(error)` 说明只有首次关闭原因可被消费者观察。

- [ ] **Step 2: 补充 channel 终止状态注释**

  为 `pop_wait_for()`、`close(std::error_code)`、`close_error()`、`discard_pending()` 及新增字段说明超时与关闭的区别、首次错误保留规则，以及丢弃排队值不改变关闭状态。

- [ ] **Step 3: 补充共享 Awaitable 生成器注释**

  为 `Yield::is_closed()` 和 `generate(std::shared_ptr<Awaitable<T>>)` 说明共享句柄的强持有、10 ms 有界轮询仅用于感知输出端取消、源超时不会终止仍开放的流、空句柄产生空流。

- [ ] **Step 4: 补充 Qt 调度泵首次启动注释**

  在 `QtFiberScheduler::suspend_until()` 的 `startedPump` 分支前用 Doxygen 行注释解释首次调用先启动 Qt 事件泵并立即返回，避免在事件泵尚未运行时进入基类休眠。

- [ ] **Step 5: 验证 Task 1 只修改注释**

  Run: `git diff --word-diff=porcelain -- coro/await/awaitable.hpp coro/await/generator.hpp coro/detail/fiberchannel.hpp coro/executor/scheduler/qtfiberscheduler.cpp`

  Expected: 新增内容仅为 Doxygen 注释标记和中文正文；既有 C++ token 不变。

---

### Task 2: Socket 包装器和内部辅助代码注释

**Files:**
- Modify: `coro/await/detail/socketawait.hpp`
- Modify: `coro/await/detail/socketerror.hpp`
- Modify: `coro/await/corosocket.hpp`
- Modify: `coro/await/corolocalsocket.hpp`
- Modify: `coro/await/corotcpserver.hpp`
- Modify: `coro/await/corolocalserver.hpp`
- Modify: `coro/await/coroudpsocket.hpp`
- Modify: `coro/await/corosslsocket.hpp`

**Interfaces:**
- Consumes: 现有 TCP、local socket、TCP/local server、UDP、SSL wrapper 和 `detail` 生命周期/错误适配函数。
- Produces: 每个类、构造函数、公开 awaitable 工厂、`coro()` 重载和关键私有辅助函数的完整中文 Doxygen 契约。

- [ ] **Step 1: 注释 socket 生命周期注册表**

  为 `SocketConnectionRegistry` 及其方法说明注册与清理可并发发生；清理开始后的新连接会立即断开，新回调会立即执行；连接和清理回调均移出锁后调用。为 `SocketConnections`、各注册函数、`socket_awaitable()` 与 `bind_socket_lifecycle()` 记录参数、返回值及 source/application 销毁时的关闭行为。

- [ ] **Step 2: 注释 Qt 错误类别适配**

  为三个 `std::error_category` 类和六个类别/错误码函数补充中文 Doxygen，说明 category 名称稳定、枚举整数值被保留、错误消息来自 Qt 语义，并区分传输、本地 socket 与 TLS 错误域。

- [ ] **Step 3: 注释 TCP 与 local stream wrapper**

  每个类注释必须包含：wrapper 不拥有源对象、调用在对象线程执行或排队、返回 shared awaitable、回调强捕获、超时不取消。为 `readAll()` 说明它持续发出非空字节块直至关闭；为 wait/connect/disconnect 方法说明成功条件和错误结果；为 `coro()` 说明空指针不会取得所有权，且之后调用 wrapper 操作会返回立即以默认 `no_message` 正常关闭的 Awaitable；不应把 wrapper 本身描述为“可关闭”。

- [ ] **Step 4: 注释 TCP 与 local server wrapper**

  为 `nextConnection()` 说明它是连接流，会先排空 pending 队列；原始 socket 指针仍由 Qt server 管理，消费者不得令其越过 server 生命周期并必须遵守线程亲和。解释 10 ms 定时器用于检测无停止信号的 `close()`，而不是连接超时。

- [ ] **Step 5: 注释 UDP 与 SSL wrapper**

  `receiveDatagram()` 说明每个值对应一个完整数据报并保留发送端元数据；socket 关闭、销毁或报错时流终止。SSL 方法说明 `waitForEncrypted()` 只等待当前握手，`connectToHostEncrypted()` 发起连接和握手，传输错误使用 socket error category，证书/握手错误使用 SSL category。

- [ ] **Step 6: 验证 Task 2 公开接口覆盖**

  Run: `rg -n '^class Coro|^    std::shared_ptr<Awaitable|^inline Coro|^class .*ErrorCategory|^inline .*error_(category|code)' coro/await`

  Expected: 每个匹配声明的紧邻上方均存在中文 Doxygen 块；私有线程切换和等待辅助函数也有契约注释。

---

### Task 3: Socket 示例与测试注释

**Files:**
- Modify: `example/socket_pingpong/main.cpp`
- Modify: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: socket ping-pong 示例和 `786f05a..67b71a7` 新增的 socket 测试辅助类及测试槽函数。
- Produces: 示例失败路径、测试资源所有权和各回归目标的中文 Doxygen 说明。

- [ ] **Step 1: 完善示例注释**

  将文件级英文注释改为中文 Doxygen。为端口探测、服务端任务、客户端任务和主函数说明：所有外部等待均有界；每个 `Result` 必须检查；第一次短等待演示超时但不会取消 read stream；释放的端口用于稳定演示连接拒绝。

- [ ] **Step 2: 注释测试辅助类**

  为 `LocalServerNameGuard`、`SslLoopbackServer`、`PlainTextServer` 及其关键成员补充中文 Doxygen，说明临时资源清理、证书/私钥所有权和测试服务器行为。

- [ ] **Step 3: 注释新增 socket 测试槽函数**

  根据函数名和断言，为从基线后新增或实质修改的 TCP、local、UDP、SSL、共享 awaitable、关闭清理、超时与错误传播测试添加 `@brief` 和必要的 `@details`。注释描述被验证的不变量，不逐行翻译测试实现。

- [ ] **Step 4: 验证示例和测试注释覆盖**

  Run: `git diff 786f05a..67b71a7 --unified=0 -- test/testfiberawait/tst_testfiberawait.cpp | rg '^\+void TestFiberAwait::test_case_|^\+\s+void test_case_'`

  Expected: 每个新增 socket 测试函数在当前文件中都有紧邻的中文 Doxygen 注释。

---

### Task 4: 统一复核与验证

**Files:**
- Review: `coro/await/*.hpp`
- Review: `coro/await/detail/*.hpp`
- Review: `coro/detail/fiberchannel.hpp`
- Review: `coro/executor/scheduler/qtfiberscheduler.cpp`
- Review: `example/socket_pingpong/main.cpp`
- Review: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: Tasks 1-3 的注释改动。
- Produces: 术语一致、Doxygen 格式有效的最终系列；其中明确标记上述两项用户批准的行为补全，并确保 `46ee146` 之后的最终审查修复不改变 C++ 行为 token。

- [ ] **Step 1: 检查中文和 Doxygen 格式**

  Run: `rg -n '/\*\*|///|@brief|@details|@param|@return|@note|@warning|@tparam' coro/await coro/detail/fiberchannel.hpp coro/executor/scheduler/qtfiberscheduler.cpp example/socket_pingpong/main.cpp test/testfiberawait/tst_testfiberawait.cpp`

  Expected: 新增注释为中文完整句，标签参数与实际签名一致，没有无主的 `@param` 或错误的返回值描述。

- [ ] **Step 2: 检查差异与空白**

  Run: `git diff --check`

  Expected: 无输出，退出码为 0。

- [ ] **Step 3: 干净构建测试和示例**

  Run: 分别在 `/tmp` 新建构建目录，执行 `qmake` 和 `make -j2` 构建 `test/testfiberawait/testfiberawait.pro` 与 `example/socket_pingpong/socket_pingpong.pro`。

  Expected: 两个目标均构建成功；只允许既有 range-loop copy 警告。

- [ ] **Step 4: 运行完整测试**

  Run: `LSAN_OPTIONS=detect_leaks=0 timeout 90s <test-build>/testfiberawait -maxwarnings 0`

  Expected: `57 passed, 0 failed, 0 skipped`。

- [ ] **Step 5: 运行 socket 示例**

  Run: `timeout 20s <example-build>/socket_pingpong`

  Expected: 输出监听端口、收到 `ping`、连接拒绝信息、流关闭和 `socket ping-pong passed`，退出码为 0。

- [ ] **Step 6: 提交注释改动**

  该步骤保留原始提交计划的历史记录。用户批准的 UDP 行为与回归测试、以及示例的 20 ms timeout-then-resume 展示已分别在后续提交完成；最终审查修复使用 `docs: complete Doxygen lifecycle contracts`。

  ```bash
  git add -- coro/await/awaitable.hpp coro/await/generator.hpp \
    coro/await/detail/socketawait.hpp coro/await/detail/socketerror.hpp \
    coro/await/corosocket.hpp coro/await/corolocalsocket.hpp \
    coro/await/corotcpserver.hpp coro/await/corolocalserver.hpp \
    coro/await/coroudpsocket.hpp coro/await/corosslsocket.hpp \
    coro/detail/fiberchannel.hpp \
    coro/executor/scheduler/qtfiberscheduler.cpp \
    example/socket_pingpong/main.cpp test/testfiberawait/tst_testfiberawait.cpp \
    docs/superpowers/plans/2026-07-21-doxygen-comments.md
  git commit -m "docs: add Chinese Doxygen comments to socket code"
  ```

  Expected: 提交不包含 `docs/research/` 或其他用户文件。
