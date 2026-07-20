# Socket Awaitable Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete TCP, local socket, SSL, and UDP Awaitable wrappers with shared ownership, distinct timeout/close/error results, tests, documentation, and runnable examples.

**Architecture:** Socket factories return `std::shared_ptr<Awaitable<T>>`, and Qt callbacks strongly capture that pointer and call `resolve()` only for successful values. `FiberChannel` owns the terminal `std::error_code`, so `Awaitable` can distinguish success, timeout, normal close, and Qt socket failure without introducing a Resolver type. Focused headers implement each socket family while shared detail headers centralize error conversion and connection cleanup.

**Tech Stack:** C++17, Qt 5.15 Core/Network/SSL, Boost.Fiber 1.89, qmake, QtTest, AddressSanitizer.

## Global Constraints

- Preserve all existing by-value `Awaitable`, `await`, `await_for`, and `generate` interfaces.
- Qt signal callbacks strongly capture `std::shared_ptr<Awaitable<T>>`; do not introduce Resolver/Producer handles or weak captures.
- `resolve()` carries successful values only; `close()` is normal termination and `close(error)` is failed termination.
- `await_for()` timeout must not close the Awaitable or cancel the underlying socket operation.
- QObject methods run on the object's owning thread; do not add direct cross-thread socket operations.
- Target gcc >= 9.0, C++17, Qt >= 5.12, Boost >= 1.89, Linux x86_64.
- Keep `docs/research/qt-blocking-apis-awaitable.md` outside feature commits unless the user explicitly requests otherwise.

---

## File Map

- Modify `coro/detail/fiberchannel.hpp`: terminal error storage, first-close-wins, and true timeout status.
- Modify `coro/await/awaitable.hpp`: error close and shared-pointer consumption overloads.
- Modify `coro/await/generator.hpp`: shared-pointer Awaitable stream adapter.
- Create `coro/await/detail/socketerror.hpp`: Qt socket/local/SSL `std::error_category` conversion.
- Create `coro/await/detail/socketawait.hpp`: shared callback/connection registration helpers.
- Modify `coro/await/corosocket.hpp`: complete QAbstractSocket/TCP methods.
- Modify `coro/await/corotcpserver.hpp`: shared Awaitable and accept errors.
- Modify `coro/await/corolocalsocket.hpp`: complete local socket methods.
- Create `coro/await/corolocalserver.hpp`: local server connection stream.
- Create `coro/await/coroudpsocket.hpp`: datagram-preserving receive stream.
- Create `coro/await/corosslsocket.hpp`: encrypted connection waits.
- Modify `coro/await/coro.hpp`, `coro/all.hpp`, and `AsyncTask.pri`: export new headers.
- Modify `test/testfiberawait/tst_testfiberawait.cpp`: terminal, pointer, TCP, local, UDP, SSL, and lifetime tests.
- Create `test/testfiberawait/data/server-cert.pem` and `test/testfiberawait/data/server-key.pem`: local SSL fixture.
- Modify `test/testfiberawait/testfiberawait.pro`: expose SSL fixture paths to tests.
- Modify `example/socket_pingpong/main.cpp`: checked Result/timeout/error example.
- Modify `ReadMe.md`, `doc/架构设计.md`, `doc/软件设计说明.md`, and `skill/using-asynctask/SKILL.md`: public contract.

---

### Task 1: Terminal Error and Timeout Semantics

**Files:**
- Modify: `coro/detail/fiberchannel.hpp`
- Modify: `coro/await/awaitable.hpp`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Produces: `FiberChannel<T>::close(std::error_code)`, `FiberChannel<T>::close_error()`, `Awaitable<T>::close(std::error_code)`.
- Preserves: `close()` defaults to `std::errc::no_message` and queued values drain before terminal error.

- [ ] **Step 1: Add failing terminal-state tests**

Add QtTest slots `test_case_channel_terminal_error()` and `test_case_await_timeout_then_value()`. The first must assert normal close returns `no_message`, error close returns `connection_refused`, queued value is returned before the terminal error, and a second close does not replace the first. The second must use `await_for(a, 5ms)`, assert `timed_out`, then resolve `42` and assert a later `await(a)` returns `42`.

```cpp
void TestFiberAwait::test_case_channel_terminal_error()
{
    Coro::Awaitable<int> normal;
    normal.close();
    QCOMPARE(normal.await().error(), std::make_error_code(std::errc::no_message));

    Coro::Awaitable<int> failed;
    failed.resolve(7);
    failed.close(std::make_error_code(std::errc::connection_refused));
    failed.close(std::make_error_code(std::errc::timed_out));
    QCOMPARE(failed.await().value(), 7);
    QCOMPARE(failed.await().error(), std::make_error_code(std::errc::connection_refused));
}

void TestFiberAwait::test_case_await_timeout_then_value()
{
    Coro::Awaitable<int> value;
    auto timeout = Coro::await_for(value, std::chrono::milliseconds(5));
    QCOMPARE(timeout.error(), std::make_error_code(std::errc::timed_out));
    QVERIFY(value.resolve(42));
    QCOMPARE(Coro::await(value).value(), 42);
}
```

- [ ] **Step 2: Run the tests and verify red**

Run:

```bash
mkdir -p /tmp/asynctask-socket-build
qmake test/testfiberawait/testfiberawait.pro -o /tmp/asynctask-socket-build/Makefile
make -C /tmp/asynctask-socket-build -j2
LSAN_OPTIONS=detect_leaks=0 /tmp/asynctask-socket-build/testfiberawait test_case_channel_terminal_error test_case_await_timeout_then_value
```

Expected: compilation fails because `Awaitable::close(std::error_code)` does not exist, or the timeout/close assertions fail.

- [ ] **Step 3: Implement first-terminal-wins and timeout status**

In `FiberChannel<T>`, store `std::error_code close_error_{std::make_error_code(std::errc::no_message)}` under `mtx_`. `close(error)` must lock, return immediately when already closed, save a non-empty error (or `no_message`), then set `closed_` and notify. `pop_wait_for()` must inspect the boolean returned by `cv_consumer_.wait_for`; return `channel_op_status::timeout` when the predicate was not satisfied and `closed` only when the queue is empty after a real close.

In both Awaitable specializations, map channel status using one helper rule:

```cpp
if (status == boost::fibers::channel_op_status::success) return value;
if (status == boost::fibers::channel_op_status::timeout)
    return std::make_error_code(std::errc::timed_out);
return ch_->close_error();
```

- [ ] **Step 4: Run focused tests and verify green**

Run the command from Step 2. Expected: both selected slots pass with exit code 0.

- [ ] **Step 5: Commit the terminal semantics**

```bash
git add coro/detail/fiberchannel.hpp coro/await/awaitable.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "fix(await): distinguish timeout close and terminal errors"
```

---

### Task 2: Shared-Pointer Awaitable Consumption

**Files:**
- Modify: `coro/await/awaitable.hpp`
- Modify: `coro/await/generator.hpp`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: terminal semantics from Task 1.
- Produces: `await(shared_ptr)`, `await_for(shared_ptr, duration)`, and `generate(shared_ptr)`.

- [ ] **Step 1: Add failing pointer-consumption tests**

Add a slot that verifies value, timeout-then-value, streaming close, and null pointers:

```cpp
void TestFiberAwait::test_case_shared_awaitable()
{
    auto once = std::make_shared<Coro::Awaitable<int>>();
    once->resolve(9);
    QCOMPARE(Coro::await(once).value(), 9);

    std::shared_ptr<Coro::Awaitable<int>> nullAwaitable;
    QCOMPARE(Coro::await(nullAwaitable).error(),
             std::make_error_code(std::errc::invalid_argument));

    auto stream = std::make_shared<Coro::Awaitable<int>>();
    stream->resolve(1);
    stream->resolve(2);
    stream->close();
    int total = 0;
    for (int value : Coro::generate(stream)) total += value;
    QCOMPARE(total, 3);
}
```

- [ ] **Step 2: Verify the new test does not compile**

Run the Task 1 build command with `test_case_shared_awaitable`. Expected: no matching `await`/`generate` overload.

- [ ] **Step 3: Implement pointer overloads**

Add const-reference shared-pointer overloads for one-shot consumption and a by-value shared-pointer overload for generation. Null pointers return `invalid_argument`; `generate(nullptr)` must return a closed generator rather than dereference null. The generator lambda holds the shared pointer strongly until iteration ends.

- [ ] **Step 4: Run the pointer test and all pre-socket tests**

Expected: `test_case_shared_awaitable`, awaiter, generator, signal, and IODevice tests pass.

- [ ] **Step 5: Commit**

```bash
git add coro/await/awaitable.hpp coro/await/generator.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): consume shared awaitable handles"
```

---

### Task 3: Shared Socket Error and Lifecycle Helpers

**Files:**
- Create: `coro/await/detail/socketerror.hpp`
- Create: `coro/await/detail/socketawait.hpp`
- Modify: `AsyncTask.pri`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Produces: `detail::socket_error_code(QAbstractSocket::SocketError)`, `detail::local_socket_error_code(QLocalSocket::LocalSocketError)`, and reusable connection cleanup.
- Error values retain the original Qt enum integer and a stable category name.

- [ ] **Step 1: Add failing conversion and lifetime tests**

Assert `ConnectionRefusedError` maps to an error whose value equals the Qt enum, category is `qt.socket`, and message is non-empty. Add an analogous local socket assertion. Add a `QPointer`/`weak_ptr` test showing a signal lambda strongly retains an Awaitable until its sender is deleted.

- [ ] **Step 2: Verify red**

Build the focused test. Expected: missing `socketerror.hpp` and conversion functions.

- [ ] **Step 3: Implement focused helpers**

Define stateless `std::error_category` subclasses with function-local singleton accessors. Messages switch over every Qt 5.15 enum value and fall back to `unknown socket error`. `socketawait.hpp` must create shared connection handles and a cleanup callback that disconnects every registered connection; it must not own QObject pointers beyond `QPointer` checks.

- [ ] **Step 4: Verify conversion and lifetime tests pass**

Expected: category/value/message assertions and reference release after sender deletion pass.

- [ ] **Step 5: Commit**

```bash
git add coro/await/detail/socketerror.hpp coro/await/detail/socketawait.hpp AsyncTask.pri test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): add Qt socket error and lifecycle helpers"
```

---

### Task 4: Complete TCP Socket and Server Wrappers

**Files:**
- Modify: `coro/await/corosocket.hpp`
- Modify: `coro/await/corotcpserver.hpp`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Produces: all `CoroAbstractSocket` methods from design section 6.1 and shared `CoroTcpServer::nextConnection()`.
- Returns: `std::shared_ptr<Awaitable<T>>` from every socket method.

- [ ] **Step 1: Replace the monolithic stress test with deterministic failing TCP slots**

Add separate slots for successful loopback ping-pong, connection refusal, disconnect completion, and server connection streaming. Bind servers to port `0`, read `serverPort()`, and use calls such as `await_for(coro(client).waitForConnected(), std::chrono::seconds(2))` for every one-shot wait. The refusal test connects to a port obtained from a temporary server that is closed before connecting, then asserts the error category is `qt.socket`.

- [ ] **Step 2: Verify red**

Expected: current methods return values rather than shared pointers, omit bytes-written/disconnect actions, and connection failure collapses to `no_message` or timeout.

- [ ] **Step 3: Implement TCP wrappers**

For each method, connect success, `errorOccurred`, source destroyed, and application shutdown before checking current state. `readAll()` drains existing bytes immediately and closes normally on `RemoteHostClosedError`/`disconnected`; other errors use `close(socket_error_code(error))`. Add both `connectToHost` overloads and `disconnectFromHost()`.

- [ ] **Step 4: Run all TCP slots**

Expected: successful ping-pong preserves bytes, refusal returns `qt.socket`, disconnect resolves, and server generator closes after server deletion.

- [ ] **Step 5: Commit**

```bash
git add coro/await/corosocket.hpp coro/await/corotcpserver.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): complete TCP socket awaitables"
```

---

### Task 5: Complete Local Socket and Server Wrappers

**Files:**
- Modify: `coro/await/corolocalsocket.hpp`
- Create: `coro/await/corolocalserver.hpp`
- Modify: `coro/await/coro.hpp`
- Modify: `coro/all.hpp`
- Modify: `AsyncTask.pri`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Produces: local socket methods from design section 6.3 and `CoroLocalServer::nextConnection()`.

- [ ] **Step 1: Add failing local socket tests**

Use a server name formed from `QCoreApplication::applicationPid()` and a counter. Remove any stale name before and after the test. Assert connect, write/read, disconnect, connection stream, and missing-server client error behavior with finite waits. Check `QLocalServer::listen()` synchronously because Qt 5.15 exposes no asynchronous accept-error signal.

- [ ] **Step 2: Verify red**

Expected: missing local server wrapper and missing local read/write/disconnect methods.

- [ ] **Step 3: Implement local wrappers and exports**

Mirror TCP completion/error handling using `QLocalSocket::LocalSocketError`. `CoroLocalServer::nextConnection()` drains all pending local connections and closes normally on server destruction/application exit; it does not invent a server error signal that Qt 5.15 does not provide. Export the header through both umbrella headers and qmake.

- [ ] **Step 4: Run all local socket tests**

Expected: all local operations pass and the server name is removed even after assertion-safe cleanup.

- [ ] **Step 5: Commit**

```bash
git add coro/await/corolocalsocket.hpp coro/await/corolocalserver.hpp coro/await/coro.hpp coro/all.hpp AsyncTask.pri test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): add complete local socket awaitables"
```

---

### Task 6: Add Datagram-Preserving UDP Wrapper

**Files:**
- Create: `coro/await/coroudpsocket.hpp`
- Modify: `coro/await/coro.hpp`
- Modify: `coro/all.hpp`
- Modify: `AsyncTask.pri`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Produces: `CoroUdpSocket::receiveDatagram() -> shared_ptr<Awaitable<QNetworkDatagram>>` and `coro(QUdpSocket*)`.

- [ ] **Step 1: Add a failing two-datagram test**

Bind receiver and sender to loopback ephemeral ports, create the Awaitable before writes, send payloads `first` and `second`, await two values, and assert payloads are separate and both sender ports equal `sender.localPort()`.

- [ ] **Step 2: Verify red**

Expected: `coro(QUdpSocket*)` resolves to the generic wrapper and has no `receiveDatagram()`.

- [ ] **Step 3: Implement UDP receive stream**

Register `readyRead`, socket error, destruction, and shutdown connections. The delivery callback must execute:

```cpp
while (socket && socket->hasPendingDatagrams()) {
    QNetworkDatagram datagram = socket->receiveDatagram();
    if (datagram.isValid()) awaitable->resolve(datagram);
}
```

Call it once immediately after connections are established.

- [ ] **Step 4: Run UDP and regression socket tests**

Expected: two datagrams remain separate with correct metadata; TCP/local tests still pass.

- [ ] **Step 5: Commit**

```bash
git add coro/await/coroudpsocket.hpp coro/await/coro.hpp coro/all.hpp AsyncTask.pri test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): add UDP datagram receive stream"
```

---

### Task 7: Add SSL Socket Wrapper

**Files:**
- Create: `coro/await/corosslsocket.hpp`
- Modify: `coro/await/coro.hpp`
- Modify: `coro/all.hpp`
- Modify: `AsyncTask.pri`
- Create: `test/testfiberawait/data/server-cert.pem`
- Create: `test/testfiberawait/data/server-key.pem`
- Modify: `test/testfiberawait/testfiberawait.pro`
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Produces: `waitForEncrypted()` and `connectToHostEncrypted()` on `CoroSslSocket`.

- [ ] **Step 1: Add failing SSL success and failure tests**

Use a loopback `QTcpServer`; wrap accepted descriptors in a server-mode `QSslSocket` configured with the checked-in PEM certificate and key. Configure the client to trust that certificate, await encryption on both peers, and exchange one payload. A second test connects `QSslSocket` to a plain server and asserts a finite handshake error.

- [ ] **Step 2: Verify red**

Expected: missing `CoroSslSocket` and encrypted wait methods.

- [ ] **Step 3: Implement SSL wrapper**

Success is `encrypted()` or an already-encrypted state. Socket error, `sslErrors`, and `peerVerifyError` close with the first error. `connectToHostEncrypted()` creates the wait before initiating the Qt call. Do not automatically ignore certificate errors in production code.

- [ ] **Step 4: Run SSL tests when `QSslSocket::supportsSsl()`**

Expected: success and error slots pass; when the runtime has no SSL backend, tests call `QSKIP` with a diagnostic message.

- [ ] **Step 5: Commit**

```bash
git add coro/await/corosslsocket.hpp coro/await/coro.hpp coro/all.hpp AsyncTask.pri test/testfiberawait/testfiberawait.pro test/testfiberawait/tst_testfiberawait.cpp test/testfiberawait/data/server-cert.pem test/testfiberawait/data/server-key.pem
git commit -m "feat(await): add SSL handshake awaitables"
```

---

### Task 8: Documentation, Example, and Full Verification

**Files:**
- Modify: `example/socket_pingpong/main.cpp`
- Modify: `ReadMe.md`
- Modify: `doc/架构设计.md`
- Modify: `doc/软件设计说明.md`
- Modify: `skill/using-asynctask/SKILL.md`

**Interfaces:**
- Documents all public interfaces produced by Tasks 1-7.

- [ ] **Step 1: Update the socket example**

Use ephemeral server port, `connectToHost()` Awaitable, finite `await_for` calls, checked `Result`, bytes-written wait, deterministic connection failure, and coordinated server shutdown before `quit()`. Comments must explain strong shared ownership, timeout-not-canceling, and generator close behavior.

- [ ] **Step 2: Update public documentation and Skill**

Document shared-pointer return semantics, `close(error)`, timeout distinction, all wrapper methods, UDP boundaries, SSL errors, local server support, and QObject thread affinity. Remove statements that socket Awaitables are move-only values.

- [ ] **Step 3: Build the complete test target and example from clean temporary directories**

Run:

```bash
mkdir -p /tmp/asynctask-socket-final-test /tmp/asynctask-socket-final-example
qmake test/testfiberawait/testfiberawait.pro -o /tmp/asynctask-socket-final-test/Makefile
make -C /tmp/asynctask-socket-final-test -j2
LSAN_OPTIONS=detect_leaks=0 timeout 60s /tmp/asynctask-socket-final-test/testfiberawait -maxwarnings 0
qmake example/socket_pingpong/socket_pingpong.pro -o /tmp/asynctask-socket-final-example/Makefile
make -C /tmp/asynctask-socket-final-example -j2
timeout 20s /tmp/asynctask-socket-final-example/socket_pingpong
```

Expected: both builds exit 0, QtTest reports zero failed tests, and the example exits 0 within 20 seconds.

- [ ] **Step 4: Run static consistency checks**

```bash
rg -n "Coro(LocalServer|UdpSocket|SslSocket)|close\(std::error_code|shared_ptr<Awaitable" coro ReadMe.md doc skill example
git diff --check
git status --short
```

Expected: new APIs appear in code and documentation, `git diff --check` prints nothing, and status lists only intended feature files plus the pre-existing untracked `docs/research/` report.

- [ ] **Step 5: Commit documentation and example**

```bash
git add example/socket_pingpong/main.cpp ReadMe.md doc/架构设计.md doc/软件设计说明.md skill/using-asynctask/SKILL.md
git commit -m "docs: document complete socket awaitable APIs"
```

- [ ] **Step 6: Review final history and diff**

```bash
git log --oneline -10
git diff HEAD~8 --stat
```

Expected: one focused commit per task, no `docs/research/qt-blocking-apis-awaitable.md` in the feature history, and no unrelated files.
