# 共享 Awaitable（广播消费）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让一个 `Awaitable<T>` 能被多个消费者各自完整消费（每条消息被每个消费者处理一遍），同时完全保留现有的抢占式多消费者行为。

**Architecture:** 扇出放在 `FiberChannel::push` —— 因为生产者只捕获 `channel()`，从不持有 `Awaitable`。channel 内部维护一条镜像列表（`std::unique_ptr<std::vector<std::weak_ptr<FiberChannel<T>>>>`，未使用时为空指针），`push` / `close` / `discard_pending` 同步扇出到每条镜像。`Awaitable` 新增 `shared()`，返回一个普通的 `std::shared_ptr<Awaitable<T>>`，因此 `Coro::await` / `await_for` / `generate` 全部原样可用——不引入虚函数、不引入常驻 fiber。

**Tech Stack:** C++17、boost.fiber、Qt 5.15.13、qmake 3.1、QtTest（`test/testfiberawait` 开启了 AddressSanitizer）

## Global Constraints

- 只修改两个文件：`coro/detail/fiberchannel.hpp` 与 `coro/await/awaitable.hpp`。不新增头文件，不修改 `AsyncTask.pri`、不修改 `coro/all.hpp`。
- 未使用 `shared()` 的代码路径行为必须逐字节不变；`sizeof(FiberChannel<T>)` 从 160 增至 168 字节（`unique_ptr`，非 `vector`）。
- 新增测试一律使用 `test_case_broadcast_` 前缀。**不要复用 `test_case_shared_awaitable*` 这几个名字——它们已存在，测的是 `std::shared_ptr<Awaitable>` 的处理，与本功能无关。**
- 每个测试函数都要在 `TestFiberAwait` 的 `private slots:` 列表（`tst_testfiberawait.cpp:167-224`）中登记，位置放在 `void test_case_shared_awaitable_void();` 之后、`void test_case_socket_error_conversion();` 之前。
- `addMirror()` 必须保持 private + friend，绝不作为公开接口暴露（公开会让调用方构造出互为镜像的环，导致死锁）。
- 测试代码注释沿用现有风格：函数上方用 `/// @brief` 中文单行说明。

## 构建与运行命令

全部命令在仓库根目录 `/home/david/zpj/Framework-dev/AsyncTask` 下执行。

```bash
# 构建
cd test/testfiberawait/build && qmake ../testfiberawait.pro && make -j$(nproc)

# 跑单个用例
cd test/testfiberawait/build && ./testfiberawait test_case_broadcast_basic

# 跑全部用例
cd test/testfiberawait/build && ./testfiberawait
```

## File Structure

| 文件 | 职责 | 本计划中的改动 |
|---|---|---|
| `coro/detail/fiberchannel.hpp` | 跨线程/跨协程队列，本功能的扇出机制所在层 | 新增 `Awaitable` 前向声明、friend 声明、`mirrors_` 成员、私有 `addMirror()`；`push` / `close(error)` / `discard_pending` 各加扇出 |
| `coro/await/awaitable.hpp` | 面向使用者的等待器，本功能的接口层 | `Awaitable<T>` 与 `Awaitable<void>` 各新增 `shared()` |
| `test/testfiberawait/tst_testfiberawait.cpp` | 唯一测试落点 | 新增 9 个 `test_case_broadcast_*` 用例及其 slot 登记 |
| `doc/使用说明.md` | 用户指南 | 新增「广播消费」小节 |
| `doc/需求规格说明.md` / `doc/软件设计说明.md` | SRS / SDD | 补充共享消费的需求项与设计说明 |

---

### Task 1: 镜像登记与 push 扇出

建立扇出骨架：channel 侧的镜像列表 + `Awaitable<T>::shared()`。本任务完成后广播的主路径即可工作。

**Files:**
- Modify: `coro/detail/fiberchannel.hpp`（includes 第 4-9 行；`push()` 第 56-64 行；私有成员第 220-227 行）
- Modify: `coro/await/awaitable.hpp`（`Awaitable<T>` 的 `channel()` 第 149 行之后）
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: 现有 `FiberChannel<T>::push/pop/is_closed`、`Awaitable<T>::channel()`
- Produces:
  - `void FiberChannel<T>::addMirror(const std::shared_ptr<FiberChannel<T>>& mirror)` —— private，仅 `Awaitable` 可调用
  - `std::shared_ptr<Awaitable<T>> Awaitable<T>::shared()` —— 后续所有任务都依赖此签名

- [ ] **Step 1: 写失败的测试**

在 `tst_testfiberawait.cpp` 的 `private slots:` 列表中，`void test_case_shared_awaitable_void();` 之后加入三行：

```cpp
    void test_case_broadcast_basic();
    void test_case_broadcast_no_replay();
    void test_case_broadcast_raii_unsubscribe();
```

在 `void TestFiberAwait::test_case_socket_error_conversion()` 函数定义之前插入三个测试函数：

```cpp
/// @brief 验证每个 shared() 订阅者各自收到完整序列，且源队列保留全量。
void TestFiberAwait::test_case_broadcast_basic()
{
    Coro::Awaitable<int> source;
    auto first = source.shared();
    auto second = source.shared();

    QVERIFY(source.resolve(1));
    QVERIFY(source.resolve(2));
    QVERIFY(source.resolve(3));

    QCOMPARE(first->await().value(), 1);
    QCOMPARE(first->await().value(), 2);
    QCOMPARE(first->await().value(), 3);

    QCOMPARE(second->await().value(), 1);
    QCOMPARE(second->await().value(), 2);
    QCOMPARE(second->await().value(), 3);

    // 源队列不因订阅者消费而减少，直接消费者仍取得全量
    QCOMPARE(source.await().value(), 1);
    QCOMPARE(source.await().value(), 2);
    QCOMPARE(source.await().value(), 3);
}

/// @brief 验证订阅之前产生的值对订阅者不可见，且不影响源队列。
void TestFiberAwait::test_case_broadcast_no_replay()
{
    Coro::Awaitable<int> source;
    QVERIFY(source.resolve(1));
    QVERIFY(source.resolve(2));

    auto late = source.shared();
    QVERIFY(source.resolve(3));
    source.close();

    QCOMPARE(late->await().value(), 3);
    QCOMPARE(late->await().error(), std::make_error_code(std::errc::no_message));

    QCOMPARE(source.await().value(), 1);
    QCOMPARE(source.await().value(), 2);
    QCOMPARE(source.await().value(), 3);
}

/// @brief 验证订阅句柄析构即自动退订，且不影响其他订阅者。
void TestFiberAwait::test_case_broadcast_raii_unsubscribe()
{
    Coro::Awaitable<int> source;
    auto keep = source.shared();
    std::weak_ptr<Coro::Awaitable<int>> observed;
    {
        auto temporary = source.shared();
        observed = temporary;
        QVERIFY(source.resolve(1));
        QCOMPARE(temporary->await().value(), 1);
    }
    QVERIFY(observed.expired());

    // 失效槽位在下一次 push 时被剔除，存活订阅者不受影响
    QVERIFY(source.resolve(2));
    source.close();

    QCOMPARE(keep->await().value(), 1);
    QCOMPARE(keep->await().value(), 2);
    QCOMPARE(keep->await().error(), std::make_error_code(std::errc::no_message));
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

```bash
cd test/testfiberawait/build && qmake ../testfiberawait.pro && make -j$(nproc)
```

Expected: 编译失败，报错类似 `error: 'class Coro::Awaitable<int>' has no member named 'shared'`

- [ ] **Step 3: 在 `fiberchannel.hpp` 加入前向声明与头文件**

把文件顶部的 include 块（第 4-9 行）改为：

```cpp
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/exceptions.hpp>
#include <deque>
#include <memory>
#include <system_error>
#include <vector>
```

在 `namespace Coro {` 之后、`FiberChannel` 的文档注释之前，加入前向声明（`fiberchannel.hpp` 不 include `awaitable.hpp`，依赖方向相反，没有这行 friend 声明无法引用 `Awaitable`）：

```cpp
template<typename T> class Awaitable;
```

- [ ] **Step 4: 加入 `mirrors_` 成员与 `addMirror()`**

在 `FiberChannel` 的 `private:` 段（第 220 行起）末尾、`close_error_` 之后加入成员：

```cpp
    ///< 镜像通道列表；无人调用 shared() 时保持空指针，不产生堆分配
    std::unique_ptr<std::vector<std::weak_ptr<FiberChannel<T>>>> mirrors_;
```

在同一 `private:` 段中加入 friend 声明与 `addMirror()`：

```cpp
    template<typename U> friend class Awaitable;

    /**
     * @brief 注册一条镜像通道，此后每次 push 都会同步投递一份副本。
     * @details 源已关闭时不注册，直接以源首次关闭时记录的终止原因关闭该镜像，
     *          避免订阅者永久挂起。仅由 Awaitable::shared() 调用；保持内部可见，
     *          公开会让调用方构造出互为镜像的环从而死锁。
     * @param mirror 接收副本的镜像通道
     */
    void addMirror(const std::shared_ptr<FiberChannel<T>>& mirror){
        if(!mirror){
            return;
        }
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            const std::error_code error = close_error_;
            lck.unlock();
            mirror->close(error);
            return;
        }
        if(!mirrors_){
            mirrors_ = std::make_unique<std::vector<std::weak_ptr<FiberChannel<T>>>>();
        }
        mirrors_->push_back(mirror);
    }
```

- [ ] **Step 5: 在 `push()` 中加入扇出**

把 `push()`（第 56-64 行）整体替换为：

```cpp
    channel_status push(T value){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if ( BOOST_UNLIKELY( is_closed() ) ) {
            return channel_status::closed;
        }
        if(mirrors_){
            auto& list = *mirrors_;
            for(std::size_t i = 0; i < list.size(); ){
                if(auto mirror = list[i].lock()){
                    mirror->push(value);
                    ++i;
                }else{
                    // swap-and-pop：扇出顺序无关，剔除失效项 O(1)，不做 memmove
                    list[i] = std::move(list.back());
                    list.pop_back();
                }
            }
        }
        queue_.push_back(std::move(value));
        cv_consumer_.notify_one();
        return channel_status::success;
    }
```

- [ ] **Step 6: 在 `awaitable.hpp` 加入 `Awaitable<T>::shared()`**

在 `Awaitable<T>` 的 `channel()`（第 149 行）之后加入：

```cpp
    /**
     * @brief 注册一个共享订阅者，此后源产生的每个值都会同步复制一份投递给它。
     *
     * 返回的是普通 Awaitable，因此 Coro::await / await_for / generate 均原样可用。
     * 订阅者之间互为广播（各得全量），与直接 await 本对象的抢占式消费者也不竞争。
     * 不做 replay：本次调用之前已产生的值对订阅者不可见。订阅句柄析构即自动退订。
     * @return 共享订阅句柄；源已关闭时返回的句柄立即以源的终止原因收敛
     * @code
     * auto src = Coro::coro(sock).readAll();
     * auto sync = src->shared();      // 数据同步
     * auto audit = src->shared();     // 日志分发
     * Coro::makeTask([sync]{ while(auto c = Coro::await(sync)) apply(c.value()); return 0; });
     * Coro::makeTask([audit]{ for(const auto& c : Coro::generate(audit)) log(c); return 0; });
     * @endcode
     */
    std::shared_ptr<Awaitable<T>> shared(){
        auto sub = std::make_shared<Awaitable<T>>();
        if(ch_){
            ch_->addMirror(sub->channel());
        }
        return sub;
    }
```

- [ ] **Step 7: 运行测试，确认通过**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_basic test_case_broadcast_no_replay test_case_broadcast_raii_unsubscribe
```

Expected: `Totals: 5 passed, 0 failed`（3 个新用例 + `initTestCase` + `cleanupTestCase`）

- [ ] **Step 8: 提交**

```bash
git add coro/detail/fiberchannel.hpp coro/await/awaitable.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): add shared() broadcast subscription via channel mirrors"
```

---

### Task 2: close 扇出与关闭边界行为

终止必须传播到每条镜像，否则订阅者永久挂在 `await` 上。

**Files:**
- Modify: `coro/detail/fiberchannel.hpp`（`close(std::error_code)` 第 182-192 行）
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: Task 1 的 `addMirror()`、`Awaitable<T>::shared()`
- Produces: 无新签名；改变 `FiberChannel<T>::close(std::error_code)` 的行为——关闭时以**规范化后**的 `close_error_`（空错误码会被替换为 `no_message`）关闭全部镜像并清空镜像列表

- [ ] **Step 1: 写失败的测试**

在 `private slots:` 列表中 `void test_case_broadcast_raii_unsubscribe();` 之后加入：

```cpp
    void test_case_broadcast_terminal_error();
    void test_case_broadcast_subscribe_after_close();
    void test_case_broadcast_mirror_close_isolated();
```

在 `void TestFiberAwait::test_case_socket_error_conversion()` 之前插入：

```cpp
/// @brief 验证源关闭时终止原因传播给每个订阅者，且排队余量先于终止错误被消费。
void TestFiberAwait::test_case_broadcast_terminal_error()
{
    Coro::Awaitable<int> source;
    auto first = source.shared();
    auto second = source.shared();

    QVERIFY(source.resolve(7));
    source.close(std::make_error_code(std::errc::connection_reset));
    source.close(std::make_error_code(std::errc::timed_out));   // 首次错误不得被覆盖

    QCOMPARE(first->await().value(), 7);
    QCOMPARE(first->await().error(), std::make_error_code(std::errc::connection_reset));
    QCOMPARE(second->await().value(), 7);
    QCOMPARE(second->await().error(), std::make_error_code(std::errc::connection_reset));
}

/// @brief 验证对已关闭的源调用 shared() 时，返回的句柄立即收敛而不挂起。
void TestFiberAwait::test_case_broadcast_subscribe_after_close()
{
    using namespace std::chrono_literals;
    Coro::Awaitable<int> source;
    source.close(std::make_error_code(std::errc::connection_refused));

    auto late = source.shared();
    auto result = Coro::await_for(late, 50ms);
    QVERIFY(!result);
    QCOMPARE(result.error(), std::make_error_code(std::errc::connection_refused));
}

/// @brief 验证单个订阅者关闭只终止自己，源与其他订阅者不受影响。
void TestFiberAwait::test_case_broadcast_mirror_close_isolated()
{
    Coro::Awaitable<int> source;
    auto first = source.shared();
    auto second = source.shared();

    first->close(std::make_error_code(std::errc::operation_canceled));
    QVERIFY(source.resolve(5));
    source.close();

    QCOMPARE(first->await().error(), std::make_error_code(std::errc::operation_canceled));
    QCOMPARE(second->await().value(), 5);
    QCOMPARE(second->await().error(), std::make_error_code(std::errc::no_message));
    QCOMPARE(source.await().value(), 5);
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_terminal_error test_case_broadcast_subscribe_after_close
```

Expected: `test_case_broadcast_terminal_error` 失败——`first->await()` 取完 7 之后不会返回 `connection_reset`，而是卡住直到测试超时（close 尚未扇出）。

- [ ] **Step 3: 在 `close(std::error_code)` 中加入扇出**

把 `close(std::error_code)`（第 182-192 行）整体替换为：

```cpp
    void close(std::error_code error) noexcept {
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        if(closed_.load()){
            return;
        }
        close_error_ = error == std::error_code{}
                ? std::make_error_code(std::errc::no_message)
                : error;
        closed_.store(true);
        cv_consumer_.notify_all();
        // 终止必须传播，否则镜像的消费者会永久挂在 await 上
        if(mirrors_){
            for(auto& weak : *mirrors_){
                if(auto mirror = weak.lock()) mirror->close(close_error_);
            }
            mirrors_.reset();
        }
    }
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_terminal_error test_case_broadcast_subscribe_after_close test_case_broadcast_mirror_close_isolated
```

Expected: `Totals: 5 passed, 0 failed`

- [ ] **Step 5: 提交**

```bash
git add coro/detail/fiberchannel.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): propagate channel close to mirrors"
```

---

### Task 3: discard_pending 扇出（悬空指针防护）

`nextConnection()` 推送的 `QTcpSocket*` 是 `QTcpServer` 的子对象。server 析构时 Qt 连带删除它们，`discard_pending()` 负责清掉队列里即将悬空的指针。不扇出的话，镜像队列里会留下野指针——一个只在「server 先于消费者析构」时序下触发的 use-after-free。

**Files:**
- Modify: `coro/detail/fiberchannel.hpp`（`discard_pending()` 第 216-219 行）
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: Task 1、Task 2 的成果；`Coro::coro(server).nextConnection()`（现有工厂，`corotcpserver.hpp:119`）
- Produces: 无新签名；`FiberChannel<T>::discard_pending()` 改为同时清空全部镜像队列

- [ ] **Step 1: 写失败的测试**

在 `private slots:` 列表中 `void test_case_broadcast_mirror_close_isolated();` 之后加入：

```cpp
    void test_case_broadcast_server_destroy_purges_mirror();
```

在 `void TestFiberAwait::test_case_socket_error_conversion()` 之前插入：

```cpp
/// @brief 验证服务器销毁时镜像队列中的悬空连接指针一并被丢弃。
/// @details 排队的 QTcpSocket* 是 server 的子对象，server 析构会删除它们；
///          若 discard_pending 不扇出，订阅者会取到已删除对象（ASan 报 use-after-free）。
void TestFiberAwait::test_case_broadcast_server_destroy_purges_mirror()
{
    using namespace std::chrono_literals;
    auto server = new QTcpServer;
    QVERIFY(server->listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(server).nextConnection();
    auto audit = incoming->shared();
    QSignalSpy connectionSignal(server, &QTcpServer::newConnection);

    QTcpSocket client;
    client.connectToHost(QHostAddress::LocalHost, server->serverPort());
    QTRY_COMPARE_WITH_TIMEOUT(client.state(), QAbstractSocket::ConnectedState, 2000);
    QTRY_VERIFY_WITH_TIMEOUT(connectionSignal.count() > 0, 2000);
    QVERIFY(!server->hasPendingConnections());

    delete server;   // 子 QTcpSocket 一并删除，两侧队列中的指针全部悬空

    auto mirrored = Coro::await_for(audit, 100ms);
    QVERIFY(!mirrored);
    QCOMPARE(mirrored.error(), std::make_error_code(std::errc::no_message));

    auto finished = Coro::await_for(incoming, 100ms);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_server_destroy_purges_mirror
```

Expected: 失败。`Coro::await_for(audit, 100ms)` 会返回一个**值**（那个已被删除的 `QTcpSocket*`）而非 `no_message`，`QVERIFY(!mirrored)` 因此失败；AddressSanitizer 也可能先一步报 heap-use-after-free。两种表现都算「按预期失败」。

- [ ] **Step 3: 在 `discard_pending()` 中加入扇出**

把 `discard_pending()`（第 216-219 行）整体替换为：

```cpp
    void discard_pending(){
        std::unique_lock<boost::fibers::mutex> lck{mtx_};
        queue_.clear();
        // 镜像里存的是同一批即将悬空的值，必须一并丢弃
        if(mirrors_){
            for(auto& weak : *mirrors_){
                if(auto mirror = weak.lock()) mirror->discard_pending();
            }
        }
    }
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_server_destroy_purges_mirror
```

Expected: `Totals: 3 passed, 0 failed`，且无 AddressSanitizer 报告

- [ ] **Step 5: 提交**

```bash
git add coro/detail/fiberchannel.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "fix(await): purge mirror queues on discard_pending"
```

---

### Task 4: `Awaitable<void>` 特化

`Awaitable<void>` 内部用 `FiberChannel<int>` 承载「事件发生一次」。`FiberChannel` 的改动是模板级的，`FiberChannel<int>` 已自动具备扇出能力，本任务只需补接口。

**Files:**
- Modify: `coro/await/awaitable.hpp`（`Awaitable<void>` 的 `channel()` 第 316 行之后）
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: Task 1-3 的 channel 侧能力
- Produces: `std::shared_ptr<Awaitable<void>> Awaitable<void>::shared()`

- [ ] **Step 1: 写失败的测试**

在 `private slots:` 列表中 `void test_case_broadcast_server_destroy_purges_mirror();` 之后加入：

```cpp
    void test_case_broadcast_void();
```

在 `void TestFiberAwait::test_case_socket_error_conversion()` 之前插入：

```cpp
/// @brief 验证 void 特化的广播：每个订阅者各自收到全部事件与终止原因。
void TestFiberAwait::test_case_broadcast_void()
{
    Coro::Awaitable<void> source;
    auto first = source.shared();
    auto second = source.shared();

    QVERIFY(source.resolve());
    QVERIFY(source.resolve());
    source.close(std::make_error_code(std::errc::connection_reset));

    QVERIFY(first->await().has_value());
    QVERIFY(first->await().has_value());
    QCOMPARE(first->await().error(), std::make_error_code(std::errc::connection_reset));

    QVERIFY(second->await().has_value());
    QVERIFY(second->await().has_value());
    QCOMPARE(second->await().error(), std::make_error_code(std::errc::connection_reset));
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

```bash
cd test/testfiberawait/build && make -j$(nproc)
```

Expected: 编译失败，`error: 'class Coro::Awaitable<void>' has no member named 'shared'`

- [ ] **Step 3: 加入 `Awaitable<void>::shared()`**

在 `Awaitable<void>` 的 `channel()`（第 316 行）之后加入：

```cpp
    /**
     * @brief 注册一个共享订阅者，此后每次 resolve() 都会同步通知它一次。
     *
     * 语义与 Awaitable<T>::shared() 相同：订阅者之间互为广播，与直接 await
     * 本对象的抢占式消费者不竞争，不做 replay，句柄析构即自动退订。
     * @return 共享订阅句柄；源已关闭时返回的句柄立即以源的终止原因收敛
     * @code
     * Coro::Awaitable<void> done;
     * auto watcher = done.shared();     // 与直接 await(done) 的消费者各得一份
     * @endcode
     */
    std::shared_ptr<Awaitable<void>> shared(){
        auto sub = std::make_shared<Awaitable<void>>();
        if(ch_){
            ch_->addMirror(sub->channel());
        }
        return sub;
    }
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_void
```

Expected: `Totals: 3 passed, 0 failed`

- [ ] **Step 5: 提交**

```bash
git add coro/await/awaitable.hpp test/testfiberawait/tst_testfiberawait.cpp
git commit -m "feat(await): add shared() to Awaitable<void> specialization"
```

---

### Task 5: 抢占模型共存与跨线程消费

纯测试任务，不改产品代码。锁定两条设计性质：广播组与抢占式消费者互不干扰；订阅者可被其他线程上的 fiber 消费。

**Files:**
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: `Awaitable<T>::shared()`；`Coro::makeTask(func, Coro::Priority::Normal, Coro::Affinity::sticky())`（见 `tst_testfiberawait.cpp:245` 现有用法）；`Coro::QtFiberThread`（见 `example/thread_init`）；`Coro::msleep`
- Produces: 无

- [ ] **Step 1: 写失败的测试**

在 `private slots:` 列表中 `void test_case_broadcast_void();` 之后加入：

```cpp
    void test_case_broadcast_coexists_with_competing_consumers();
    void test_case_broadcast_cross_thread_consumers();
```

在 `void TestFiberAwait::test_case_socket_error_conversion()` 之前插入：

```cpp
/// @brief 验证广播组与抢占式消费者共存：直接消费者互抢且不重不漏，订阅者各得全量。
void TestFiberAwait::test_case_broadcast_coexists_with_competing_consumers()
{
    Coro::Awaitable<int> source;
    auto first = source.shared();
    auto second = source.shared();

    auto producer = Coro::makeTask([&source](){
        for(int i = 0; i < 50; i++){
            Coro::msleep(1);
            source.resolve(1);
        }
        source.close();
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    auto competingA = Coro::makeTask([&source](){
        int total{};
        while(auto value = source.await()) total += value.value();
        return total;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto competingB = Coro::makeTask([&source](){
        int total{};
        while(auto value = source.await()) total += value.value();
        return total;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    auto subscriberA = Coro::makeTask([first](){
        int total{};
        while(auto value = first->await()) total += value.value();
        return total;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto subscriberB = Coro::makeTask([second](){
        int total{};
        while(auto value = second->await()) total += value.value();
        return total;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    producer.get();
    const int competing = competingA.get().value() + competingB.get().value();
    TQVERIFY(competing == 50);                    // 两个直接消费者合起来不重不漏
    TQVERIFY(subscriberA.get().value() == 50);    // 每个订阅者各得全量
    TQVERIFY(subscriberB.get().value() == 50);
}

/// @brief 验证订阅者可被其他线程上的 fiber 消费，扇出跨线程投递正确。
void TestFiberAwait::test_case_broadcast_cross_thread_consumers()
{
    auto worker = new Coro::QtFiberThread();
    worker->start();
    QThread::msleep(50);

    Coro::Awaitable<int> source;
    auto first = source.shared();
    auto second = source.shared();

    auto subscriberA = Coro::makeTask([first](){
        int total{};
        while(auto value = first->await()) total += value.value();
        return total;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto subscriberB = Coro::makeTask([second](){
        int total{};
        while(auto value = second->await()) total += value.value();
        return total;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    auto producer = Coro::makeTask([&source](){
        for(int i = 0; i < 30; i++){
            Coro::msleep(1);
            source.resolve(2);
        }
        source.close();
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    producer.get();
    TQVERIFY(subscriberA.get().value() == 60);
    TQVERIFY(subscriberB.get().value() == 60);

    worker->quit();
    delete worker;
}
```

- [ ] **Step 2: 运行测试**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_coexists_with_competing_consumers test_case_broadcast_cross_thread_consumers
```

Expected: `Totals: 4 passed, 0 failed`。这两个用例应当**直接通过**——它们锁定的是 Task 1-4 已经实现的性质。若失败，说明前面任务的实现有缺陷，回到对应任务修复，不要修改测试的断言。

- [ ] **Step 3: 提交**

```bash
git add test/testfiberawait/tst_testfiberawait.cpp
git commit -m "test(await): cover broadcast coexistence with competing and cross-thread consumers"
```

---

### Task 6: socket 流端到端广播

真实场景验证：`readAll()` 上开两个订阅者，一个解析一个落日志，socket 断开后两条消费循环都自然收敛。

**Files:**
- Test: `test/testfiberawait/tst_testfiberawait.cpp`

**Interfaces:**
- Consumes: `Coro::coro(sock).readAll()`（`corosocket.hpp:213`）；`Coro::coro(server).nextConnection()`（`corotcpserver.hpp:119`）；`Awaitable<T>::shared()`
- Produces: 无

- [ ] **Step 1: 阅读现有的 socket 读取测试作为参照**

```bash
sed -n '937,964p' test/testfiberawait/tst_testfiberawait.cpp
```

`test_case_tcp_read_then_remote_close` 就是本任务的骨架：用普通 `QTcpServer` + `nextConnection()` 拿到对端 socket，写数据，再 `disconnectFromHost()`。下面的测试逐行沿用它，只在 `readAll()` 之后多开两个订阅者。

**不要使用 `PlainTextServer`**（`tst_testfiberawait.cpp:143`）——它是给 TLS 握手失败用例准备的，写完 `"not TLS"` 就丢弃 socket，没有暴露对端的访问器。

- [ ] **Step 2: 写测试**

在 `private slots:` 列表中 `void test_case_broadcast_cross_thread_consumers();` 之后加入：

```cpp
    void test_case_broadcast_tcp_read_stream();
```

在 `void TestFiberAwait::test_case_socket_error_conversion()` 之前插入：

```cpp
/// @brief 验证 socket 读取流的广播：两个订阅者各自收到完整字节流，远端关闭后一并收敛。
void TestFiberAwait::test_case_broadcast_tcp_read_stream()
{
    using namespace std::chrono_literals;
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(&server).nextConnection();

    QTcpSocket client;
    QVERIFY(Coro::await_for(Coro::coro(&client).connectToHost(
        QHostAddress::LocalHost, server.serverPort()), 2s));
    auto accepted = Coro::await_for(incoming, 2s);
    QVERIFY(accepted);
    QTcpSocket* peer = accepted.value();

    auto stream = Coro::coro(&client).readAll();
    auto parser = stream->shared();
    auto audit  = stream->shared();

    auto written = Coro::coro(peer).waitForBytesWritten();
    QCOMPARE(peer->write("final-bytes"), qint64(11));
    QVERIFY(Coro::await_for(written, 2s));
    peer->disconnectFromHost();

    // 两个订阅者各自收到完整字节流
    QCOMPARE(Coro::await_for(parser, 2s).value(), QByteArray("final-bytes"));
    QCOMPARE(Coro::await_for(audit, 2s).value(), QByteArray("final-bytes"));

    // 远端关闭 → 源流关闭 → 两个订阅者都收敛，不再挂起
    auto parserEnd = Coro::await_for(parser, 2s);
    QVERIFY(!parserEnd);
    QCOMPARE(parserEnd.error(), std::make_error_code(std::errc::no_message));
    auto auditEnd = Coro::await_for(audit, 2s);
    QVERIFY(!auditEnd);
    QCOMPARE(auditEnd.error(), std::make_error_code(std::errc::no_message));

    delete peer;
}
```

- [ ] **Step 3: 运行测试，确认通过**

```bash
cd test/testfiberawait/build && make -j$(nproc) && ./testfiberawait test_case_broadcast_tcp_read_stream
```

Expected: `Totals: 3 passed, 0 failed`

- [ ] **Step 4: 提交**

```bash
git add test/testfiberawait/tst_testfiberawait.cpp
git commit -m "test(await): cover broadcast over tcp read stream end to end"
```

---

### Task 7: 全量回归与文档同步

**Files:**
- Modify: `doc/使用说明.md`
- Modify: `doc/需求规格说明.md`
- Modify: `doc/软件设计说明.md`

**Interfaces:**
- Consumes: Task 1-6 的全部成果
- Produces: 无

- [ ] **Step 1: 跑 testfiberawait 全量用例**

```bash
cd test/testfiberawait/build && ./testfiberawait
```

Expected: `0 failed`，且无 AddressSanitizer 报告。**进程必须自行退出**——若挂住不退，说明扇出引入了未被唤醒的等待者，回到 Task 2 检查 close 传播。

- [ ] **Step 2: 跑其余三个测试工程回归**

```bash
for t in testfibertask testexecutor test_scheduler; do
  mkdir -p test/$t/build && (cd test/$t/build && qmake ../*.pro && make -j$(nproc)) || echo "BUILD FAILED: $t"
done
(cd test/testfibertask/build && ./testfibertask)
(cd test/testexecutor/build && ./testexecutor)
(cd test/test_scheduler/build && ./testscheduler)
```

Expected: 三个工程全部 `0 failed`。这验证未使用 `shared()` 的既有路径行为未变。

- [ ] **Step 3: 在 `doc/使用说明.md` 新增「广播消费」小节**

先定位插入点：

```bash
grep -n "^#\|^##" doc/使用说明.md | head -40
```

在讲 `await` / `Awaitable` 的章节之后插入下述内容（标题层级与相邻小节对齐）：

````markdown
### 广播消费（多个消费者各自处理每一条）

默认情况下，多个消费者 `await` 同一个 `Awaitable` 是**抢占**关系——一条消息只会被其中一个消费者取到。这适合工作队列，但不适合「每条消息都要被所有消费者各自处理一遍」的场景（数据同步、日志分发）。

用 `shared()` 注册共享订阅者：

```cpp
auto stream = Coro::coro(sock).readAll();

auto sync  = stream->shared();     // 订阅者 1
auto audit = stream->shared();     // 订阅者 2

Coro::makeTask([sync]{
    while(auto chunk = Coro::await(sync)) apply(chunk.value());
    return 0;
});
Coro::makeTask([audit]{
    for(const QByteArray& chunk : Coro::generate(audit)) log(chunk);
    return 0;
});
```

`shared()` 返回的是普通 `Awaitable`，`Coro::await`、`Coro::await_for`、`Coro::generate` 全部照常可用。

三条需要记住的规则：

- **不做 replay。** 只有 `shared()` 之后产生的数据对订阅者可见。需要一条不漏时，先订阅再启动数据源。
- **订阅者之间是广播，与直接 `await(stream)` 的消费者也不竞争。** 源队列保留全量给直接消费者互抢，每个订阅者另外各得全量。
- **退订是自动的。** 订阅句柄析构即退订，无需显式调用。订阅句柄的 `close()` 只终止自己这一路，要关掉整条流仍然调用源的 `close()`。

订阅者队列无界：某个订阅者长期不 `await`，它自己那条队列会持续增长。此外，扇出在生产者线程上同步完成，订阅者数量较多时会按比例占用事件循环——按需订阅，用完让句柄析构。
````

- [ ] **Step 4: 在 SRS 与 SDD 中补充条目**

```bash
grep -n "^#\|^##" doc/需求规格说明.md | head -40
grep -n "^#\|^##" doc/软件设计说明.md | head -40
```

`doc/需求规格说明.md`：在描述 Awaitable 消费模型的功能需求章节中，按该文档既有的需求编号格式新增一条，内容为：

> 系统应支持一个 Awaitable 被多个消费者共享消费，每条消息被每个共享消费者各自处理一遍；共享订阅不回放订阅之前产生的数据，订阅句柄析构即自动退订；共享消费不影响直接消费者之间既有的抢占式行为。

`doc/软件设计说明.md`：在描述 `FiberChannel` / `Awaitable` 的设计章节中新增说明，内容覆盖：

> 扇出机制位于 `FiberChannel::push`，因为生产者只捕获 `channel()` 而不持有 `Awaitable`。channel 以 `std::unique_ptr<std::vector<std::weak_ptr<FiberChannel<T>>>>` 持有镜像列表，未使用共享功能时为空指针，`sizeof(FiberChannel<T>)` 由 160 增至 168 字节，因 `make_shared` 落入同一 glibc 分配桶而实际堆占用不变。`push` / `close` / `discard_pending` 三处同步扇出；`discard_pending` 的扇出用于避免 `nextConnection()` 场景下镜像队列残留悬空的 `QTcpSocket*`。失效镜像在 `push` 中以 swap-and-pop 剔除。锁顺序恒为「源 → 镜像」，`addMirror()` 保持 private + friend 以杜绝互为镜像的环。

- [ ] **Step 5: 确认文档中的代码示例能编译**

把 Step 3 中的示例片段贴进一个临时 `.cpp` 做语法核对，或直接对照 `example/socket_pingpong` 的写法逐行检查 `shared()` 的调用形式与本计划 Task 1 的签名一致。

- [ ] **Step 6: 提交**

```bash
git add doc/使用说明.md doc/需求规格说明.md doc/软件设计说明.md
git commit -m "docs: document shared() broadcast consumption"
```

---

## 完成标准

- `coro/detail/fiberchannel.hpp` 与 `coro/await/awaitable.hpp` 之外没有产品代码改动
- `test/testfiberawait` 全部用例通过、无 ASan 报告、进程自行退出
- `testfibertask` / `testexecutor` / `test_scheduler` 全部通过
- `doc/使用说明.md`、`doc/需求规格说明.md`、`doc/软件设计说明.md` 已同步
