#include <QtTest>

// add necessary includes here
#include "executor/fiberpool.h"
#include "detail/asyncdefine.h"
#include "task/fibertask.h"
#include "task/fiberapplication.h"
#include "await/awaitable.hpp"
#include "await/generator.hpp"
#include "detail/result.hpp"
#include "await/coro.hpp"
#include "await/detail/socketawait.hpp"
#include "await/detail/autodisconnect.hpp"
#include "await/detail/socketerror.hpp"
#include <QBuffer>
#include <QAbstractSocket>
#include <QCoreApplication>
#include <QLocalServer>
#include <QLocalSocket>
#include <QPointer>
#include <QNetworkProxy>
#include <QNetworkDatagram>
#include <QSslCertificate>
#include <QSslConfiguration>
#include <QSslKey>
#include <QSslSocket>
#include <QTcpSocket>
#include <QTcpServer>
#include <QUdpSocket>
#include <QFile>
#include <QThread>
#include <chrono>
#include <atomic>
#include <type_traits>

#define TQVERIFY(statement) \
do {\
    if (!QTest::qVerify(static_cast<bool>(statement), #statement, "", __FILE__, __LINE__))\
        break;\
} while (false)

/// @brief 记录拷贝次数的测试用元素类型，用于观察扇出是否仍在向已关闭镜像投递。
struct CopyCounted
{
    static int copies;
    int value{0};
    CopyCounted() = default;
    explicit CopyCounted(int v):value(v){}
    CopyCounted(const CopyCounted& other):value(other.value){ ++copies; }
    CopyCounted& operator=(const CopyCounted& other){ value = other.value; ++copies; return *this; }
    CopyCounted(CopyCounted&&) noexcept = default;
    CopyCounted& operator=(CopyCounted&&) noexcept = default;
};
int CopyCounted::copies = 0;

class SigObject : public QObject
{
    Q_OBJECT
public:
    SigObject():QObject(nullptr){}
    void fire(){ emit sig1(); }
    void fire2(int v){ emit sig2(v, QString()); }
    void run(){
        QTimer::singleShot(500, this, [this](){
            emit this->sig1();
            emit this->sig2(10, QString("aaaa"));
        });
    }
signals:
    void sig1();
    void sig2(int, QString);
};

/**
 * @brief 为本地 socket 测试分配并回收唯一的服务器名。
 * @details 构造和析构都移除同名端点，防止临时文件或上次失败测试残留影响后续用例。
 */
class LocalServerNameGuard
{
public:
    /// @brief 创建进程内唯一名称并清理可能遗留的端点。
    LocalServerNameGuard()
        : name_(QStringLiteral("asynctask-local-%1-%2")
                    .arg(QCoreApplication::applicationPid())
                    .arg(counter_.fetch_add(1)))
    {
        QLocalServer::removeServer(name_);
    }

    /// @brief 析构时移除临时本地服务器资源。
    ~LocalServerNameGuard()
    {
        QLocalServer::removeServer(name_);
    }

    /// @brief 返回由守卫拥有、可用于本次测试的服务器名。
    const QString& name() const { return name_; }

private:
    /// @brief 区分同一进程内连续创建的测试服务器名。
    static std::atomic<unsigned int> counter_;
    /// @brief 守卫负责在生命周期两端清理的本地端点名称。
    QString name_;
};

std::atomic<unsigned int> LocalServerNameGuard::counter_{0};

/**
 * @brief 在 loopback 上接受 TCP 连接并升级为 TLS 的测试服务器。
 * @details 服务器按值拥有测试证书和私钥；接受的 QSslSocket 由服务器 QObject
 *          父对象拥有，随服务器销毁时清理。
 */
class SslLoopbackServer final : public QTcpServer
{
public:
    /// @brief 复制测试所需的证书和私钥，供后续接受的连接使用。
    SslLoopbackServer(const QSslCertificate& certificate, const QSslKey& privateKey)
        : certificate_(certificate), privateKey_(privateKey){}

    /// @brief 返回服务器拥有的已接受 TLS 对端，未接受连接时返回空指针。
    QSslSocket* peer() const { return peer_; }
    /// @brief 返回跟踪服务端握手完成的共享 awaitable。
    std::shared_ptr<Coro::Awaitable<void>> encrypted() const { return encrypted_; }

protected:
    /// @brief 为新描述符创建由服务器拥有的 TLS 对端并启动服务端握手。
    void incomingConnection(qintptr socketDescriptor) override
    {
        peer_ = new QSslSocket(this);
        peer_->setLocalCertificate(certificate_);
        peer_->setPrivateKey(privateKey_);
        peer_->setPeerVerifyMode(QSslSocket::VerifyNone);
        if(!peer_->setSocketDescriptor(socketDescriptor)){
            peer_->deleteLater();
            peer_ = nullptr;
            return;
        }
        encrypted_ = Coro::coro(peer_).waitForEncrypted();
        peer_->startServerEncryption();
    }

private:
    /// @brief 服务器按值持有的测试证书，避免引用测试调用方的临时对象。
    QSslCertificate certificate_;
    /// @brief 服务器按值持有的测试私钥，仅用于配置其接受的 TLS 对端。
    QSslKey privateKey_;
    /// @brief 由此服务器拥有的最近一个 TLS 对端。
    QSslSocket* peer_{nullptr};
    /// @brief 保持并暴露最近一次服务端握手的完成状态。
    std::shared_ptr<Coro::Awaitable<void>> encrypted_;
};

/**
 * @brief 接受 TCP 连接后发送明文的测试服务器。
 * @details 接受的 QTcpSocket 由服务器拥有，用于稳定触发 TLS 客户端握手失败。
 */
class PlainTextServer final : public QTcpServer
{
protected:
    /// @brief 接受连接并写入非 TLS 数据，以验证握手错误传播。
    void incomingConnection(qintptr socketDescriptor) override
    {
        auto peer = new QTcpSocket(this);
        if(!peer->setSocketDescriptor(socketDescriptor)){
            peer->deleteLater();
            return;
        }
        peer->write("not TLS");
        peer->flush();
    }
};

class TestFiberAwait : public QObject
{
    Q_OBJECT

public:
    TestFiberAwait();
    ~TestFiberAwait();

private slots:
    void begin();
    void test_case_awaiter();
    void test_case_close_overloads();
    void test_case_channel_terminal_error();
    void test_case_await_timeout_then_value();
    void test_case_void_terminal_error();
    void test_case_shared_awaitable();
    void test_case_shared_generator_terminal_timeout();
    void test_case_shared_awaitable_void();
    void test_case_broadcast_basic();
    void test_case_broadcast_no_replay();
    void test_case_broadcast_raii_unsubscribe();
    void test_case_broadcast_subscribe_after_close();
    void test_case_broadcast_terminal_error();
    void test_case_broadcast_mirror_close_isolated();
    void test_case_broadcast_server_destroy_purges_mirror();
    void test_case_broadcast_closed_stream_purges_mirror_on_destroy();
    void test_case_broadcast_prune_preserves_later_mirror();
    void test_case_broadcast_closed_mirror_pruned();
    void test_case_socket_error_conversion();
    void test_case_autodisconnect_until_expired();
    void test_case_autodisconnect_idempotent_late();
    void test_case_autodisconnect_until_signal();
    void test_case_generator();
    void test_case_signalawait();
    void test_case_signal_generate();
    void test_case_iodevice_await();
    void test_case_iodevice_generate();
    void test_case_tcp_ping_pong();
    void test_case_tcp_connection_refused();
    void test_case_tcp_retry_after_refusal();
    void test_case_tcp_disconnect();
    void test_case_tcp_remote_disconnect_wait();
    void test_case_tcp_read_then_remote_close();
    void test_case_tcp_read_stream_direct_close();
    void test_case_tcp_server_close();
    void test_case_tcp_server_closed_stream_release();
    void test_case_tcp_server_queued_close_release();
    void test_case_tcp_server_stream_direct_close();
    void test_case_tcp_server_destroy_purges_queued_connection();
    void test_case_tcp_closed_server_stream_purges_on_destroy();
    void test_case_tcp_server_connection_stream();
    void test_case_tcp_queued_connect_cancel();
    void test_case_ssl_error_conversion();
    void test_case_ssl_encrypted_ping_pong();
    void test_case_ssl_plain_peer_handshake_failure();
    void test_case_ssl_queued_connect_cancel();
    void test_case_local_ping_pong_disconnect();
    void test_case_local_remote_disconnect_wait();
    void test_case_local_connection_stream_and_close();
    void test_case_local_missing_server();
    void test_case_local_closed_stream_release();
    void test_case_local_server_queued_close_release();
    void test_case_local_server_stream_direct_close();
    void test_case_local_server_destroy_purges_queued_connection();
    void test_case_local_closed_server_stream_purges_on_destroy();
    void test_case_local_retry_after_missing_server();
    void test_case_local_read_then_peer_close();
    void test_case_local_read_stream_direct_close();
    void test_case_local_queued_connect_cancel();
    void test_case_udp_unconnected_socket_ends_stream();
    void test_case_udp_preserves_datagrams_and_sender_metadata();
    void test_case_udp_stream_direct_close();
    void test_case_udp_close_ends_stream_and_releases();
    void test_case_udp_destruction_ends_stream_and_releases();
    void cleanupTestCase();

};

TestFiberAwait::TestFiberAwait()
{

}

TestFiberAwait::~TestFiberAwait()
{

}

void TestFiberAwait::begin()
{

}

void TestFiberAwait::test_case_awaiter()
{
    Coro::Awaitable<int> awaiter;
    auto task1 = Coro::makeTask([&awaiter](){
        for(int i=0; i<100; i++){
            Coro::msleep(10);
            awaiter.resolve(1);
        }
        awaiter.close();
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task2 = Coro::makeTask([&awaiter](){
        int cnt{};
        while(1){
            Coro::Result<int> value = awaiter.await();
            if(value.has_value()){
                cnt += value.value();
            }else{
                break;
            }
        }
        return cnt;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    task1.get();
    Coro::Result<int> res = task2.get();
    TQVERIFY(res.value() == 100);
}

/// @brief 验证关闭接口的重载可用且清理回调至多执行一次。
void TestFiberAwait::test_case_close_overloads()
{
    void (Coro::FiberChannel<int>::*channelClose)() noexcept = &Coro::FiberChannel<int>::close;
    void (Coro::Awaitable<int>::*valueClose)() = &Coro::Awaitable<int>::close;
    void (Coro::Awaitable<void>::*voidClose)() = &Coro::Awaitable<void>::close;
    QVERIFY(channelClose != nullptr);
    QVERIFY(valueClose != nullptr);
    QVERIFY(voidClose != nullptr);

    int valueCleanupCalls = 0;
    Coro::Awaitable<int> value;
    value.setOnClose([&valueCleanupCalls]{ ++valueCleanupCalls; });
    value.close();
    value.close(std::make_error_code(std::errc::timed_out));
    QCOMPARE(valueCleanupCalls, 1);

    int voidCleanupCalls = 0;
    Coro::Awaitable<void> event;
    event.setOnClose([&voidCleanupCalls]{ ++voidCleanupCalls; });
    event.close();
    event.close(std::make_error_code(std::errc::timed_out));
    QCOMPARE(voidCleanupCalls, 1);
}

/// @brief 验证流关闭保留已产生的值，并传播首次终止错误。
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

/// @brief 验证超时只结束等待，随后仍可取得 awaitable 的值。
/// @details 该不变量确保 await_for 不会取消源 awaitable。
void TestFiberAwait::test_case_await_timeout_then_value()
{
    Coro::Awaitable<int> value;
    auto timeout = Coro::await_for(value, std::chrono::milliseconds(5));
    QCOMPARE(timeout.error(), std::make_error_code(std::errc::timed_out));
    QVERIFY(value.resolve(42));
    QCOMPARE(Coro::await(value).value(), 42);
}

/// @brief 验证 void awaitable 的终止错误和超时后成功解析。
void TestFiberAwait::test_case_void_terminal_error()
{
    Coro::Awaitable<void> failed;
    failed.close(std::make_error_code(std::errc::connection_refused));
    QCOMPARE(failed.await().error(), std::make_error_code(std::errc::connection_refused));

    Coro::Awaitable<void> value;
    auto timeout = Coro::await_for(value, std::chrono::milliseconds(5));
    QCOMPARE(timeout.error(), std::make_error_code(std::errc::timed_out));
    QVERIFY(value.resolve());
    QVERIFY(Coro::await(value).has_value());
}

/// @brief 验证共享 awaitable 的等待、流迭代、空指针错误与生命周期。
/// @details 生成器必须在持有期间延长源的生命周期，销毁未等待的生成器不得阻塞。
void TestFiberAwait::test_case_shared_awaitable()
{
    auto once = std::make_shared<Coro::Awaitable<int>>();
    once->resolve(9);
    QCOMPARE(Coro::await(once).value(), 9);

    auto delayed = std::make_shared<Coro::Awaitable<int>>();
    QCOMPARE(Coro::await_for(delayed, std::chrono::milliseconds(5)).error(),
             std::make_error_code(std::errc::timed_out));
    QVERIFY(delayed->resolve(42));
    QCOMPARE(Coro::await(delayed).value(), 42);

    std::shared_ptr<Coro::Awaitable<int>> nullAwaitable;
    QCOMPARE(Coro::await(nullAwaitable).error(),
             std::make_error_code(std::errc::invalid_argument));
    QCOMPARE(Coro::await_for(nullAwaitable, std::chrono::milliseconds(5)).error(),
             std::make_error_code(std::errc::invalid_argument));

    auto stream = std::make_shared<Coro::Awaitable<int>>();
    stream->resolve(1);
    stream->resolve(2);
    stream->close();
    int total = 0;
    for(int value : Coro::generate(stream)) total += value;
    QCOMPARE(total, 3);

    std::weak_ptr<Coro::Awaitable<int>> weakRetained;
    {
        auto retained = std::make_shared<Coro::Awaitable<int>>();
        retained->resolve(4);
        weakRetained = retained;
        auto retainedGenerator = Coro::generate(retained);
        retained.reset();
        QVERIFY(!weakRetained.expired());
        QCOMPARE(retainedGenerator.next().value(), 4);
        weakRetained.lock()->close();
        QCOMPARE(retainedGenerator.next().error(),
                 std::make_error_code(std::errc::no_message));
        QVERIFY(weakRetained.expired());
    }

    int nullTotal = 0;
    for(int value : Coro::generate(nullAwaitable)) nullTotal += value;
    QCOMPARE(nullTotal, 0);

    auto sharedSource = std::make_shared<Coro::Awaitable<int>>();
    auto destroyStart = std::chrono::steady_clock::now();
    {
        auto abandonedGenerator = Coro::generate(sharedSource);
    }
    auto destroyElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - destroyStart);
    QVERIFY(destroyElapsed < std::chrono::milliseconds(100));
    QVERIFY(sharedSource->resolve(17));
    QCOMPARE(Coro::await(sharedSource).value(), 17);
    sharedSource->close();
}

/// @brief 验证共享生成器遇到终止错误时立即结束并释放源。
/// @details 对生成器而言，终止错误统一表现为 no_message，而非把超时当作值。
void TestFiberAwait::test_case_shared_generator_terminal_timeout()
{
    auto source = std::make_shared<Coro::Awaitable<int>>();
    source->close(std::make_error_code(std::errc::timed_out));
    std::weak_ptr<Coro::Awaitable<int>> weakSource = source;
    auto generator = Coro::generate(source);
    source.reset();

    auto finishStart = std::chrono::steady_clock::now();
    QCOMPARE(generator.next().error(),
             std::make_error_code(std::errc::no_message));
    auto finishElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - finishStart);
    QVERIFY(finishElapsed < std::chrono::milliseconds(100));
    QVERIFY(weakSource.expired());
}

/// @brief 验证共享 void awaitable 支持一次结果、超时后解析和事件流。
void TestFiberAwait::test_case_shared_awaitable_void()
{
    auto once = std::make_shared<Coro::Awaitable<void>>();
    QVERIFY(once->resolve());
    QVERIFY(Coro::await(once).has_value());

    auto delayed = std::make_shared<Coro::Awaitable<void>>();
    QCOMPARE(Coro::await_for(delayed, std::chrono::milliseconds(5)).error(),
             std::make_error_code(std::errc::timed_out));
    QVERIFY(delayed->resolve());
    QVERIFY(Coro::await(delayed).has_value());

    auto stream = std::make_shared<Coro::Awaitable<void>>();
    QVERIFY(stream->resolve());
    QVERIFY(stream->resolve());
    stream->close();
    int events = 0;
    for(bool event : Coro::generate(stream)){
        QVERIFY(event);
        ++events;
    }
    QCOMPARE(events, 2);
}

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

    // 首次 await 直接取到 3，即证明订阅之前的 1、2 未被投递
    QCOMPARE(late->await().value(), 3);

    // 源侧不受影响，仍能取到全部三条
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

    QCOMPARE(keep->await().value(), 1);
    QCOMPARE(keep->await().value(), 2);
}

/// @brief 验证对已关闭的源调用 shared() 时，返回的句柄立即收敛而不挂起。
/// @details 该分支唯一的职责就是防止订阅者永久等待。
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

/// @brief 验证消费者先关闭流、随后销毁服务器时，镜像队列中的悬空连接指针一并被丢弃。
/// @details close() 之后 discard_pending() 仍须能触达镜像，否则镜像消费者会取到已删除的 QTcpSocket*。
void TestFiberAwait::test_case_broadcast_closed_stream_purges_mirror_on_destroy()
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

    incoming->close();   // 消费者先关闭流：镜像被关闭，但两侧队列仍留有排队指针
    delete server;       // 随后销毁服务器，子 QTcpSocket 被删除

    // 镜像队列必须已被清空，否则此处取出已删除对象；
    // 必须断言终止原因——只判 !mirrored 的话，等待超时同样为假，
    // 「队列清空了但镜像仍开着」的回归会静默耗满 100ms 后照常通过。
    auto mirrored = Coro::await_for(audit, 100ms);
    QVERIFY(!mirrored);
    QCOMPARE(mirrored.error(), std::make_error_code(std::errc::no_message));
}

/// @brief 验证已关闭的镜像排在存活镜像之前时，剔除不会连带跳过后者。
void TestFiberAwait::test_case_broadcast_prune_preserves_later_mirror()
{
    Coro::Awaitable<int> source;
    auto closedFirst = source.shared();   // 先注册，位于扇出列表前部
    auto live = source.shared();

    closedFirst->close();
    QVERIFY(source.resolve(11));
    QVERIFY(source.resolve(12));

    QCOMPARE(live->await().value(), 11);
    QCOMPARE(live->await().value(), 12);
}

/// @brief 验证已关闭但句柄仍存活的镜像会被剔除，此后不再为它付出拷贝代价。
/// @details resolve() 经由 push(T value) 传参，每次调用都有一次固有拷贝，与镜像无关；
///          因此断言的是「相对基线的增量」而非绝对值。
void TestFiberAwait::test_case_broadcast_closed_mirror_pruned()
{
    // 先标定：无镜像时每次 resolve 的固有拷贝代价
    Coro::Awaitable<CopyCounted> plain;
    CopyCounted::copies = 0;
    QVERIFY(plain.resolve(CopyCounted(1)));
    const int baseline = CopyCounted::copies;
    QVERIFY(baseline > 0);

    Coro::Awaitable<CopyCounted> source;
    auto mirror = source.shared();          // 句柄全程存活，weak_ptr 不会失效
    mirror->close();                        // 镜像自己关闭，但仍留在扇出列表里

    // 第一次投递仍可能拷给这条待剔除的镜像，因此不低于基线；
    // 若实现改为先判 is_closed() 再拷贝并提前剔除，则会等于基线，同样合法
    CopyCounted::copies = 0;
    QVERIFY(source.resolve(CopyCounted(1)));
    QVERIFY(CopyCounted::copies >= baseline);

    // 剔除之后，每次 resolve 只剩固有代价
    CopyCounted::copies = 0;
    QVERIFY(source.resolve(CopyCounted(2)));
    QVERIFY(source.resolve(CopyCounted(3)));
    QCOMPARE(CopyCounted::copies, baseline * 2);

    // 源侧照常收到全部三条
    QCOMPARE(source.await().value().value, 1);
    QCOMPARE(source.await().value().value, 2);
    QCOMPARE(source.await().value().value, 3);
}

/// @brief 验证 TCP 与本地 socket 错误保留 Qt 类别、数值及可读信息。
void TestFiberAwait::test_case_socket_error_conversion()
{
    const auto socketError = Coro::detail::socket_error_code(
        QAbstractSocket::ConnectionRefusedError);
    QCOMPARE(socketError.value(),
             static_cast<int>(QAbstractSocket::ConnectionRefusedError));
    QCOMPARE(QString::fromLatin1(socketError.category().name()),
             QStringLiteral("qt.socket"));
    QVERIFY(!QString::fromStdString(socketError.message()).isEmpty());

    const auto localError = Coro::detail::local_socket_error_code(
        QLocalSocket::ServerNotFoundError);
    QCOMPARE(localError.value(),
             static_cast<int>(QLocalSocket::ServerNotFoundError));
    QCOMPARE(QString::fromLatin1(localError.category().name()),
             QStringLiteral("qt.local_socket"));
    QVERIFY(!QString::fromStdString(localError.message()).isEmpty());

    QCOMPARE(QString::fromStdString(
                 Coro::detail::socket_error_code(
                     static_cast<QAbstractSocket::SocketError>(-12345)).message()),
             QStringLiteral("unknown socket error"));
    QCOMPARE(QString::fromStdString(
                 Coro::detail::local_socket_error_code(
                     static_cast<QLocalSocket::LocalSocketError>(-12345)).message()),
             QStringLiteral("unknown socket error"));
}

/// @brief 验证 SSL 错误转换保留 Qt SSL 类别和原始错误值。
void TestFiberAwait::test_case_ssl_error_conversion()
{
    const auto sslError = Coro::detail::ssl_error_code(QSslError::HostNameMismatch);
    QCOMPARE(sslError.value(), static_cast<int>(QSslError::HostNameMismatch));
    QCOMPARE(QString::fromLatin1(sslError.category().name()), QStringLiteral("qt.ssl"));
    QVERIFY(!QString::fromStdString(sslError.message()).isEmpty());
}

/// @brief 验证 untilExpired：返回句柄一旦析构即整组断开，且句柄不被连接钉住（无引用环）。
void TestFiberAwait::test_case_autodisconnect_until_expired()
{
    QPointer<SigObject> sender = new SigObject;
    auto awaitable = std::make_shared<Coro::Awaitable<int>>();
    std::weak_ptr<Coro::Awaitable<int>> observed = awaitable;
    auto scope = Coro::detail::make_auto_disconnect();
    int calls = 0;

    // 业务槽只捕 channel/计数，绝不捕 awaitable。
    scope->on(sender.data(), &SigObject::sig1, [&calls]{ ++calls; });
    scope->untilExpired(awaitable);

    sender->fire();
    QCOMPARE(calls, 1);                 // 连接生效

    awaitable.reset();                  // 丢弃返回句柄
    QVERIFY(observed.expired());        // 立即释放：连接未持有 awaitable，无引用环
    sender->fire();
    QCOMPARE(calls, 1);                 // untilExpired 已整组断开，不再触发
    delete sender.data();
}

/// @brief 验证 disconnectAll 幂等；清理后晚注册的连接立即断开、晚注册的 cleanup 立即执行。
void TestFiberAwait::test_case_autodisconnect_idempotent_late()
{
    QPointer<SigObject> sender = new SigObject;
    auto scope = Coro::detail::make_auto_disconnect();
    int calls = 0, cleanup1 = 0, cleanup2 = 0;

    scope->addCleanup([&cleanup1]{ ++cleanup1; });
    scope->addCleanup([&cleanup2]{ ++cleanup2; });
    scope->on(sender.data(), &SigObject::sig1, [&calls]{ ++calls; });

    scope->disconnectAll();
    scope->disconnectAll();             // 幂等
    QCOMPARE(cleanup1, 1);
    QCOMPARE(cleanup2, 1);
    sender->fire();
    QCOMPARE(calls, 0);                 // 已断开

    int lateCalls = 0, lateCleanup = 0;
    scope->on(sender.data(), &SigObject::sig1, [&lateCalls]{ ++lateCalls; });
    scope->addCleanup([&lateCleanup]{ ++lateCleanup; });
    QCOMPARE(lateCleanup, 1);           // 清理后 addCleanup 立即执行
    sender->fire();
    QCOMPARE(lateCalls, 0);             // 清理后 on 的连接立即断开
    delete sender.data();
}

/// @brief 验证 untilSignal（信号 / 信号+判断函数）触发时整组断开。
void TestFiberAwait::test_case_autodisconnect_until_signal()
{
    // 纯信号触发断开
    {
        QPointer<SigObject> data = new SigObject;
        QPointer<SigObject> trigger = new SigObject;
        auto scope = Coro::detail::make_auto_disconnect();
        int calls = 0;
        scope->on(data.data(), &SigObject::sig1, [&calls]{ ++calls; });
        scope->untilSignal(trigger.data(), &SigObject::sig1);
        data->fire();
        QCOMPARE(calls, 1);
        trigger->fire();                // 触发整组断开
        data->fire();
        QCOMPARE(calls, 1);
        delete data.data();
        delete trigger.data();
    }
    // 信号 + 判断函数：仅当谓词成立才断开
    {
        QPointer<SigObject> data = new SigObject;
        QPointer<SigObject> trigger = new SigObject;
        auto scope = Coro::detail::make_auto_disconnect();
        int calls = 0;
        scope->on(data.data(), &SigObject::sig1, [&calls]{ ++calls; });
        scope->untilSignal(trigger.data(), &SigObject::sig2,
                           [](int v, const QString&){ return v >= 10; });
        trigger->fire2(1);              // 谓词不成立，不断开
        data->fire();
        QCOMPARE(calls, 1);
        trigger->fire2(10);             // 谓词成立，整组断开
        data->fire();
        QCOMPARE(calls, 1);
        delete data.data();
        delete trigger.data();
    }
}

void TestFiberAwait::test_case_generator()
{
    auto t1 = std::chrono::steady_clock::now();
    int cnt{0};
    Coro::Generator<int> gen([](auto yield){
        for(int i=0; i<100; i++){
            Coro::msleep(10);
            yield(i*i);
        }
    });
    auto task1 = Coro::makeTask([&cnt, &gen](){
        int k=0;
        for(auto v:gen){
            TQVERIFY(k*k == v);
            k++;
            cnt++;
        }
    });
    task1.get();
    auto t2 = std::chrono::steady_clock::now();
    auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1);
    qDebug() << dt.count();
    TQVERIFY(dt.count()<1100);
    TQVERIFY(cnt == 100);
    qDebug() << dt.count();
}

void TestFiberAwait::test_case_signalawait()
{
    SigObject* obj = new SigObject();
    auto task1 = Coro::makeTask([obj](){
        Coro::Result<std::tuple<int, QString>> res1 = Coro::await(Coro::coro(obj, &SigObject::sig2));

        if(res1.has_value()){
            return std::get<0>(res1.value());
        }else{
            return 0;
        }
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task2 = Coro::makeTask([obj](){
        Coro::Result<int> res2 = Coro::await(Coro::coro<int>(obj, &SigObject::sig2));
        if(res2.has_value()){
            return res2.value();
        }else{
            return 0;
        }
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task3 = Coro::makeTask([obj](){
        Coro::Result<void> res3 = Coro::await(Coro::coro(obj, &SigObject::sig1));
        if(res3.has_value()){
            return 10;
        }else{
            return 0;
        }
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task4 = Coro::makeTask([obj](){
        Coro::Result<void> res4 = Coro::await(Coro::coro<void>(obj, &SigObject::sig2));
        if(res4.has_value()){
            return 10;
        }else{
            return 0;
        }
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
//    auto task5 = Coro::makeTask([obj](){
//        Coro::Result<int> res5 = Coro::await<int>(obj, SIGNAL(sig2(int, QString)));//暂时不支持
//        if(res5.has_value()){
//            return res5.value();
//        }else{
//            return 0;
//        }
//    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    obj->run();
    obj->deleteLater();
    Coro::Result<int> res1 = task1.get();
    Coro::Result<int> res2 = task2.get();
    Coro::Result<int> res3 = task3.get();
    Coro::Result<int> res4 = task4.get();
    qDebug() << QDateTime::currentDateTime() << "task get";
    TQVERIFY(res1.value() == 10);
    TQVERIFY(res2.value() == 10);
    TQVERIFY(res3.value() == 10);
    TQVERIFY(res4.value() == 10);

}

void TestFiberAwait::test_case_signal_generate()
{
    SigObject* obj = new SigObject();
    auto gen1 = Coro::generate(Coro::coro(obj, &SigObject::sig2));
    auto gen2 = Coro::generate(Coro::coro<int>(obj, &SigObject::sig2));
    auto gen3 = Coro::generate(Coro::coro<void>(obj, &SigObject::sig2));
    auto task1 = Coro::makeTask([&gen1]() mutable {
        int k=0;
        for(const auto [v_i, v_s] : gen1){
            k++;
            TQVERIFY(v_i == 10);
            TQVERIFY(v_s == "aaaa");
            if(k==10){
                gen1.close();
            }
        }
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task2 = Coro::makeTask([&gen2]() mutable {
        int k=0;
        for(const auto v_i : gen2){
            k++;
            TQVERIFY(v_i == 10);
            if(k==10){
                gen2.close();
            }
        }
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task3 = Coro::makeTask([&gen3]() mutable {
        int k=0;
        for(const auto v : gen3){
            qDebug() << k;
            k++;
            TQVERIFY(v == true);
            if(k==10){
                gen3.close();
            }
        }
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    obj->run();
    delete obj;
    task1.get();
    task2.get();
    task3.get();
}

void TestFiberAwait::test_case_iodevice_await()
{
    QBuffer * dev = new QBuffer();
    dev->open(QBuffer::ReadWrite);
    QTimer::singleShot(100, dev, [dev](){
        dev->write("aaaaaaaa");
        dev->seek(0);
    });
    connect(dev, &QBuffer::readyRead, dev, [](){
        qDebug() << "ready read";
    });
    auto res = Coro::await(Coro::coro(dev).readAll());
    TQVERIFY(res.has_value() == true);
    TQVERIFY(res.value() == "aaaaaaaa");
    dev->close();
    dev->deleteLater();
}

void TestFiberAwait::test_case_iodevice_generate()
{
    QBuffer * dev = new QBuffer();
    auto gen = Coro::generate(Coro::coro(dev).readAll());
    dev->open(QBuffer::ReadWrite);
    auto task1 = Coro::makeTask([dev]() mutable {
        for(int i=0; i<100; i++){
            dev->write("aaaaaaaa");
            dev->seek(dev->buffer().size()-8);
            Coro::msleep(10);
        }
        dev->close();
        dev->deleteLater();
        delete dev;
    }, Coro::Priority::Normal, Coro::Affinity::fixed(std::this_thread::get_id()));
    int k=0;
    auto task2 = Coro::makeTask([&gen, &k](){
        for(const auto v : gen){
            k++;
            TQVERIFY(v == QByteArray("aaaaaaaa"));
        }
    }, Coro::Priority::Normal, Coro::Affinity::fixed(std::this_thread::get_id()));
    task1.get();
    task2.get();
    TQVERIFY(k == 100);
}

/// @brief 验证 TCP 连接、双向 ping-pong 和共享 awaitable 的类型契约。
void TestFiberAwait::test_case_tcp_ping_pong()
{
    using namespace std::chrono_literals;
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(&server).nextConnection();
    static_assert(std::is_same_v<decltype(incoming),
                                 std::shared_ptr<Coro::Awaitable<QTcpSocket*>>>);

    QTcpSocket client;
    client.setProxy(QNetworkProxy::NoProxy);
    auto connected = Coro::coro(&client).connectToHost(
        QStringLiteral("127.0.0.1"), server.serverPort());
    static_assert(std::is_same_v<decltype(connected),
                                 std::shared_ptr<Coro::Awaitable<void>>>);
    QVERIFY(Coro::await_for(connected, 2s));

    auto accepted = Coro::await_for(incoming, 2s);
    QVERIFY(accepted);
    QTcpSocket* peer = accepted.value();
    QVERIFY(peer != nullptr);

    auto serverReady = Coro::coro(peer).waitForReadyRead();
    auto clientWritten = Coro::coro(&client).waitForBytesWritten();
    QCOMPARE(client.write("ping"), qint64(4));
    QVERIFY(Coro::await_for(clientWritten, 2s));
    QVERIFY(Coro::await_for(serverReady, 2s));
    auto request = Coro::await_for(Coro::coro(peer).readAll(), 2s);
    QVERIFY(request);
    QCOMPARE(request.value(), QByteArray("ping"));

    auto clientReady = Coro::coro(&client).waitForReadyRead();
    auto serverWritten = Coro::coro(peer).waitForBytesWritten();
    QCOMPARE(peer->write("pong"), qint64(4));
    QVERIFY(Coro::await_for(serverWritten, 2s));
    QVERIFY(Coro::await_for(clientReady, 2s));
    auto response = Coro::await_for(Coro::coro(&client).readAll(), 2s);
    QVERIFY(response);
    QCOMPARE(response.value(), QByteArray("pong"));

    client.abort();
    delete peer;
}

/// @brief 验证释放的 loopback 端口稳定返回 TCP 连接被拒绝错误。
/// @details 客户端在线程内创建和销毁，确保失败路径不遗留跨线程 socket。
void TestFiberAwait::test_case_tcp_connection_refused()
{
    using namespace std::chrono_literals;
    QTcpServer portProbe;
    QVERIFY(portProbe.listen(QHostAddress::LocalHost, 0));
    const quint16 unusedPort = portProbe.serverPort();
    portProbe.close();

    QThread socketThread;
    socketThread.start();
    auto client = new QTcpSocket;
    client->setProxy(QNetworkProxy::NoProxy);
    client->moveToThread(&socketThread);
    auto connected = Coro::coro(client).connectToHost(
        QStringLiteral("127.0.0.1"), unusedPort);
    auto result = Coro::await_for(connected, 2s);
    QPointer<QTcpSocket> clientGuard(client);
    const bool cleanupQueued = QMetaObject::invokeMethod(
        client, [clientGuard]{
            if(clientGuard) clientGuard->deleteLater();
            QThread::currentThread()->quit();
        }, Qt::QueuedConnection);
    if(!cleanupQueued) socketThread.quit();
    const bool stopped = socketThread.wait(2000);
    QVERIFY(stopped);
    QVERIFY(cleanupQueued);
    QVERIFY(clientGuard.isNull());
    QVERIFY(!result);
    QCOMPARE(QString::fromLatin1(result.error().category().name()),
             QStringLiteral("qt.socket"));
    QCOMPARE(result.error().value(),
             static_cast<int>(QAbstractSocket::ConnectionRefusedError));
}

/// @brief 验证 TCP 连接被拒绝后可用地址和主机名重试成功。
void TestFiberAwait::test_case_tcp_retry_after_refusal()
{
    using namespace std::chrono_literals;
    QTcpServer firstProbe;
    QTcpServer secondProbe;
    QTcpServer server;
    QVERIFY(firstProbe.listen(QHostAddress::LocalHost, 0));
    QVERIFY(secondProbe.listen(QHostAddress::LocalHost, 0));
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    const quint16 firstUnusedPort = firstProbe.serverPort();
    const quint16 secondUnusedPort = secondProbe.serverPort();
    firstProbe.close();
    secondProbe.close();
    auto incoming = Coro::coro(&server).nextConnection();

    QThread socketThread;
    socketThread.start();
    auto client = new QTcpSocket;
    client->setProxy(QNetworkProxy::NoProxy);
    client->moveToThread(&socketThread);

    auto firstFailure = Coro::await_for(
        Coro::coro(client).connectToHost(QStringLiteral("127.0.0.1"),
                                         firstUnusedPort), 2s);
    auto addressRetry = Coro::await_for(
        Coro::coro(client).connectToHost(QHostAddress::LocalHost,
                                         server.serverPort()), 2s);
    auto firstAccepted = Coro::await_for(incoming, 2s);
    auto firstDisconnect = Coro::await_for(
        Coro::coro(client).disconnectFromHost(), 2s);

    auto secondFailure = Coro::await_for(
        Coro::coro(client).connectToHost(QHostAddress::LocalHost,
                                         secondUnusedPort), 2s);
    auto stringRetry = Coro::await_for(
        Coro::coro(client).connectToHost(QStringLiteral("127.0.0.1"),
                                         server.serverPort()), 2s);
    auto secondAccepted = Coro::await_for(incoming, 2s);

    QPointer<QTcpSocket> clientGuard(client);
    const bool cleanupQueued = QMetaObject::invokeMethod(
        client, [clientGuard]{
            if(clientGuard) clientGuard->deleteLater();
            QThread::currentThread()->quit();
        }, Qt::QueuedConnection);
    if(!cleanupQueued) socketThread.quit();
    const bool stopped = socketThread.wait(2000);

    QVERIFY(stopped);
    QVERIFY(cleanupQueued);
    QVERIFY(clientGuard.isNull());
    QVERIFY(!firstFailure);
    QCOMPARE(QString::fromLatin1(firstFailure.error().category().name()),
             QStringLiteral("qt.socket"));
    QCOMPARE(firstFailure.error().value(),
             static_cast<int>(QAbstractSocket::ConnectionRefusedError));
    QVERIFY(addressRetry);
    QVERIFY(firstAccepted);
    QVERIFY(firstDisconnect);
    QVERIFY(!secondFailure);
    QCOMPARE(QString::fromLatin1(secondFailure.error().category().name()),
             QStringLiteral("qt.socket"));
    QCOMPARE(secondFailure.error().value(),
             static_cast<int>(QAbstractSocket::ConnectionRefusedError));
    QVERIFY(stringRetry);
    QVERIFY(secondAccepted);
}

/// @brief 验证本地主动断开能完成并使连接进入未连接状态。
void TestFiberAwait::test_case_tcp_disconnect()
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

    auto disconnected = Coro::coro(&client).disconnectFromHost();
    QVERIFY(Coro::await_for(disconnected, 2s));
    QCOMPARE(client.state(), QAbstractSocket::UnconnectedState);
    QVERIFY(Coro::await_for(Coro::coro(&client).waitForDisconnected(), 2s));
    delete peer;
}

/// @brief 验证对端断开会完成本地的断开等待。
void TestFiberAwait::test_case_tcp_remote_disconnect_wait()
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

    auto disconnected = Coro::coro(&client).waitForDisconnected();
    peer->disconnectFromHost();

    auto result = Coro::await_for(disconnected, 2s);
    QVERIFY(result);
    QCOMPARE(client.state(), QAbstractSocket::UnconnectedState);
    delete peer;
}

/// @brief 验证读取流先交付缓冲字节，再以正常终止结束。
void TestFiberAwait::test_case_tcp_read_then_remote_close()
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

    auto written = Coro::coro(peer).waitForBytesWritten();
    QCOMPARE(peer->write("final-bytes"), qint64(11));
    QVERIFY(Coro::await_for(written, 2s));
    peer->disconnectFromHost();

    auto bytes = Coro::await_for(stream, 2s);
    auto finished = Coro::await_for(stream, 2s);
    QVERIFY(bytes);
    QCOMPARE(bytes.value(), QByteArray("final-bytes"));
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
    delete peer;
}

/// @brief 验证直接关闭 TCP 读取流会释放 awaitable 而不影响 socket 接收数据。
void TestFiberAwait::test_case_tcp_read_stream_direct_close()
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
    std::weak_ptr<Coro::Awaitable<QByteArray>> observed = stream;
    stream->close();
    stream.reset();

    const QByteArray payload("after-close");
    QCOMPARE(peer->write(payload), qint64(payload.size()));
    peer->flush();
    QTRY_VERIFY_WITH_TIMEOUT(observed.expired() &&
                             client.bytesAvailable() == payload.size(), 1000);
    QCOMPARE(client.readAll(), payload);
    delete peer;
}

/// @brief 验证关闭 TCP 监听器会以正常终止结束连接流。
void TestFiberAwait::test_case_tcp_server_close()
{
    using namespace std::chrono_literals;
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(&server).nextConnection();

    server.close();
    auto finished = Coro::await_for(incoming, 2s);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
}

/// @brief 验证主动关闭 TCP 连接流后，连接监视资源会被释放。
void TestFiberAwait::test_case_tcp_server_closed_stream_release()
{
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(&server).nextConnection();
    std::weak_ptr<Coro::Awaitable<QTcpSocket*>> observed = incoming;

    incoming->close();
    incoming.reset();

    QTRY_VERIFY_WITH_TIMEOUT(observed.expired(), 2000);
    QVERIFY(server.isListening());
    QTRY_COMPARE_WITH_TIMEOUT(server.findChildren<QTimer*>().size(), 0, 2000);
}

/// @brief 验证跨线程关闭 TCP 连接流仍会释放 awaitable 和监视资源。
void TestFiberAwait::test_case_tcp_server_queued_close_release()
{
    auto server = new QTcpServer;
    QVERIFY(server->listen(QHostAddress::LocalHost, 0));
    QThread worker;
    server->moveToThread(&worker);

    auto incoming = Coro::coro(server).nextConnection();
    std::weak_ptr<Coro::Awaitable<QTcpSocket*>> observed = incoming;
    incoming->close();
    incoming.reset();
    worker.start();

    QElapsedTimer elapsed;
    elapsed.start();
    while(!observed.expired() && elapsed.elapsed() < 2000) QTest::qWait(10);

    bool listening = false;
    QMetaObject::invokeMethod(server, [server, &listening]{
        listening = server->isListening();
        server->close();
        delete server;
    }, Qt::BlockingQueuedConnection);
    worker.quit();
    const bool stopped = worker.wait(2000);

    QVERIFY(observed.expired());
    QVERIFY(listening);
    QVERIFY(stopped);
}

/// @brief 验证停止监视器后直接关闭 TCP 连接流不会阻止服务器接受连接。
void TestFiberAwait::test_case_tcp_server_stream_direct_close()
{
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(&server).nextConnection();
    std::weak_ptr<Coro::Awaitable<QTcpSocket*>> observed = incoming;
    const auto monitors = server.findChildren<QTimer*>();
    QCOMPARE(monitors.size(), 1);
    for(QTimer* monitor : monitors) monitor->stop();

    incoming->close();
    incoming.reset();
    const bool releasedOnClose = observed.expired();

    QTcpSocket client;
    client.connectToHost(QHostAddress::LocalHost, server.serverPort());
    QTRY_COMPARE_WITH_TIMEOUT(client.state(), QAbstractSocket::ConnectedState, 2000);
    QTest::qWait(50);

    QVERIFY(releasedOnClose);
    QVERIFY(server.hasPendingConnections());
    delete server.nextPendingConnection();
}

/// @brief 验证销毁 TCP 服务器会清理已排队但未交付的连接并终止流。
void TestFiberAwait::test_case_tcp_server_destroy_purges_queued_connection()
{
    using namespace std::chrono_literals;
    auto server = new QTcpServer;
    QVERIFY(server->listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(server).nextConnection();
    QSignalSpy connectionSignal(server, &QTcpServer::newConnection);

    QTcpSocket client;
    client.connectToHost(QHostAddress::LocalHost, server->serverPort());
    QTRY_COMPARE_WITH_TIMEOUT(client.state(), QAbstractSocket::ConnectedState, 2000);
    QTRY_VERIFY_WITH_TIMEOUT(connectionSignal.count() > 0, 2000);
    QVERIFY(!server->hasPendingConnections());

    delete server;
    auto finished = Coro::await_for(incoming, 100ms);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
}

/// @brief 验证已关闭的 TCP 连接流在服务器销毁时仍清理排队连接。
void TestFiberAwait::test_case_tcp_closed_server_stream_purges_on_destroy()
{
    using namespace std::chrono_literals;
    auto server = new QTcpServer;
    QVERIFY(server->listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(server).nextConnection();
    QSignalSpy connectionSignal(server, &QTcpServer::newConnection);

    QTcpSocket client;
    client.connectToHost(QHostAddress::LocalHost, server->serverPort());
    QTRY_COMPARE_WITH_TIMEOUT(client.state(), QAbstractSocket::ConnectedState, 2000);
    QTRY_VERIFY_WITH_TIMEOUT(connectionSignal.count() > 0, 2000);
    QVERIFY(!server->hasPendingConnections());

    incoming->close();
    delete server;
    auto finished = Coro::await_for(incoming, 100ms);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
}

/// @brief 验证 TCP 连接流按接受顺序交付多个不同的对端。
void TestFiberAwait::test_case_tcp_server_connection_stream()
{
    using namespace std::chrono_literals;
    auto server = new QTcpServer;
    QVERIFY(server->listen(QHostAddress::LocalHost, 0));
    auto incoming = Coro::coro(server).nextConnection();

    QTcpSocket firstClient;
    QTcpSocket secondClient;
    firstClient.connectToHost(QHostAddress::LocalHost, server->serverPort());
    secondClient.connectToHost(QHostAddress::LocalHost, server->serverPort());
    QVERIFY(Coro::await_for(Coro::coro(&firstClient).waitForConnected(), 2s));
    QVERIFY(Coro::await_for(Coro::coro(&secondClient).waitForConnected(), 2s));

    auto first = Coro::await_for(incoming, 2s);
    auto second = Coro::await_for(incoming, 2s);
    QVERIFY(first);
    QVERIFY(second);
    QVERIFY(first.value() != second.value());
    QPointer<QTcpSocket> firstPeer(first.value());
    QPointer<QTcpSocket> secondPeer(second.value());

    delete server;
    auto finished = Coro::await_for(incoming, 2s);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
    QVERIFY(firstPeer.isNull());
    QVERIFY(secondPeer.isNull());
    firstClient.abort();
    secondClient.abort();
}

/// @brief 验证在线程启动前关闭排队的 TCP 连接操作不会建立连接。
void TestFiberAwait::test_case_tcp_queued_connect_cancel()
{
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));

    QThread worker;
    auto client = new QTcpSocket;
    client->setProxy(QNetworkProxy::NoProxy);
    client->moveToThread(&worker);
    auto operation = Coro::coro(client).connectToHost(
        QStringLiteral("127.0.0.1"), server.serverPort());
    operation->close();
    worker.start();

    QAbstractSocket::SocketState state = QAbstractSocket::ConnectedState;
    const bool inspected = QMetaObject::invokeMethod(
        client, [client, &state]{
            state = client->state();
            delete client;
        }, Qt::BlockingQueuedConnection);
    worker.quit();
    const bool stopped = worker.wait(2000);

    QVERIFY(inspected);
    QVERIFY(stopped);
    QCOMPARE(state, QAbstractSocket::UnconnectedState);
    QVERIFY(!server.hasPendingConnections());
}

/// @brief 验证受信任的 loopback TLS 握手后可双向传递 ping-pong 数据。
/// @details 服务端证书和私钥由测试服务器持有，双方握手完成后才进行数据断言。
void TestFiberAwait::test_case_ssl_encrypted_ping_pong()
{
    using namespace std::chrono_literals;
    if(!QSslSocket::supportsSsl()) QSKIP("QSslSocket runtime SSL support is unavailable");

    QFile certificateFile(QFINDTESTDATA("data/server-cert.pem"));
    QFile privateKeyFile(QFINDTESTDATA("data/server-key.pem"));
    QVERIFY2(certificateFile.open(QIODevice::ReadOnly),
             qPrintable(certificateFile.errorString()));
    QVERIFY2(privateKeyFile.open(QIODevice::ReadOnly),
             qPrintable(privateKeyFile.errorString()));
    const QSslCertificate certificate(&certificateFile, QSsl::Pem);
    const QSslKey privateKey(&privateKeyFile, QSsl::Rsa, QSsl::Pem);
    QVERIFY(!certificate.isNull());
    QVERIFY(!privateKey.isNull());

    SslLoopbackServer server(certificate, privateKey);
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));

    QSslSocket client;
    client.setProxy(QNetworkProxy::NoProxy);
    QSslConfiguration clientConfiguration = client.sslConfiguration();
    clientConfiguration.setCaCertificates(QList<QSslCertificate>() << certificate);
    client.setSslConfiguration(clientConfiguration);
    client.setPeerVerifyMode(QSslSocket::VerifyPeer);
    client.setPeerVerifyName(QStringLiteral("localhost"));

    auto clientEncrypted = Coro::coro(&client).connectToHostEncrypted(
        QStringLiteral("127.0.0.1"), server.serverPort());
    QVERIFY(Coro::await_for(clientEncrypted, 2s));
    QVERIFY(server.peer() != nullptr);
    QVERIFY(server.encrypted());
    QVERIFY(Coro::await_for(server.encrypted(), 2s));
    QVERIFY(client.isEncrypted());
    QVERIFY(server.peer()->isEncrypted());

    auto peerReady = Coro::coro(server.peer()).waitForReadyRead();
    auto clientWritten = Coro::coro(&client).waitForBytesWritten();
    QCOMPARE(client.write("ping"), qint64(4));
    QVERIFY(Coro::await_for(clientWritten, 2s));
    QVERIFY(Coro::await_for(peerReady, 2s));
    auto request = Coro::await_for(Coro::coro(server.peer()).readAll(), 2s);
    QVERIFY(request);
    QCOMPARE(request.value(), QByteArray("ping"));

    auto clientReady = Coro::coro(&client).waitForReadyRead();
    auto peerWritten = Coro::coro(server.peer()).waitForBytesWritten();
    QCOMPARE(server.peer()->write("pong"), qint64(4));
    QVERIFY(Coro::await_for(peerWritten, 2s));
    QVERIFY(Coro::await_for(clientReady, 2s));
    auto response = Coro::await_for(Coro::coro(&client).readAll(), 2s);
    QVERIFY(response);
    QCOMPARE(response.value(), QByteArray("pong"));
}

/// @brief 验证 TLS 客户端连接明文对端时传播可诊断的握手错误。
void TestFiberAwait::test_case_ssl_plain_peer_handshake_failure()
{
    using namespace std::chrono_literals;
    if(!QSslSocket::supportsSsl()) QSKIP("QSslSocket runtime SSL support is unavailable");

    PlainTextServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));

    QSslSocket client;
    client.setProxy(QNetworkProxy::NoProxy);
    const auto result = Coro::await_for(
        Coro::coro(&client).connectToHostEncrypted(
            QStringLiteral("localhost"), server.serverPort()), 2s);

    QVERIFY(!result);
    const bool socketHandshakeFailure =
        QString::fromLatin1(result.error().category().name()) == QStringLiteral("qt.socket") &&
        result.error().value() == static_cast<int>(QAbstractSocket::SslHandshakeFailedError);
    const bool sslHandshakeFailure =
        QString::fromLatin1(result.error().category().name()) == QStringLiteral("qt.ssl");
    QVERIFY(socketHandshakeFailure || sslHandshakeFailure);
    QVERIFY(!QString::fromStdString(result.error().message()).isEmpty());
}

/// @brief 验证在线程启动前关闭排队的 TLS 连接操作不会建立连接。
void TestFiberAwait::test_case_ssl_queued_connect_cancel()
{
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));

    QThread worker;
    auto client = new QSslSocket;
    client->setProxy(QNetworkProxy::NoProxy);
    client->moveToThread(&worker);
    auto operation = Coro::coro(client).connectToHostEncrypted(
        QStringLiteral("127.0.0.1"), server.serverPort());
    operation->close();
    worker.start();

    QAbstractSocket::SocketState state = QAbstractSocket::ConnectedState;
    const bool inspected = QMetaObject::invokeMethod(
        client, [client, &state]{
            state = client->state();
            delete client;
        }, Qt::BlockingQueuedConnection);
    worker.quit();
    const bool stopped = worker.wait(2000);

    QVERIFY(inspected);
    QVERIFY(stopped);
    QCOMPARE(state, QAbstractSocket::UnconnectedState);
    QVERIFY(!server.hasPendingConnections());
}

/// @brief 验证本地 socket 的连接、双向 ping-pong 与主动断开契约。
void TestFiberAwait::test_case_local_ping_pong_disconnect()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard serverName;
    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));
    auto incoming = Coro::coro(&server).nextConnection();
    static_assert(std::is_same_v<decltype(incoming),
                                 std::shared_ptr<Coro::Awaitable<QLocalSocket*>>>);

    QLocalSocket client;
    auto connected = Coro::coro(&client).connectToServer(serverName.name());
    static_assert(std::is_same_v<decltype(connected),
                                 std::shared_ptr<Coro::Awaitable<void>>>);
    QVERIFY(Coro::await_for(connected, 2s));
    QVERIFY(Coro::await_for(Coro::coro(&client).waitForConnected(), 2s));

    auto accepted = Coro::await_for(incoming, 2s);
    QVERIFY(accepted);
    QLocalSocket* peer = accepted.value();
    QVERIFY(peer != nullptr);

    auto peerReady = Coro::coro(peer).waitForReadyRead();
    auto clientWritten = Coro::coro(&client).waitForBytesWritten();
    QCOMPARE(client.write("ping"), qint64(4));
    QVERIFY(Coro::await_for(clientWritten, 2s));
    QVERIFY(Coro::await_for(peerReady, 2s));
    auto request = Coro::await_for(Coro::coro(peer).readAll(), 2s);
    QVERIFY(request);
    QCOMPARE(request.value(), QByteArray("ping"));

    auto clientReady = Coro::coro(&client).waitForReadyRead();
    auto peerWritten = Coro::coro(peer).waitForBytesWritten();
    QCOMPARE(peer->write("pong"), qint64(4));
    QVERIFY(Coro::await_for(peerWritten, 2s));
    QVERIFY(Coro::await_for(clientReady, 2s));
    auto response = Coro::await_for(Coro::coro(&client).readAll(), 2s);
    QVERIFY(response);
    QCOMPARE(response.value(), QByteArray("pong"));

    QVERIFY(Coro::await_for(Coro::coro(&client).disconnectFromServer(), 2s));
    QVERIFY(Coro::await_for(Coro::coro(&client).waitForDisconnected(), 2s));
    QCOMPARE(client.state(), QLocalSocket::UnconnectedState);
}

/// @brief 验证本地 socket 对端断开会完成客户端的断开等待。
void TestFiberAwait::test_case_local_remote_disconnect_wait()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard serverName;
    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));
    auto incoming = Coro::coro(&server).nextConnection();

    QLocalSocket client;
    QVERIFY(Coro::await_for(
        Coro::coro(&client).connectToServer(serverName.name()), 2s));
    auto accepted = Coro::await_for(incoming, 2s);
    QVERIFY(accepted);
    QLocalSocket* peer = accepted.value();

    auto disconnected = Coro::coro(&client).waitForDisconnected();
    peer->disconnectFromServer();

    auto result = Coro::await_for(disconnected, 2s);
    QVERIFY(result);
    QCOMPARE(client.state(), QLocalSocket::UnconnectedState);
}

/// @brief 验证本地服务器连接流依次交付连接，并在关闭后终止和释放对端。
void TestFiberAwait::test_case_local_connection_stream_and_close()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard serverName;
    auto server = std::make_unique<QLocalServer>();
    QVERIFY(server->listen(serverName.name()));
    auto incoming = Coro::coro(server.get()).nextConnection();

    QLocalSocket firstClient;
    QLocalSocket secondClient;
    firstClient.connectToServer(serverName.name());
    secondClient.connectToServer(serverName.name());
    QVERIFY(Coro::await_for(Coro::coro(&firstClient).waitForConnected(), 2s));
    QVERIFY(Coro::await_for(Coro::coro(&secondClient).waitForConnected(), 2s));

    auto first = Coro::await_for(incoming, 2s);
    auto second = Coro::await_for(incoming, 2s);
    QVERIFY(first);
    QVERIFY(second);
    QVERIFY(first.value() != second.value());
    QPointer<QLocalSocket> firstPeer(first.value());
    QPointer<QLocalSocket> secondPeer(second.value());

    server->close();
    auto finished = Coro::await_for(incoming, 2s);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
    server.reset();
    QVERIFY(firstPeer.isNull());
    QVERIFY(secondPeer.isNull());
}

/// @brief 验证连接不存在的本地服务器返回 ServerNotFoundError。
void TestFiberAwait::test_case_local_missing_server()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard missingName;
    QLocalSocket client;
    auto result = Coro::await_for(
        Coro::coro(&client).connectToServer(missingName.name()), 2s);

    QVERIFY(!result);
    QCOMPARE(QString::fromLatin1(result.error().category().name()),
             QStringLiteral("qt.local_socket"));
    QCOMPARE(result.error().value(),
             static_cast<int>(QLocalSocket::ServerNotFoundError));
}

/// @brief 验证关闭本地服务器连接流会释放 awaitable 和监视计时器。
void TestFiberAwait::test_case_local_closed_stream_release()
{
    LocalServerNameGuard serverName;
    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));
    auto incoming = Coro::coro(&server).nextConnection();
    std::weak_ptr<Coro::Awaitable<QLocalSocket*>> observed = incoming;

    incoming->close();
    incoming.reset();

    QTRY_VERIFY_WITH_TIMEOUT(observed.expired(), 500);
    QVERIFY(server.isListening());
    QTRY_COMPARE_WITH_TIMEOUT(server.findChildren<QTimer*>().size(), 0, 2000);
}

/// @brief 验证跨线程关闭本地连接流会释放 awaitable，且服务器仍可正常清理。
void TestFiberAwait::test_case_local_server_queued_close_release()
{
    LocalServerNameGuard serverName;
    auto server = new QLocalServer;
    QVERIFY(server->listen(serverName.name()));
    QThread worker;
    server->moveToThread(&worker);

    auto incoming = Coro::coro(server).nextConnection();
    std::weak_ptr<Coro::Awaitable<QLocalSocket*>> observed = incoming;
    incoming->close();
    incoming.reset();
    worker.start();

    QElapsedTimer elapsed;
    elapsed.start();
    while(!observed.expired() && elapsed.elapsed() < 2000) QTest::qWait(10);

    bool listening = false;
    QMetaObject::invokeMethod(server, [server, &listening]{
        listening = server->isListening();
        server->close();
        delete server;
    }, Qt::BlockingQueuedConnection);
    worker.quit();
    const bool stopped = worker.wait(2000);

    QVERIFY(observed.expired());
    QVERIFY(listening);
    QVERIFY(stopped);
}

/// @brief 验证停止监视器后直接关闭本地连接流不影响服务器接受连接。
void TestFiberAwait::test_case_local_server_stream_direct_close()
{
    LocalServerNameGuard serverName;
    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));
    auto incoming = Coro::coro(&server).nextConnection();
    std::weak_ptr<Coro::Awaitable<QLocalSocket*>> observed = incoming;
    const auto monitors = server.findChildren<QTimer*>();
    QCOMPARE(monitors.size(), 1);
    for(QTimer* monitor : monitors) monitor->stop();

    incoming->close();
    incoming.reset();
    const bool releasedOnClose = observed.expired();

    QLocalSocket client;
    client.connectToServer(serverName.name());
    QTRY_COMPARE_WITH_TIMEOUT(client.state(), QLocalSocket::ConnectedState, 2000);
    QTest::qWait(50);

    QVERIFY(releasedOnClose);
    QVERIFY(server.hasPendingConnections());
    delete server.nextPendingConnection();
}

/// @brief 验证销毁本地服务器会清理未交付的排队连接并终止流。
void TestFiberAwait::test_case_local_server_destroy_purges_queued_connection()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard serverName;
    auto server = new QLocalServer;
    QVERIFY(server->listen(serverName.name()));
    auto incoming = Coro::coro(server).nextConnection();
    QSignalSpy connectionSignal(server, &QLocalServer::newConnection);

    QLocalSocket client;
    client.connectToServer(serverName.name());
    QTRY_COMPARE_WITH_TIMEOUT(client.state(), QLocalSocket::ConnectedState, 2000);
    QTRY_VERIFY_WITH_TIMEOUT(connectionSignal.count() > 0, 2000);
    QVERIFY(!server->hasPendingConnections());

    delete server;
    auto finished = Coro::await_for(incoming, 100ms);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
}

/// @brief 验证已关闭的本地连接流在服务器销毁时仍清理排队连接。
void TestFiberAwait::test_case_local_closed_server_stream_purges_on_destroy()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard serverName;
    auto server = new QLocalServer;
    QVERIFY(server->listen(serverName.name()));
    auto incoming = Coro::coro(server).nextConnection();
    QSignalSpy connectionSignal(server, &QLocalServer::newConnection);

    QLocalSocket client;
    client.connectToServer(serverName.name());
    QTRY_COMPARE_WITH_TIMEOUT(client.state(), QLocalSocket::ConnectedState, 2000);
    QTRY_VERIFY_WITH_TIMEOUT(connectionSignal.count() > 0, 2000);
    QVERIFY(!server->hasPendingConnections());

    incoming->close();
    delete server;
    auto finished = Coro::await_for(incoming, 100ms);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
}

/// @brief 验证本地服务器缺失失败后，同一客户端可连接新服务器。
void TestFiberAwait::test_case_local_retry_after_missing_server()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard missingName;
    LocalServerNameGuard serverName;
    QLocalSocket client;

    auto missing = Coro::await_for(
        Coro::coro(&client).connectToServer(missingName.name()), 2s);
    QVERIFY(!missing);
    QCOMPARE(QString::fromLatin1(missing.error().category().name()),
             QStringLiteral("qt.local_socket"));
    QCOMPARE(missing.error().value(),
             static_cast<int>(QLocalSocket::ServerNotFoundError));

    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));
    auto incoming = Coro::coro(&server).nextConnection();
    auto retry = Coro::await_for(
        Coro::coro(&client).connectToServer(serverName.name()), 2s);
    auto accepted = Coro::await_for(incoming, 2s);

    QVERIFY(retry);
    QVERIFY(accepted);
    QVERIFY(accepted.value() != nullptr);
}

/// @brief 验证本地读取流先交付末尾字节，再以正常终止结束。
void TestFiberAwait::test_case_local_read_then_peer_close()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard serverName;
    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));
    auto incoming = Coro::coro(&server).nextConnection();

    QLocalSocket client;
    QVERIFY(Coro::await_for(
        Coro::coro(&client).connectToServer(serverName.name()), 2s));
    auto accepted = Coro::await_for(incoming, 2s);
    QVERIFY(accepted);
    QLocalSocket* peer = accepted.value();
    QVERIFY(peer != nullptr);
    auto stream = Coro::coro(&client).readAll();

    auto written = Coro::coro(peer).waitForBytesWritten();
    QCOMPARE(peer->write("final-bytes"), qint64(11));
    QVERIFY(Coro::await_for(written, 2s));
    peer->disconnectFromServer();

    auto bytes = Coro::await_for(stream, 2s);
    auto finished = Coro::await_for(stream, 2s);
    QVERIFY(bytes);
    QCOMPARE(bytes.value(), QByteArray("final-bytes"));
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));
}

/// @brief 验证直接关闭本地读取流会释放 awaitable 而不吞掉后续 socket 数据。
void TestFiberAwait::test_case_local_read_stream_direct_close()
{
    using namespace std::chrono_literals;
    LocalServerNameGuard serverName;
    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));
    auto incoming = Coro::coro(&server).nextConnection();

    QLocalSocket client;
    QVERIFY(Coro::await_for(
        Coro::coro(&client).connectToServer(serverName.name()), 2s));
    auto accepted = Coro::await_for(incoming, 2s);
    QVERIFY(accepted);
    QLocalSocket* peer = accepted.value();

    auto stream = Coro::coro(&client).readAll();
    std::weak_ptr<Coro::Awaitable<QByteArray>> observed = stream;
    stream->close();
    stream.reset();

    const QByteArray payload("after-close");
    QCOMPARE(peer->write(payload), qint64(payload.size()));
    peer->flush();
    QTRY_VERIFY_WITH_TIMEOUT(observed.expired() &&
                             client.bytesAvailable() == payload.size(), 1000);
    QCOMPARE(client.readAll(), payload);
}

/// @brief 验证在线程启动前关闭排队的本地连接操作不会建立连接。
void TestFiberAwait::test_case_local_queued_connect_cancel()
{
    LocalServerNameGuard serverName;
    QLocalServer server;
    QVERIFY(server.listen(serverName.name()));

    QThread worker;
    auto client = new QLocalSocket;
    client->moveToThread(&worker);
    auto operation = Coro::coro(client).connectToServer(serverName.name());
    operation->close();
    worker.start();

    QLocalSocket::LocalSocketState state = QLocalSocket::ConnectedState;
    const bool inspected = QMetaObject::invokeMethod(
        client, [client, &state]{
            state = client->state();
            delete client;
        }, Qt::BlockingQueuedConnection);
    worker.quit();
    const bool stopped = worker.wait(2000);

    QVERIFY(inspected);
    QVERIFY(stopped);
    QCOMPARE(state, QLocalSocket::UnconnectedState);
    QVERIFY(!server.hasPendingConnections());
}

/// @brief 验证未绑定且未连接的 UDP socket 会立即正常结束数据报流。
/// @details 该行为不能伪装成超时，消费者必须收到 no_message 终止错误。
void TestFiberAwait::test_case_udp_unconnected_socket_ends_stream()
{
    using namespace std::chrono_literals;
    QUdpSocket receiver;
    auto datagrams = Coro::coro(&receiver).receiveDatagram();

    auto finished = Coro::await_for(datagrams, 100ms);
    QVERIFY(!finished);
    QVERIFY2(finished.error() == std::make_error_code(std::errc::no_message),
             finished.error().message().c_str());
}

/// @brief 验证 UDP 数据报流保留数据报边界、顺序和发送方元数据。
void TestFiberAwait::test_case_udp_preserves_datagrams_and_sender_metadata()
{
    using namespace std::chrono_literals;
    QUdpSocket receiver;
    QUdpSocket sender;
    QVERIFY(receiver.bind(QHostAddress(QHostAddress::LocalHost), quint16(0)));
    QVERIFY(sender.bind(QHostAddress(QHostAddress::LocalHost), quint16(0)));

    auto datagrams = Coro::coro(&receiver).receiveDatagram();
    static_assert(std::is_same_v<decltype(datagrams),
                                 std::shared_ptr<Coro::Awaitable<QNetworkDatagram>>>);

    QCOMPARE(sender.writeDatagram("first", QHostAddress::LocalHost,
                                  receiver.localPort()), qint64(5));
    QCOMPARE(sender.writeDatagram("second", QHostAddress::LocalHost,
                                  receiver.localPort()), qint64(6));

    auto first = Coro::await_for(datagrams, 2s);
    auto second = Coro::await_for(datagrams, 2s);
    QVERIFY(first);
    QVERIFY(second);
    QCOMPARE(first.value().data(), QByteArray("first"));
    QCOMPARE(second.value().data(), QByteArray("second"));
    QCOMPARE(first.value().senderPort(), sender.localPort());
    QCOMPARE(second.value().senderPort(), sender.localPort());
    QCOMPARE(first.value().senderAddress(), QHostAddress::LocalHost);
    QCOMPARE(second.value().senderAddress(), QHostAddress::LocalHost);
}

/// @brief 验证直接关闭 UDP 数据报流会释放 awaitable 而不影响 socket 缓冲数据。
void TestFiberAwait::test_case_udp_stream_direct_close()
{
    QUdpSocket receiver;
    QUdpSocket sender;
    QVERIFY(receiver.bind(QHostAddress(QHostAddress::LocalHost), quint16(0)));
    QVERIFY(sender.bind(QHostAddress(QHostAddress::LocalHost), quint16(0)));
    auto datagrams = Coro::coro(&receiver).receiveDatagram();
    std::weak_ptr<Coro::Awaitable<QNetworkDatagram>> observed = datagrams;

    datagrams->close();
    datagrams.reset();
    const QByteArray payload("after-close");
    QCOMPARE(sender.writeDatagram(payload, QHostAddress::LocalHost,
                                  receiver.localPort()), qint64(payload.size()));

    QTRY_VERIFY_WITH_TIMEOUT(observed.expired() &&
                             receiver.hasPendingDatagrams(), 1000);
    QCOMPARE(receiver.receiveDatagram().data(), payload);
}

/// @brief 验证关闭 UDP socket 会正常终止数据报流并释放 awaitable。
void TestFiberAwait::test_case_udp_close_ends_stream_and_releases()
{
    using namespace std::chrono_literals;
    QUdpSocket receiver;
    QVERIFY(receiver.bind(QHostAddress(QHostAddress::LocalHost), quint16(0)));
    auto datagrams = Coro::coro(&receiver).receiveDatagram();
    std::weak_ptr<Coro::Awaitable<QNetworkDatagram>> observed = datagrams;

    receiver.close();
    auto finished = Coro::await_for(datagrams, 2s);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));

    datagrams.reset();
    QTRY_VERIFY_WITH_TIMEOUT(observed.expired(), 2000);
}

/// @brief 验证销毁 UDP socket 会正常终止数据报流并释放 awaitable。
void TestFiberAwait::test_case_udp_destruction_ends_stream_and_releases()
{
    using namespace std::chrono_literals;
    auto receiver = new QUdpSocket;
    QVERIFY(receiver->bind(QHostAddress(QHostAddress::LocalHost), quint16(0)));
    auto datagrams = Coro::coro(receiver).receiveDatagram();
    std::weak_ptr<Coro::Awaitable<QNetworkDatagram>> observed = datagrams;

    delete receiver;
    auto finished = Coro::await_for(datagrams, 2s);
    QVERIFY(!finished);
    QCOMPARE(finished.error(), std::make_error_code(std::errc::no_message));

    datagrams.reset();
    QTRY_VERIFY_WITH_TIMEOUT(observed.expired(), 2000);
}

void TestFiberAwait::cleanupTestCase()
{
    Coro::quit();
    qDebug() << "instance quit";
}

int main(int argc, char *argv[])
{
//    TESTLIB_SELFCOVERAGE_START(#TestObject)
//    QT_PREPEND_NAMESPACE(QTest::Internal::callInitMain)<TestFiberAwait>();
    QCoreApplication app(argc, argv);
    Coro::installFiberApplication();
    TestFiberAwait tc;
    QTEST_SET_MAIN_SOURCE_PATH
    QTest::qExec(&tc, argc, argv);
    return 0;
}
#include "tst_testfiberawait.moc"
