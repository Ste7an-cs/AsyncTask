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

class SigObject : public QObject
{
    Q_OBJECT
public:
    SigObject():QObject(nullptr){}
    void fire(){ emit sig1(); }
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

class LocalServerNameGuard
{
public:
    LocalServerNameGuard()
        : name_(QStringLiteral("asynctask-local-%1-%2")
                    .arg(QCoreApplication::applicationPid())
                    .arg(counter_.fetch_add(1)))
    {
        QLocalServer::removeServer(name_);
    }

    ~LocalServerNameGuard()
    {
        QLocalServer::removeServer(name_);
    }

    const QString& name() const { return name_; }

private:
    static std::atomic<unsigned int> counter_;
    QString name_;
};

std::atomic<unsigned int> LocalServerNameGuard::counter_{0};

class SslLoopbackServer final : public QTcpServer
{
public:
    SslLoopbackServer(const QSslCertificate& certificate, const QSslKey& privateKey)
        : certificate_(certificate), privateKey_(privateKey){}

    QSslSocket* peer() const { return peer_; }
    std::shared_ptr<Coro::Awaitable<void>> encrypted() const { return encrypted_; }

protected:
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
    QSslCertificate certificate_;
    QSslKey privateKey_;
    QSslSocket* peer_{nullptr};
    std::shared_ptr<Coro::Awaitable<void>> encrypted_;
};

class PlainTextServer final : public QTcpServer
{
protected:
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
    void test_case_socket_error_conversion();
    void test_case_socket_awaitable_lifetime();
    void test_case_socket_connection_cleanup();
    void test_case_application_lifetime_cleanup();
    void test_case_generator();
    void test_case_signalawait();
    void test_case_signal_generate();
    void test_case_iodevice_await();
    void test_case_iodevice_generate();
    void test_case_tcp_ping_pong();
    void test_case_tcp_connection_refused();
    void test_case_tcp_retry_after_refusal();
    void test_case_tcp_disconnect();
    void test_case_tcp_read_then_remote_close();
    void test_case_tcp_server_close();
    void test_case_tcp_server_closed_stream_release();
    void test_case_tcp_server_queued_close_release();
    void test_case_tcp_server_connection_stream();
    void test_case_ssl_error_conversion();
    void test_case_ssl_encrypted_ping_pong();
    void test_case_ssl_plain_peer_handshake_failure();
    void test_case_local_ping_pong_disconnect();
    void test_case_local_connection_stream_and_close();
    void test_case_local_missing_server();
    void test_case_local_closed_stream_release();
    void test_case_local_server_queued_close_release();
    void test_case_local_retry_after_missing_server();
    void test_case_local_read_then_peer_close();
    void test_case_udp_preserves_datagrams_and_sender_metadata();
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

void TestFiberAwait::test_case_close_overloads()
{
    void (Coro::FiberChannel<int>::*channelClose)() noexcept = &Coro::FiberChannel<int>::close;
    void (Coro::Awaitable<int>::*valueClose)() = &Coro::Awaitable<int>::close;
    void (Coro::Awaitable<void>::*voidClose)() = &Coro::Awaitable<void>::close;
    QVERIFY(channelClose != nullptr);
    QVERIFY(valueClose != nullptr);
    QVERIFY(voidClose != nullptr);
}

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

void TestFiberAwait::test_case_ssl_error_conversion()
{
    const auto sslError = Coro::detail::ssl_error_code(QSslError::HostNameMismatch);
    QCOMPARE(sslError.value(), static_cast<int>(QSslError::HostNameMismatch));
    QCOMPARE(QString::fromLatin1(sslError.category().name()), QStringLiteral("qt.ssl"));
    QVERIFY(!QString::fromStdString(sslError.message()).isEmpty());
}

void TestFiberAwait::test_case_socket_awaitable_lifetime()
{
    QPointer<SigObject> sender = new SigObject;
    auto connections = Coro::detail::socket_connections();
    auto awaitable = Coro::detail::socket_awaitable<int>(connections);
    auto channel = awaitable->channel();
    std::weak_ptr<Coro::Awaitable<int>> observed = awaitable;

    Coro::detail::register_socket_connection(
        connections,
        QObject::connect(sender, &SigObject::sig1,
                         [awaitable]{ awaitable->resolve(1); }));
    Coro::detail::bind_socket_lifecycle(sender, awaitable, connections);
    awaitable.reset();

    QVERIFY(!observed.expired());
    delete sender.data();
    QVERIFY(sender.isNull());
    QVERIFY(channel->is_closed());
    QCOMPARE(channel->close_error(), std::make_error_code(std::errc::no_message));
    QVERIFY(observed.expired());
}

void TestFiberAwait::test_case_socket_connection_cleanup()
{
    QPointer<SigObject> sender = new SigObject;
    auto connections = Coro::detail::socket_connections();
    auto awaitable = Coro::detail::socket_awaitable<int>(connections);
    std::weak_ptr<Coro::Awaitable<int>> observed = awaitable;
    int firstCalls = 0;
    int secondCalls = 0;
    int firstCleanupCalls = 0;
    int secondCleanupCalls = 0;

    Coro::detail::register_socket_cleanup(
        connections, [&firstCleanupCalls]{ ++firstCleanupCalls; });
    Coro::detail::register_socket_cleanup(
        connections, [&secondCleanupCalls]{ ++secondCleanupCalls; });

    Coro::detail::register_socket_connection(
        connections,
        QObject::connect(sender, &SigObject::sig1,
                         [awaitable, &firstCalls]{
        ++firstCalls;
        awaitable->resolve(1);
    }));
    Coro::detail::register_socket_connection(
        connections,
        QObject::connect(sender, &SigObject::sig1,
                         [awaitable, &secondCalls]{
        ++secondCalls;
        awaitable->resolve(2);
    }));

    Coro::detail::cleanup_socket_connections(connections);
    Coro::detail::cleanup_socket_connections(connections);
    QCOMPARE(firstCleanupCalls, 1);
    QCOMPARE(secondCleanupCalls, 1);
    awaitable.reset();
    QVERIFY(observed.expired());
    sender->fire();
    QCOMPARE(firstCalls, 0);
    QCOMPARE(secondCalls, 0);

    auto lateAwaitable = Coro::detail::socket_awaitable<int>(connections);
    std::weak_ptr<Coro::Awaitable<int>> lateObserved = lateAwaitable;
    int lateCalls = 0;
    Coro::detail::register_socket_connection(
        connections,
        QObject::connect(sender, &SigObject::sig1,
                         [lateAwaitable, &lateCalls]{
        ++lateCalls;
        lateAwaitable->resolve(3);
    }));
    lateAwaitable.reset();
    QVERIFY(lateObserved.expired());
    int lateCleanupCalls = 0;
    Coro::detail::register_socket_cleanup(
        connections, [&lateCleanupCalls]{ ++lateCleanupCalls; });
    QCOMPARE(lateCleanupCalls, 1);
    sender->fire();
    QCOMPARE(lateCalls, 0);
    delete sender.data();
}

void TestFiberAwait::test_case_application_lifetime_cleanup()
{
    QPointer<SigObject> sender = new SigObject;
    QPointer<QObject> applicationLifetime = new QObject;
    auto connections = Coro::detail::socket_connections();
    auto awaitable = Coro::detail::socket_awaitable<int>(connections);
    auto channel = awaitable->channel();
    std::weak_ptr<Coro::Awaitable<int>> observed = awaitable;

    Coro::detail::register_socket_connection(
        connections,
        QObject::connect(sender, &SigObject::sig1,
                         [awaitable]{ awaitable->resolve(1); }));
    Coro::detail::bind_socket_lifecycle(sender, awaitable, connections,
                                        applicationLifetime);
    awaitable.reset();

    QVERIFY(!observed.expired());
    delete applicationLifetime.data();
    QVERIFY(applicationLifetime.isNull());
    QVERIFY(channel->is_closed());
    QCOMPARE(channel->close_error(), std::make_error_code(std::errc::no_message));
    QVERIFY(observed.expired());
    delete sender.data();
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
