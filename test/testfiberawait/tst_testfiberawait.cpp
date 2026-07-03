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
#include <QBuffer>
#include <QAbstractSocket>
#include <QTcpSocket>
#include <QTcpServer>

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

class TestFiberAwait : public QObject
{
    Q_OBJECT

public:
    TestFiberAwait();
    ~TestFiberAwait();

private slots:
    void begin();
    void test_case_awaiter();
    void test_case_generator();
    void test_case_signalawait();
    void test_case_signal_generate();
    void test_case_iodevice_await();
    void test_case_iodevice_generate();
    void test_case_socket_await();
    void end();

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

///
/// \brief TestFiberAwait::test_case_socket_await 测试TCP客户端/服务器 ping-pong模式
///
void TestFiberAwait::test_case_socket_await()
{
    QTcpServer *server = new QTcpServer();
    connect(server, &QTcpServer::acceptError, [](QAbstractSocket::SocketError socketError){
        qDebug() << "QTcpServer acceptError" << socketError;
    });
    auto task_server = Coro::makeTask([server](){
        server->listen(QHostAddress::LocalHost, 40080);
        Coro::Generator<QTcpSocket*> gen = Coro::generate(Coro::coro(server).nextConnection());
        int client_cnt{0};
        for(QTcpSocket* p_socket : gen){
            client_cnt++;
            connect(p_socket, &QTcpSocket::aboutToClose, [client_cnt](){
                qDebug() << "p_socket about to close" << client_cnt;
            });
            connect(p_socket, &QTcpSocket::stateChanged, [client_cnt](QAbstractSocket::SocketState state){
                QMetaEnum metaEnum = QMetaEnum::fromType<QAbstractSocket::SocketState>();
                qDebug() << "p_socket stateChanged "<< client_cnt << metaEnum.valueToKey(state);
            });
            Coro::makeTask([p_socket, client_cnt](){
                int k = 0;
                auto gen_msg = Coro::generate(Coro::coro(p_socket).readAll());
                for(const auto & msg : gen_msg){///ping pong服务端
                    p_socket->write(msg);
                    boost::this_fiber::yield();
                }
                qDebug() << "p_socket done " << client_cnt;
            });
            boost::this_fiber::yield();
        }
    });
    auto task_client = Coro::makeTask([](){
        for(int i=0; i<100; i++){
            Coro::makeTask([i](){
                QTcpSocket *client = new QTcpSocket();
                client->connectToHost(QHostAddress::LocalHost, 40080);
                Coro::await(Coro::coro(client).waitForConnected());
                connect(client, &QTcpSocket::aboutToClose, [i](){
                    qDebug() << "client about to close" << i;
                });
                for(int k=0; k<10; k++){
                    if(client->state() != QTcpSocket::ConnectedState){
                        break ;
                    }
                    int size = client->write("aaaaaa");
                    Coro::Result<QByteArray> r = Coro::await(Coro::coro(client).readAll());
                    if(r){
                        TQVERIFY(r.value() == "aaaaaa");
                        qDebug() << "awaitReadAll true" << i << " " << k;
                    }else{
//                        break;
                        qDebug() << "awaitReadAll false" << i;
                    }
                    boost::this_fiber::yield();
                }
                client->close();
                client->deleteLater();
            });
        }
    });
    task_client.get();
    Coro::sleep(4);
    server->close();
    // 立即销毁 server：触发 coro(server).nextConnection() 的 destroyed 收尾，使
    // 服务端生成器结束、task_server.get() 返回。此时全部 ping-pong 已完成、
    // 各连接处理协程均已结束，销毁其子 socket 是安全的。
    delete server;
    task_server.get();
}

void TestFiberAwait::end()
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
