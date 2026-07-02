#include <QtTest>

// add necessary includes here
#include "executor/fiberpool.h"
#include "executor/qtfiberthread.h"
#include "executor/scheduler/qtfiberscheduler.h"
#include "detail/asyncdefine.h"

class test_executor : public QObject
{
    Q_OBJECT

public:
    test_executor();
    ~test_executor();

private slots:
    void test_case_qtfiberthread();
    void test_case_fiberpool();
};

test_executor::test_executor()
{

}

test_executor::~test_executor()
{

}
class TObject : public QObject{
    Q_OBJECT
public slots:
    void run(){
        for(int i=0; i<100; i++){
            boost::fibers::fiber fb(Coro::launch_properties(
                 [this](){
                     for(int i=0; i<10; i++){
                         boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                     }
                     shared_cnt.fetch_add(1);
                 },
                Coro::Priority::High, Coro::Affinity::shared()));
            fb.detach();
        }
        for(int i=0; i<100; i++){
            boost::fibers::fiber fb(Coro::launch_properties(
                 [this](){
                     for(int i=0; i<10; i++){
                         boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                     }
                     fixed_cnt.fetch_add(1);
                     qDebug() << "fb finish" << QDateTime::currentDateTime();
                 },
                Coro::Priority::High, Coro::Affinity::fixed(std::this_thread::get_id())));
            fb.detach();
        }
    }
public:
    ~TObject(){
        qDebug() << "~TObject";
    }
    std::atomic_int shared_cnt{0};
    std::atomic_int fixed_cnt{0};
};

void test_executor::test_case_qtfiberthread()
{
    boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
    Coro::QtFiberThread *thread = new Coro::QtFiberThread();

    TObject *obj = new TObject();
    obj->moveToThread(thread);
    connect(thread, &QThread::started, obj, &TObject::run);
    thread->start();
    ///在主线程中创建fiber，测试shared模式下能否正确调度
    boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();

    QObject::connect(thread, &Coro::QtFiberThread::finished, thread, &Coro::QtFiberThread::deleteLater);
    QObject::connect(thread, &Coro::QtFiberThread::finished, [](){
        qDebug() << "QtFiberThread::finished" << QDateTime::currentDateTime();
    });
    QObject::connect(thread, &Coro::QtFiberThread::finished, obj, &TObject::deleteLater);
    qDebug() << "begin" << QDateTime::currentDateTime();
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    qDebug() << "VERIFY" << QDateTime::currentDateTime();
    QVERIFY(obj->shared_cnt==100);
    QVERIFY(obj->fixed_cnt==100);
    thread->quit();
    thread->deleteLater();
    qDebug() << "thread->quit()";

}
void test_executor::test_case_fiberpool()
{
    boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
    Coro::FibersPool::instance();
    ///在主线程中创建fiber，测试shared模式下能否正确调度
    std::atomic_int shared_cnt{0};
    for(int i=0; i<100; i++){
        boost::fibers::fiber fb(Coro::launch_properties(
             [&shared_cnt](){
                 for(int i=0; i<10; i++){
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                 }
                 shared_cnt.fetch_add(1);
             },
            Coro::Priority::High, Coro::Affinity::shared()));
        fb.detach();
    }
    std::atomic_int sticky_cnt{0};
    for(int i=0; i<100; i++){
        boost::fibers::fiber fb(Coro::launch_properties(
             [&sticky_cnt](){
                 for(int i=0; i<10; i++){
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                 }
                 sticky_cnt.fetch_add(1);
                 qDebug() << "fb2 finish" << QDateTime::currentDateTime();
             },
            Coro::Priority::High, Coro::Affinity::sticky()));
        fb.detach();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    Coro::FibersPool::instance().close();
    qDebug() << "FibersPool close" << QDateTime::currentDateTime();
    QVERIFY(shared_cnt==100);
    QVERIFY(sticky_cnt==100);
}


QTEST_GUILESS_MAIN(test_executor)

#include "tst_test_executor.moc"
