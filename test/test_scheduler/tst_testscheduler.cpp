#include <QtTest>

// add necessary includes here
#include <vector>
#include <tuple>
#include <boost/fiber/all.hpp>
#include <executor/scheduler/fiberscheduler.h>
#include <executor/scheduler/fiberthreadblock.h>
#include <boost/fiber/future.hpp>
#include <executor/scheduler/fibertaskqueue.h>
#include <executor/scheduler/qtfiberscheduler.h>
#include <executor/scheduler/qtlocalfiberscheduler.h>
#include "detail/asyncdefine.h"

class TestScheduler : public QObject
{
    Q_OBJECT

public:
    TestScheduler();
    ~TestScheduler();

private slots:

    void test_case_taskqueue();
    void test_case_global_taskqueue();
    void test_case_thread_affine1();
    void test_case_thread_affine2();
    void test_case_thread_block();
    void test_case_properties_change();
    void test_case_qtfiber_scheduler();

};

TestScheduler::TestScheduler()
{

}

TestScheduler::~TestScheduler()
{

}
///
/// \brief TestScheduler::test_case_taskqueue 测试FiberTaskQueue的pop能否能按优先级排序，size是否正确
///
void TestScheduler::test_case_taskqueue()
{
    Coro::FiberTaskQueue queue;
    queue.emplace_back(Coro::MetaContext(Coro::Priority::High, Coro::Affinity::shared(), nullptr));
    queue.emplace_back(Coro::MetaContext(Coro::Priority::Normal, Coro::Affinity::shared(), nullptr));
    queue.emplace_back(Coro::MetaContext(Coro::Priority::Low, Coro::Affinity::shared(), nullptr));
    QVERIFY(queue.size() == 3);

    std::optional<Coro::MetaContext> meta;
    meta = queue.pop_front();
    QVERIFY(meta.value().priority() == Coro::Priority::High);
    QVERIFY(meta.value().affinity() == Coro::Affinity::shared());
    QVERIFY(queue.size() == 2);
    meta = queue.pop_front();
    QVERIFY(meta.value().priority() == Coro::Priority::Normal);
    QVERIFY(meta.value().affinity() == Coro::Affinity::shared());
    QVERIFY(queue.size() == 1);
    meta = queue.pop_front();
    QVERIFY(meta.value().priority() == Coro::Priority::Low);
    QVERIFY(meta.value().affinity() == Coro::Affinity::shared());
    QVERIFY(queue.size() == 0);
}

///
/// \brief TestScheduler::test_case_global_taskqueue 测试FiberGlobalQueue的pop能否能按Meta类型输出，并按优先级排序，size是否正确
///
void TestScheduler::test_case_global_taskqueue()
{
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::High, Coro::Affinity::shared(), nullptr));
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::Normal, Coro::Affinity::shared(), nullptr));
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::Low, Coro::Affinity::shared(), nullptr));
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 3);
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::High, Coro::Affinity::fixed(std::this_thread::get_id()), nullptr));
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::Normal, Coro::Affinity::fixed(std::this_thread::get_id()), nullptr));
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::Low, Coro::Affinity::fixed(std::this_thread::get_id()), nullptr));
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 6);
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::High, Coro::Affinity::sticky(), nullptr));
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::Normal, Coro::Affinity::sticky(), nullptr));
    Coro::FiberGlobalQueue::instance()->emplace_back(Coro::MetaContext(Coro::Priority::Low, Coro::Affinity::sticky(), nullptr));
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 9);

    std::optional<Coro::MetaContext> meta;
    /// 弹出队列, shared
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::shared());
    QVERIFY(meta.value().priority() == Coro::Priority::High);
    QVERIFY(meta.value().affinity() == Coro::Affinity::shared());
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 8);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::shared());
    QVERIFY(meta.value().priority() == Coro::Priority::Normal);
    QVERIFY(meta.value().affinity() == Coro::Affinity::shared());
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 7);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::shared());
    QVERIFY(meta.value().priority() == Coro::Priority::Low);
    QVERIFY(meta.value().affinity() == Coro::Affinity::shared());
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 6);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::shared());//弹出空
    QVERIFY(meta.has_value()==false);
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 6);
    /// 弹出队列, sticky
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::sticky());
    QVERIFY(meta.value().priority() == Coro::Priority::High);
    QVERIFY(meta.value().affinity() == Coro::Affinity::sticky());
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 5);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::sticky());
    QVERIFY(meta.value().priority() == Coro::Priority::Normal);
    QVERIFY(meta.value().affinity() == Coro::Affinity::sticky());
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 4);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::sticky());
    QVERIFY(meta.value().priority() == Coro::Priority::Low);
    QVERIFY(meta.value().affinity() == Coro::Affinity::sticky());
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 3);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::sticky());//弹出空
    QVERIFY(meta.has_value()==false);
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 3);
    /// 弹出队列, fixed
    std::thread::id empty_id(0xfffffff);//空id
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::fixed(empty_id));//弹出不存在的线程ID
    QVERIFY(meta.has_value()==false);
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 3);

    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::fixed(std::this_thread::get_id()));
    QVERIFY(meta.value().priority() == Coro::Priority::High);
    QVERIFY(meta.value().affinity() == Coro::Affinity::fixed(std::this_thread::get_id()));
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 2);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::fixed(std::this_thread::get_id()));
    QVERIFY(meta.value().priority() == Coro::Priority::Normal);
    QVERIFY(meta.value().affinity() == Coro::Affinity::fixed(std::this_thread::get_id()));
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 1);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::fixed(std::this_thread::get_id()));
    QVERIFY(meta.value().priority() == Coro::Priority::Low);
    QVERIFY(meta.value().affinity() == Coro::Affinity::fixed(std::this_thread::get_id()));
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 0);
    meta = Coro::FiberGlobalQueue::instance()->pop_front_affinity(Coro::Affinity::fixed(std::this_thread::get_id()));//弹出空
    QVERIFY(meta.has_value()==false);
    QVERIFY(Coro::FiberGlobalQueue::instance()->size() == 0);

}

///
/// \brief TestScheduler::test_case_thread_affine1 测试线程依附设置是否正确
///     测试fixed模式下，协程是否正确分配至指定线程中
///     测试sticky模式下，协程是否正确绑定至第一次执行的线程中
///
void TestScheduler::test_case_thread_affine1()
{
    std::vector<std::thread> vec_th;
    for(int i=0; i<10; i++){
        vec_th.emplace_back(std::thread([](){
            boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
            auto id = std::this_thread::get_id();
            boost::fibers::fiber fb1(Coro::launch_properties(
                [id](){
                    qDebug() << "start fb1";
                    auto t1 = std::chrono::steady_clock::now();
                    for(int i=0; i<15; i++){
                        boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                        QVERIFY(std::this_thread::get_id() == id);//判断与绑定的线程id是否一致
                    }
                    auto t2 = std::chrono::steady_clock::now();
                    qDebug() << "finish fb1" << std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1).count();
                },
                Coro::Priority::High, Coro::Affinity::fixed(std::this_thread::get_id())));
            boost::fibers::fiber fb2(Coro::launch_properties(
                [id](){
                    auto fb_id = std::this_thread::get_id();//第一次执行时的线程id
                    for(int i=0; i<10; i++){
                        boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
                        if(std::this_thread::get_id() != fb_id){
                            std::ostringstream oss;
                            oss << std::this_thread::get_id() << " " << fb_id;
                            qDebug() << QString::fromStdString(oss.str()) << (int)boost::this_fiber::properties<Coro::MetaContext>().affinity().mode;
                        }
                        QVERIFY(std::this_thread::get_id() == fb_id);//判断与第一次执行的线程id是否一致

                    }
                    if(id != fb_id){
                        qDebug() << "dffi";
                    }
                },
                Coro::Priority::High, Coro::Affinity::sticky()));
            fb2.detach();
            fb1.join();
            qDebug() << "thread finish";
        }));
    }
    for(int i=0; i< (int)vec_th.size(); i++){
        vec_th[i].join();
    }
}

///
/// \brief TestScheduler::test_case_thread_affine2 测试跨线程调度是否正确
///         在主线程中创建fiber,创建多个可执行协程的子线程；
///         此时主线程阻塞，无法执行协程，测试主线程中创建的fiber是否能够正确调度至子线程中并执行
///
void TestScheduler::test_case_thread_affine2()
{
    boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
    std::vector<std::thread> vec_th;
    //开启20个线程用于接收协程任务
    for(int i=0; i<20; i++){
        vec_th.emplace_back(std::thread([](){
            boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
            boost::fibers::fiber fb1(Coro::launch_properties(
                 [](){
                     for(int i=0; i<10; i++){
                         boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                     }
                 },
                Coro::Priority::High, Coro::Affinity::fixed(std::this_thread::get_id())));
            fb1.join();
            qDebug() << "thread finish";
        }));
    }
    ///在主线程中创建fiber，测试shared模式下能否正确调度
    std::atomic_int finish_cnt{0};
    for(int i=0; i<100; i++){
        boost::fibers::fiber fb(Coro::launch_properties(
             [&finish_cnt](){
                 for(int i=0; i<10; i++){
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
                 }
                 finish_cnt.fetch_add(1);
             },
            Coro::Priority::High, Coro::Affinity::shared()));
        fb.detach();
    }
    for(int i=0; i<100; i++){
        boost::fibers::fiber fb(Coro::launch_properties(
             [&finish_cnt](){
                 for(int i=0; i<10; i++){
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
                 }
                 finish_cnt.fetch_add(1);
             },
            Coro::Priority::High, Coro::Affinity::sticky()));
        fb.detach();
    }
    for(int i=0; i<100; i++){
        auto thread_id = vec_th[i%vec_th.size()].get_id();
        boost::fibers::fiber fb(Coro::launch_properties(
             [&finish_cnt](){
                 for(int i=0; i<10; i++){
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
                 }
                 finish_cnt.fetch_add(1);
             },
            Coro::Priority::High, Coro::Affinity::fixed(thread_id)));
        fb.detach();
    }
    //线程阻塞，该线程调度器不会调度
    for(int i=0; i< (int)vec_th.size(); i++){
        vec_th[i].join();
    }
    QVERIFY(finish_cnt==300);
}

void TestScheduler::test_case_thread_block()
{
    boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
    std::vector<std::thread> vec_th;
    std::vector<Coro::FiberThreadBlock> vec_block;
    Coro::FiberThreadBlock block;
    //开启20个线程用于接收协程任务
    for(int i=0; i<20; i++){
        vec_th.emplace_back(std::thread([&block]() mutable{
            boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
            block.wait();
        }));
    }
    //  在一个线程中控制block停止等待，用于控制调度器结束
    std::thread block_th = std::thread([&block](){
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        block.close();
    });
    ///在主线程中创建fiber，测试shared模式下能否正确调度
    std::atomic_int shared_cnt{0};
    for(int i=0; i<100; i++){
        boost::fibers::fiber fb(Coro::launch_properties(
             [&shared_cnt](){
                 for(int i=0; i<10; i++){
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
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
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
                 }
                 sticky_cnt.fetch_add(1);
             },
            Coro::Priority::High, Coro::Affinity::sticky()));
        fb.detach();
    }
    std::atomic_int fixed_cnt{0};
    for(int i=0; i<100; i++){
        auto thread_id = vec_th[i%vec_th.size()].get_id();
        boost::fibers::fiber fb(Coro::launch_properties(
             [&fixed_cnt](){
                 for(int i=0; i<10; i++){
                     boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
                 }
                 fixed_cnt.fetch_add(1);
             },
            Coro::Priority::High, Coro::Affinity::fixed(thread_id)));
        fb.detach();
    }
    //线程阻塞，该线程调度器不会调度
    for(int i=0; i< (int)vec_th.size(); i++){
        vec_th[i].join();
    }
    block_th.join();
    QVERIFY(shared_cnt==100);
    QVERIFY(sticky_cnt==100);
    QVERIFY(fixed_cnt==100);
}

void TestScheduler::test_case_properties_change()
{
    boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
    std::vector<std::thread> vec_th;
    std::vector<Coro::FiberThreadBlock> vec_block;
    Coro::FiberThreadBlock block;
    //开启20个线程用于接收协程任务
    for(int i=0; i<20; i++){
        vec_th.emplace_back(std::thread([&block]() mutable{
            boost::fibers::use_scheduling_algorithm<Coro::FiberScheduler>();
            qDebug() << "begin wait" << QDateTime::currentDateTime();
            block.wait();
            qDebug() << "finish wait" << QDateTime::currentDateTime();
            return;
        }));
    }
    ///在主线程中创建fiber，测试shared模式下能否正确调度
    std::atomic_int shared_cnt{0};
    for(int i=0; i<200; i++){
        auto id1 = vec_th[i%vec_th.size()].get_id();
        auto id2 = vec_th[(i+1)%vec_th.size()].get_id();
        boost::fibers::fiber fb(Coro::launch_properties(
             [&shared_cnt, id1, id2](){
                Coro::MetaContext& meta1 = boost::this_fiber::properties<Coro::MetaContext>();
                meta1.setAffinity(Coro::Affinity::fixed(id1));
                qDebug() << "start fb" << QDateTime::currentDateTime();
                boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                QVERIFY(id1 == std::this_thread::get_id());
                qDebug() << "fb 1" << QDateTime::currentDateTime();
                Coro::MetaContext& meta2 = boost::this_fiber::properties<Coro::MetaContext>();
                meta2.setAffinity(Coro::Affinity::fixed(id2));
                boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                QVERIFY(id2 == std::this_thread::get_id());
                boost::this_fiber::sleep_for(std::chrono::milliseconds(500));
                shared_cnt.fetch_add(1);
                qDebug() << "end fb" << QDateTime::currentDateTime();
             },
            Coro::Priority::High, Coro::Affinity::shared(), "fb1"));
        fb.detach();
    }
    qDebug() << "begin sleep" << QDateTime::currentDateTime();
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    qDebug() << "close wait" << QDateTime::currentDateTime();
    block.close();
    //线程阻塞，该线程调度器不会调度
    for(int i=0; i< (int)vec_th.size(); i++){
        vec_th[i].join();
    }    
    qDebug() << shared_cnt;
    QVERIFY(shared_cnt==200);
}

///
/// \brief test_executor::test_case_qtfiber_scheduler
///     测试QtFiberScheduler能否正确调度
///
void TestScheduler::test_case_qtfiber_scheduler()
{
    boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
    std::vector<std::thread> vec_th;
    std::vector<Coro::FiberThreadBlock> vec_block;
    Coro::FiberThreadBlock block;
    //开启20个线程用于接收协程任务
    for(int i=0; i<20; i++){
        vec_th.emplace_back(std::thread([&block]() mutable{
            boost::fibers::use_scheduling_algorithm<Coro::QtFiberScheduler>();
            block.wait();
        }));
    }
    ///在主线程中创建fiber，测试shared模式下能否正确调度
    std::atomic_int shared_cnt{0};
    for(int i=0; i<200; i++){
        auto id1 = vec_th[i%vec_th.size()].get_id();
        auto id2 = vec_th[(i+1)%vec_th.size()].get_id();
        boost::fibers::fiber fb(Coro::launch_properties(
             [&shared_cnt, id1, id2](){
                Coro::MetaContext& meta1 = boost::this_fiber::properties<Coro::MetaContext>();
                meta1.setAffinity(Coro::Affinity::fixed(id1));
                boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                QVERIFY(id1 == std::this_thread::get_id());
                Coro::MetaContext& meta2 = boost::this_fiber::properties<Coro::MetaContext>();
                meta2.setAffinity(Coro::Affinity::fixed(id2));
                boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
                QVERIFY(id2 == std::this_thread::get_id());
                shared_cnt.fetch_add(1);
             },
            Coro::Priority::High, Coro::Affinity::shared()));
        fb.detach();
    }
    /// 在fiber中创建定时器, 使用sticky模式
    std::atomic_int cnt{0};
    std::atomic_int d_time{0};
    for (int i=0; i<100; i++){
        boost::fibers::fiber fb(Coro::launch_properties(
             [&cnt, &d_time](){
                QTimer *timer = new QTimer();
                int *tp = new int(0);
                auto t1 = QDateTime::currentDateTime();

                connect(timer, &QTimer::timeout, timer, [timer, tp, t1, &cnt, &d_time](){
                    *tp = *tp+1;
                    if(*tp>=10){
                        cnt++;
                        auto t2 = QDateTime::currentDateTime();
                        auto dt = std::abs(t1.msecsTo(t2)-1000);
                        d_time += dt;
                        timer->stop();
                        timer->deleteLater();
                        delete tp;
                    }
                });
                timer->start(100);
             },
            Coro::Priority::High, Coro::Affinity::sticky()));
        fb.detach();
    }
    qDebug() << "FiberGlobalQueue " <<Coro::FiberGlobalQueue::instance()->size();
    boost::this_fiber::sleep_for(std::chrono::milliseconds(2000));
    block.close();
    qDebug() << "FiberGlobalQueue "  << Coro::FiberGlobalQueue::instance()->size();
    //线程阻塞，该线程调度器不会调度
    for(int i=0; i< (int)vec_th.size(); i++){
        vec_th[i].join();
    }
    qDebug() << shared_cnt;
    auto div_time_tick = d_time.load();
    qDebug() << div_time_tick;
    QVERIFY(shared_cnt==200);
    /// 100个定时器，
    /// 每个定时器延时100ms，触发10次，耗时1000ms
    /// 若每个定时器的累计误差为50ms，100个定时器的误差上限为5000
    QVERIFY( div_time_tick < 5000);
    QVERIFY(cnt.load() == 100);
}

QTEST_GUILESS_MAIN(TestScheduler)

#include "tst_testscheduler.moc"
