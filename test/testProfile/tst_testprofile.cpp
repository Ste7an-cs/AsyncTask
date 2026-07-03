#include <QtTest>

// add necessary includes here
#include "executor/fiberpool.h"
#include "detail/asyncdefine.h"
#include "task/fibertask.h"
#include "task/fiberapplication.h"
#include "await/awaitable.hpp"
#include "await/generator.hpp"
#include "detail/result.hpp"

class TestProfile : public QObject
{
    Q_OBJECT

public:
    TestProfile();
    ~TestProfile();

private slots:
    void test_case1();
    void end();

};

TestProfile::TestProfile()
{

}

TestProfile::~TestProfile()
{

}

void TestProfile::test_case1()
{
    for(int i=0; i<10; i++){
        std::vector<Coro::FiberTask<QString>> tsk;
        for(int i=0; i<100; i++){
            Coro::Generator<int> gen([](auto resolve){
                for(int i=0; i<100000; i++){
                    resolve(i*i);
                    boost::this_fiber::yield();
                }
            });
            std::shared_ptr<Coro::Awaitable<int>> awaiter = std::make_shared<Coro::Awaitable<int>>();
            auto task1 = Coro::makeTask([awaiter](){
                for(int i=0; i<100; i++){
                    awaiter->resolve(i*i);
                    boost::this_fiber::yield();
                }
                return 10;
            }, Coro::Priority::Normal, Coro::Affinity::sticky()).then([awaiter](int v1)->int{
                for(int i=0; i< v1; i++){
                    awaiter->resolve(i*100+120);
                }
                return 20;
            }).on_finally([awaiter](){
                awaiter->close();
            }).on_finally([](){

            }).then([&gen, awaiter](int v2){
                for(auto v: gen){
                    awaiter->resolve(v2*(v+120+10000));
                }
                return;
            }).then([]()->QString{
                return "finish";
            });
            auto task2 = Coro::makeTask([awaiter](){
                int cnt{0};
                for(;;){
                    Coro::Result<int> res = awaiter->await();
                    if(res){
                        cnt++;
                    }else{
                        break;
                    }
                }
                return cnt;
            }, Coro::Priority::Normal, Coro::Affinity::sticky()).then([](int cnt)->QString{
                for(int i=0; i<cnt; i++){
                    boost::this_fiber::yield();
                }
                return "finish";
            });
            tsk.emplace_back(std::move(task1));
            tsk.emplace_back(std::move(task2));
        }
        for(auto& t: tsk){
            t.get();
        }
    }
}

void TestProfile::end()
{
    Coro::FiberApplication::instance()->quit();
    qDebug() << "instance quit";
}

int main(int argc, char *argv[])
{
//    TESTLIB_SELFCOVERAGE_START(#TestObject)
//    QT_PREPEND_NAMESPACE(QTest::Internal::callInitMain)<test_fiber_task>();
    QCoreApplication app(argc, argv);
    Coro::installFiberApplication();
    TestProfile tc;
    QTEST_SET_MAIN_SOURCE_PATH
    QTest::qExec(&tc, argc, argv);
    qDebug() << "finish qExec";
    return 0;
}
#include "tst_testprofile.moc"
