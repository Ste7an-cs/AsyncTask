#include <QtTest>

// add necessary includes here
#include "executor/fiberpool.h"
#include "detail/asyncdefine.h"
#include "task/fibertask.h"
#include "task/fiberapplication.h"

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
class test_fiber_task : public QObject
{
    Q_OBJECT

public:
    test_fiber_task();
    ~test_fiber_task();

private slots:
    void begin();
    void test_case_chain_task();
    void test_case_chain_task_execpt();
    void end();

};

test_fiber_task::test_fiber_task()
{

}

test_fiber_task::~test_fiber_task()
{

}

void test_fiber_task::begin()
{
    bool value = std::is_same_v<Coro::result_inner_type_t<Coro::Result<void>>, void>;
    QVERIFY(value == true);
    value = std::is_same_v<Coro::result_inner_type_t<Coro::Result<Coro::Result<void>>>, void>;
    QVERIFY(value == true);
    value = std::is_same_v<Coro::flatten_result_type_t<Coro::Result<Coro::Result<void>>>, Coro::Result<void>>;
    QVERIFY(value == true);

    value = std::is_same_v<Coro::flatten_result_type_t<Coro::Result<Coro::Result<bool>, std::string>>, Coro::Result<bool, std::string>>;
    QVERIFY(value == true);
    value = std::is_same_v<Coro::flatten_result_type_t<Coro::Result<Coro::Result<void>, std::string>>, Coro::Result<void, std::string>>;
    QVERIFY(value == true);

}
///
/// \brief test_fiber_task::test_case_chain_task
///     结构化并发的测试用例，定义链式调用，并注册结束处理函数
///
void test_fiber_task::test_case_chain_task()
{
    std::vector<std::thread::id> vec_id;
    std::atomic<int> cnt{0};
    auto task1 = Coro::makeTask([&vec_id](){
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::sleep_for(std::chrono::milliseconds(100));
        return 10;
    }, Coro::Priority::Normal, Coro::Affinity::sticky()).then([&vec_id](int v1)->int{
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        TQVERIFY(v1 == 10);
        return 20;
    }).on_finally([&cnt](){
        cnt++;
    }).on_finally([&cnt](){
        cnt++;
    }).then([&vec_id](int v2){
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        TQVERIFY(v2 == 20);
        return;
    }).then([&vec_id]()->QString{
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        return "done";
    });
    QString res = task1.get().value_or("");
    TQVERIFY(res == "done");
    auto id0 = vec_id[0];
    for(auto id: vec_id){
        TQVERIFY(id == id0);
    }
    TQVERIFY(cnt == 2);

}

///
/// \brief test_fiber_task::test_case_chain_task_execpt
///     结构化并发的测试用例，定义链式调用，注册结束处理函数，并在运行过程中终止运行
///
void test_fiber_task::test_case_chain_task_execpt()
{

    std::vector<std::thread::id> vec_id;
    std::vector<int> cnt{};
    auto task = Coro::makeTask([&vec_id, &cnt](){
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::sleep_for(std::chrono::milliseconds(100));
        cnt.emplace_back(1);
        qDebug() << "then 1";
        return 10;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task1 = task.then([&vec_id, &cnt](int v1)->int{
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        TQVERIFY(v1 == 10);
        cnt.emplace_back(2);
        qDebug() << "then 2";
        return 20;
    }).on_finally([&cnt](){
        cnt.emplace_back(5);
    }).on_finally([&cnt](){
        cnt.emplace_back(5);
    }).then([&vec_id, &cnt, &task](int v2){
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        TQVERIFY(v2 == 20);
        task.cancel();
        qDebug() << "then 3 cancel";
        cnt.emplace_back(3);
        return;
    }).then([&vec_id, &cnt]()->QString{
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        cnt.emplace_back(4);
        qDebug() << "then 4";
        return "done";
    });
    auto res = task1.get();
    QString str = res.value_or("");
    TQVERIFY(res.has_value() == false);
    TQVERIFY(str == "");
    auto id0 = vec_id[0];
    for(auto id: vec_id){
        TQVERIFY(id == id0);
    }
    TQVERIFY(cnt.size() == 5);
    TQVERIFY(cnt[0] == 1);
    TQVERIFY(cnt[1] == 2);
    TQVERIFY(cnt[2] == 3);
    TQVERIFY(cnt[3] == 5);
    TQVERIFY(cnt[4] == 5);

}

void test_fiber_task::end()
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
    test_fiber_task tc;
    QTEST_SET_MAIN_SOURCE_PATH
    QTest::qExec(&tc, argc, argv);
    qDebug() << "finish qExec";
    return 0;
}
#include "tst_test_fiber_task.moc"
