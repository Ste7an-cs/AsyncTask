#include <QtTest>

#include "await/awaitable.hpp"
#include "detail/taskcancel.hpp"
#include <QElapsedTimer>
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <system_error>
#include <thread>

// add necessary includes here
#include "executor/fiberpool.h"
#include "detail/asyncdefine.h"
#include "task/fibertask.h"
#include "task/fiberapplication.h"
#include <vector>
#include <array>
#include <map>
#include <future>

#define TQVERIFY(statement) \
do {\
    if (!QTest::qVerify(static_cast<bool>(statement), #statement, "", __FILE__, __LINE__))\
        break;\
} while (false)

namespace {

std::error_code interruptedError()
{
    return std::make_error_code(std::errc::interrupted);
}

/**
 * @brief 有界等待 done；超时则执行 cleanup 再等，避免失败用例把任务永久挂起。
 * @return done 最终是否为 true
 */
bool waitForDone(std::atomic_bool& done, int timeoutMs,
                 const std::function<void()>& cleanup)
{
    QElapsedTimer timer;
    timer.start();
    while(!done.load() && timer.elapsed() < timeoutMs){
        QTest::qWait(1);
    }
    if(!done.load()){
        if(cleanup){
            cleanup();
        }
        timer.restart();
        while(!done.load() && timer.elapsed() < 2000){
            QTest::qWait(1);
        }
    }
    return done.load();
}

/**
 * @brief 默认构造立即抛出的值类型：让 await 在登记取消节点之后、等待之前抛出。
 */
struct ThrowingValue {
    struct Boom {};
    ThrowingValue(){ throw Boom{}; }
    ThrowingValue(const ThrowingValue&){ throw Boom{}; }
    ThrowingValue(ThrowingValue&&){ throw Boom{}; }
    ThrowingValue& operator=(const ThrowingValue&){ throw Boom{}; }
    ThrowingValue& operator=(ThrowingValue&&){ throw Boom{}; }
};

} // namespace

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
    void test_case_cancel_slot_aborts_active_node();
    void test_case_finally_one_shot_after_then();
    void test_case_finally_late_registration_runs_immediately();
    void test_case_finally_throwing_callback_does_not_terminate();
    void test_case_chain_task();
    void test_case_chain_task_execpt();
    void test_case_default_construct_task();
    void test_case_cancel_aborts_current_await();
    void test_case_cancel_aborts_void_await();
    void test_case_cancel_keeps_following_awaits_immediate();
    void test_case_cancel_closes_last_consumer_hub();
    void test_case_cancel_await_for_returns_interrupted();
    void test_case_cancel_keeps_sibling_shared_handle();
    void test_case_cancel_before_await_is_immediate();
    void test_case_cancel_completed_task_is_noop();
    void test_case_cancel_from_other_thread();
    void test_case_cancel_await_for_void();
    void test_case_cancel_after_throwing_value_is_safe();
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

/**
 * @brief TaskCancelSlot 的独立组件测试：登记、取消、注销与幂等。
 */
void test_fiber_task::test_case_cancel_slot_aborts_active_node()
{
    struct Probe {
        int closed{0};
        std::error_code seen{};
        static void close(void* obj, std::error_code ec) noexcept
        {
            auto* self = static_cast<Probe*>(obj);
            ++self->closed;
            self->seen = ec;
        }
    } probe;

    Coro::detail::TaskCancelSlot slot;
    Coro::detail::AwaitableCancelNode node{&probe, &Probe::close};

    QVERIFY(slot.add(&node));
    QVERIFY(!slot.isCancelled());

    slot.cancel(std::make_error_code(std::errc::interrupted));
    QVERIFY(slot.isCancelled());
    QCOMPARE(probe.closed, 1);
    QVERIFY(probe.seen == std::make_error_code(std::errc::interrupted));

    QVERIFY(!slot.add(&node));                  // 已取消后不再登记
    slot.remove(&node);                         // active 已置空，安全空转
    slot.cancel(std::make_error_code(std::errc::interrupted));
    QCOMPARE(probe.closed, 1);                  // 幂等
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
    std::mutex cnt_mutex;
    std::vector<int> cnt{};
    std::atomic_int finalizerCount{0};
    auto task = Coro::makeTask([&vec_id, &cnt, &cnt_mutex](){
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::sleep_for(std::chrono::milliseconds(100));
        {
            std::lock_guard<std::mutex> lock(cnt_mutex);
            cnt.emplace_back(1);
        }
        qDebug() << "then 1";
        return 10;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());
    auto task1 = task.then([&vec_id, &cnt, &cnt_mutex](int v1)->int{
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        TQVERIFY(v1 == 10);
        {
            std::lock_guard<std::mutex> lock(cnt_mutex);
            cnt.emplace_back(2);
        }
        qDebug() << "then 2";
        return 20;
    }).on_finally([&cnt, &cnt_mutex, &finalizerCount](){
        {
            std::lock_guard<std::mutex> lock(cnt_mutex);
            cnt.emplace_back(5);
            finalizerCount.fetch_add(1);
        }
    }).on_finally([&cnt, &cnt_mutex, &finalizerCount](){
        {
            std::lock_guard<std::mutex> lock(cnt_mutex);
            cnt.emplace_back(5);
            finalizerCount.fetch_add(1);
        }
    }).then([&vec_id, &cnt, &cnt_mutex, &task](int v2){
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        TQVERIFY(v2 == 20);
        task.cancel();
        qDebug() << "then 3 cancel";
        {
            std::lock_guard<std::mutex> lock(cnt_mutex);
            cnt.emplace_back(3);
        }
        return;
    }).then([&vec_id, &cnt, &cnt_mutex]()->QString{
        vec_id.emplace_back(std::this_thread::get_id());
        Coro::msleep(100);
        {
            std::lock_guard<std::mutex> lock(cnt_mutex);
            cnt.emplace_back(4);
        }
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
    // get() 在 then 节点 set_value() 时即返回，on_finally 由后续 done_nodes() 执行；
    // 等两个收尾回调都追加完，再在锁内取快照断言。
    QTRY_VERIFY_WITH_TIMEOUT(finalizerCount.load() == 2, 2000);
    std::vector<int> snapshot;
    {
        std::lock_guard<std::mutex> lock(cnt_mutex);
        snapshot = cnt;
    }
    QVERIFY2(snapshot.size() == 5,
             qPrintable(QStringLiteral("cnt.size()=%1, finalizers=%2")
                            .arg(snapshot.size()).arg(finalizerCount.load())));
    QCOMPARE(snapshot[0], 1);
    QCOMPARE(snapshot[1], 2);
    QCOMPARE(snapshot[2], 3);
    QCOMPARE(snapshot[3], 5);
    QCOMPARE(snapshot[4], 5);

}

/**
 * @brief 整链结束后追加 then：已消费的 finalizer 不得随新的 1->0 再度执行。
 */
void test_fiber_task::test_case_finally_one_shot_after_then()
{
    std::atomic_int finalizers{0};
    auto task = Coro::makeTask([]{ return 1; },
                               Coro::Priority::Normal,
                               Coro::Affinity::sticky())
                    .on_finally([&]{ finalizers.fetch_add(1); });
    // 不先用根句柄 get()：boost future 只能取一次，留给后继节点消费。
    QTRY_COMPARE_WITH_TIMEOUT(finalizers.load(), 1, 2000);

    // 完成后追加后继：pending_cnt 0->1->0；finalizer 已消费，不得重跑。
    auto tail = task.then([](int v){ return v + 1; });
    auto second = tail.get();
    QVERIFY(second.has_value());
    QCOMPARE(second.value(), 2);
    QTest::qWait(50);
    QCOMPARE(finalizers.load(), 1);
}

/**
 * @brief 任务链结束后注册 on_finally：立即执行一次；回调抛出也不得终止进程。
 */
void test_fiber_task::test_case_finally_late_registration_runs_immediately()
{
    std::atomic_int early{0};
    std::atomic_int late{0};
    auto task = Coro::makeTask([]{ return 7; },
                               Coro::Priority::Normal,
                               Coro::Affinity::sticky())
                    .on_finally([&]{ early.fetch_add(1); });
    auto result = task.get();
    QVERIFY(result.has_value());
    QCOMPARE(result.value(), 7);
    QTRY_COMPARE_WITH_TIMEOUT(early.load(), 1, 2000);

    // 已结束：新注册的回调立即执行（不得保留到未来节点）。
    task.on_finally([]{ throw 1; });
    task.on_finally([&]{ late.fetch_add(1); });
    QCOMPARE(late.load(), 1);
    QTest::qWait(30);
    QCOMPARE(late.load(), 1);
    QCOMPARE(early.load(), 1);
}

/**
 * @brief 收尾回调抛异常时只跳过该回调，后续回调仍执行，进程不终止。
 */
void test_fiber_task::test_case_finally_throwing_callback_does_not_terminate()
{
    std::atomic_int ran{0};
    auto task = Coro::makeTask([]{ return 3; },
                               Coro::Priority::Normal,
                               Coro::Affinity::sticky())
                    .on_finally([]{ throw 1; })
                    .on_finally([&]{ ran.fetch_add(1); });
    auto result = task.get();
    QVERIFY(result.has_value());
    QCOMPARE(result.value(), 3);
    QTRY_COMPARE_WITH_TIMEOUT(ran.load(), 1, 2000);
}

/**
 * @brief 默认构造的空任务：可放进需要值初始化的容器，且在其上操作不崩
 *
 * get() 返回 no_state 失败结果，then() 返回同样失败的后继任务（不启动协程），
 * cancel() 安全空转；被真实任务赋值后恢复正常语义。
 */
void test_fiber_task::test_case_default_construct_task()
{
    const std::error_code no_state = std::make_error_code(std::future_errc::no_state);

    // 1) 需要值初始化的容器操作可以编译并运行
    std::vector<Coro::FiberTask<int>> vec;
    vec.resize(3);
    QCOMPARE(int(vec.size()), 3);

    // 2) 空任务 get() 返回失败，错误码为 no_state
    for(auto& t : vec){
        auto r = t.get();
        QVERIFY(!r);
        QCOMPARE(r.error(), no_state);
    }

    // 3) void 特化同样成立
    Coro::FiberTask<void> empty_void;
    auto rv = empty_void.get();
    QVERIFY(!rv);
    QCOMPARE(rv.error(), no_state);

    // 4) 空任务上 then() 不崩，后继同样是失败任务
    Coro::FiberTask<int> empty_int;
    auto chained = empty_int.then([](int v){ return v + 1; });
    auto rc = chained.get();
    QVERIFY(!rc);
    QCOMPARE(rc.error(), no_state);

    // 5) 空任务上 cancel() / on_finally() 安全空转
    empty_int.cancel();
    empty_int.on_finally([]{});

    // 6) 被真实任务赋值后恢复正常语义
    vec[0] = Coro::makeTask([]{ return 42; });
    auto r0 = vec[0].get();
    QVERIFY(r0);
    QCOMPARE(r0.value(), 42);

    // 7) std::array / map::operator[] 也依赖默认构造
    std::array<Coro::FiberTask<int>, 2> arr;
    QVERIFY(!arr[0].get());
    std::map<int, Coro::FiberTask<int>> m;
    QVERIFY(!m[7].get());
}

/**
 * @brief cancel() 时任务正阻塞在 await()，当前等待应立即以 interrupted 收敛。
 */
void test_fiber_task::test_case_cancel_aborts_current_await()
{
    Coro::Awaitable<int> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0}; // 1 成功，2 interrupted，3 其他错误

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r = source.await();
        if(r){
            state.store(1);
        }else if(r.error() == interruptedError()){
            state.store(2);
        }else{
            state.store(3);
        }
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);                    // 让协程真正进入 await
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not leave await()");
    QCOMPARE(state.load(), 2);
}

/**
 * @brief cancel() 时任务正阻塞在 Awaitable<void>::await()，同样应以 interrupted 收敛。
 */
void test_fiber_task::test_case_cancel_aborts_void_await()
{
    Coro::Awaitable<void> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0}; // 1 成功，2 interrupted，3 其他错误

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r = source.await();
        if(r){
            state.store(1);
        }else if(r.error() == interruptedError()){
            state.store(2);
        }else{
            state.store(3);
        }
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);                    // 让协程真正进入 await
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not leave Awaitable<void>::await()");
    QCOMPARE(state.load(), 2);
}

/**
 * @brief 取消后，同一任务后续 await 必须立即中断，且不再关闭新句柄。
 */
void test_fiber_task::test_case_cancel_keeps_following_awaits_immediate()
{
    Coro::Awaitable<int> first;
    Coro::Awaitable<int> second;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  firstState{0};
    std::atomic_int  secondState{0};
    std::atomic_llong secondMs{-1};

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r1 = first.await();
        firstState.store(r1 ? 1 : (r1.error() == interruptedError() ? 2 : 3));

        const auto t0 = std::chrono::steady_clock::now();
        auto r2 = second.await();
        secondMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - t0).count());
        secondState.store(r2 ? 1 : (r2.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        first.close(std::make_error_code(std::errc::operation_canceled));
        second.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(firstState.load(), 2);
    QCOMPARE(secondState.load(), 2);
    QVERIFY(secondMs.load() >= 0);
    QVERIFY(secondMs.load() < 1000);
    QVERIFY(!second.isClosed());
}

/**
 * @brief 当前句柄是唯一消费者时，取消引起的队列关闭应让 hub 归零并跑一次清理。
 */
void test_fiber_task::test_case_cancel_closes_last_consumer_hub()
{
    Coro::Awaitable<int> source;
    std::atomic_int cleanups{0};
    source.setOnClose([&]{ cleanups.fetch_add(1); });

    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0};

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r = source.await();
        state.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(state.load(), 2);
    QTRY_VERIFY_WITH_TIMEOUT(cleanups.load() == 1, 1000);
    QVERIFY(source.channel()->is_closed());
}

/**
 * @brief cancel() 时任务阻塞在 await_for()，应返回 interrupted 而不是 timed_out。
 */
void test_fiber_task::test_case_cancel_await_for_returns_interrupted()
{
    Coro::Awaitable<int> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0}; // 1 成功，2 interrupted，3 timed_out，4 其他
    std::atomic_llong elapsedMs{-1};

    auto task = Coro::makeTask([&]{
        started.store(true);
        const auto t0 = std::chrono::steady_clock::now();
        auto r = source.await_for(std::chrono::seconds(5));
        elapsedMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count());
        if(r){
            state.store(1);
        }else if(r.error() == interruptedError()){
            state.store(2);
        }else if(r.error() == std::make_error_code(std::errc::timed_out)){
            state.store(3);
        }else{
            state.store(4);
        }
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not leave await_for()");
    QCOMPARE(state.load(), 2);
    QVERIFY(elapsedMs.load() >= 0);
    QVERIFY(elapsedMs.load() < 1000);
}

/**
 * @brief 取消一个 shared() 句柄不影响同 hub 的其它句柄。
 */
void test_fiber_task::test_case_cancel_keeps_sibling_shared_handle()
{
    Coro::Awaitable<int> source;
    auto h1 = source.shared();
    auto h2 = source.shared();

    std::atomic_bool started1{false};
    std::atomic_bool started2{false};
    std::atomic_bool done1{false};
    std::atomic_bool done2{false};
    std::atomic_int  state1{0};
    std::atomic_int  value2{-1};

    auto t1 = Coro::makeTask([h1, &started1, &done1, &state1]{
        started1.store(true);
        auto r = h1->await();
        state1.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done1.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    auto t2 = Coro::makeTask([h2, &started2, &done2, &value2]{
        started2.store(true);
        auto r = h2->await();
        if(r){
            value2.store(r.value());
        }
        done2.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started1.load() && started2.load(), 3000);
    QTest::qWait(100);
    t1.cancel();
    const bool done1Ok = waitForDone(done1, 2000, cleanup);
    if(!done1Ok || state1.load() != 2){
        // 红阶段/失败路径：cleanup 已关闭 source，等 h2 也收尾，避免提前 return 后局部变量被使用
        (void)waitForDone(done2, 2000, cleanup);
    }
    QVERIFY2(done1Ok, "h1 waiter did not finish");
    QCOMPARE(state1.load(), 2);
    QVERIFY(!done2.load());
    QVERIFY(h1->isClosed());
    QVERIFY(!h2->isClosed());

    QVERIFY(source.resolve(7));
    QVERIFY2(waitForDone(done2, 2000, cleanup), "h2 waiter did not finish");
    QCOMPARE(value2.load(), 7);
    QVERIFY(!h2->isClosed());
}

/**
 * @brief 取消发生在任务进入 await 之前，下一次 await 应立即中断且不关闭句柄。
 */
void test_fiber_task::test_case_cancel_before_await_is_immediate()
{
    Coro::Awaitable<int> source;
    std::atomic_bool entered{false};
    std::atomic_bool gate{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0};
    std::atomic_llong elapsedMs{-1};

    auto task = Coro::makeTask([&]{
        entered.store(true);
        while(!gate.load()){
            Coro::msleep(1);
        }
        const auto t0 = std::chrono::steady_clock::now();
        auto r = source.await();
        elapsedMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count());
        state.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        gate.store(true);
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(entered.load(), 3000);
    task.cancel();
    gate.store(true);
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(state.load(), 2);
    QVERIFY(elapsedMs.load() >= 0);
    QVERIFY(elapsedMs.load() < 1000);
    QVERIFY(!source.isClosed());
}

/**
 * @brief 已完成任务再 cancel 不应崩溃，也不得重复触发结束回调。
 */
void test_fiber_task::test_case_cancel_completed_task_is_noop()
{
    std::atomic_int finalizers{0};
    auto task = Coro::makeTask([]{ return 42; },
                               Coro::Priority::Normal,
                               Coro::Affinity::sticky())
                    .on_finally([&]{ finalizers.fetch_add(1); });
    auto first = task.get();
    QVERIFY(first.has_value());
    QCOMPARE(first.value(), 42);
    // get() 在根节点 set_value() 时即返回，on_finally 由后续 done_nodes() 执行，
    // 因此这里等收尾回调跑完再断言（与本文件 test_case_chain_task_execpt 的等待同理）。
    QTRY_COMPARE_WITH_TIMEOUT(finalizers.load(), 1, 2000);

    // 已完成：没有活动 await；cancel 只置位标志，不得崩溃或重跑收尾。
    task.cancel();
    QTest::qWait(20);
    QCOMPARE(finalizers.load(), 1);
}

/**
 * @brief 从没有 fiber 调度器的普通 std::thread 调用 cancel() 也要安全。
 */
void test_fiber_task::test_case_cancel_from_other_thread()
{
    Coro::Awaitable<int> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0};

    auto task = Coro::makeTask([&]{
        started.store(true);
        auto r = source.await();
        state.store(r ? 1 : (r.error() == interruptedError() ? 2 : 3));
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);

    std::thread canceller([task]() mutable { task.cancel(); });
    canceller.join();

    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not finish");
    QCOMPARE(state.load(), 2);
}

/**
 * @brief cancel() 时任务阻塞在 Awaitable<void>::await_for()，应返回 interrupted 而不是 timed_out。
 */
void test_fiber_task::test_case_cancel_await_for_void()
{
    Coro::Awaitable<void> source;
    std::atomic_bool started{false};
    std::atomic_bool done{false};
    std::atomic_int  state{0}; // 1 成功，2 interrupted，3 timed_out，4 其他
    std::atomic_llong elapsedMs{-1};

    auto task = Coro::makeTask([&]{
        started.store(true);
        const auto t0 = std::chrono::steady_clock::now();
        auto r = source.await_for(std::chrono::seconds(5));
        elapsedMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count());
        if(r){
            state.store(1);
        }else if(r.error() == interruptedError()){
            state.store(2);
        }else if(r.error() == std::make_error_code(std::errc::timed_out)){
            state.store(3);
        }else{
            state.store(4);
        }
        done.store(true);
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    const auto cleanup = [&]{
        source.close(std::make_error_code(std::errc::operation_canceled));
    };

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    QTest::qWait(100);
    task.cancel();
    QVERIFY2(waitForDone(done, 2000, cleanup), "task did not leave Awaitable<void>::await_for()");
    QCOMPARE(state.load(), 2);
    QVERIFY(elapsedMs.load() >= 0);
    QVERIFY(elapsedMs.load() < 1000);
}

/**
 * @brief 值类型构造抛出时，await 的取消登记必须在异常路径上注销。
 * @details 旧实现把栈上节点登记后不设防：T value{} 抛出会让节点随栈帧销毁，而槽内
 *          仍指向它；任务结束后再 cancel 就会读已释放的节点（ASan UAF）。
 *          新实现由 RAII 注销，cancel 不得再关闭这个仍存活的句柄。
 */
void test_fiber_task::test_case_cancel_after_throwing_value_is_safe()
{
    Coro::Awaitable<ThrowingValue> source;
    std::atomic_bool started{false};

    auto task = Coro::makeTask([&]{
        started.store(true);
        (void)source.await();          // T value{} 抛 Boom
        return 0;
    }, Coro::Priority::Normal, Coro::Affinity::sticky());

    QTRY_VERIFY_WITH_TIMEOUT(started.load(), 3000);
    auto result = task.get();
    QVERIFY(!result);
    QCOMPARE(result.error(), interruptedError());

    // 等 await 栈帧和任务 fiber 销毁；旧实现在这里 cancel 会踩已释放的栈节点。
    QTest::qWait(50);
    task.cancel();
    QVERIFY(!source.isClosed());       // 节点已注销：cancel 不应关闭这个句柄
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
