#ifndef FIBERTASK_H
#define FIBERTASK_H
#include <boost/fiber/all.hpp>
#include "detail/asyncdefine.h"
#include "detail/result.hpp"

namespace Coro {

/**
 * @brief 每个 Task 共享的状态，用于记录在途节点数、取消标志与结束回调。
 *
 * 一条任务链上的所有 FiberTask 共享同一个 SharedState。
 * @code
 * // pending_cnt 记在途节点、is_runnable 为取消标志、finally_cbs 收集结束回调；
 * // 由 makeTask/then 自动维护，使用方通常不直接操作
 * auto t = Coro::makeTask([]{ return 1; }).then([](int v){ return v + 1; });
 * @endcode
 */
struct SharedState{
    std::atomic_int pending_cnt{0};///< 在途节点计数
    std::atomic_bool is_runnable{true};///< 取消标志，false 表示已取消
    std::vector<std::function<void()>> finally_cbs;///< 结束回调列表
    /**
     * @brief 登记一个节点（在途计数加一）
     * @code
     * state->add_nodes();      // makeTask/then 创建节点时调用
     * @endcode
     */
    void add_nodes(){
        pending_cnt.fetch_add(1);
    }
    /**
     * @brief 完成一个节点（在途计数减一），归零时触发全部结束回调
     * @code
     * state->done_nodes();     // 节点结束时调用；计数归零即触发 on_finally
     * @endcode
     */
    void done_nodes() noexcept {
        if(pending_cnt.fetch_sub(1) == 0){
            for(auto& func : finally_cbs){
                func();
            }
        }
    }
};

/**
 * @brief 用于终止任务的异常
 * @code
 * // 由 Coro::terminal_task() 抛出，在协程边界被捕获并转为中断结果
 * try {
 *     Coro::terminal_task();
 * } catch(const Coro::TaskExit& e) {
 *     qDebug() << e.what();
 * }
 * @endcode
 */
class TaskExit : public std::exception{
public:
    /**
     * @brief 构造
     * @param msg 终止任务时的消息
     * @code
     * throw Coro::TaskExit("user cancelled");
     * @endcode
     */
    TaskExit(const std::string& msg = "task exit"):msg_(msg){}
    /**
     * @brief 异常描述
     * @return 消息字符串
     * @code
     * catch(const Coro::TaskExit& e){ qDebug() << e.what(); }
     * @endcode
     */
    const char * what() const noexcept override{
        return msg_.c_str();
    }
private:
    std::string msg_;///< 终止消息
};

/**
 * @brief 终止当前任务（抛出 TaskExit 异常）
 * @code
 * Coro::makeTask([]{
 *     if(!precondition()) Coro::terminal_task();   // 提前结束本任务
 *     return work();                               // 结果为 interrupted 错误
 * });
 * @endcode
 */
inline void terminal_task(){
    throw TaskExit();
}

template<typename T>
class FiberTask;

/**
 * @brief 结构化并发任务句柄。
 *
 * 持有结果 future、共享状态与本任务的优先级/线程模型；then/on_finally/cancel/
 * get 构成链式编排接口。
 * @tparam T 任务返回值类型
 * @code
 * // 链式编排：then 以前驱结果为入参，on_finally 在整链结束时触发
 * auto task = Coro::makeTask([]{ return 10; })
 *                 .then([](int v){ return v + 1; })
 *                 .on_finally([]{ qDebug() << "chain done"; });
 * Coro::Result<int> r = task.get();     // 让出式等待，不阻塞线程
 * // task.cancel();                     // 取消：尚未开始的后继短路
 * @endcode
 */
template<typename T>
class FiberTask
{
public:

    /**
     * @brief 链式调用构造函数（内部使用）
     * @code
     * // 由 makeTask/then 内部构造，使用方不直接调用
     * @endcode
     * @param f 结果 future
     * @param s 任务链共享状态
     * @param pri 优先级
     * @param affine 线程模型
     */
    explicit FiberTask(std::shared_ptr<boost::fibers::future<Result<T>>> f, std::shared_ptr<SharedState> s, Priority pri=Priority::Normal, Affinity affine=Affinity::shared())
        : future_ptr_(f), state_ptr_(s), pri_(pri), affine_(affine){}

    /**
     * @brief 取消任务链（尚未开始的后继在启动前短路）
     * @code
     * auto task = Coro::makeTask(step1).then(step2);
     * task.cancel();      // 协作式取消：不打断正在运行的节点
     * @endcode
     */
    void cancel(){
        if(state_ptr_){
            this->state_ptr_->is_runnable.store(false);
        }
    }

    /**
     * @brief 定义下一个任务，下一个任务的优先级和线程模型继承自上一个任务
     * @code
     * auto task = Coro::makeTask([]{ return 10; })
     *                 .then([](int v){ return v * 2; })    // 前驱结果作为入参
     *                 .then([](int v){ return v + 1; });
     * @endcode
     * @tparam Func 后继函数类型
     * @param func 后继函数
     * @return 后继任务句柄
     */
    template<typename Func>
    auto then(Func&& func)
//    ->FiberTask<std::invoke_result_t<Func, T>>
    {
        return then(func, this->pri_, this->affine_);
    }
    /**
     * @brief 定义下一个任务，并指定优先级和线程模型
     * @code
     * // 把重计算放到任意工作线程，避免占住当前线程
     * auto task = Coro::makeTask([]{ return load(); })
     *                 .then([](Data d){ return heavy(d); },
     *                       Coro::Priority::Low, Coro::Affinity::shared());
     * @endcode
     * @tparam Func 后继函数类型
     * @param func 后继函数（以前驱结果为入参）
     * @param pri 后继优先级
     * @param affine 后继线程模型
     * @return 后继任务句柄
     */
    template<typename Func>
    auto then(Func&& func, Priority pri, Affinity affine)
//    ->FiberTask<std::invoke_result_t<Func, T>>
    {
        using ReturnType = std::invoke_result_t<Func, T>;
        auto f_ptr = future_ptr_;
        auto promise_ptr = std::make_shared<promise<Result<ReturnType>>>();
        auto next_future_ptr = std::make_shared<future<Result<ReturnType>>>(promise_ptr->get_future());
        if(state_ptr_){// 调用链计数+1
            state_ptr_->add_nodes();
        }
        // 创建下一个任务的fiber并绑定关联上一个任务的future
        boost::fibers::fiber fiber = launch_properties(
                    [this, f_ptr, s_ptr = state_ptr_,
                     func = std::forward<Func>(func),
                     p_ptr = promise_ptr]() mutable {
            ///如果s_ptr共享状态指针不为空，每次运行前会检测任务是否可用
            try {
                Result<T> val = f_ptr->get();
                if(val.has_value() && true == s_ptr->is_runnable.load()){
                    if constexpr (std::is_void_v<ReturnType>){
                        func(val.value());
                        p_ptr->set_value(Result<ReturnType>());
                    }else{
                        p_ptr->set_value(Result<ReturnType>(func(val.value())));
                    }
                }else{
                    p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
                }

            } catch (...) {
                p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
            }
            s_ptr->done_nodes();///标记任务已完成

        }, pri, affine);
        fiber.detach();
        return FiberTask<ReturnType>(next_future_ptr, state_ptr_, pri, affine);
    }
    /**
     * @brief 注册任务链全部结束后执行的回调
     * @code
     * // 可登记多个；在整链最后一个节点结束时统一触发，适合集中释放资源
     * auto task = Coro::makeTask(work)
     *                 .on_finally([]{ cleanup(); })
     *                 .on_finally([]{ qDebug() << "done"; });
     * @endcode
     * @tparam Func 回调类型
     * @param func 结束回调
     * @return 自身任务句柄（便于链式调用）
     */
    template<typename Func>
    auto on_finally(Func&& func){
        if(state_ptr_){
            state_ptr_->finally_cbs.push_back(std::forward<Func>(func));
        }
        return FiberTask<T>(future_ptr_, state_ptr_, pri_, affine_);
    }
    /**
     * @brief 获取任务结果（结果未就绪时协程让出等待）
     * @code
     * // 让出式等待：不阻塞线程；也是多协程 join 的手段
     * auto job = Coro::makeTask([]{ return 42; });
     * Coro::makeTask([job]{
     *     auto r = job.get();          // 等 job 完成
     *     if(r) qDebug() << r.value();
     *     return 0;
     * });
     * @endcode
     * @return 任务结果；异常/取消时返回 interrupted 错误
     */
    Result<T> get() {
        try {
            return future_ptr_->get();
        } catch(...) {
            return std::make_error_code(std::errc::interrupted);
        }
    }
protected:

private:
    std::shared_ptr<boost::fibers::future<Result<T>>> future_ptr_;///< 结果 future
    std::shared_ptr<SharedState> state_ptr_;///< 任务链共享状态
    Priority pri_{Priority::Normal};///< 本任务优先级
    Affinity affine_{Affinity::shared()};///< 本任务线程模型
};

/**
 * @brief FiberTask 在返回值为 void 时的特化
 * @code
 * // 协程无返回值时使用；then 的后继不接收入参
 * auto task = Coro::makeTask([]{ doWork(); })
 *                 .then([]{ qDebug() << "next"; })
 *                 .on_finally([]{ cleanup(); });
 * task.get();      // Result<void>：可直接当 bool 判断
 * @endcode
 */
template<>
class FiberTask<void>
{
public:
    /**
     * @brief 链式调用构造函数（内部使用）
     * @code
     * // 由 makeTask/then 内部构造，使用方不直接调用
     * @endcode
     * @param f 结果 future
     * @param s 任务链共享状态
     * @param pri 优先级
     * @param affine 线程模型
     */
    explicit FiberTask(std::shared_ptr<boost::fibers::future<Result<void>>> f, std::shared_ptr<SharedState> s, Priority pri=Priority::Normal, Affinity affine=Affinity::shared())
        : future_ptr_(f), state_ptr_(s), pri_(pri), affine_(affine){}
    /**
     * @brief 取消任务链（尚未开始的后继在启动前短路）
     * @code
     * auto task = Coro::makeTask(step1).then(step2);
     * task.cancel();      // 协作式取消：不打断正在运行的节点
     * @endcode
     */
    void cancel(){
        if(state_ptr_){
            this->state_ptr_->is_runnable.store(false);
        }
    }

    /**
     * @brief 定义下一个任务，优先级和线程模型继承自上一个任务
     * @code
     * // void 前驱：后继函数不接收入参
     * auto task = Coro::makeTask([]{ doWork(); })
     *                 .then([]{ return finish(); });
     * @endcode
     * @tparam Func 后继函数类型
     * @param func 后继函数
     * @return 后继任务句柄
     */
    template<typename Func>
    auto then(Func&& func)
//    ->FiberTask<std::invoke_result_t<Func>>
    {
        return then(func, this->pri_, this->affine_);
    }

    /**
     * @brief 定义下一个任务，并指定优先级和线程模型
     * @code
     * // 把重计算放到任意工作线程，避免占住当前线程
     * auto task = Coro::makeTask([]{ return load(); })
     *                 .then([](Data d){ return heavy(d); },
     *                       Coro::Priority::Low, Coro::Affinity::shared());
     * @endcode
     * @tparam Func 后继函数类型
     * @param func 后继函数
     * @param pri 后继优先级
     * @param affine 后继线程模型
     * @return 后继任务句柄
     */
    template<typename Func>
    auto then(Func&& func, Priority pri, Affinity affine)
//    ->FiberTask<std::invoke_result_t<Func>>
    {
        using ReturnType = std::invoke_result_t<Func>;
        auto f_ptr = future_ptr_;
        auto promise_ptr = std::make_shared<promise<Result<ReturnType>>>();
        auto next_future_ptr = std::make_shared<future<Result<ReturnType>>>(promise_ptr->get_future());
        boost::fibers::fiber fiber = launch_properties(
                    [f_ptr, s_ptr = state_ptr_,
                     func = std::forward<Func>(func),
                     p_ptr = promise_ptr]() mutable {
            try {
                Result<void> val = f_ptr->get();
                if(val.has_value() && true == s_ptr->is_runnable.load()){
                    if constexpr (std::is_void_v<ReturnType>){
                        func();
                        p_ptr->set_value(Result<ReturnType>());
                    }else{
                        p_ptr->set_value(Result<ReturnType>(func()));
                    }
                }else{
                    p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
                }
            } catch (...) {
                p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
            }
            s_ptr->done_nodes();
        }, pri, affine);
        fiber.detach();
        return FiberTask<ReturnType>(next_future_ptr, state_ptr_, pri, affine);
    }
    /**
     * @brief 注册任务链全部结束后执行的回调
     * @code
     * // 可登记多个；在整链最后一个节点结束时统一触发，适合集中释放资源
     * auto task = Coro::makeTask(work)
     *                 .on_finally([]{ cleanup(); })
     *                 .on_finally([]{ qDebug() << "done"; });
     * @endcode
     * @tparam Func 回调类型
     * @param func 结束回调
     * @return 自身任务句柄（便于链式调用）
     */
    template<typename Func>
    auto on_finally(Func&& func){
        if(state_ptr_){
            state_ptr_->finally_cbs.push_back(std::forward<Func>(func));
        }
        return FiberTask<void>(future_ptr_, state_ptr_, pri_, affine_);
    }
    /**
     * @brief 获取任务结果（结果未就绪时协程让出等待）
     * @code
     * // 让出式等待：不阻塞线程；也是多协程 join 的手段
     * auto job = Coro::makeTask([]{ return 42; });
     * Coro::makeTask([job]{
     *     auto r = job.get();          // 等 job 完成
     *     if(r) qDebug() << r.value();
     *     return 0;
     * });
     * @endcode
     * @return 任务结果；异常/取消时返回 interrupted 错误
     */
    Result<void> get() {
        try {
            return future_ptr_->get();
        } catch(...) {
            return std::make_error_code(std::errc::interrupted);
        }
    }
private:
    std::shared_ptr<boost::fibers::future<Result<void>>> future_ptr_;///< 结果 future
    std::shared_ptr<SharedState> state_ptr_;///< 任务链共享状态
    Priority pri_{Priority::Normal};///< 本任务优先级
    Affinity affine_{Affinity::shared()};///< 本任务线程模型
};

/**
 * @brief 创建一个 FiberTask（启动任务链的首个协程）。
 * @code
 * // 最常用的入口：提交可调用体即得任务句柄，创建方不被阻塞
 * auto task = Coro::makeTask([]{ return compute(); });
 *
 * // 指定优先级与线程亲和（默认 Normal + 固定到当前线程）
 * Coro::makeTask([]{ return heavy(); },
 *                Coro::Priority::Low, Coro::Affinity::shared());
 *
 * // 可在任意 Qt 槽/回调中调用，立即返回、协程在后台推进
 * connect(btn, &QPushButton::clicked, []{ Coro::makeTask(handleClick); });
 * @endcode
 *
 * 若为 sticky 模式，先执行一个空函数获取可用的线程 id 再绑定。
 * @tparam Func 首个任务函数类型
 * @param func 第一个 task 执行函数
 * @param pri 优先级
 * @param affine 线程模型（默认固定到当前线程）
 * @return 任务句柄
 */
template <typename Func>
auto makeTask(Func&& func, Priority pri=Priority::Normal, Affinity affine=Affinity::fixed(std::this_thread::get_id()))->FiberTask<std::invoke_result_t<Func>>{
    using ReturnType = std::invoke_result_t<Func>;
    ///如果是sticky模式，先执行一个函数获取可用的线程id
    if(Affinity::sticky() == affine){
        Affinity affined_sticky{affine};
        boost::fibers::fiber fb = launch_properties([&affined_sticky](){
            affined_sticky.fixed_id = std::this_thread::get_id();
        }, pri, affine);
        fb.join();
        return makeTask(func, pri, affined_sticky);
    }
    std::shared_ptr<promise<Result<ReturnType>>> promise_ptr = std::make_shared<promise<Result<ReturnType>>>();
    std::shared_ptr<SharedState> state_ptr = std::make_shared<SharedState>();

    state_ptr->add_nodes();
    boost::fibers::fiber fiber = launch_properties(
                [func = std::forward<Func>(func),
                 p_ptr = promise_ptr, state_ptr]() mutable {
        try {
            if constexpr (std::is_void_v<ReturnType>){
                func();
                p_ptr->set_value(Result<ReturnType>());
            }else{
                ReturnType val = func();;
                p_ptr->set_value(Result<ReturnType>(val));
            }
        } catch (...) {
            p_ptr->set_value(Result<ReturnType>(std::make_error_code(std::errc::interrupted)));
        }
        if(state_ptr){
            state_ptr->done_nodes();
        }
    }, pri, affine);
    fiber.detach();
    std::shared_ptr<future<Result<ReturnType>>> future_ptr = std::make_shared<future<Result<ReturnType>>>(promise_ptr->get_future());
    return FiberTask<ReturnType>(std::move(future_ptr), state_ptr, pri, affine);
}

}

#endif // FIBERTASK_H
