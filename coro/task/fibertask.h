#ifndef FIBERTASK_H
#define FIBERTASK_H
#include <boost/fiber/all.hpp>
#include "detail/asyncdefine.h"
#include "detail/result.hpp"

namespace Coro {

///
/// \brief The SharedState struct 每个Task共享的状态，用于记录Finally
///
struct SharedState{
    std::atomic_int pending_cnt{0};
    std::atomic_bool is_runnable{true};
    std::vector<std::function<void()>> finally_cbs;
    void add_nodes(){
        pending_cnt.fetch_add(1);
    }
    void done_nodes() noexcept {
        if(pending_cnt.fetch_sub(1) == 0){
            for(auto& func : finally_cbs){
                func();
            }
        }
    }
};

///
/// \brief The TaskExit class 用于终止任务的异常
///
class TaskExit : public std::exception{
public:
    TaskExit(const std::string& msg = "task exit"):msg_(msg){}
    const char * what() const noexcept override{
        return msg_.c_str();
    }
private:
    std::string msg_;
};

///
/// \brief terminal_task 终止任务
/// \param msg  终止任务时的消息
///
inline void terminal_task(){
    throw TaskExit();
}

template<typename T>
class FiberTask;

///
/// \brief 结构化并发任务
///
template<typename T>
class FiberTask
{
public:

    ///
    /// \brief FiberTask 链式调用构造函数，该函数不会被使用
    /// \param f
    /// \param s
    /// \param pri
    /// \param affine
    ///
    explicit FiberTask(std::shared_ptr<boost::fibers::future<Result<T>>> f, std::shared_ptr<SharedState> s, Priority pri=Priority::Normal, Affinity affine=Affinity::shared())
        : future_ptr_(f), state_ptr_(s), pri_(pri), affine_(affine){}

    ///
    /// \brief cancel 取消任务
    ///
    void cancel(){
        if(state_ptr_){
            this->state_ptr_->is_runnable.store(false);
        }
    }

    ///
    /// \brief 定义下一个任务，下一个任务的优先级和线程模型继承自上一个任务
    ///
    template<typename Func>
    auto then(Func&& func)
//    ->FiberTask<std::invoke_result_t<Func, T>>
    {
        return then(func, this->pri_, this->affine_);
    }
    ///
    /// \brief 定义下一个任务，并指定优先级和线程模型
    ///
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
    /// 注册任务结束后执行的函数
    template<typename Func>
    auto on_finally(Func&& func){
        if(state_ptr_){
            state_ptr_->finally_cbs.push_back(std::forward<Func>(func));
        }
        return FiberTask<T>(future_ptr_, state_ptr_, pri_, affine_);
    }
    /// 获取结果
    Result<T> get() {
        try {
            return future_ptr_->get();
        } catch(...) {
            return std::make_error_code(std::errc::interrupted);
        }
    }
protected:

private:
    std::shared_ptr<boost::fibers::future<Result<T>>> future_ptr_;
    std::shared_ptr<SharedState> state_ptr_;
    Priority pri_{Priority::Normal};
    Affinity affine_{Affinity::shared()};
};

///
/// \brief FiberTask在返回值为void时的重载
///
template<>
class FiberTask<void>
{
public:
    explicit FiberTask(std::shared_ptr<boost::fibers::future<Result<void>>> f, std::shared_ptr<SharedState> s, Priority pri=Priority::Normal, Affinity affine=Affinity::shared())
        : future_ptr_(f), state_ptr_(s), pri_(pri), affine_(affine){}
    ///
    /// \brief cancel 取消任务
    ///
    void cancel(){
        if(state_ptr_){
            this->state_ptr_->is_runnable.store(false);
        }
    }

    template<typename Func>
    auto then(Func&& func)
//    ->FiberTask<std::invoke_result_t<Func>>
    {
        return then(func, this->pri_, this->affine_);
    }

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
    template<typename Func>
    auto on_finally(Func&& func){
        if(state_ptr_){
            state_ptr_->finally_cbs.push_back(std::forward<Func>(func));
        }
        return FiberTask<void>(future_ptr_, state_ptr_, pri_, affine_);
    }
    Result<void> get() {
        try {
            return future_ptr_->get();
        } catch(...) {
            return std::make_error_code(std::errc::interrupted);
        }
    }
private:
    std::shared_ptr<boost::fibers::future<Result<void>>> future_ptr_;
    std::shared_ptr<SharedState> state_ptr_;
    Priority pri_{Priority::Normal};
    Affinity affine_{Affinity::shared()};
};

///
/// \brief makeTask 创建一个fibertask
/// \param func 第一个task执行函数
/// \param pri  优先级
/// \param affine   线程模型
/// \return
///
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
