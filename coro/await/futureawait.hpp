#ifndef FUTUREAWAIT_HPP
#define FUTUREAWAIT_HPP

#include "awaitable.hpp"
#ifdef ASYNC_HAS_QTCORE
#include <QFuture>
#include <QFutureWatcher>
#endif
namespace Coro {

template <typename T>
auto await(std::future<T>&& fu){
    Awaitable<T> awaiter{};
    auto th = std::thread([&awaiter, &fu](){
        T res = fu.get();
        awaiter.resolve(res);
    });
    th.detach();
    return awaiter.await();
}
template <typename T, typename Rep, typename Period>
auto await(std::future<T>&& fu, const std::chrono::duration<Rep, Period>& timeout){
    Awaitable<T> awaiter{};
    auto th = std::thread([&awaiter, &fu, timeout](){
        std::future_status status = fu.wait_for(timeout);
        if(std::future_status::ready == status){
            awaiter.resolve(fu.get());
        }
        return;
    });
    th.detach();
    return awaiter.await_for(timeout);
}
#ifdef ASYNC_HAS_QTCORE
template <class T>
Result<T> await(const QFuture<T> f){
    Awaitable<T> awaiter{};
    QFutureWatcher<T> watcher;
    QObject::connect(&watcher, &QFutureWatcherBase::finished, [&awaiter, &watcher](){
        awaiter.resolve(watcher.result());
    });
    watcher.setFuture(f);
    if(f.isFinished()){
        return f.result();
    }else{
        return awaiter.await();
    }
}
template <class T, typename Rep, typename Period>
Result<T> await_for(const QFuture<T> f, const std::chrono::duration<Rep, Period>& timeout){
    Awaitable<T> awaiter{};
    QFutureWatcher<T> watcher;
    QObject::connect(&watcher, &QFutureWatcherBase::finished, [&awaiter, &watcher](){
        awaiter.resolve(watcher.result());
    });
    watcher.setFuture(f);
    if(f.isFinished()){
        return f.result();
    }else{
        return awaiter.await_for(timeout);
    }
}
template <>
Result<void> await(const QFuture<void> f){
    Awaitable<void> awaiter{};
    QFutureWatcher<void> watcher;
    QObject::connect(&watcher, &QFutureWatcherBase::finished, [&awaiter](){
        awaiter.resolve();
    });
    watcher.setFuture(f);
    if(f.isFinished()){
        return Result<void>();
    }else{
        return awaiter.await();
    }
}
template <typename Rep, typename Period>
Result<void> await_for(const QFuture<void> f, const std::chrono::duration<Rep, Period>& timeout){
    Awaitable<void> awaiter{};
    QFutureWatcher<void> watcher;
    QObject::connect(&watcher, &QFutureWatcherBase::finished, [&awaiter](){
        awaiter.resolve();
    });
    watcher.setFuture(f);
    if(f.isFinished()){
        return Result<void>();
    }else{
        return awaiter.await_for(timeout);
    }
}
#endif
}

#endif // FUTUREAWAIT_HPP
