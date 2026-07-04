#ifndef COROFUTURE_HPP
#define COROFUTURE_HPP

/**
 * @file corofuture.hpp
 * @brief future 的协程等待工厂：coro(std::future<T>&&) / coro(QFuture<T>) 返回 Awaitable<T>。
 */

#include <future>
#include <thread>
#include <memory>

#include "awaitable.hpp"

#ifdef ASYNC_HAS_QTCORE
#include <QObject>
#include <QFuture>
#include <QFutureWatcher>
#endif

namespace Coro {

/**
 * @brief 等待一个 std::future，返回 Awaitable<T>（在独立线程等待完成后 push 结果）
 * @tparam T future 的结果类型
 * @param fu 待等待的 std::future（右值，接管所有权）
 * @return 对应的 Awaitable
 */
template<class T>
Awaitable<T> coro(std::future<T>&& fu){
    Awaitable<T> a;
    auto ch = a.channel();
    auto f  = std::make_shared<std::future<T>>(std::move(fu));
    std::thread([ch, f]{ ch->push(f->get()); }).detach();
    return a;
}

#ifdef ASYNC_HAS_QTCORE
/**
 * @brief 等待一个 QFuture，返回 Awaitable<T>
 * @tparam T QFuture 的结果类型
 * @param f 待等待的 QFuture
 * @return 对应的 Awaitable
 */
template<class T>
Awaitable<T> coro(const QFuture<T>& f){
    Awaitable<T> a;
    auto ch      = a.channel();
    auto watcher = std::make_shared<QFutureWatcher<T>>();
    QObject::connect(watcher.get(), &QFutureWatcherBase::finished, [ch, watcher]{
        ch->push(watcher->result());
    });
    watcher->setFuture(f);
    if(f.isFinished()){
        ch->push(f.result());
    }
    a.setOnClose([watcher]{ /* watcher 随 Awaitable 生命周期一并释放 */ });
    return a;
}
#endif

}

#endif // COROFUTURE_HPP
