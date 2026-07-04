#ifndef COROSIGNAL_HPP
#define COROSIGNAL_HPP

/**
 * @file corosignal.hpp
 * @brief Qt 信号的协程等待工厂：coro(obj, &T::sig) 返回 Awaitable。
 *
 * 信号无参 -> Awaitable<void>；单参 -> Awaitable<Value>；多参 -> Awaitable<tuple<...>>。
 */

#include <memory>
#include <tuple>
#include <utility>
#include <type_traits>
#include <QObject>

#include "awaitable.hpp"
#include "detail/signalpack.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

namespace detail {

/**
 * @brief 信号 -> Awaitable：连接信号，把参数打包 push 进 channel；析构/来源销毁/退出时收尾。
 * @tparam Obj 发送者对象类型
 * @tparam Sig 信号（成员函数指针）类型
 * @tparam A 信号参数类型包
 * @param obj 发送者对象
 * @param sig 信号
 * @return 对应的 Awaitable
 */
template<class Obj, class Sig, class... A>
auto coro_signal_impl(Obj* obj, Sig sig, std::tuple<A...>*){
    using PR = pack_result<A...>;
    using R  = typename PR::type;
    Awaitable<R> a;
    auto ch   = a.channel();
    auto conn = std::make_shared<QMetaObject::Connection>();
    *conn = QObject::connect(obj, sig, [ch](A... args){
        if constexpr (std::is_void_v<R>){
            (void)sizeof...(args);
            ch->push(1);
        }else{
            ch->push(PR::make(args...));
        }
    });
    a.setOnClose(bind_close(obj, ch, {conn}));
    return a;
}

/**
 * @brief 信号 -> Awaitable（指定所需类型 Want...，取信号前 K 个参数构造 R）
 * @tparam Obj 发送者对象类型
 * @tparam Sig 信号（成员函数指针）类型
 * @tparam Want 期望取用并构造的类型包
 * @tparam A 信号参数类型包
 * @param obj 发送者对象
 * @param sig 信号
 * @return 对应的 Awaitable
 */
template<class Obj, class Sig, class... Want, class... A>
auto coro_signal_typed_impl(Obj* obj, Sig sig, std::tuple<Want...>*, std::tuple<A...>*){
    constexpr std::size_t K = sizeof...(Want);
    constexpr std::size_t N = sizeof...(A);
    static_assert(K <= N, "Coro::coro<Types...>(obj, signal): 指定的形参个数超过信号参数个数");
    using R = typename pack_result<std::decay_t<Want>...>::type;
    Awaitable<R> a;
    auto ch   = a.channel();
    auto conn = std::make_shared<QMetaObject::Connection>();
    if constexpr (std::is_void_v<R>){
        *conn = QObject::connect(obj, sig, [ch](A...){ ch->push(1); });
    }else{
        *conn = QObject::connect(obj, sig, [ch](A... args){
            std::tuple<std::decay_t<A>...> all{args...};
            R res = make_typed<R, Want...>(all, std::make_index_sequence<K>{});
            ch->push(res);
        });
    }
    a.setOnClose(bind_close(obj, ch, {conn}));
    return a;
}

} // detail

/**
 * @brief 等待一个 Qt 信号，返回 Awaitable。
 *
 * 用 await(coro(obj,sig)) 取一次，或 generate(coro(obj,sig)) 流式迭代。
 * @tparam Obj 发送者对象类型
 * @tparam Sig 信号（成员函数指针）类型
 * @param obj 发送者对象
 * @param sig 信号
 * @return 对应的 Awaitable
 */
template<class Obj, class Sig>
auto coro(Obj* obj, Sig sig){
    return detail::coro_signal_impl(
        obj, sig, static_cast<typename detail::signal_args<Sig>::type*>(nullptr));
}

/**
 * @brief 等待一个 Qt 信号并指定所需类型，如 coro<int>(obj, &Obj::sig2)。
 * @tparam W0 期望取用的首个类型
 * @tparam Wr 期望取用的其余类型
 * @tparam Obj 发送者对象类型
 * @tparam Sig 信号（成员函数指针）类型
 * @param obj 发送者对象
 * @param sig 信号
 * @return 对应的 Awaitable
 */
template<class W0, class... Wr, class Obj, class Sig>
auto coro(Obj* obj, Sig sig){
    return detail::coro_signal_typed_impl(
        obj, sig,
        static_cast<std::tuple<W0, Wr...>*>(nullptr),
        static_cast<typename detail::signal_args<Sig>::type*>(nullptr));
}

}

#endif // COROSIGNAL_HPP
