#ifndef SIGNALAWAIT_HPP
#define SIGNALAWAIT_HPP

#include <QObject>
#include <QCoreApplication>
#include "awaitable.hpp"
#include "generator.hpp"

namespace Coro {

namespace detail {

/// 模板工具，提取函数的返回值
template<class T> struct signal_args;
template<class C, class R, class ...A>
struct signal_args<R(C::*)(A...)>{
    using type = std::tuple<std::decay_t<A>...>;
};

/// 模板工具，根据参数个数确定type的类型
/// A为一个，为类型A
/// A为void,为类型void
/// A为多个，为tuple类型
template <class ... A>
struct pack_result{
    using type = std::tuple<A...>;
    static type make(A... a){
        return type(a...);
    }
};
template <class A>
struct pack_result<A>{
    using type = A;
    static type make(A a){
        return a;
    }
};
template <>
struct pack_result<void>{
    using type = void;
};
template <>
struct pack_result<>{
    using type = void;
};
/// 根据传入的tuple类型，获得对应的pack_result
template <class ...A>
constexpr auto tuple_pack_type(std::tuple<A...>*){
    using R = typename pack_result<std::decay_t<A>...>::type;
    return R();
};

///
/// \brief await_single_impl 单次触发await的实现，信号触发一次后disconnect
/// \param obj               sender
/// \param sig               信号
///
template<class Obj, class Sig, class ...A>
auto await_single_impl(Obj* obj, Sig sig, std::tuple<A...>*){
    ///获取类型
    using PR = pack_result<A...>;
    using R = typename PR::type;

    std::shared_ptr<Awaitable<R>> awaitable = std::make_shared<Awaitable<R>>();
    ///绑定信号
    auto conn = std::make_shared<QMetaObject::Connection>();
    *conn = QObject::connect(obj, sig, [awaitable, conn](A... a){
        QObject::disconnect(*conn);//单次触发
        if constexpr(std::is_void_v<R>){
            awaitable->resolve();
        }else{
            awaitable->resolve(PR::make(a...));
        }
        awaitable->close();
    });
    ///释放资源
    QObject::connect(obj, &QObject::destroyed, [awaitable, conn](){
        QObject::disconnect(*conn);//单次触发
        awaitable->close();
    });
    QObject::connect(QCoreApplication::instance(), &QCoreApplication::aboutToQuit, [awaitable, conn](){
        QObject::disconnect(*conn);
        awaitable->close();
    });
    return awaitable;
}

///
/// \brief await_signal_impl 信号await的实现，信号在sender析构或await关闭时disconnect
/// \param obj               sender
/// \param sig               信号
///
template<class Obj, class Sig, class ...A>
auto await_impl(Obj* obj, Sig sig, std::tuple<A...>*){
    ///获取类型
    using PR = pack_result<A...>;
    using R = typename PR::type;

    std::shared_ptr<Awaitable<R>> awaitable = std::make_shared<Awaitable<R>>();
    ///绑定信号
    auto conn = std::make_shared<QMetaObject::Connection>();
    *conn = QObject::connect(obj, sig, [awaitable, conn](A... a){
        bool ret{false};
        if constexpr(std::is_void_v<R>){
            ret = awaitable->resolve();
        }else{
            ret = awaitable->resolve(PR::make(a...));
        }
        if(false == ret){
            QObject::disconnect(*conn);
        }
    });
    ///释放资源
    QObject::connect(obj, &QObject::destroyed, [awaitable, conn](){
        QObject::disconnect(*conn);//单次触发
        awaitable->close();
    });
    QObject::connect(QCoreApplication::instance(), &QCoreApplication::aboutToQuit, [awaitable, conn](){
        QObject::disconnect(*conn);
        awaitable->close();
    });
    return awaitable;
}

template <class R, class... Want, class Tuple, std::size_t... I>
R make_typed(Tuple& t, std::index_sequence<I...>){
    return R(static_cast<std::decay_t<Want>>(std::get<I>(t))...);
}

///
/// \brief await_typed_single_impl 单次触发await的实现，并指定返回值类型，信号触发一次后disconnect
/// \param obj               sender
/// \param sig               信号
///
template<class Obj, class Sig, class ...Want, class... A>
auto await_typed_single_impl(Obj *obj, Sig sig, std::tuple<Want...>*, std::tuple<A...>*){
    ///获取类型
    constexpr std::size_t K = sizeof... (Want);
    constexpr std::size_t N = sizeof... (A);
    static_assert (K<=N, "Coro::await<Types...>(obj, signal): 指定的形参个数超过信号参数个数");
    using R = typename pack_result<std::decay_t<Want>...>::type;
    std::shared_ptr<Awaitable<R>> awaitable = std::make_shared<Awaitable<R>>();
    ///绑定信号
    auto conn = std::make_shared<QMetaObject::Connection>();
    if constexpr (std::is_void_v<R>){
        *conn = QObject::connect(obj, sig, [awaitable, conn](){
            QObject::disconnect(*conn);//单次触发
            awaitable->resolve();
            awaitable->close();
        });
    }else{
        *conn = QObject::connect(obj, sig, [awaitable, conn](A... a){
            QObject::disconnect(*conn);//单次触发
            std::tuple<std::decay_t<A>...> all{a...};
            R res = make_typed<R, Want...>(all, std::make_index_sequence<K>{});
            awaitable->resolve(res);
            awaitable->close();
        });
    }
    ///释放资源
    QObject::connect(obj, &Obj::destroyed, [awaitable, conn](){
        QObject::disconnect(*conn);
        awaitable->close();
    });
    QObject::connect(QCoreApplication::instance(), &QCoreApplication::aboutToQuit, [awaitable, conn](){
        QObject::disconnect(*conn);
        awaitable->close();
    });
    return awaitable;
}

///
/// \brief await_typed_impl 信号await的实现，并指定返回类型，信号在sender析构或await关闭时disconnect
/// \param obj               sender
/// \param sig               信号
///
template<class Obj, class Sig, class ...Want, class... A>
auto await_typed_impl(Obj *obj, Sig sig, std::tuple<Want...>*, std::tuple<A...>*){
    ///获取类型
    constexpr std::size_t K = sizeof... (Want);
    constexpr std::size_t N = sizeof... (A);
    static_assert (K<=N, "Coro::await<Types...>(obj, signal): 指定的形参个数超过信号参数个数");
    using R = typename pack_result<std::decay_t<Want>...>::type;

    std::shared_ptr<Awaitable<R>> awaitable = std::make_shared<Awaitable<R>>();
    ///绑定信号
    auto conn = std::make_shared<QMetaObject::Connection>();
    if constexpr(std::is_void_v<R>){
        *conn = QObject::connect(obj, sig, [awaitable, conn](){
            bool ret = awaitable->resolve();
            if(false == ret){
                QObject::disconnect(*conn);
            }
        });
    }else{
        *conn = QObject::connect(obj, sig, [awaitable, conn](A... a){
            std::tuple<std::decay_t<A>...> all{a...};
            R res = make_typed<R, Want...>(all, std::make_index_sequence<K>{});
            bool ret = awaitable->resolve(res);
            if(false == ret){
                QObject::disconnect(*conn);
            }
        });
    }
    ///释放资源
    QObject::connect(obj, &Obj::destroyed, [awaitable, conn](){
        QObject::disconnect(*conn);
        awaitable->close();
    });
    QObject::connect(QCoreApplication::instance(), &QCoreApplication::aboutToQuit, [awaitable, conn](){
        QObject::disconnect(*conn);
        awaitable->close();
    });
    return awaitable;
}
} // detail

///
/// \brief await 单次协程等待Qt信号的触发,如果需要持续循环等待信号，建议使用generate，避免构造的开销
/// \param obj  发送对象Sender
/// \param sig  信号
/// \return 信号无参数时为Result<void>, 信号只有一个参数时为Result<Value>,信号有多个值时为Result<tuple<...>>
///
template <class Obj, class Sig>
auto await(Obj* obj, Sig sig){
    auto awaitable = detail::await_single_impl(
                obj, sig, static_cast<typename detail::signal_args<Sig>::type*>(nullptr));
    return awaitable->await();
}

///
/// \brief generate 信号生成器，绑定一个Qt的信号，并持续等待信号的触发
/// \param obj  发送对象Sender
/// \param sig  信号
///
template <class Obj, class Sig>
auto generate(Obj* obj, Sig sig){
    using R = typename detail::signal_args<Sig>::type;
    auto awaitable = detail::await_impl(
                obj, sig, static_cast<R*>(nullptr));
    Generator<R> gen([awaitable](auto yield){
        while(1){
            Result<R> res = awaitable->await();
            if(res.has_value()){
                if constexpr (std::is_void_v<R>){
                    yield();
                }else{
                    yield(res.value());
                }
            }else{
                return;
            }
        }
    });
    return gen;
}

///
/// \brief await 单次协程等待Qt信号的触发,并设定超时时间
///
template <class Obj, class Sig, class TimeOut>
auto await_for(Obj* obj, Sig sig, TimeOut t){
    auto awaitable = detail::await_single_impl(
                obj, sig, static_cast<typename detail::signal_args<Sig>::type*>(nullptr));
    return awaitable->await_for(t);
}
///
/// \brief await 单次协程等待Qt信号的触发，并指定所需的类型，如await<int,string>(obj, sig)
///     如果需要持续循环等待信号，建议使用generate，避免构造的开销
/// \param obj  发送对象Sender
/// \param sig  信号
/// \return 信号无参数时为Result<void>, 信号只有一个参数时为Result<Value>,信号有多个值时为Result<tuple<...>>
///
template <class W0, class... Wr, class Obj, class Sig>
auto await(Obj* obj, Sig sig){
    auto awaitable = detail::await_typed_single_impl(
                obj, sig, static_cast<std::tuple<W0, Wr...>*>(nullptr),
                static_cast<std::tuple<W0, Wr...>*>(nullptr));
    return awaitable->await();
}

///
/// \brief generate 信号生成器，绑定一个Qt的信号，并持续等待信号的触发
/// \param obj  发送对象Sender
/// \param sig  信号
///
template <class W0, class... Wr, class Obj, class Sig>
auto generate(Obj* obj, Sig sig){
    using R = decltype (detail::tuple_pack_type(static_cast<std::tuple<W0, Wr...>*>(nullptr)));
    auto awaitable = detail::await_typed_impl(
                obj, sig, static_cast<std::tuple<W0, Wr...>*>(nullptr),
                static_cast<std::tuple<W0, Wr...>*>(nullptr));
    Generator<R> gen([awaitable](auto yield){
        while(1){
            Result<R> res = awaitable->await();
            if(res.has_value()){
                if constexpr (std::is_void_v<R>){
                    yield();
                }else{
                    yield(res.value());
                }
            }else{
                return;
            }
        }
    });
    return gen;
}

///
/// \brief await 单次协程等待Qt信号的触发,并设定超时时间
///
template <class W0, class... Wr, class Obj, class Sig, class TimeOut>
auto await_for(Obj* obj, Sig sig, TimeOut t){
    auto awaitable = detail::await_typed_single_impl(
                obj, sig, static_cast<std::tuple<W0, Wr...>*>(nullptr),
                static_cast<std::tuple<W0, Wr...>*>(nullptr));
    return awaitable->await_for(t);
}



//auto generator()

}

#endif // SIGNALAWAIT_HPP
