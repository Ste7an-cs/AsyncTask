#ifndef SIGNALPACK_HPP
#define SIGNALPACK_HPP

#include <tuple>
#include <utility>
#include <type_traits>

namespace Coro {
namespace detail {

/// 模板工具，提取信号(成员函数指针)的参数类型为 tuple
template<class T> struct signal_args;
template<class C, class R, class ...A>
struct signal_args<R(C::*)(A...)>{
    using type = std::tuple<std::decay_t<A>...>;
};

/// 模板工具，根据参数个数确定打包结果类型：
///   无参 -> void；单参 -> 该类型；多参 -> tuple
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

/// 根据传入的 tuple 类型，获得对应的 pack_result 结果类型
template <class ...A>
constexpr auto tuple_pack_type(std::tuple<A...>*){
    using R = typename pack_result<std::decay_t<A>...>::type;
    return R();
}

/// 从信号参数 tuple 中，按指定类型 Want... 取前 K 个并构造目标类型 R
template <class R, class... Want, class Tuple, std::size_t... I>
R make_typed(Tuple& t, std::index_sequence<I...>){
    return R(static_cast<std::decay_t<Want>>(std::get<I>(t))...);
}

} // detail
} // Coro

#endif // SIGNALPACK_HPP
