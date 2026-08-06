#ifndef SIGNALPACK_HPP
#define SIGNALPACK_HPP

#include <tuple>
#include <utility>
#include <type_traits>

namespace Coro {
namespace detail {

/**
 * @brief 提取信号（成员函数指针）参数类型为 tuple 的模板工具（主模板）
 * @tparam T 成员函数指针类型
 * @code
 * // 仅声明；实际推导由下方偏特化完成
 * using Args = Coro::detail::signal_args<decltype(&Obj::sig2)>::type;
 * @endcode
 */
template<class T> struct signal_args;
/**
 * @brief signal_args 偏特化：提取成员函数参数为 decay 后的 tuple
 * @tparam C 类类型
 * @tparam R 返回类型
 * @tparam A 参数类型包
 * @code
 * // 信号 void sig2(int, QString) -> std::tuple<int, QString>
 * using Args = Coro::detail::signal_args<decltype(&Obj::sig2)>::type;
 * static_assert(std::tuple_size_v<Args> == 2);
 * @endcode
 */
template<class C, class R, class ...A>
struct signal_args<R(C::*)(A...)>{
    using type = std::tuple<std::decay_t<A>...>;///< 参数类型 tuple
};

/**
 * @brief 根据参数个数确定打包结果类型（多参 -> tuple）
 * @tparam A 参数类型包
 * @code
 * // 决定 coro(obj, sig) 的 Awaitable 元素类型：多参信号打包成 tuple
 * using R = Coro::detail::pack_result<int, QString>::type;   // std::tuple<int,QString>
 * @endcode
 */
template <class ... A>
struct pack_result{
    using type = std::tuple<A...>;///< 打包结果类型
    /**
     * @brief 打包多个参数为 tuple
     * @param a 参数
     * @return 打包后的 tuple
     * @code
     * auto t = Coro::detail::pack_result<int, QString>::make(1, QStringLiteral("a"));
     * @endcode
     */
    static type make(A... a){
        return type(a...);
    }
};
/**
 * @brief pack_result 偏特化：单参 -> 该类型本身
 * @tparam A 单个参数类型
 * @code
 * // 单参信号不套 tuple：coro(obj,&Obj::valueChanged) 得到 Awaitable<int>
 * using R = Coro::detail::pack_result<int>::type;    // int
 * @endcode
 */
template <class A>
struct pack_result<A>{
    using type = A;///< 打包结果类型
    /**
     * @brief 打包单个参数（原样返回）
     * @param a 参数
     * @return 该参数
     * @code
     * int v = Coro::detail::pack_result<int>::make(42);   // 42
     * @endcode
     */
    static type make(A a){
        return a;
    }
};
/**
 * @brief pack_result 特化：显式 void -> void
 * @code
 * using R = Coro::detail::pack_result<void>::type;   // void -> Awaitable<void>
 * @endcode
 */
template <>
struct pack_result<void>{
    using type = void;///< 打包结果类型
};
/**
 * @brief pack_result 特化：无参 -> void
 * @code
 * // 无参信号（如 QTimer::timeout）得到 Awaitable<void>
 * using R = Coro::detail::pack_result<>::type;       // void
 * @endcode
 */
template <>
struct pack_result<>{
    using type = void;///< 打包结果类型
};

/**
 * @brief 根据传入的 tuple 类型获得对应的 pack_result 结果类型
 * @tparam A tuple 元素类型包
 * @return 结果类型的默认值（仅用于 decltype 推导）
 * @code
 * // 仅用于类型推导，不产生实际调用
 * using R = decltype(Coro::detail::tuple_pack_type(
 *     static_cast<std::tuple<int, QString>*>(nullptr)));   // std::tuple<int,QString>
 * @endcode
 */
template <class ...A>
constexpr auto tuple_pack_type(std::tuple<A...>*){
    using R = typename pack_result<std::decay_t<A>...>::type;
    return R();
}

/**
 * @brief 从信号参数 tuple 中按指定类型 Want... 取前 K 个并构造目标类型 R
 * @tparam R 目标类型
 * @tparam Want 目标类型的各参数类型
 * @tparam Tuple 源 tuple 类型
 * @tparam I 取用的下标序列
 * @param t 源 tuple
 * @return 构造出的目标类型对象
 * @code
 * // 支撑 coro<Types...>(obj, sig)：从信号全部参数中取前 K 个构造目标类型
 * std::tuple<int, QString> all{42, QStringLiteral("x")};
 * int v = Coro::detail::make_typed<int, int>(all, std::make_index_sequence<1>{});
 * @endcode
 */
template <class R, class... Want, class Tuple, std::size_t... I>
R make_typed(Tuple& t, std::index_sequence<I...>){
    return R(static_cast<std::decay_t<Want>>(std::get<I>(t))...);
}

} // detail
} // Coro

#endif // SIGNALPACK_HPP
