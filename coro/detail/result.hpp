#ifndef RESULT_HPP
#define RESULT_HPP

#include <exception>
#include <utility>
#include <system_error>
#include <type_traits>

namespace Coro {
/**
 * @brief 参考 C++23 std::expected 的简要实现，替代 std::optional。
 *
 * std::optional 无法在类型为 void 时使用，且无法携带错误消息；Result 以
 * 联合体存放"值或错误"二者之一，支持 void 特化并可携带 error_code。
 * @tparam T 正确结果的类型
 * @tparam E 错误消息的类型
 * @code
 * // 承载成功值
 * Coro::Result<int> ok = 42;
 * if(ok) qDebug() << ok.value();                 // 42
 *
 * // 承载错误：一切失败都以错误码表达，不抛异常
 * Coro::Result<int> bad = std::make_error_code(std::errc::timed_out);
 * if(!bad) qDebug() << bad.error().message().c_str();
 *
 * // await 系列的返回值就是 Result
 * auto r = Coro::await_for(Coro::coro(obj, &Obj::sig), std::chrono::seconds(1));
 * int v = r.value_or(0);                         // 失败时取默认值
 * @endcode
 */
template <typename T, typename E=std::error_code>
class Result{
public:
    /**
     * @brief 由右值构造正确结果
     * @param v 正确结果值
     * @code
     * Coro::Result<QString> r{QString("done")};
     * @endcode
     */
    Result(T&& v):has_value_(true){construct_value(std::move(v));}
    /**
     * @brief 由左值构造正确结果
     * @param v 正确结果值
     * @code
     * int v = 10;
     * Coro::Result<int> r{v};
     * @endcode
     */
    Result(const T& v):has_value_(true){construct_value(v);}
    /**
     * @brief 由右值构造错误结果
     * @param e 错误消息
     * @code
     * Coro::Result<int> r{std::make_error_code(std::errc::timed_out)};
     * @endcode
     */
    Result(E&& e):has_value_(false){construct_error(std::move(e));}
    /**
     * @brief 由左值构造错误结果
     * @param e 错误消息
     * @code
     * auto ec = std::make_error_code(std::errc::io_error);
     * Coro::Result<int> r{ec};
     * @endcode
     */
    Result(const E& e):has_value_(false){construct_error(e);}

    /** @brief 析构，按当前状态销毁值或错误 */
    ~Result(void){
        destory();
    }

    /**
     * @brief 拷贝构造
     * @param other 被拷贝的源对象
     * @code
     * Coro::Result<int> a = 1;
     * Coro::Result<int> b = a;                    // b 也持有 1
     * @endcode
     */
    Result(const Result& other):has_value_(other.has_value_){
        if(has_value_){
            construct_value(other.value_);
        }else{
            construct_error(other.error_);
        }
    }
    /**
     * @brief 移动构造
     * @param other 被移动的源对象
     * @code
     * Coro::Result<QByteArray> a = QByteArray("data");
     * Coro::Result<QByteArray> b = std::move(a);  // 避免拷贝大对象
     * @endcode
     */
    Result(Result&& other):has_value_(other.has_value_){
        if(has_value_){
            construct_value(std::move(other.value_));
        }else{
            construct_error(std::move(other.error_));
        }
    }
    /**
     * @brief 拷贝赋值
     * @param other 被拷贝的源对象
     * @return 自身引用
     * @code
     * Coro::Result<int> a = 1, b = 2;
     * b = a;                                      // b 变为持有 1
     * @endcode
     */
    Result& operator=(const Result& other){
        if(this != &other){
            destory();
            has_value_ = other.has_value_;
            if(has_value_){
                construct_value(other.value_);
            }else{
                construct_error(other.error_);
            }
        }
        return *this;
    }
    /**
     * @brief 移动赋值
     * @param other 被移动的源对象
     * @return 自身引用
     * @code
     * Coro::Result<QByteArray> a = QByteArray("data"), b = QByteArray();
     * b = std::move(a);
     * @endcode
     */
    Result& operator=(Result&& other){
        if(this != &other){
            destory();
            has_value_ = other.has_value_;
            if(has_value_){
                construct_value(std::move(other.value_));
            }else{
                construct_error(std::move(other.error_));
            }
        }
        return *this;
    }
    /**
     * @brief 判断是否有正确结果
     * @return 有正确结果返回 true
     * @code
     * auto r = Coro::await(Coro::coro(obj, &Obj::valueChanged));
     * if(r){ use(r.value()); } else { handle(r.error()); }
     * @endcode
     */
    explicit operator bool() const noexcept{return has_value_;}
    /**
     * @brief 判断是否有正确结果
     * @return 有正确结果返回 true
     * @code
     * if(r.has_value()) use(r.value());
     * @endcode
     */
    bool has_value() const noexcept{return has_value_;}

    /**
     * @brief 返回正确结果的引用，在 has_value 为 false 时返回的数值不可用
     * @return 正确结果的左值引用
     * @code
     * Coro::Result<int> r = 1;
     * r.value() = 5;                              // 就地修改
     * @endcode
     */
    T& value() & {return value_;}
    /**
     * @brief 返回正确结果的常量引用，在 has_value 为 false 时返回的数值不可用
     * @return 正确结果的常量左值引用
     * @code
     * const Coro::Result<int> r = 1;
     * int v = r.value();
     * @endcode
     */
    const T& value() const& {return value_;}
    /**
     * @brief 返回正确结果的右值，在 has_value 为 false 时返回的数值不可用
     * @return 正确结果的右值引用
     * @code
     * // 直接从临时 Result 中移走数据，避免拷贝
     * QByteArray data = Coro::await(Coro::coro(dev).readAll()).value();
     * @endcode
     */
    T&& value() && {return  std::move(value_);}

    /**
     * @brief 返回错误结果的引用
     * @return 错误消息的左值引用
     * @code
     * Coro::Result<int> r = std::make_error_code(std::errc::timed_out);
     * if(!r) qDebug() << r.error().message().c_str();
     * @endcode
     */
    E& error() & {return error_;}
    /**
     * @brief 返回错误结果的常量引用
     * @return 错误消息的常量左值引用
     * @code
     * const Coro::Result<int> r = std::make_error_code(std::errc::io_error);
     * if(!r) qDebug() << r.error().value();
     * @endcode
     */
    const E& error() const& {return error_;}
    /**
     * @brief 返回错误结果的右值
     * @return 错误消息的右值引用
     * @code
     * std::error_code ec = Coro::Result<int>{
     *     std::make_error_code(std::errc::timed_out)}.error();
     * @endcode
     */
    E&& error() && {return  std::move(error_);}

    /**
     * @brief 若有正确结果则返回值，否则返回给定默认值
     * @tparam U 默认值的类型
     * @param default_value 无正确结果时返回的默认值
     * @return 正确结果或默认值
     * @code
     * auto r = Coro::await_for(Coro::coro(obj, &Obj::sig), 100ms);
     * int v = r.value_or(-1);                     // 超时则得到 -1
     * @endcode
     */
    template<typename U>
    T value_or(U&& default_value) const& {
        return has_value_? value_ : std::forward<U>(default_value);
    }
    /**
     * @brief 若有正确结果则移动返回值，否则返回给定默认值
     * @tparam U 默认值的类型
     * @param default_value 无正确结果时返回的默认值
     * @return 正确结果或默认值
     * @code
     * QByteArray data = Coro::await(Coro::coro(dev).readAll())
     *                       .value_or(QByteArray());
     * @endcode
     */
    template<typename U>
    T value_or(U&& default_value) && {
        return has_value_? std::move(value_) : std::forward<U>(default_value);
    }
private:
    union{
        T value_;///< 正确结果存储
        E error_;///< 错误消息存储
    };
    bool has_value_;///< 状态标志：true 表示存放值，false 表示存放错误
    /** @brief 按当前状态析构联合体中的值或错误 */
    void destory(void) noexcept{
        if(has_value_){
            value_.~T();
        }else{
            error_.~E();
        }
    }
    /**
     * @brief 在联合体上原地构造正确结果
     * @tparam U 转发类型
     * @param v 待构造的值
     */
    template<typename U>
    void construct_value(U&& v){
        new(&value_) T(std::forward<U>(v));
    }
    /**
     * @brief 在联合体上原地构造错误结果
     * @tparam U 转发类型
     * @param v 待构造的错误
     */
    template<typename U>
    void construct_error(U&& v){
        new(&error_) E(std::forward<U>(v));
    }

};
/**
 * @brief Result 的 void 特化，只表达"成功或错误"，不携带正确结果值
 * @tparam E 错误消息的类型
 * @code
 * // 等待无参信号得到的就是 Result<void>：可直接当 bool 用
 * Coro::Result<void> r = Coro::await(Coro::coro(sock).waitForConnected());
 * if(!r) qDebug() << r.error().message().c_str();
 * @endcode
 */
template <typename E>
class Result<void, E>{
public:
    /**
     * @brief 构造成功结果
     * @code
     * Coro::Result<void> ok;                      // 表示"成功发生一次"
     * @endcode
     */
    Result():has_value_(true){}
    /**
     * @brief 由右值构造错误结果
     * @param e 错误消息
     * @code
     * Coro::Result<void> r{std::make_error_code(std::errc::timed_out)};
     * @endcode
     */
    Result(E&& e):has_value_(false), error_(std::move(e)){}
    /**
     * @brief 由左值构造错误结果
     * @param e 错误消息
     * @code
     * auto ec = std::make_error_code(std::errc::no_message);
     * Coro::Result<void> r{ec};
     * @endcode
     */
    Result(const E& e):has_value_(false), error_(e){}

    /**
     * @brief 判断是否成功
     * @return 成功返回 true
     * @code
     * if(Coro::await(Coro::coro(sock).waitForConnected())) startWork();
     * @endcode
     */
    explicit operator bool() const noexcept{return has_value_;}
    /**
     * @brief 判断是否成功
     * @return 成功返回 true
     * @code
     * auto r = Coro::await(Coro::coro(obj, &Obj::finished));
     * if(r.has_value()) next();
     * @endcode
     */
    bool has_value() const noexcept{return has_value_;}
    /**
     * @brief 返回错误结果的引用
     * @return 错误消息的左值引用
     * @code
     * auto r = Coro::await_for(Coro::coro(sock).waitForConnected(), 2s);
     * if(!r) qWarning() << r.error().message().c_str();
     * @endcode
     */
    E& error() & {return error_;}
    /**
     * @brief 返回错误结果的常量引用
     * @return 错误消息的常量左值引用
     * @code
     * const auto r = Coro::await(Coro::coro(sock).waitForDisconnected());
     * if(!r) log(r.error());
     * @endcode
     */
    const E& error() const& {return error_;}
    /**
     * @brief 返回错误结果的右值
     * @return 错误消息的右值引用
     * @code
     * std::error_code ec =
     *     Coro::await_for(Coro::coro(sock).waitForConnected(), 1s).error();
     * @endcode
     */
    E&& error() && {return  std::move(error_);}

private:
    bool has_value_;///< 状态标志：true 表示成功
    E error_;///< 错误消息存储
};

/**
 * @brief 判断类型是否为 Result 的萃取（主模板：非 Result）
 * @tparam 任意类型
 * @code
 * static_assert(!Coro::is_result_type<int>::value, "int 不是 Result");
 * @endcode
 */
template<typename>
struct is_result_type : std::false_type{};
/**
 * @brief 判断类型是否为 Result 的萃取（偏特化：是 Result）
 * @tparam U 正确结果类型
 * @tparam E 错误类型
 * @code
 * static_assert(Coro::is_result_type<Coro::Result<int>>::value, "是 Result");
 * @endcode
 */
template<typename U, typename E>
struct is_result_type<Result<U, E>> : std::true_type{};

/**
 * @brief 递归萃取 `Result<T>` 最内层非 Result 的类型。
 *
 * 对 `Result<Result<T>>` 提取类型 T（主模板：非 Result，类型即自身）。
 * @tparam T 待萃取类型
 * @code
 * using T = Coro::result_inner_type<int>::type;   // int（非 Result 即自身）
 * @endcode
 */
template <typename T>
struct result_inner_type{
    using type = T;///< 萃取结果类型
};

/**
 * @brief 偏特化：T 为 `Result<U>`，继续递归萃取内层类型
 * @tparam U 内层类型
 * @code
 * using T = Coro::result_inner_type<Coro::Result<int>>::type;   // int
 * @endcode
 */
template <typename U>
struct result_inner_type<Result<U>>{
    using type = typename result_inner_type<U>::type;///< 萃取结果类型
};
/**
 * @brief 偏特化：T 为 `Result<U, E>`，继续递归萃取内层类型
 * @tparam U 内层类型
 * @tparam E 错误类型
 * @code
 * // 多层嵌套一次性剥到底
 * using T = Coro::result_inner_type<
 *     Coro::Result<Coro::Result<int>>>::type;                   // int
 * @endcode
 */
template <typename U, typename E>
struct result_inner_type<Result<U, E>>{
    using type = typename result_inner_type<U>::type;///< 萃取结果类型
};

/**
 * @brief result_inner_type 的类型别名
 * @tparam T 待萃取类型
 * @code
 * using T = Coro::result_inner_type_t<Coro::Result<Coro::Result<int>>>;  // int
 * @endcode
 */
template <typename T>
using result_inner_type_t = typename result_inner_type<T>::type;

/**
 * @brief 将嵌套 Result 摊平为单层 Result 的萃取（主模板）
 * @tparam U 待摊平类型
 * @code
 * using R = Coro::flatten_result_type<int>::type;      // Coro::Result<int>
 * @endcode
 */
template <typename U>
struct flatten_result_type{
    using type = Result<result_inner_type_t<U>>;///< 摊平后的 Result 类型
};
/**
 * @brief 偏特化：保留错误类型 E 地摊平嵌套 Result
 * @tparam U 内层类型
 * @tparam E 错误类型
 * @code
 * // Result<Result<int>> 摊平为 Result<int>，错误类型保持不变
 * using R = Coro::flatten_result_type<
 *     Coro::Result<Coro::Result<int>>>::type;          // Coro::Result<int>
 * @endcode
 */
template <typename U, typename E>
struct flatten_result_type<Result<U, E>>{
    using type = Result<result_inner_type_t<U>, E>;///< 摊平后的 Result 类型
};
/**
 * @brief flatten_result_type 的类型别名
 * @tparam T 待摊平类型
 * @code
 * // 任务链中用于把嵌套结果统一成单层 Result
 * using R = Coro::flatten_result_type_t<Coro::Result<Coro::Result<int>>>;
 * @endcode
 */
template <typename T>
using flatten_result_type_t = typename flatten_result_type<T>::type;



}

#endif // RESULT_HPP
