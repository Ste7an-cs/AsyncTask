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
 */
template <typename T, typename E=std::error_code>
class Result{
public:
    /**
     * @brief 由右值构造正确结果
     * @param v 正确结果值
     */
    Result(T&& v):has_value_(true){construct_value(std::move(v));}
    /**
     * @brief 由左值构造正确结果
     * @param v 正确结果值
     */
    Result(const T& v):has_value_(true){construct_value(v);}
    /**
     * @brief 由右值构造错误结果
     * @param e 错误消息
     */
    Result(E&& e):has_value_(false){construct_error(std::move(e));}
    /**
     * @brief 由左值构造错误结果
     * @param e 错误消息
     */
    Result(const E& e):has_value_(false){construct_error(e);}

    /** @brief 析构，按当前状态销毁值或错误 */
    ~Result(void){
        destory();
    }

    /**
     * @brief 拷贝构造
     * @param other 被拷贝的源对象
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
     */
    explicit operator bool() const noexcept{return has_value_;}
    /**
     * @brief 判断是否有正确结果
     * @return 有正确结果返回 true
     */
    bool has_value() const noexcept{return has_value_;}

    /**
     * @brief 返回正确结果的引用，在 has_value 为 false 时返回的数值不可用
     * @return 正确结果的左值引用
     */
    T& value() & {return value_;}
    /**
     * @brief 返回正确结果的常量引用，在 has_value 为 false 时返回的数值不可用
     * @return 正确结果的常量左值引用
     */
    const T& value() const& {return value_;}
    /**
     * @brief 返回正确结果的右值，在 has_value 为 false 时返回的数值不可用
     * @return 正确结果的右值引用
     */
    T&& value() && {return  std::move(value_);}

    /**
     * @brief 返回错误结果的引用
     * @return 错误消息的左值引用
     */
    E& error() & {return error_;}
    /**
     * @brief 返回错误结果的常量引用
     * @return 错误消息的常量左值引用
     */
    const E& error() const& {return error_;}
    /**
     * @brief 返回错误结果的右值
     * @return 错误消息的右值引用
     */
    E&& error() && {return  std::move(error_);}

    /**
     * @brief 若有正确结果则返回值，否则返回给定默认值
     * @tparam U 默认值的类型
     * @param default_value 无正确结果时返回的默认值
     * @return 正确结果或默认值
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
 */
template <typename E>
class Result<void, E>{
public:
    /** @brief 构造成功结果 */
    Result():has_value_(true){}
    /**
     * @brief 由右值构造错误结果
     * @param e 错误消息
     */
    Result(E&& e):has_value_(false), error_(std::move(e)){}
    /**
     * @brief 由左值构造错误结果
     * @param e 错误消息
     */
    Result(const E& e):has_value_(false), error_(e){}

    /**
     * @brief 判断是否成功
     * @return 成功返回 true
     */
    explicit operator bool() const noexcept{return has_value_;}
    /**
     * @brief 判断是否成功
     * @return 成功返回 true
     */
    bool has_value() const noexcept{return has_value_;}
    /**
     * @brief 返回错误结果的引用
     * @return 错误消息的左值引用
     */
    E& error() & {return error_;}
    /**
     * @brief 返回错误结果的常量引用
     * @return 错误消息的常量左值引用
     */
    const E& error() const& {return error_;}
    /**
     * @brief 返回错误结果的右值
     * @return 错误消息的右值引用
     */
    E&& error() && {return  std::move(error_);}

private:
    bool has_value_;///< 状态标志：true 表示成功
    E error_;///< 错误消息存储
};

/**
 * @brief 判断类型是否为 Result 的萃取（主模板：非 Result）
 * @tparam 任意类型
 */
template<typename>
struct is_result_type : std::false_type{};
/**
 * @brief 判断类型是否为 Result 的萃取（偏特化：是 Result）
 * @tparam U 正确结果类型
 * @tparam E 错误类型
 */
template<typename U, typename E>
struct is_result_type<Result<U, E>> : std::true_type{};

/**
 * @brief 递归萃取 `Result<T>` 最内层非 Result 的类型。
 *
 * 对 `Result<Result<T>>` 提取类型 T（主模板：非 Result，类型即自身）。
 * @tparam T 待萃取类型
 */
template <typename T>
struct result_inner_type{
    using type = T;///< 萃取结果类型
};

/**
 * @brief 偏特化：T 为 `Result<U>`，继续递归萃取内层类型
 * @tparam U 内层类型
 */
template <typename U>
struct result_inner_type<Result<U>>{
    using type = typename result_inner_type<U>::type;///< 萃取结果类型
};
/**
 * @brief 偏特化：T 为 `Result<U, E>`，继续递归萃取内层类型
 * @tparam U 内层类型
 * @tparam E 错误类型
 */
template <typename U, typename E>
struct result_inner_type<Result<U, E>>{
    using type = typename result_inner_type<U>::type;///< 萃取结果类型
};

/**
 * @brief result_inner_type 的类型别名
 * @tparam T 待萃取类型
 */
template <typename T>
using result_inner_type_t = typename result_inner_type<T>::type;

/**
 * @brief 将嵌套 Result 摊平为单层 Result 的萃取（主模板）
 * @tparam U 待摊平类型
 */
template <typename U>
struct flatten_result_type{
    using type = Result<result_inner_type_t<U>>;///< 摊平后的 Result 类型
};
/**
 * @brief 偏特化：保留错误类型 E 地摊平嵌套 Result
 * @tparam U 内层类型
 * @tparam E 错误类型
 */
template <typename U, typename E>
struct flatten_result_type<Result<U, E>>{
    using type = Result<result_inner_type_t<U>, E>;///< 摊平后的 Result 类型
};
/**
 * @brief flatten_result_type 的类型别名
 * @tparam T 待摊平类型
 */
template <typename T>
using flatten_result_type_t = typename flatten_result_type<T>::type;



}

#endif // RESULT_HPP
