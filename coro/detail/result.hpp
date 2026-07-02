#ifndef RESULT_HPP
#define RESULT_HPP

#include <exception>
#include <utility>
#include <system_error>
#include <type_traits>

namespace Coro {
///
/// \brief 参考C++23 std::expected的简要实现，替代std::optional，
///         std::optional无法在类型为void时使用，且无法携带错误消息
/// \value T 正确结果的类型
/// \value E 错误消息的类型
///
template <typename T, typename E=std::error_code>
class Result{
public:
    /// 正确结果构造
    Result(T&& v):has_value_(true){construct_value(std::move(v));}
    Result(const T& v):has_value_(true){construct_value(v);}
    /// 错误消息构造
    Result(E&& e):has_value_(false){construct_error(std::move(e));}
    Result(const E& e):has_value_(false){construct_error(e);}

    ~Result(void){
        destory();
    }

    /// 拷贝构造
    Result(const Result& other):has_value_(other.has_value_){
        if(has_value_){
            construct_value(other.value_);
        }else{
            construct_error(other.error_);
        }
    }
    /// 移动构造
    Result(Result&& other):has_value_(other.has_value_){
        if(has_value_){
            construct_value(std::move(other.value_));
        }else{
            construct_error(std::move(other.error_));
        }
    }
    /// 拷贝赋值
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
    /// 移动赋值
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
    ///
    /// \brief operator bool 判断是否有正确结果
    ///
    explicit operator bool() const noexcept{return has_value_;}
    ///
    /// \brief operator bool 判断是否有正确结果
    ///
    bool has_value() const noexcept{return has_value_;}

    ///
    /// \brief value    返回正确结果的引用,在has_value为false时返回的数值不可用
    ///
    T& value() & {return value_;}
    ///
    /// \brief value    返回正确结果的拷贝,在has_value为false时返回的数值不可用
    ///
    const T& value() const& {return value_;}
    ///
    /// \brief value    返回正确结果的右值,在has_value为false时返回的数值不可用
    ///
    T&& value() && {return  std::move(value_);}

    ///
    /// \brief error 返回错误结果
    ///
    E& error() & {return error_;}
    const E& error() const& {return error_;}
    E&& error() && {return  std::move(error_);}

    ///
    /// \brief value_or 若有正确结果，则返回值，否则返回default_value的默认值
    ///
    template<typename U>
    T value_or(U&& default_value) const& {
        return has_value_? value_ : std::forward<U>(default_value);
    }
    template<typename U>
    T value_or(U&& default_value) && {
        return has_value_? std::move(value_) : std::forward<U>(default_value);
    }
private:
    union{
        T value_;
        E error_;
    };
    bool has_value_;
    void destory(void) noexcept{
        if(has_value_){
            value_.~T();
        }else{
            error_.~E();
        }
    }
    template<typename U>
    void construct_value(U&& v){
        new(&value_) T(std::forward<U>(v));
    }
    template<typename U>
    void construct_error(U&& v){
        new(&error_) E(std::forward<U>(v));
    }

};
template <typename E>
class Result<void, E>{
public:
    /// 正确消息构造
    Result():has_value_(true){}
    /// 错误消息构造
    Result(E&& e):has_value_(false), error_(std::move(e)){}
    Result(const E& e):has_value_(false), error_(e){}

    ///
    /// \brief operator bool 判断是否有正确结果
    ///
    explicit operator bool() const noexcept{return has_value_;}
    ///
    /// \brief operator bool 判断是否有正确结果
    ///
    bool has_value() const noexcept{return has_value_;}
    ///
    /// \brief error 返回错误结果
    ///
    E& error() & {return error_;}
    const E& error() const& {return error_;}
    E&& error() && {return  std::move(error_);}

private:
    bool has_value_;
    E error_;
};

template<typename>
struct is_result_type : std::false_type{};
template<typename U, typename E>
struct is_result_type<Result<U, E>> : std::true_type{};

///
/// \brief 递归萃取Result<T>最内层非Result的类型，对Result<Result<T>>的类型，提取类型T
///
template <typename T>
struct result_inner_type{
    using type = T;
};

///偏特化 T是Result<U>，继续递归
template <typename U>
struct result_inner_type<Result<U>>{
    using type = typename result_inner_type<U>::type;
};
template <typename U, typename E>
struct result_inner_type<Result<U, E>>{
    using type = typename result_inner_type<U>::type;
};

///类型别名
template <typename T>
using result_inner_type_t = typename result_inner_type<T>::type;

template <typename U>
struct flatten_result_type{
    using type = Result<result_inner_type_t<U>>;
};
template <typename U, typename E>
struct flatten_result_type<Result<U, E>>{
    using type = Result<result_inner_type_t<U>, E>;
};
template <typename T>
using flatten_result_type_t = typename flatten_result_type<T>::type;



}

#endif // RESULT_HPP
