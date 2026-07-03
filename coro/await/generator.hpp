#ifndef GENERATOR_HPP
#define GENERATOR_HPP

#include <boost/fiber/operations.hpp>
#include "executor/scheduler/fiberproperty.h"
#include "detail/asyncdefine.h"
#include "detail/result.hpp"
#include "awaitable.hpp"

namespace Coro {
///
/// \brief 序列生成器，基于boost::fibers::unbuffered_channel
///         生产者、消费者协程
///         生产者运行在独立的fiber中，通过yield推送值
///         消费者通过迭代器或next()拉取值
///
template <typename T>
class Generator{
public:
    ///
    /// \brief The Yield class,仅暴露push操作给生产者函数
    ///
    class Yield{
        Awaitable<T> *p_awaiter_;
    public:
        explicit Yield(Awaitable<T> *c):p_awaiter_(c){}
        void operator()(const T& v){p_awaiter_->resolve(v);}
    };

    template<typename F>
    explicit Generator(F&& func){
        MetaContext prop = boost::this_fiber::properties<MetaContext>();
        fb_ = launch_properties([this, f=std::forward<F>(func)]() mutable {
            Yield gen_yield(&this->awaiter_);
            f(gen_yield);
            ///生成函数结束时，推送nullopt，通知Generator next和迭代器结束
            this->awaiter_.close();
        }, prop.priority(), prop.affinity());
    }
    Generator(Generator&&) = default ;
    Generator& operator=(Generator&&) = default ;
    Generator(const Generator&) = delete ;
    Generator& operator=(const Generator&) = delete ;

    ~Generator(){
        awaiter_.close();
        fb_.join();
    }
    Result<T> next(){
        return awaiter_.await();
    }

    void close(){
        awaiter_.close();
    }
    ///
    /// \brief The Iterator class 迭代器支持，迭代器类型为input_iterator_tag
    ///     迭代器为单次遍历
    ///
    class Iterator{
        Generator *gen_{nullptr};
        Result<T> cur_{std::make_error_code(std::errc::no_message)};
        void advance(){
            if(gen_){
                cur_ = gen_->next();
                if(!cur_.has_value()){
                    gen_ = nullptr;
                    cur_ = std::make_error_code(std::errc::no_message);
                }
            }
        }
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = T*;
        using reference = T&;
        Iterator() = default;
        explicit Iterator(Generator *g):gen_(g){advance();}
        Iterator& operator++(){advance(); return *this;}
        Iterator& operator++(int){advance(); return *this;}

        T& operator*(){return cur_.value();}
        T* operator->(){return &cur_.value();}

        bool operator==(const Iterator& o) const{return gen_ == o.gen_;};
        bool operator!=(const Iterator& o) const{return gen_ != o.gen_;};

    };
    Iterator begin(){
        return Iterator(this);
    }
    Iterator end(){
        return Iterator();
    }
private:
    Awaitable<T> awaiter_;
    boost::fibers::fiber fb_;
};
template <>
class Generator<void>{
public:
    ///
    /// \brief The Yield class,仅暴露push操作给生产者函数
    ///
    class Yield{
        Awaitable<void> *p_awaiter_;
    public:
        explicit Yield(Awaitable<void> *c):p_awaiter_(c){}
        void operator()(){p_awaiter_->resolve();}
    };

    template<typename F>
    explicit Generator(F&& func){
        MetaContext prop = boost::this_fiber::properties<MetaContext>();
        fb_ = launch_properties([this, f=std::forward<F>(func)]() mutable {
            Yield gen_yield(&this->awaiter_);
            f(gen_yield);
            ///生成函数结束时，推送nullopt，通知Generator next和迭代器结束
            this->awaiter_.close();
        }, prop.priority(), prop.affinity());
    }
    Generator(Generator&&) = default ;
    Generator& operator=(Generator&&) = default ;
    Generator(const Generator&) = delete ;
    Generator& operator=(const Generator&) = delete ;

    ~Generator(){
        awaiter_.close();
        fb_.join();
    }
    Result<void> next(){
        return awaiter_.await();
    }

    void close(){
        awaiter_.close();
    }
    ///
    /// \brief The Iterator class 迭代器支持，迭代器类型为input_iterator_tag
    ///     迭代器为单次遍历
    ///
    class Iterator{
        Generator *gen_{nullptr};
        Result<void> cur_{std::make_error_code(std::errc::no_message)};
        void advance(){
            if(gen_){
                cur_ = gen_->next();
                if(!cur_.has_value()){
                    gen_ = nullptr;
                    cur_ = std::make_error_code(std::errc::no_message);
                }
            }
        }
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = bool;
        using difference_type = std::ptrdiff_t;
        using pointer = bool*;
        using reference = bool&;
        Iterator() = default;
        explicit Iterator(Generator *g):gen_(g){advance();}
        Iterator& operator++(){advance(); return *this;}
        Iterator& operator++(int){advance(); return *this;}

        bool operator*(){return true;}
        bool operator->(){return true;}

        bool operator==(const Iterator& o) const{return gen_ == o.gen_;};
        bool operator!=(const Iterator& o) const{return gen_ != o.gen_;};

    };
    Iterator begin(){
        return Iterator(this);
    }
    Iterator end(){
        return Iterator();
    }
private:
    Awaitable<void> awaiter_;
    boost::fibers::fiber fb_;
};

///
/// \brief generate 把一个 Awaitable 适配为 Generator（流式消费）。
///     按值接管 Awaitable；生成器 fiber 内循环 await：有值则 yield，关闭/出错则结束。
///     用法：for(auto v : Coro::generate(coro(obj, &T::sig))){ ... }
///
template<typename T>
Generator<T> generate(Awaitable<T> a){
    // Awaitable move-only，而 launch_properties 会拷贝 fiber 函数，
    // 故用 shared_ptr 持有以保证生产者 lambda 可拷贝（单一所有权仍在此 shared_ptr）。
    auto sa = std::make_shared<Awaitable<T>>(std::move(a));
    return Generator<T>([sa](auto yield){
        for(;;){
            Result<T> r = sa->await();
            if(!r.has_value()){
                return;
            }
            if constexpr (std::is_void_v<T>){
                yield();
            }else{
                yield(r.value());
            }
        }
    });
}
}

#endif // GENERATOR_H
