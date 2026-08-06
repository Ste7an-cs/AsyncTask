#ifndef GENERATOR_HPP
#define GENERATOR_HPP

#include <boost/fiber/operations.hpp>
#include "executor/scheduler/fiberproperty.h"
#include "detail/asyncdefine.h"
#include "detail/result.hpp"
#include "awaitable.hpp"

namespace Coro {
/**
 * @brief 序列生成器，生产者/消费者协程模型。
 *
 * 生产者运行在独立的 fiber 中，通过 yield 推送值；消费者通过迭代器或 next()
 * 拉取值。
 * @tparam T 产出的元素类型
 * @code
 * // 1) 自建生产者：yield 推值，消费侧 range-for 拉值
 * Coro::Generator<int> squares([](auto yield){
 *     for(int i = 0; i < 5; i++){ Coro::msleep(10); yield(i * i); }
 * });
 * for(int v : squares) qDebug() << v;      // 0 1 4 9 16
 *
 * // 2) 更常见：由 generate() 把 Awaitable 适配成流
 * for(QTcpSocket* peer : Coro::generate(Coro::coro(server).nextConnection())){
 *     handle(peer);
 * }
 * @endcode
 */
template <typename T>
class Generator{
public:
    /**
     * @brief 仅暴露 push 操作给生产者函数的产出器
     * @code
     * // 生产者函数的形参即 Yield：只能产出值，不能读取/关闭消费端
     * Coro::Generator<int> gen([](auto yield){
     *     yield(1);
     *     yield(2);
     * });
     * @endcode
     */
    class Yield{
        Awaitable<T> *p_awaiter_;///< 关联的等待器
    public:
        /**
         * @brief 构造
         * @param c 关联的等待器
         * @code
         * // 由 Generator 内部构造并传给生产者函数，使用方一般不直接构造
         * Coro::Awaitable<int> a;
         * Coro::Generator<int>::Yield y(&a);
         * @endcode
         */
        explicit Yield(Awaitable<T> *c):p_awaiter_(c){}
        /**
         * @brief 产出一个值
         * @param v 待产出的值
         * @code
         * Coro::Generator<QString> gen([](auto yield){
         *     yield(QStringLiteral("hello"));   // 消费侧 next()/迭代即可取到
         * });
         * @endcode
         */
        void operator()(const T& v){p_awaiter_->resolve(v);}
        /**
         * @brief 查询输出侧是否已关闭。
         * @details 供生产者感知消费者关闭。共享 Awaitable 适配器在每次有界轮询后
         *          使用它退出，避免继续等待已关闭的输出端。
         * @return 输出侧已关闭返回 true。
         * @code
         * // 长流生产者：消费端关闭后及时收工，避免空转
         * Coro::Generator<int> gen([](auto yield){
         *     int i = 0;
         *     while(!yield.is_closed()) yield(i++);
         * });
         * @endcode
         */
        bool is_closed() const{return p_awaiter_->channel()->is_closed();}
    };

    /**
     * @brief 构造：在独立 fiber 中运行生产者函数（继承当前协程的优先级与线程模型）
     * @tparam F 生产者函数类型
     * @param func 生产者函数，形参为 Yield
     * @code
     * // 生产者在独立协程里跑；msleep 期间让出线程，消费者可继续推进
     * Coro::Generator<int> gen([](auto yield){
     *     for(int i = 0; i < 3; i++){ Coro::msleep(100); yield(i); }
     * });
     * for(int v : gen) qDebug() << v;
     * @endcode
     */
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
    /** @brief 移动构造 */
    Generator(Generator&&) = default ;
    /** @brief 移动赋值 */
    Generator& operator=(Generator&&) = default ;
    /** @brief 禁止拷贝构造 */
    Generator(const Generator&) = delete ;
    /** @brief 禁止拷贝赋值 */
    Generator& operator=(const Generator&) = delete ;

    /** @brief 析构：关闭等待器并 join 生产者 fiber */
    ~Generator(){
        awaiter_.close();
        fb_.join();
    }
    /**
     * @brief 拉取下一个值
     * @return 产出的值；序列结束返回 no_message 错误
     * @code
     * // 手动拉取（等价于迭代器写法）
     * while(auto v = gen.next()) use(v.value());
     * @endcode
     */
    Result<T> next(){
        return awaiter_.await();
    }

    /**
     * @brief 关闭生成器，结束迭代
     * @code
     * for(int v : gen){
     *     if(v > 100){ gen.close(); break; }   // 提前终止，生产者随之收工
     * }
     * @endcode
     */
    void close(){
        awaiter_.close();
    }
    /**
     * @brief 迭代器支持，类型为 input_iterator_tag（单次遍历）
     * @code
     * // 支持 range-for；单次遍历，不可回退或重复遍历
     * for(auto it = gen.begin(); it != gen.end(); ++it) use(*it);
     * @endcode
     */
    class Iterator{
        Generator *gen_{nullptr};///< 关联的生成器
        Result<T> cur_{std::make_error_code(std::errc::no_message)};///< 当前值
        /** @brief 前进一个元素，序列结束则置空 */
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
        /**
         * @brief 构造 end 迭代器
         * @code
         * auto last = gen.end();       // 与 begin() 比较以判断是否遍历结束
         * @endcode
         */
        Iterator() = default;
        /**
         * @brief 构造 begin 迭代器并取首个元素
         * @param g 关联的生成器
         * @code
         * auto it = gen.begin();       // 构造即取到首个元素
         * @endcode
         */
        explicit Iterator(Generator *g):gen_(g){advance();}
        /**
         * @brief 前置自增
         * @return 自身引用
         * @code
         * ++it;                        // 前进到下一个元素（可能让出协程等待）
         * @endcode
         */
        Iterator& operator++(){advance(); return *this;}
        /**
         * @brief 后置自增（单次遍历，语义同前置）
         * @return 自身引用
         * @code
         * it++;                        // 输入迭代器：不保存旧值，语义同 ++it
         * @endcode
         */
        Iterator& operator++(int){advance(); return *this;}

        /**
         * @brief 解引用取当前值
         * @return 当前值引用
         * @code
         * auto it = gen.begin();
         * int v = *it;
         * @endcode
         */
        T& operator*(){return cur_.value();}
        /**
         * @brief 成员访问
         * @return 当前值指针
         * @code
         * Coro::Generator<QByteArray> gen(...);
         * auto it = gen.begin();
         * qDebug() << it->size();
         * @endcode
         */
        T* operator->(){return &cur_.value();}

        /**
         * @brief 相等比较
         * @param o 另一个迭代器
         * @return 关联生成器相同返回 true
         * @code
         * if(gen.begin() == gen.end()) qDebug() << "空序列";
         * @endcode
         */
        bool operator==(const Iterator& o) const{return gen_ == o.gen_;};
        /**
         * @brief 不等比较
         * @param o 另一个迭代器
         * @return 关联生成器不同返回 true
         * @code
         * for(auto it = gen.begin(); it != gen.end(); ++it) use(*it);
         * @endcode
         */
        bool operator!=(const Iterator& o) const{return gen_ != o.gen_;};

    };
    /**
     * @brief 起始迭代器
     * @return begin 迭代器
     * @code
     * for(auto v : gen) use(v);     // range-for 内部调用 begin()/end()
     * @endcode
     */
    Iterator begin(){
        return Iterator(this);
    }
    /**
     * @brief 结束迭代器
     * @return end 迭代器
     * @code
     * auto it = gen.begin();
     * while(it != gen.end()){ use(*it); ++it; }
     * @endcode
     */
    Iterator end(){
        return Iterator();
    }
private:
    Awaitable<T> awaiter_;///< 底层等待器
    boost::fibers::fiber fb_;///< 生产者 fiber
};
/**
 * @brief Generator 的 void 特化（产出"事件发生"而无数据负载）
 * @code
 * // 只关心"发生了几次"，不携带数据，例如无参信号流
 * for(auto ok : Coro::generate(Coro::coro(&timer, &QTimer::timeout))){
 *     Q_UNUSED(ok);
 *     tick();
 * }
 * @endcode
 */
template <>
class Generator<void>{
public:
    /**
     * @brief 仅暴露 push 操作给生产者函数的产出器
     * @code
     * // 生产者函数的形参即 Yield：只能产出值，不能读取/关闭消费端
     * Coro::Generator<int> gen([](auto yield){
     *     yield(1);
     *     yield(2);
     * });
     * @endcode
     */
    class Yield{
        Awaitable<void> *p_awaiter_;///< 关联的等待器
    public:
        /**
         * @brief 构造
         * @param c 关联的等待器
         * @code
         * Coro::Awaitable<void> a;
         * Coro::Generator<void>::Yield y(&a);
         * @endcode
         */
        explicit Yield(Awaitable<void> *c):p_awaiter_(c){}
        /**
         * @brief 产出一次事件
         * @code
         * Coro::Generator<void> ticks([](auto yield){
         *     for(int i = 0; i < 3; i++){ Coro::msleep(100); yield(); }
         * });
         * @endcode
         */
        void operator()(){p_awaiter_->resolve();}
        /**
         * @brief 查询输出侧是否已关闭。
         * @details 供生产者感知消费者关闭。共享 Awaitable 适配器在每次有界轮询后
         *          使用它退出，避免继续等待已关闭的输出端。
         * @return 输出侧已关闭返回 true。
         * @code
         * // 长流生产者：消费端关闭后及时收工，避免空转
         * Coro::Generator<int> gen([](auto yield){
         *     int i = 0;
         *     while(!yield.is_closed()) yield(i++);
         * });
         * @endcode
         */
        bool is_closed() const{return p_awaiter_->channel()->is_closed();}
    };

    /**
     * @brief 构造：在独立 fiber 中运行生产者函数
     * @tparam F 生产者函数类型
     * @param func 生产者函数，形参为 Yield
     * @code
     * Coro::Generator<void> ticks([](auto yield){
     *     for(int i = 0; i < 5; i++){ Coro::msleep(50); yield(); }
     * });
     * for(auto t : ticks){ Q_UNUSED(t); onTick(); }
     * @endcode
     */
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
    /** @brief 移动构造 */
    Generator(Generator&&) = default ;
    /** @brief 移动赋值 */
    Generator& operator=(Generator&&) = default ;
    /** @brief 禁止拷贝构造 */
    Generator(const Generator&) = delete ;
    /** @brief 禁止拷贝赋值 */
    Generator& operator=(const Generator&) = delete ;

    /** @brief 析构：关闭等待器并 join 生产者 fiber */
    ~Generator(){
        awaiter_.close();
        fb_.join();
    }
    /**
     * @brief 拉取下一次事件
     * @return 成功 Result；序列结束返回 no_message 错误
     * @code
     * while(ticks.next()) onTick();     // 序列结束时循环自然退出
     * @endcode
     */
    Result<void> next(){
        return awaiter_.await();
    }

    /**
     * @brief 关闭生成器，结束迭代
     * @code
     * for(int v : gen){
     *     if(v > 100){ gen.close(); break; }   // 提前终止，生产者随之收工
     * }
     * @endcode
     */
    void close(){
        awaiter_.close();
    }
    /**
     * @brief 迭代器支持，类型为 input_iterator_tag（单次遍历）
     * @code
     * // 支持 range-for；单次遍历，不可回退或重复遍历
     * for(auto it = gen.begin(); it != gen.end(); ++it) use(*it);
     * @endcode
     */
    class Iterator{
        Generator *gen_{nullptr};///< 关联的生成器
        Result<void> cur_{std::make_error_code(std::errc::no_message)};///< 当前状态
        /** @brief 前进一个元素，序列结束则置空 */
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
        /**
         * @brief 构造 end 迭代器
         * @code
         * auto last = gen.end();       // 与 begin() 比较以判断是否遍历结束
         * @endcode
         */
        Iterator() = default;
        /**
         * @brief 构造 begin 迭代器并取首个事件
         * @param g 关联的生成器
         * @code
         * auto it = gen.begin();       // 构造即取到首个元素
         * @endcode
         */
        explicit Iterator(Generator *g):gen_(g){advance();}
        /**
         * @brief 前置自增
         * @return 自身引用
         * @code
         * ++it;                        // 前进到下一个元素（可能让出协程等待）
         * @endcode
         */
        Iterator& operator++(){advance(); return *this;}
        /**
         * @brief 后置自增（单次遍历，语义同前置）
         * @return 自身引用
         * @code
         * it++;                        // 输入迭代器：不保存旧值，语义同 ++it
         * @endcode
         */
        Iterator& operator++(int){advance(); return *this;}

        /**
         * @brief 解引用（void 特化恒为 true）
         * @return true
         * @code
         * for(auto ok : ticks){ Q_UNUSED(ok); onTick(); }  // 值无意义，只表示发生
         * @endcode
         */
        bool operator*(){return true;}
        /**
         * @brief 成员访问（void 特化恒为 true）
         * @return true
         * @code
         * auto it = ticks.begin();
         * bool happened = it.operator->();     // 恒为 true，无数据负载
         * @endcode
         */
        bool operator->(){return true;}

        /**
         * @brief 相等比较
         * @param o 另一个迭代器
         * @return 关联生成器相同返回 true
         * @code
         * if(gen.begin() == gen.end()) qDebug() << "空序列";
         * @endcode
         */
        bool operator==(const Iterator& o) const{return gen_ == o.gen_;};
        /**
         * @brief 不等比较
         * @param o 另一个迭代器
         * @return 关联生成器不同返回 true
         * @code
         * for(auto it = gen.begin(); it != gen.end(); ++it) use(*it);
         * @endcode
         */
        bool operator!=(const Iterator& o) const{return gen_ != o.gen_;};

    };
    /**
     * @brief 起始迭代器
     * @return begin 迭代器
     * @code
     * for(auto v : gen) use(v);     // range-for 内部调用 begin()/end()
     * @endcode
     */
    Iterator begin(){
        return Iterator(this);
    }
    /**
     * @brief 结束迭代器
     * @return end 迭代器
     * @code
     * auto it = gen.begin();
     * while(it != gen.end()){ use(*it); ++it; }
     * @endcode
     */
    Iterator end(){
        return Iterator();
    }
private:
    Awaitable<void> awaiter_;///< 底层等待器
    boost::fibers::fiber fb_;///< 生产者 fiber
};

/**
 * @brief 把一个 Awaitable 适配为 Generator（流式消费）。
 *
 * 按值接管 Awaitable；生成器 fiber 内循环 await：有值则 yield，关闭/出错则结束。
 * 用法：for(auto v : Coro::generate(coro(obj, &T::sig))){ ... }
 * @tparam T 产出的元素类型
 * @param a 待接管的等待器
 * @return 适配后的 Generator
 * @code
 * // 把信号流当容器遍历（Awaitable 为 move-only，此处按值接管）
 * for(auto v : Coro::generate(Coro::coro(obj, &Obj::valueChanged))){
 *     use(v);
 * }
 * @endcode
 */
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

/**
 * @brief 把共享 Awaitable 适配为 Generator（流式消费）。
 * @details 生成器强持有共享句柄直至源迭代结束。每次最多轮询 10 ms，此有界轮询仅用于
 *          感知输出端取消；源等待超时而 channel 仍开放时不会终止流。空句柄产生一个
 *          可安全结束的空流。
 * @tparam T 产出的元素类型。
 * @param a 待消费等待器的共享句柄。
 * @return 适配后的 Generator；空句柄返回可安全结束的关闭生成器。
 * @code
 * // socket 族方法返回 shared_ptr，直接交给 generate 做流式消费
 * for(QTcpSocket* peer : Coro::generate(Coro::coro(server).nextConnection())){
 *     handle(peer);          // server 关闭或销毁时迭代自然结束
 * }
 * // 流式读取数据块
 * for(const QByteArray& chunk : Coro::generate(Coro::coro(sock).readAll())){
 *     append(chunk);
 * }
 * @endcode
 */
template<typename T>
Generator<T> generate(std::shared_ptr<Awaitable<T>> a){
    return Generator<T>([a = std::move(a)](auto yield) mutable {
        if(!a){
            return;
        }
        while(!yield.is_closed()){
            Result<T> r = a->await_for(std::chrono::milliseconds(10));
            if(!r.has_value()){
                if(r.error() == std::make_error_code(std::errc::timed_out)){
                    auto channel = a->channel();
                    if(channel && !channel->is_closed()){
                        continue;
                    }
                }
                break;
            }
            if constexpr (std::is_void_v<T>){
                yield();
            }else{
                yield(r.value());
            }
        }
        a.reset();
    });
}
}

#endif // GENERATOR_H
