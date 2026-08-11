# 共享 Awaitable（广播消费）设计

日期：2026-08-11

## 1. 背景与问题

`Awaitable<T>` 内部持有单个 `FiberChannel<T>`，`await()` 走 `ch_->pop()`，是**破坏性出队**——元素被取走后即从队列移除。因此多个消费者持有同一个 `Awaitable`（或同一个 `std::shared_ptr<Awaitable<T>>`）时会互相抢占：一条消息只会被其中一个消费者看到。

这对"抢占式工作队列"是正确语义，但无法表达另一类需求：**多个消费者逻辑上共享同一条消息，每条消息需要被所有消费者各自处理一遍**（数据同步、日志分发等）。

## 2. 目标与非目标

### 目标

- 一个数据源可被多个消费者各自完整消费，互不干扰、一条不漏。
- 复用现有消费接口：`Coro::await` / `Coro::await_for` / `Coro::generate` 均原样可用，不新增平行命名。
- 现有单消费者与抢占式多消费者行为**完全不变**。
- 改动范围尽可能小，且未使用共享功能时不付出任何运行期与存储代价。

### 非目标

- **不做 replay。** 订阅之后才产生的数据才对订阅者可见。
- **不做背压。** 队列无界，与现有 `FiberChannel` 一致。
- **不做有限缓冲/丢弃策略。**
- 不改变 `Awaitable` 的既有语义，不引入虚函数、不引入常驻 fiber。

## 3. 语义规格

### 3.1 核心语义

调用 `Awaitable<T>::shared()` 注册一条**镜像通道**，返回一个普通的 `std::shared_ptr<Awaitable<T>>`。此后源 channel 每次 `push` 都会把值同步复制投递给每一条镜像。

结果是三方各得全量，彼此之间没有竞争：

```
push A B C D，两个 shared() 订阅者 + 两个直接消费者 X/Y

源队列 : A B C D  →  X 与 Y 互相抢，两人合起来消费掉 A B C D（不重不漏）
sub1   : A B C D  ←  独立一份，一条不漏
sub2   : A B C D  ←  独立一份，一条不漏
```

- **源队列保留全量**：直接 `await(src)` 的消费者之间仍是抢占模型，行为与今天完全一致。
- **每个订阅者各得全量**：订阅者之间是广播，互不影响。
- **源与订阅者之间不竞争**：订阅者不会因为直接消费者取走了数据而漏收。

### 3.2 已决策项

| 决策点 | 结论 | 理由 |
|---|---|---|
| 共享语义 | 广播/扇出，每人收到每一条 | 场景是数据同步、日志分发 |
| 迟到订阅者 | 无 replay，只收订阅之后的数据 | 内存有界性优先；长生命周期流不能永久保留历史 |
| 慢消费者 | 无界队列，任其增长 | 与现有 `FiberChannel` 语义一致 |
| 上游生命周期 | 由源句柄持有，与订阅者数量无关 | 避免"订阅者数降为 0 时误关上游且不可恢复"这类难复现的 bug |
| 退订方式 | 纯 RAII（`weak_ptr` 失效即自动剔除） | 无需 `unsubscribe()` 接口 |
| 扇出位置 | `FiberChannel::push` | 生产者只捕获 `channel()`，从不持有 `Awaitable` |
| 扇出时机 | 生产者线程同步完成 | 避免引入常驻泵 fiber 及其退出期回收风险 |

### 3.3 边界行为

- **源已关闭时调用 `shared()`**：返回的订阅句柄立即处于关闭状态，并携带源首次关闭时记录的错误码。不注册镜像。避免订阅者永久挂起。
- **源关闭时的余量**：每个订阅者先取完自己队列中已投递的值，之后才观察到终止错误。此行为由现有 `pop()` 在 `closed_ && !queue_.empty()` 时仍返回 `success` 的逻辑自动保证。
- **订阅句柄单独 `close()`**：只终止自己这一路，源与其他订阅者不受影响。
- **订阅句柄的 `guard_`**：订阅句柄由默认构造产生，从未 `setOnClose`，因此其关闭或析构不会断开上游 Qt 连接。
- **失效槽位的剔除时机**：订阅句柄析构后，其 `weak_ptr` 要等下一次 `push` 或 `discard_pending()` 才被剔除；`close()` 不剔除（它刻意保留整份列表，见下）。若源之后既不再 `push` 也不 `discard_pending()`，槽位会一直挂着——无害（`lock()` 恒失败），不为此增加额外机制。
- **源句柄未 `close()` 就析构**：`FiberChannel` 析构时以 `connection_aborted` 关闭所有仍存活的镜像，使订阅者收敛而非永久挂起；与正常结束的 `no_message` 用错误码区分，便于定位"忘了 close() 就丢了源句柄"这类问题。

## 4. 接口

```cpp
// Awaitable<T> 与 Awaitable<void> 各新增一个方法
std::shared_ptr<Awaitable<T>> shared();
```

用法：

```cpp
auto src = Coro::coro(sock).readAll();     // 现有工厂，一行不改

// 抢占式消费：今天的写法，行为不变
Coro::makeTask([src]{ while(auto c = Coro::await(src)) work(c.value()); return 0; });

// 广播消费：各得全量
auto a = src->shared();
auto b = src->shared();
Coro::makeTask([a]{ while(auto c = Coro::await(a)) sync(c.value()); return 0; });
Coro::makeTask([b]{ for(const auto& c : Coro::generate(b)) log(c); return 0; });
```

返回值是货真价实的普通 `Awaitable<T>`，因此：

- `Coro::await(a)` / `Coro::await_for(a, 2s)` 直接可用（命中现有 `shared_ptr` 重载）。
- `Coro::generate(a)` 直接可用（命中 `generate(std::shared_ptr<Awaitable<T>>)` 重载）。
- 无虚函数、无模板虚化问题、无切片陷阱。

## 5. 实现

### 5.1 `coro/detail/fiberchannel.hpp`

新增成员（未共享时为空指针，不产生堆分配）：

```cpp
std::unique_ptr<std::vector<std::weak_ptr<FiberChannel<T>>>> mirrors_;
```

新增内部方法，仅由 `Awaitable::shared()` 调用。注意 `fiberchannel.hpp` 不包含 `awaitable.hpp`（依赖方向相反），因此需在文件顶部对 `Awaitable` 作前向声明后再写 friend 声明：

```cpp
// 文件顶部，namespace Coro 内
template<typename T> class Awaitable;

// FiberChannel 类内
template<typename U> friend class Awaitable;

/// 注册一条镜像通道。源已关闭时不注册，直接以源的终止原因关闭该镜像。
void addMirror(const std::shared_ptr<FiberChannel<T>>& mirror){
    std::unique_lock<boost::fibers::mutex> lck{mtx_};
    if(closed_.load()){
        const auto error = close_error_;
        lck.unlock();
        mirror->close(error);
        return;
    }
    if(!mirrors_){
        mirrors_ = std::make_unique<std::vector<std::weak_ptr<FiberChannel<T>>>>();
    }
    mirrors_->push_back(mirror);
}
```

`push()` 增加扇出：`weak_ptr` 已过期（订阅句柄已析构）的槽位仍用 swap-and-pop 剔除（O(1)，无 memmove）；但**已关闭的镜像不剔除，只跳过投递**——`close()` 不清空 `mirrors_`，`discard_pending()` 靠这份列表在源销毁时把镜像队列里即将悬空的值一并丢弃，erase 会让这条路径永久失效（详见下面 `close`/`discard_pending` 的说明）：

```cpp
channel_status push(T value){
    std::unique_lock<boost::fibers::mutex> lck{mtx_};
    if(BOOST_UNLIKELY(is_closed())){
        return channel_status::closed;
    }
    if(mirrors_){
        auto& list = *mirrors_;
        for(std::size_t i = 0; i < list.size(); ){
            auto mirror = list[i].lock();
            if(!mirror){
                // 句柄已析构：weak_ptr 失效，swap-and-pop 剔除
                list[i] = std::move(list.back());
                list.pop_back();
                continue;
            }
            // 已关闭的镜像跳过投递（省掉一次加锁与一次 T 拷贝），但保留在列表中，
            // 以便 discard_pending() 仍能清掉它队列里即将悬空的值
            if(!mirror->is_closed()){
                mirror->push(value);
            }
            ++i;
        }
    }
    queue_.push_back(std::move(value));
    cv_consumer_.notify_one();
    return channel_status::success;
}
```

`close(std::error_code)` 增加扇出——**终止必须传播，否则订阅者永久挂起**。`mirrors_` 本身**不清空**：`discard_pending()` 在 `close()` 之后仍需经由它触达镜像（见 `corotcpserver.hpp:135-137` 的用例：消费者先 `close()` 再 `delete server`）：

```cpp
void close(std::error_code error) noexcept {
    std::unique_lock<boost::fibers::mutex> lck{mtx_};
    if(closed_.load()){
        return;
    }
    close_error_ = error == std::error_code{}
            ? std::make_error_code(std::errc::no_message)
            : error;
    closed_.store(true);
    cv_consumer_.notify_all();
    if(mirrors_){
        for(auto& weak : *mirrors_){
            if(auto mirror = weak.lock()) mirror->close(close_error_);
        }
    }
}
```

`discard_pending()` 增加扇出——它用于源销毁时清掉即将悬空的指针（如 `QTcpSocket*`），不扇出会在镜像里留下野指针。`push()` 关闭后即早返回、不再遍历镜像，`close()` 也不剔除失效槽位，因此这里是唯一还会遍历一个已关闭 channel 的镜像列表的扇出点，顺带用同样的 swap-and-pop 剔除失效槽位：

```cpp
void discard_pending(){
    std::unique_lock<boost::fibers::mutex> lck{mtx_};
    queue_.clear();
    if(mirrors_){
        auto& list = *mirrors_;
        for(std::size_t i = 0; i < list.size(); ){
            auto mirror = list[i].lock();
            if(!mirror){
                list[i] = std::move(list.back());
                list.pop_back();
                continue;
            }
            mirror->discard_pending();
            ++i;
        }
    }
}
```

析构函数——源 channel 未 `close()` 就消亡时，令镜像收敛而非永久挂起消费者；用 `connection_aborted` 与正常结束的 `no_message` 区分。此时引用计数已归零，不存在其他持有者，无需加锁：

```cpp
~FiberChannel(){
    if(mirrors_){
        for(auto& weak : *mirrors_){
            if(auto mirror = weak.lock()){
                mirror->close(std::make_error_code(std::errc::connection_aborted));
            }
        }
    }
}
```

`FiberChannel` 已经因为显式删除拷贝构造/拷贝赋值而不再隐式生成移动构造/赋值（用户声明拷贝特殊成员函数会抑制移动特殊成员函数的隐式生成，与本次新增析构函数无关）；`FiberChannel` 全程只以 `shared_ptr` 持有，代码库里也没有依赖它可移动的用法，因此新增析构函数不改变既有行为。

### 5.2 `coro/await/awaitable.hpp`

`Awaitable<T>` 与 `Awaitable<void>` 各新增一个方法（后者内部是 `FiberChannel<int>`，类型自洽）：

```cpp
std::shared_ptr<Awaitable<T>> shared(){
    auto sub = std::make_shared<Awaitable<T>>();
    if(ch_) ch_->addMirror(sub->channel());
    return sub;
}
```

### 5.3 改动清单

| 文件 | 改动 |
|---|---|
| `coro/detail/fiberchannel.hpp` | 新增 1 个成员、1 个内部方法、1 条 friend 声明；`push` / `close(error)` / `discard_pending` 各加一段扇出 |
| `coro/await/awaitable.hpp` | 两个特化各新增一个 `shared()` 方法 |

不新增文件，`AsyncTask.pri` 与 `coro/all.hpp` 无需改动。

## 6. 存储与性能

实测于 x86-64 / libstdc++ / glibc；目标平台不同时需重测。

`sizeof(FiberChannel<T>)` 当前为 **160 字节，且与 `T` 无关**——`std::deque<T>` 恒为 80 字节（只存 map 指针与两个迭代器，已用 `int` / 100B / 4KB 三种元素类型验证），其余为 `boost::fibers::mutex` 32 + `condition_variable` 24 + `error_code` 16 + `atomic_bool`。

| 方案 | `sizeof` | `make_shared` 请求 | glibc 实占 |
|---|---|---|---|
| 现状 | 160 | 176 | **184** |
| `+ std::vector` | 184 | 200 | **200** |
| `+ std::unique_ptr<vector>`（采用） | 168 | 184 | **184** |

`FiberChannel` 一律由 `make_shared` 创建，加 16 字节控制块后落入 glibc 分配桶。现状请求 176 已经实占 184，那 8 字节本就在浪费；用 `unique_ptr` 恰好填上，**实际堆占用零增长**。直接内嵌 `vector` 则跨入下一个桶，每个 channel 真多 16 字节。

未使用共享功能时的代价：`push` / `close` / `discard_pending` 各多一次空指针判断（相对于已经持有的 fiber mutex 可忽略），无堆分配，无内存增长。

使用共享功能时的代价：每次 `push` 增加 n 次 `weak_ptr::lock()`（各一个原子 RMW）与 n 次值拷贝，n 为订阅者数。对 `QByteArray`、`QTcpSocket*` 等 Qt 类型，拷贝基本是引用计数或指针复制。

替代 `weak_ptr` 的方案（镜像持反向指针、显式注销）会给**所有** channel 增加 16 字节，劣于当前方案，不采用。

## 7. 跨线程与锁顺序

`FiberChannel` 本身已跨线程安全（fiber 版 mutex/condition_variable），镜像只是同类 channel，不引入新的同步原语。消费侧不变：订阅句柄可被任意线程上的 fiber 消费。

**锁顺序恒为"源 → 镜像"。** 扇出是持源锁时去取镜像锁。`a->shared()->shared()` 这类链式订阅构成树，天然无环。唯一可能死锁的是两个 channel 互设为对方的镜像——`shared()` 无法构造出这种结构，因此 **`addMirror()` 保持内部可见（friend），不作为公开接口暴露**。

**扇出在生产者线程（通常是 Qt 事件循环线程）上同步完成。** 每条镜像的临界区仅为一次 `deque::push_back` 加 `notify_one`，很短；但耗时随订阅者数量线性增长，会按比例占用事件循环。这是换掉常驻泵 fiber 所付的代价，对个位数订阅者可忽略。

## 8. 已知限制

- 订阅者队列无界。某个订阅者长期不 `await` 会导致其自身队列持续增长，直至内存耗尽。该行为与现有 `FiberChannel` 一致，属既定选择。
- 无 replay。`shared()` 之前产生的数据对订阅者不可见。若需要"先订阅再启动数据源"，由调用方保证顺序。
- 失效订阅槽位延迟到下一次 `push` / `close` 才剔除。
- 订阅者数量较多时会拖慢生产者所在的事件循环。

## 9. 测试计划

位置：`test/testfiberawait`。

| 用例 | 验证内容 |
|---|---|
| 组内不漏 | 1 源 + 2 个 `shared()`，push N 条，两个订阅者各自收齐 N 条且顺序一致 |
| 源队列照常抢占 | 源上 2 个消费者互抢，合起来恰好收齐 N 条（不重不漏）；同时 2 个订阅者各收齐 N 条 |
| 无 replay | 先 push 3 条，再 `shared()`，订阅者只收到之后的；源的那 3 条不受影响 |
| RAII 退订 | 订阅句柄析构后源继续 push，不崩溃，镜像列表收缩 |
| 终止传播 | 源 `close(connection_reset)` → 每个订阅者先取完余量，再拿到同一错误码 |
| 关闭后订阅 | 源已关闭时调 `shared()`，返回的句柄立即以源的错误码收敛，不挂起 |
| 镜像独立关闭 | 某个订阅者 `close()`，源与其他订阅者不受影响 |
| `void` 特化 | `Awaitable<void>::shared()` |
| 跨线程 | 订阅者在另一个 `QtFiberThread` 上消费 |
| 正常退出 | 进程能正常退出（回归已知的 `testfiberawait` 退出挂起问题） |
| 端到端 | `Coro::coro(sock).readAll()` 上开两个 `shared()`，一个解析一个落日志，socket 断开后两条消费循环均自然收敛 |

回归范围：`test/testfiberawait`、`test/testfibertask`、`test/testexecutor`、`test/test_scheduler` 全部通过，确认未共享路径行为未变。

## 10. 文档更新

实现完成后同步：

- `doc/使用说明.md`：新增"广播消费"小节，说明 `shared()` 的用法与无 replay 约定。
- `doc/需求规格说明.md` / `doc/软件设计说明.md`：补充共享消费的需求项与设计说明。
