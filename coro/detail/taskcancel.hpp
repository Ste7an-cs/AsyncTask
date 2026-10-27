#ifndef TASKCANCEL_HPP
#define TASKCANCEL_HPP

#include <atomic>
#include <cassert>
#include <mutex>
#include <system_error>

namespace Coro {
namespace detail {

/**
 * @brief 一次 Awaitable 等待的取消登记项。
 * @details 对象位于等待协程的栈上；cancel() 通过函数指针关闭对应句柄的消费者队列。
 */
struct AwaitableCancelNode {
    /** @brief 关闭回调类型；必须 noexcept，只做队列 close */
    using CloseFn = void (*)(void* obj, std::error_code ec) noexcept;

    void*   obj{nullptr};   ///< 正在等待的 Awaitable 句柄
    CloseFn close{nullptr}; ///< 调该句柄的私有 closeConsumerQueue
};

/**
 * @brief 每个任务链一个的取消槽。
 * @details 同一任务 fiber 同一时刻至多一个活动 await；add/remove/cancel 由 mtx_ 串行化。
 */
class TaskCancelSlot {
public:
    TaskCancelSlot() = default;
    TaskCancelSlot(const TaskCancelSlot&) = delete;
    TaskCancelSlot& operator=(const TaskCancelSlot&) = delete;

    /**
     * @brief 登记当前等待；槽已取消时返回 false
     * @param node 栈上节点，调用方保证其存活到 remove()
     * @return 登记成功返回 true
     */
    bool add(AwaitableCancelNode* node)
    {
        assert(node != nullptr);
        assert(node->close != nullptr);
        std::lock_guard<std::mutex> lk(mtx_);
        if(cancelled_.load(std::memory_order_acquire)){
            return false;
        }
        assert(active_ == nullptr);
        active_ = node;
        return true;
    }

    /**
     * @brief 注销当前等待；节点未被登记或已被 cancel() 取走时安全空转
     * @param node 之前传给 add() 的节点
     */
    void remove(AwaitableCancelNode* node) noexcept
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if(active_ == node){
            active_ = nullptr;
        }
    }

    /**
     * @brief 置取消标志并关闭当前登记的消费者队列
     * @param ec 关闭错误码
     * @details 持锁调用 node->close；等待者返回后 remove() 会阻塞到本函数退出。
     *          close 只做 noexcept 的 FiberChannel::close，不跑用户清理。
     */
    void cancel(std::error_code ec) noexcept
    {
        cancelled_.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lk(mtx_);
        if(active_ != nullptr){
            AwaitableCancelNode* node = active_;
            active_ = nullptr;
            node->close(node->obj, ec);
        }
    }

    /** @brief 是否已请求取消 */
    bool isCancelled() const noexcept
    {
        return cancelled_.load(std::memory_order_acquire);
    }

private:
    std::mutex           mtx_;
    AwaitableCancelNode* active_{nullptr};
    std::atomic_bool     cancelled_{false};
};

TaskCancelSlot* currentTaskCancelSlot() noexcept;
void setCurrentTaskCancelSlot(TaskCancelSlot* slot) noexcept;

/**
 * @brief 将当前 fiber 的 MetaContext 临时指向任务取消槽。
 * @details makeTask/then 在调用用户函数前构造，函数返回/抛出后析构恢复。
 */
class TaskCancelScope {
public:
    explicit TaskCancelScope(TaskCancelSlot* slot) noexcept
        : previous_(currentTaskCancelSlot()){
        setCurrentTaskCancelSlot(slot);
    }
    ~TaskCancelScope(){
        setCurrentTaskCancelSlot(previous_);
    }
    TaskCancelScope(const TaskCancelScope&) = delete;
    TaskCancelScope& operator=(const TaskCancelScope&) = delete;
private:
    TaskCancelSlot* previous_{nullptr};
};

} // namespace detail
} // namespace Coro

#endif // TASKCANCEL_HPP
