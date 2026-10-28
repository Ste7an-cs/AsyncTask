#ifndef TASKCANCEL_HPP
#define TASKCANCEL_HPP

#include <atomic>
#include <cassert>
#include <memory>
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

    void*   obj{nullptr};   ///< 待关闭的 FiberChannel 裸指针
    CloseFn close{nullptr}; ///< 关闭该队列的 thunk
    std::shared_ptr<void> alive{};///< 关闭目标的保活引用；close 可能让出协程时目标不被销毁
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
     * @brief 置取消标志并在锁外关闭当前登记的消费者队列
     * @param ec 关闭错误码
     * @details 锁内只复制节点并清空 active_，随后解锁再调用 close。close 会经过
     *          FiberChannel 的 fiber 互斥量，可能让出当前协程；若持 std::mutex
     *          让出，同一调度线程上的唤醒者会卡在 remove() 上，形成死锁。
     *          节点副本的 alive 保持目标存活，close 期间的栈展开不受影响。
     */
    void cancel(std::error_code ec) noexcept
    {
        cancelled_.store(true, std::memory_order_release);
        AwaitableCancelNode node{};
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if(active_ != nullptr){
                node = *active_;
                active_ = nullptr;
            }
        }
        if(node.close != nullptr){
            node.close(node.obj, ec);
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

/**
 * @brief 一次 await 的取消登记 RAII 守卫。
 * @details 构造时登记；析构时若仍登记则注销。await 在构造值、等待或取错误等任意
 *          步骤抛出时，栈展开都会先注销，避免槽里留下指向已销毁栈节点的悬空指针。
 */
class TaskCancelRegistration {
public:
    /**
     * @brief 登记节点
     * @param slot 当前任务的取消槽；非任务上下文可为空
     * @param node 调用方栈上的节点，生命周期必须覆盖本守卫
     */
    TaskCancelRegistration(TaskCancelSlot* slot, AwaitableCancelNode* node)
        : slot_(slot), node_(node), registered_(slot != nullptr && slot->add(node)){}
    /** @brief 未 release() 时注销登记 */
    ~TaskCancelRegistration(){
        if(registered_){
            slot_->remove(node_);
        }
    }
    TaskCancelRegistration(const TaskCancelRegistration&) = delete;
    TaskCancelRegistration& operator=(const TaskCancelRegistration&) = delete;

    /**
     * @brief 是否登记成功
     * @return 槽存在且未取消、登记成功返回 true
     */
    bool registered() const noexcept { return registered_; }
    /**
     * @brief 提前注销（幂等）
     * @details 正常取到值或超时后调用，缩小取消与完成并发时的关闭窗口。
     */
    void release() noexcept {
        if(registered_){
            slot_->remove(node_);
            registered_ = false;
        }
    }
private:
    TaskCancelSlot*      slot_{nullptr};
    AwaitableCancelNode* node_{nullptr};
    bool                 registered_{false};
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
