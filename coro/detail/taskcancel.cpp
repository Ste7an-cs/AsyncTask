#include "taskcancel.hpp"
#include "executor/scheduler/fiberproperty.h"

#include <boost/fiber/context.hpp>

namespace Coro {
namespace detail {

/**
 * @brief 读取当前 fiber 的 MetaContext 上登记的任务取消槽。
 * @details 非框架 fiber / 非 fiber 上下文返回 nullptr；不调用会 yield 的
 *          boost::this_fiber::properties()。
 */
TaskCancelSlot* currentTaskCancelSlot() noexcept
{
    try {
        boost::fibers::context* ctx = boost::fibers::context::active();
        if(ctx == nullptr){
            return nullptr;
        }
        auto* meta = dynamic_cast<MetaContext*>(ctx->get_properties());
        return meta != nullptr ? meta->taskCancelSlot() : nullptr;
    } catch (...) {
        return nullptr;
    }
}

/**
 * @brief 设置当前 fiber 的 MetaContext 上的任务取消槽。
 * @details 非框架 fiber / 非 fiber 上下文为空操作。
 */
void setCurrentTaskCancelSlot(TaskCancelSlot* slot) noexcept
{
    try {
        boost::fibers::context* ctx = boost::fibers::context::active();
        if(ctx == nullptr){
            return;
        }
        if(auto* meta = dynamic_cast<MetaContext*>(ctx->get_properties())){
            meta->setTaskCancelSlot(slot);
        }
    } catch (...) {
        // 非 fiber 上下文：不支持任务取消，保持无操作。
    }
}

} // namespace detail
} // namespace Coro
