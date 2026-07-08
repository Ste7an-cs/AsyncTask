#ifndef ALL_HPP
#define ALL_HPP

/**
 * @file all.hpp
 * @brief 总汇入口头文件：一次性引入 AsyncTask 的公开 API。
 *
 * 用法：#include "all.hpp" 即可使用 makeTask / FiberTask / Result / Awaitable /
 * Generator / coro / await / generate 等全部对外接口。也可按需单独引入下列各头，
 * 只拉入用到的部分。Qt 相关能力（协程线程、应用集成、coro() 等待工厂）在宿主
 * 工程启用 Qt（定义 ASYNC_HAS_QTCORE）时才引入。
 */

// —— 基础层 ——
#include "detail/result.hpp"        // Result<T, E>
#include "detail/fiberchannel.hpp"  // FiberChannel<T>
#include "detail/asyncdefine.h"     // launch_properties / sleep / msleep

// —— 调度与执行器层 ——
#include "executor/scheduler/fiberproperty.h"  // Priority / Affinity / MetaContext
#include "executor/fiberpool.h"                // FibersPool

// —— 任务层 ——
#include "task/fibertask.h"         // makeTask / FiberTask

// —— 等待层（与 Qt 解耦部分）——
#include "await/awaitable.hpp"      // Awaitable / await(a) / await_for(a, timeout)
#include "await/generator.hpp"      // Generator / generate(a)

// —— Qt 相关能力（仅在启用 Qt 时引入）——
#ifdef ASYNC_HAS_QTCORE
#include "executor/qtfiberthread.h" // QtFiberThread
#include "task/fiberapplication.h"  // installFiberApplication / exec / quit
#include "await/coro.hpp"           // coro(...) 工厂伞头（信号 / socket / future ...）
#endif

#endif // ALL_HPP
