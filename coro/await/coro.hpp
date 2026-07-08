#ifndef CORO_HPP
#define CORO_HPP

/**
 * @file coro.hpp
 * @brief 统一协程等待工厂 coro(...) 的伞头：一次性引入全部包装器。
 *
 * 也可按需单独 include 下列各头，只拉入用到的类型：
 *   - corosignal.hpp      —— coro(obj, &T::sig) 信号
 *   - corofuture.hpp      —— coro(std::future/QFuture)
 *   - coroiodevice.hpp    —— coro(QIODevice*)
 *   - corosocket.hpp      —— coro(QAbstractSocket*)
 *   - corotcpserver.hpp   —— coro(QTcpServer*)
 *   - corolocalsocket.hpp —— coro(QLocalSocket*)
 *
 * 消费统一用自由函数 await(a)/await_for(a,timeout)（awaitable.hpp）与 generate(a)（generator.hpp）。
 * 全部 Qt 依赖集中在这些 coro* 头文件中；Awaitable 本体与 Qt 解耦。
 */

#include "awaitable.hpp"   // 消费: await(a) / await_for(a, timeout)
#include "generator.hpp"   // 消费: generate(a)
#include "corosignal.hpp"
#include "corofuture.hpp"
#include "coroiodevice.hpp"
#include "corosocket.hpp"
#include "corotcpserver.hpp"
#include "corolocalsocket.hpp"

#endif // CORO_HPP
