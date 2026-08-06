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
 *   - corosslsocket.hpp   —— coro(QSslSocket*)
 *   - corotcpserver.hpp   —— coro(QTcpServer*)
 *   - corolocalsocket.hpp —— coro(QLocalSocket*)
 *   - corolocalserver.hpp —— coro(QLocalServer*)
 *   - coroudpsocket.hpp   —— coro(QUdpSocket*)
 *
 * 消费统一用自由函数 await(a)/await_for(a,timeout)（awaitable.hpp）与 generate(a)（generator.hpp）。
 * 全部 Qt 依赖集中在这些 coro* 头文件中；Awaitable 本体与 Qt 解耦。
 *
 * @code
 * #include "await/coro.hpp"        // 一次性引入全部来源的 coro()
 * using namespace Coro;
 *
 * // 同一个 coro() 入口适配不同来源，消费方式统一
 * await(coro(obj, &Obj::valueChanged));                  // Qt 信号
 * await(coro(std::move(fut)));                           // future
 * await(coro(dev).readAll());                            // QIODevice
 * await_for(coro(sock).connectToHost(host, port), 2s);   // socket
 * for(auto* p : generate(coro(server).nextConnection())) handle(p);   // server
 * @endcode
 */

#include "awaitable.hpp"   // 消费: await(a) / await_for(a, timeout)
#include "generator.hpp"   // 消费: generate(a)
#include "corosignal.hpp"
#include "corofuture.hpp"
#include "coroiodevice.hpp"
#include "corosocket.hpp"
#include "corosslsocket.hpp"
#include "corotcpserver.hpp"
#include "corolocalsocket.hpp"
#include "corolocalserver.hpp"
#include "coroudpsocket.hpp"

#endif // CORO_HPP
