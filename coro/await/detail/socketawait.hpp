#ifndef CORO_SOCKETAWAIT_HPP
#define CORO_SOCKETAWAIT_HPP

/**
 * @file socketawait.hpp
 * @brief socket 包装器共用的跨 Qt 版本错误信号连接助手。
 * @details 连接生命周期/断连统一由 detail::AutoDisconnect 管理（见 autodisconnect.hpp）；
 *          本文件仅保留 QAbstractSocket / QLocalSocket 错误信号在不同 Qt 版本下的
 *          连接兼容封装。
 */

#include <QAbstractSocket>
#include <QLocalSocket>
#include <QMetaObject>
#include <QObject>
#include <QtGlobal>
#include <utility>

namespace Coro {
namespace detail {

/**
 * @brief 连接 QAbstractSocket 的跨 Qt 版本错误信号。
 * @tparam Function 可接收 QAbstractSocket::SocketError 的回调类型。
 * @param socket 错误信号来源。
 * @param function 接收 SocketError 的回调。
 * @return 建立的 Qt 信号连接。
 */
template<typename Function>
QMetaObject::Connection connect_socket_error(QAbstractSocket* socket,
                                             Function&& function){
#if QT_VERSION >= QT_VERSION_CHECK(5, 15, 0)
    return QObject::connect(socket, &QAbstractSocket::errorOccurred,
                            std::forward<Function>(function));
#else
    return QObject::connect(
        socket,
        static_cast<void (QAbstractSocket::*)(QAbstractSocket::SocketError)>(
            &QAbstractSocket::error),
        std::forward<Function>(function));
#endif
}

/**
 * @brief 连接 QLocalSocket 的跨 Qt 版本错误信号。
 * @tparam Function 可接收 QLocalSocket::LocalSocketError 的回调类型。
 * @param socket 错误信号来源。
 * @param function 接收 LocalSocketError 的回调。
 * @return 建立的 Qt 信号连接。
 */
template<typename Function>
QMetaObject::Connection connect_local_socket_error(QLocalSocket* socket,
                                                   Function&& function){
#if QT_VERSION >= QT_VERSION_CHECK(5, 15, 0)
    return QObject::connect(socket, &QLocalSocket::errorOccurred,
                            std::forward<Function>(function));
#else
    return QObject::connect(
        socket,
        static_cast<void (QLocalSocket::*)(QLocalSocket::LocalSocketError)>(
            &QLocalSocket::error),
        std::forward<Function>(function));
#endif
}

} // namespace detail
} // namespace Coro

#endif // CORO_SOCKETAWAIT_HPP
