#ifndef CORO_SOCKETERROR_HPP
#define CORO_SOCKETERROR_HPP

#include <QAbstractSocket>
#include <QLocalSocket>
#include <QSslError>
#include <system_error>

namespace Coro {
namespace detail {

/**
 * @brief QAbstractSocket 传输错误的 std::error_category 适配器。
 * @details category 名称固定为 "qt.socket"，保留 Qt 枚举的整数值；消息遵循 Qt
 *          传输 socket 错误语义。
 */
class SocketErrorCategory final : public std::error_category {
public:
    /** @brief 返回稳定的传输 socket category 名称。 */
    const char* name() const noexcept override { return "qt.socket"; }

    /**
     * @brief 将 QAbstractSocket::SocketError 的整数值转为可读消息。
     * @param value 保留的 Qt 枚举整数值。
     * @return 与 Qt 传输 socket 错误语义一致的英文消息。
     */
    std::string message(int value) const override {
        switch(static_cast<QAbstractSocket::SocketError>(value)){
        case QAbstractSocket::ConnectionRefusedError: return "connection refused";
        case QAbstractSocket::RemoteHostClosedError: return "remote host closed";
        case QAbstractSocket::HostNotFoundError: return "host not found";
        case QAbstractSocket::SocketAccessError: return "socket access error";
        case QAbstractSocket::SocketResourceError: return "socket resource error";
        case QAbstractSocket::SocketTimeoutError: return "socket timeout";
        case QAbstractSocket::DatagramTooLargeError: return "datagram too large";
        case QAbstractSocket::NetworkError: return "network error";
        case QAbstractSocket::AddressInUseError: return "address in use";
        case QAbstractSocket::SocketAddressNotAvailableError: return "socket address not available";
        case QAbstractSocket::UnsupportedSocketOperationError: return "unsupported socket operation";
        case QAbstractSocket::UnfinishedSocketOperationError: return "unfinished socket operation";
        case QAbstractSocket::ProxyAuthenticationRequiredError: return "proxy authentication required";
        case QAbstractSocket::SslHandshakeFailedError: return "SSL handshake failed";
        case QAbstractSocket::ProxyConnectionRefusedError: return "proxy connection refused";
        case QAbstractSocket::ProxyConnectionClosedError: return "proxy connection closed";
        case QAbstractSocket::ProxyConnectionTimeoutError: return "proxy connection timeout";
        case QAbstractSocket::ProxyNotFoundError: return "proxy not found";
        case QAbstractSocket::ProxyProtocolError: return "proxy protocol error";
        case QAbstractSocket::OperationError: return "operation error";
        case QAbstractSocket::SslInternalError: return "SSL internal error";
        case QAbstractSocket::SslInvalidUserDataError: return "SSL invalid user data";
        case QAbstractSocket::TemporaryError: return "temporary socket error";
        case QAbstractSocket::UnknownSocketError: return "unknown socket error";
        }
        return "unknown socket error";
    }
};

/**
 * @brief QLocalSocket 本地进程间通信错误的 std::error_category 适配器。
 * @details category 名称固定为 "qt.local_socket"，保留 Qt 枚举的整数值；消息遵循
 *          Qt 本地 socket 错误语义，错误域与网络传输 socket 分离。
 */
class LocalSocketErrorCategory final : public std::error_category {
public:
    /** @brief 返回稳定的本地 socket category 名称。 */
    const char* name() const noexcept override { return "qt.local_socket"; }

    /**
     * @brief 将 QLocalSocket::LocalSocketError 的整数值转为可读消息。
     * @param value 保留的 Qt 枚举整数值。
     * @return 与 Qt 本地 socket 错误语义一致的英文消息。
     */
    std::string message(int value) const override {
        switch(static_cast<QLocalSocket::LocalSocketError>(value)){
        case QLocalSocket::ConnectionRefusedError: return "connection refused";
        case QLocalSocket::PeerClosedError: return "peer closed";
        case QLocalSocket::ServerNotFoundError: return "server not found";
        case QLocalSocket::SocketAccessError: return "socket access error";
        case QLocalSocket::SocketResourceError: return "socket resource error";
        case QLocalSocket::SocketTimeoutError: return "socket timeout";
        case QLocalSocket::DatagramTooLargeError: return "datagram too large";
        case QLocalSocket::ConnectionError: return "connection error";
        case QLocalSocket::UnsupportedSocketOperationError: return "unsupported socket operation";
        case QLocalSocket::OperationError: return "operation error";
        case QLocalSocket::UnknownSocketError: return "unknown socket error";
        }
        return "unknown socket error";
    }
};

/**
 * @brief QSslError 证书和握手错误的 std::error_category 适配器。
 * @details category 名称固定为 "qt.ssl"，保留 Qt 枚举的整数值；消息直接采用 Qt TLS
 *          错误语义，错误域与传输 socket 错误分离。
 */
class SslErrorCategory final : public std::error_category {
public:
    /** @brief 返回稳定的 TLS category 名称。 */
    const char* name() const noexcept override { return "qt.ssl"; }

    /**
     * @brief 将 QSslError::SslError 的整数值转为 Qt 提供的错误消息。
     * @param value 保留的 Qt 枚举整数值。
     * @return QSslError::errorString() 的消息。
     */
    std::string message(int value) const override {
        return QSslError(static_cast<QSslError::SslError>(value))
            .errorString().toStdString();
    }
};

/**
 * @brief 取得稳定的网络传输 socket 错误类别。
 * @return 进程内唯一的 "qt.socket" category。
 */
inline const std::error_category& socket_error_category() noexcept {
    static const SocketErrorCategory category;
    return category;
}

/**
 * @brief 取得稳定的本地 socket 错误类别。
 * @return 进程内唯一的 "qt.local_socket" category。
 */
inline const std::error_category& local_socket_error_category() noexcept {
    static const LocalSocketErrorCategory category;
    return category;
}

/**
 * @brief 取得稳定的 TLS 证书与握手错误类别。
 * @return 进程内唯一的 "qt.ssl" category。
 */
inline const std::error_category& ssl_error_category() noexcept {
    static const SslErrorCategory category;
    return category;
}

/**
 * @brief 将 Qt 网络传输 socket 错误转为 std::error_code。
 * @param error QAbstractSocket 错误枚举。
 * @return 保留 error 整数值并使用 qt.socket category 的错误码。
 */
inline std::error_code socket_error_code(QAbstractSocket::SocketError error) noexcept {
    return {static_cast<int>(error), socket_error_category()};
}

/**
 * @brief 将 Qt 本地 socket 错误转为 std::error_code。
 * @param error QLocalSocket 错误枚举。
 * @return 保留 error 整数值并使用 qt.local_socket category 的错误码。
 */
inline std::error_code local_socket_error_code(QLocalSocket::LocalSocketError error) noexcept {
    return {static_cast<int>(error), local_socket_error_category()};
}

/**
 * @brief 将 Qt TLS 证书或握手错误转为 std::error_code。
 * @param error QSslError 错误枚举。
 * @return 保留 error 整数值并使用 qt.ssl category 的错误码。
 */
inline std::error_code ssl_error_code(QSslError::SslError error) noexcept {
    return {static_cast<int>(error), ssl_error_category()};
}

} // namespace detail
} // namespace Coro

#endif // CORO_SOCKETERROR_HPP
