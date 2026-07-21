#ifndef CORO_SOCKETERROR_HPP
#define CORO_SOCKETERROR_HPP

#include <QAbstractSocket>
#include <QLocalSocket>
#include <QSslError>
#include <system_error>

namespace Coro {
namespace detail {

class SocketErrorCategory final : public std::error_category {
public:
    const char* name() const noexcept override { return "qt.socket"; }

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

class LocalSocketErrorCategory final : public std::error_category {
public:
    const char* name() const noexcept override { return "qt.local_socket"; }

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

class SslErrorCategory final : public std::error_category {
public:
    const char* name() const noexcept override { return "qt.ssl"; }

    std::string message(int value) const override {
        return QSslError(static_cast<QSslError::SslError>(value))
            .errorString().toStdString();
    }
};

inline const std::error_category& socket_error_category() noexcept {
    static const SocketErrorCategory category;
    return category;
}

inline const std::error_category& local_socket_error_category() noexcept {
    static const LocalSocketErrorCategory category;
    return category;
}

inline const std::error_category& ssl_error_category() noexcept {
    static const SslErrorCategory category;
    return category;
}

inline std::error_code socket_error_code(QAbstractSocket::SocketError error) noexcept {
    return {static_cast<int>(error), socket_error_category()};
}

inline std::error_code local_socket_error_code(QLocalSocket::LocalSocketError error) noexcept {
    return {static_cast<int>(error), local_socket_error_category()};
}

inline std::error_code ssl_error_code(QSslError::SslError error) noexcept {
    return {static_cast<int>(error), ssl_error_category()};
}

} // namespace detail
} // namespace Coro

#endif // CORO_SOCKETERROR_HPP
