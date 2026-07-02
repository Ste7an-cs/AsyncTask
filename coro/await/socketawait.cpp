#include "socketawait.hpp"

Coro::Result<void> Coro::awaitForConnected(QAbstractSocket* socket, int msecs)
{
    if(!socket){
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto awaitable = detail::await_single_impl(socket, &QAbstractSocket::connected, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(socket, &QAbstractSocket::aboutToClose, [awaitable](){
        awaitable->close();
    });
    if(socket->state() == QAbstractSocket::ConnectedState){
        return Result<void>();
    }
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<void> Coro::awaitForDisconnected(QAbstractSocket* socket, int msecs)
{
    if(!socket){
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto awaitable = detail::await_single_impl(socket, &QAbstractSocket::disconnected, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(socket, &QAbstractSocket::aboutToClose, [awaitable](){
        awaitable->resolve();
        awaitable->close();
    });
    if(socket->state() == QAbstractSocket::UnconnectedState){
        return Result<void>();
    }
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<void> Coro::awaitConnectToHost(QAbstractSocket* socket, const QString &hostName, quint16 port, int msecs, QIODevice::OpenMode openMode, QAbstractSocket::NetworkLayerProtocol protocol)
{
    if(!socket){
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto awaitable = detail::await_single_impl(socket, &QAbstractSocket::connected, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(socket, &QAbstractSocket::aboutToClose, [awaitable](){
        awaitable->close();
    });
    if(socket->state() == QAbstractSocket::ConnectedState){
        return Result<void>();
    }
    socket->connectToHost(hostName, port, openMode, protocol);
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<void> Coro::awaitConnectToHost(QAbstractSocket* socket, const QHostAddress &address, quint16 port, int msecs, QIODevice::OpenMode openMode)
{
    if(!socket){
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto awaitable = detail::await_single_impl(socket, &QAbstractSocket::connected, static_cast<std::tuple<>*>(nullptr));
    if(socket->state() == QAbstractSocket::ConnectedState){
        return Result<void>();
    }
    QObject::connect(socket, &QAbstractSocket::aboutToClose, [awaitable](){
        awaitable->close();
    });
    socket->connectToHost(address, port, openMode);
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<void> Coro::awaitConnectToServer(QPointer<QLocalSocket> local, int msecs, QIODevice::OpenMode openMode)
{
    if(!local){
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto awaitable = detail::await_single_impl(local.data(), &QLocalSocket::connected, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(local, &QLocalSocket::aboutToClose, [awaitable](){
        awaitable->close();
    });
    if(local->state() == QLocalSocket::ConnectedState){
        return Result<void>();
    }
    local->connectToServer(openMode);
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<void> Coro::awaitConnectToServer(QPointer<QLocalSocket> local, const QString &name, int msecs, QIODevice::OpenMode openMode)
{
    if(!local){
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto awaitable = detail::await_single_impl(local.data(), &QLocalSocket::connected, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(local, &QLocalSocket::aboutToClose, [awaitable](){
        awaitable->close();
    });
    if(local->state() == QLocalSocket::ConnectedState){
        return Result<void>();
    }
    local->connectToServer(name, openMode);
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<QTcpSocket *> Coro::awaitForNewConnection(QPointer<QTcpServer> server, int msec)
{
    if(nullptr == server){
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto awaitable = detail::await_single_impl(server.data(), &QTcpServer::newConnection, static_cast<std::tuple<>*>(nullptr));
    if(server->hasPendingConnections()){
        return server->nextPendingConnection();
    }
    if(msec>0){
        awaitable->await_for(std::chrono::milliseconds(msec));
        if(server->hasPendingConnections()){
            return server->nextPendingConnection();
        }
    }else{
        awaitable->await();
        if(server->hasPendingConnections()){
            return server->nextPendingConnection();
        }
    }
    return std::make_error_code(std::errc::timed_out);
}

auto Coro::generateNewConnection(QPointer<QTcpServer> server)->Generator<QTcpSocket*>
{
    auto awaitable = detail::await_impl(server.data(), &QTcpServer::newConnection, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(server.data(), &QTcpServer::destroyed, [awaitable](){
        awaitable->close();
    });
    Generator<QTcpSocket*> gen([awaitable, server](auto yield){
        if(!server){
            return ;
        }
        while(1){
            Result<void> res = awaitable->await_for(std::chrono::milliseconds(1000));
            if(res.has_value()){
                if(server){
                    yield(server->nextPendingConnection());
                }else{
                    return;//server释放，生成器退出
                }
            }else{
                if(server->isListening()){
                    continue;
                }else{
                    return;//awaitable终止，生成器退出
                }
            }
        }
    });
    return gen;
}

Coro::Result<QByteArray> Coro::awaitReadAll(QAbstractSocket* dev)
{
    if(!dev){
        return std::make_error_code(std::errc::no_message);
    }
    auto awaitable = detail::await_single_impl(dev, &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QAbstractSocket::disconnected, [awaitable](){
        awaitable->close();
    });
    if(!dev->isReadable() || !dev->isOpen() || QAbstractSocket::UnconnectedState == dev->state()){
        return std::make_error_code(std::errc::connection_aborted);
    }
    auto res = awaitable->await();
    if(res.has_value()){
        if(dev){
            return dev->readAll();
        }
    }
    return std::make_error_code(std::errc::no_message);
}

Coro::Result<QByteArray> Coro::awaitForReadAll(QAbstractSocket* dev, int msecs)
{
    if(!dev){
        return std::make_error_code(std::errc::no_message);
    }
    auto awaitable = detail::await_single_impl(dev, &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QAbstractSocket::disconnected, [awaitable](){
        awaitable->close();
    });
    if(!dev->isReadable() || !dev->isOpen() || QAbstractSocket::UnconnectedState == dev->state()){
        return std::make_error_code(std::errc::connection_aborted);
    }
    auto res = awaitable->await_for(std::chrono::milliseconds(msecs));
    if(res.has_value()){
        if(dev){
            return dev->readAll();
        }
    }
    return std::make_error_code(std::errc::no_message);
}

Coro::Generator<QByteArray> Coro::generateReadAll(QAbstractSocket* dev)
{
    auto awaitable = detail::await_impl(dev, &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QAbstractSocket::disconnected, [awaitable](){
        awaitable->close();
    });
    Generator<QByteArray> gen([awaitable, dev](auto resolve){
        if(!dev->isReadable() || !dev->isOpen() || QAbstractSocket::UnconnectedState == dev->state()){
            return ;
        }
        while(1){
            if(!dev->isReadable() || !dev->isOpen() || QAbstractSocket::UnconnectedState == dev->state()){
                return ;
            }
            Result<void> res = awaitable->await();
            if(res.has_value()){
                if(dev){
                    resolve(dev->readAll());
                }else{
                    return;//dev释放，生成器退出
                }
            }else{
                return;//awaitable终止，生成器退出
            }
        }
    });
    return gen;
}

Coro::Result<void> Coro::awaitForReadyRead(QAbstractSocket* dev, int msecs)
{
    auto awaitable = detail::await_single_impl(dev, &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QAbstractSocket::disconnected, [awaitable](){
        awaitable->close();
    });
    if(dev->isReadable() && dev->isOpen()){
        return Result<void>();
    }
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<void> Coro::awaitForBytesWritten(QAbstractSocket* dev, int msecs)
{
    auto awaitable = detail::await_single_impl(dev, &QIODevice::bytesWritten, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QAbstractSocket::disconnected, [awaitable](){
        awaitable->close();
    });
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}
