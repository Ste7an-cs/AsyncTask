#include "iodeviceawait.hpp"

Coro::Result<QByteArray> Coro::awaitReadAll(QPointer<QIODevice> dev){
    if(!dev){
        return std::make_error_code(std::errc::no_message);
    }
    auto awaitable = detail::await_single_impl(dev.data(), &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QIODevice::aboutToClose, [awaitable](){
        awaitable->close();
    });
    if(!(dev->isReadable() && dev->isOpen())){
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

Coro::Generator<QByteArray> Coro::generateReadAll(QPointer<QIODevice> dev){
    auto awaitable = detail::await_impl(dev.data(), &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QIODevice::aboutToClose, [awaitable](){
        awaitable->close();
    });
    Generator<QByteArray> gen([awaitable, dev](auto resolve){
        if(!dev){
            return ;
        }
        while(1){
            if(!(dev->isReadable() && dev->isOpen())){
                return;//dev关闭
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

Coro::Result<void> Coro::awaitForReadyRead(QPointer<QIODevice> dev, int msecs)
{
    auto awaitable = detail::await_single_impl(dev.data(), &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QIODevice::aboutToClose, [awaitable](){
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

Coro::Result<void> Coro::awaitForBytesWritten(QPointer<QIODevice> dev, int msecs)
{
    auto awaitable = detail::await_single_impl(dev.data(), &QIODevice::bytesWritten, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QIODevice::aboutToClose, [awaitable](){
        awaitable->close();
    });
    if(msecs>0){
        return awaitable->await_for(std::chrono::milliseconds(msecs));
    }else{
        return awaitable->await();
    }
}

Coro::Result<QByteArray> Coro::awaitForReadAll(QPointer<QIODevice> dev, int msecs){
    if(!dev){
        return std::make_error_code(std::errc::no_message);
    }
    auto awaitable = detail::await_single_impl(dev.data(), &QIODevice::readyRead, static_cast<std::tuple<>*>(nullptr));
    QObject::connect(dev, &QIODevice::aboutToClose, [awaitable](){
        awaitable->close();
    });
    auto res = awaitable->await_for(std::chrono::milliseconds(msecs));
    if(res.has_value()){
        if(dev){
            return dev->readAll();
        }
    }
    return std::make_error_code(std::errc::no_message);
}
