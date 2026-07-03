#ifndef COROIODEVICE_HPP
#define COROIODEVICE_HPP

///
/// \file coroiodevice.hpp
/// \brief QIODevice 的协程包装器：coro(QIODevice*).readAll()/waitForReadyRead()/... 返回 Awaitable。
///

#include <memory>
#include <QObject>
#include <QPointer>
#include <QIODevice>

#include "awaitable.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

///
/// \brief The CoroIODevice class QIODevice 的协程包装器（镜像原方法名）。
///
class CoroIODevice{
    QPointer<QIODevice> dev_;
public:
    explicit CoroIODevice(QIODevice* dev): dev_(dev){}

    /// 等待可读并返回读取的全部数据；可 generate 流式读取
    Awaitable<QByteArray> readAll(){
        Awaitable<QByteArray> a;
        auto ch = a.channel();
        QPointer<QIODevice> dev = dev_;
        auto c1 = std::make_shared<QMetaObject::Connection>();
        if(dev_){
            *c1 = QObject::connect(dev_, &QIODevice::readyRead, [ch, dev]{ if(dev) ch->push(dev->readAll()); });
            QObject::connect(dev_, &QIODevice::aboutToClose, [ch]{ ch->close(); });
            // 避免 check-then-wait 竞态：若数据已就绪(在连接建立前已到达)，立即投递
            if(dev_->bytesAvailable() > 0){ ch->push(dev_->readAll()); }
        }
        a.setOnClose(detail::bind_close(dev_.data(), ch, {c1}));
        return a;
    }
    /// 等待可读（不取数据）
    Awaitable<void> waitForReadyRead(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(dev_) *c = QObject::connect(dev_, &QIODevice::readyRead, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(dev_.data(), ch, {c}));
        return a;
    }
    /// 等待数据写出
    Awaitable<void> waitForBytesWritten(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(dev_) *c = QObject::connect(dev_, &QIODevice::bytesWritten, [ch](qint64){ ch->push(1); });
        a.setOnClose(detail::bind_close(dev_.data(), ch, {c}));
        return a;
    }
};

inline CoroIODevice coro(QIODevice* dev){ return CoroIODevice(dev); }

}

#endif // COROIODEVICE_HPP
