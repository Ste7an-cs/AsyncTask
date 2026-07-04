#ifndef COROIODEVICE_HPP
#define COROIODEVICE_HPP

/**
 * @file coroiodevice.hpp
 * @brief QIODevice 的协程包装器：coro(QIODevice*).readAll()/waitForReadyRead()/... 返回 Awaitable。
 */

#include <memory>
#include <QObject>
#include <QPointer>
#include <QIODevice>

#include "awaitable.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

/**
 * @brief QIODevice 的协程包装器（方法名镜像原 Qt API）
 */
class CoroIODevice{
    QPointer<QIODevice> dev_;///< 被包装的设备（弱引用）
public:
    /**
     * @brief 构造
     * @param dev 被包装的 QIODevice
     */
    explicit CoroIODevice(QIODevice* dev): dev_(dev){}

    /**
     * @brief 等待可读并返回读取的全部数据；可 generate 流式读取
     * @return 产出 QByteArray 的 Awaitable
     */
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
    /**
     * @brief 等待可读（不取数据）
     * @return 就绪时触发一次的 Awaitable<void>
     */
    Awaitable<void> waitForReadyRead(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(dev_) *c = QObject::connect(dev_, &QIODevice::readyRead, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(dev_.data(), ch, {c}));
        return a;
    }
    /**
     * @brief 等待数据写出
     * @return 写出时触发一次的 Awaitable<void>
     */
    Awaitable<void> waitForBytesWritten(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(dev_) *c = QObject::connect(dev_, &QIODevice::bytesWritten, [ch](qint64){ ch->push(1); });
        a.setOnClose(detail::bind_close(dev_.data(), ch, {c}));
        return a;
    }
};

/**
 * @brief 构造 QIODevice 的协程包装器
 * @param dev 被包装的 QIODevice
 * @return CoroIODevice 包装器
 */
inline CoroIODevice coro(QIODevice* dev){ return CoroIODevice(dev); }

}

#endif // COROIODEVICE_HPP
