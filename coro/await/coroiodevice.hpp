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
 * @code
 * // 由 coro(dev) 产生；方法名与 Qt 同名，无需记新名字
 * QFile file("data.bin");
 * file.open(QIODevice::ReadOnly);
 * QByteArray data = Coro::await(Coro::coro(&file).readAll()).value_or(QByteArray());
 * @endcode
 */
class CoroIODevice{
    QPointer<QIODevice> dev_;///< 被包装的设备（弱引用）
public:
    /**
     * @brief 构造
     * @param dev 被包装的 QIODevice
     * @code
     * // 一般用工厂函数 coro(dev) 而非直接构造
     * Coro::CoroIODevice w(&file);
     * auto data = Coro::await(w.readAll());
     * @endcode
     */
    explicit CoroIODevice(QIODevice* dev): dev_(dev){}

    /**
     * @brief 等待可读并返回读取的全部数据；可 generate 流式读取
     * @return 产出 QByteArray 的 Awaitable
     * @code
     * // 取一次当前可读数据
     * auto chunk = Coro::await(Coro::coro(dev).readAll());
     *
     * // 流式：设备关闭前持续产出数据块
     * for(const QByteArray& c : Coro::generate(Coro::coro(dev).readAll())) append(c);
     * @endcode
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
     * @code
     * // 等到有数据可读后自行决定怎么读
     * if(Coro::await(Coro::coro(dev).waitForReadyRead())){
     *     QByteArray head = dev->read(4);
     * }
     * @endcode
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
     * @code
     * // 先建等待器再 write，避免 bytesWritten 早于建立而被漏掉
     * auto written = Coro::coro(dev).waitForBytesWritten();
     * dev->write("ping");
     * Coro::await(written);
     * @endcode
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
 * @code
 * // 适用于任何 QIODevice：QFile、QBuffer、QSerialPort 等
 * QBuffer buf(&bytes);
 * buf.open(QIODevice::ReadWrite);
 * auto data = Coro::await(Coro::coro(&buf).readAll());
 * @endcode
 */
inline CoroIODevice coro(QIODevice* dev){ return CoroIODevice(dev); }

}

#endif // COROIODEVICE_HPP
