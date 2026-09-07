#ifndef COROIODEVICE_HPP
#define COROIODEVICE_HPP

/**
 * @file coroiodevice.hpp
 * @brief QIODevice 的协程包装器：coro(QIODevice*).readAll()/waitForReadyRead()/... 返回 Awaitable。
 */

#include <memory>
#include <utility>
#include <QObject>
#include <QPointer>
#include <QIODevice>
#include <QThread>
#include <QCoreApplication>

#include "awaitable.hpp"
#include "detail/autodisconnect.hpp"

namespace Coro {

/**
 * @brief QIODevice 的非拥有协程包装器（方法名镜像原 Qt API）
 * @details 不取得传入 Qt 对象的所有权。所有触及设备的操作都在其所属线程直接执行，
 *          或投递到该线程执行。公开工厂返回 shared Awaitable，Qt 槽只捕获
 *          channel 等数据载体，**绝不捕获 Awaitable**；整组订阅经
 *          AutoDisconnect::untilExpired 锚在返回句柄上，句柄 close/析构即整组断开。
 *          因此反复创建等待器不会残留订阅、不会互相截取数据。
 *
 *          设备 aboutToClose、源对象销毁或应用结束时，已返回的 awaitable 以默认
 *          no_message 正常关闭并同时断开全部订阅；消费者仍会先取完已排队的值。
 * @code
 * // 由 coro(dev) 产生；方法名与 Qt 同名，无需记新名字
 * QFile file("data.bin");
 * file.open(QIODevice::ReadOnly);
 * QByteArray data = Coro::await(Coro::coro(&file).readAll()).value_or(QByteArray());
 * @endcode
 */
class CoroIODevice{
    QPointer<QIODevice> dev_;///< 被包装的设备（弱引用）

    /**
     * @brief 在设备所属线程执行或排队执行函数。
     * @details 建立等待器时的同步探测（drain / check）必须发生在设备线程，否则会跨线程
     *          触碰 QIODevice 的读缓冲。信号槽本身无需投递：无 context 对象的 connect
     *          为直连，槽天然在发送者线程执行。
     * @tparam Function 可用 QIODevice* 调用的函数类型。
     * @param device 非拥有的设备守卫指针。
     * @param function 要在对象线程运行的函数。
     * @return 已执行或成功投递时为 true；设备已销毁或投递失败时为 false。
     * @code
     * // 内部使用：保证所有 QIODevice 操作都发生在设备所属线程
     * onDeviceThread(device, [channel](QIODevice* d){
     *     if(d->bytesAvailable() > 0) channel->push(d->readAll());
     * });
     * @endcode
     */
    template<typename Function>
    static bool onDeviceThread(QPointer<QIODevice> device, Function function){
        if(!device) return false;
        if(device->thread() == QThread::currentThread()){
            function(device.data());
            return true;
        }
        return QMetaObject::invokeMethod(
            device.data(),
            [device, function = std::move(function)]() mutable {
                if(device) function(device.data());
            },
            Qt::QueuedConnection);
    }

    /**
     * @brief 一次性等待某信号的内部辅助函数。
     * @details 目标信号到达即 resolve、关闭 awaitable 并整组断开；check 提供"已就绪则
     *          立即成功"的同步快路径，避免 check-then-wait 竞态。
     * @tparam Signal 可传给 QObject::connect() 的目标信号类型。
     * @tparam Check 可用 QIODevice* 调用并返回完成状态的检查函数类型。
     * @param signal 到达时完成等待的目标信号。
     * @param check 建立订阅后用于同步 fast path 的完成状态检查函数。
     * @return 完成一次即关闭的共享 awaitable。
     * @code
     * // 内部使用：waitForReadyRead 即由它组合而成
     * return waitForSignal(&QIODevice::readyRead,
     *                      [](QIODevice* d){ return d->bytesAvailable() > 0; });
     * @endcode
     */
    template<typename Signal, typename Check>
    std::shared_ptr<Awaitable<void>> waitForSignal(Signal signal, Check check){
        auto awaitable = std::make_shared<Awaitable<void>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QIODevice> device = dev_;

        auto succeed = [channel, scope]{
            channel->push(1);          // resolve void
            channel->close();
            scope->disconnectAll();
        };
        auto closeStop = [channel, scope]{
            channel->close();
            scope->disconnectAll();
        };
        if(device){
            scope->on(device.data(), signal, [succeed](auto...){ succeed(); });
            scope->on(device.data(), &QIODevice::aboutToClose, closeStop);
            scope->on(device.data(), &QObject::destroyed, closeStop);
            if(auto app = QCoreApplication::instance()){
                scope->on(app, &QObject::destroyed, closeStop);
                scope->on(app, &QCoreApplication::aboutToQuit, closeStop);
            }
        }
        scope->untilExpired(awaitable);
        if(!onDeviceThread(device, [channel, succeed, check = std::move(check)](
                                       QIODevice* current) mutable {
            if(channel->is_closed()) return;
            if(check(current)) succeed();
        })){
            closeStop();
        }
        return awaitable;
    }

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
     * @return 每个值都是非空的当前可读字节块，直到设备 aboutToClose/销毁为止的共享 Awaitable。
     * @note await_for() 超时不停止读取流，也不取消 Qt 信号订阅；订阅随句柄析构一并取消。
     * @code
     * // 取一次当前可读数据
     * auto chunk = Coro::await(Coro::coro(dev).readAll());
     *
     * // 流式：设备关闭前持续产出数据块
     * for(const QByteArray& c : Coro::generate(Coro::coro(dev).readAll())) append(c);
     * @endcode
     */
    std::shared_ptr<Awaitable<QByteArray>> readAll(){
        auto awaitable = std::make_shared<Awaitable<QByteArray>>();
        auto channel = awaitable->channel();
        auto scope = detail::make_auto_disconnect();
        QPointer<QIODevice> device = dev_;

        // 业务槽只捕获 channel（+scope 用于终止时整组断开），绝不捕获 awaitable。
        auto drain = [channel](QIODevice* current){
            if(current->bytesAvailable() > 0){
                const QByteArray bytes = current->readAll();
                if(!bytes.isEmpty()) channel->push(bytes);   // push 锁内自判 closed
            }
        };
        auto closeStop = [channel, scope]{
            channel->close();
            scope->disconnectAll();
        };
        if(device){
            scope->on(device.data(), &QIODevice::readyRead, [channel, device, drain]{
                if(device && !channel->is_closed()) drain(device.data());
            });
            scope->on(device.data(), &QIODevice::aboutToClose,
                      [channel, device, scope, drain]{
                if(channel->is_closed()) return;
                if(device) drain(device.data());   // 关闭前把剩余数据交给消费者
                channel->close();
                scope->disconnectAll();
            });
            scope->on(device.data(), &QObject::destroyed, closeStop);
            if(auto app = QCoreApplication::instance()){
                scope->on(app, &QObject::destroyed, closeStop);
                scope->on(app, &QCoreApplication::aboutToQuit, closeStop);
            }
        }
        scope->untilExpired(awaitable);   // 句柄 close/析构 → 整组断开
        // 避免 check-then-wait 竞态：若数据已就绪(在连接建立前已到达)，立即投递
        if(!onDeviceThread(device, [channel, drain](QIODevice* current){
            if(channel->is_closed()) return;
            drain(current);
        })){
            closeStop();
        }
        return awaitable;
    }
    /**
     * @brief 等待可读（不取数据）
     * @return 就绪一次即关闭并断开全部订阅的共享 Awaitable<void>；
     *         建立时已有可读数据则立即成功。
     * @code
     * // 等到有数据可读后自行决定怎么读
     * if(Coro::await(Coro::coro(dev).waitForReadyRead())){
     *     QByteArray head = dev->read(4);
     * }
     * @endcode
     */
    std::shared_ptr<Awaitable<void>> waitForReadyRead(){
        return waitForSignal(&QIODevice::readyRead, [](QIODevice* device){
            return device->bytesAvailable() > 0;
        });
    }
    /**
     * @brief 等待数据写出
     * @return 写出一次即关闭并断开全部订阅的共享 Awaitable<void>
     * @code
     * // 先建等待器再 write，避免 bytesWritten 早于建立而被漏掉
     * auto written = Coro::coro(dev).waitForBytesWritten();
     * dev->write("ping");
     * Coro::await(written);
     * @endcode
     */
    std::shared_ptr<Awaitable<void>> waitForBytesWritten(){
        return waitForSignal(&QIODevice::bytesWritten, [](QIODevice*){
            return false;
        });
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
