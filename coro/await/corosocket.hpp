#ifndef COROSOCKET_HPP
#define COROSOCKET_HPP

/**
 * @file corosocket.hpp
 * @brief QAbstractSocket 的协程包装器。
 *
 * coro(QAbstractSocket*).readAll()/waitForConnected()/connectToHost()/... 返回 Awaitable。
 */

#include <memory>
#include <QObject>
#include <QPointer>
#include <QIODevice>
#include <QAbstractSocket>
#include <QHostAddress>

#include "awaitable.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

/**
 * @brief QAbstractSocket 的协程包装器（方法名镜像原 Qt API）
 */
class CoroAbstractSocket{
    QPointer<QAbstractSocket> sock_;///< 被包装的套接字（弱引用）
public:
    /**
     * @brief 构造
     * @param s 被包装的套接字
     */
    explicit CoroAbstractSocket(QAbstractSocket* s): sock_(s){}

    /**
     * @brief 等待可读并返回读取的全部数据；socket 断开时结束（用于 generate 流式读取）
     * @return 产出 QByteArray 的 Awaitable
     */
    Awaitable<QByteArray> readAll(){
        Awaitable<QByteArray> a;
        auto ch = a.channel();
        QPointer<QAbstractSocket> dev = sock_;
        auto c1 = std::make_shared<QMetaObject::Connection>();
        if(sock_){
            *c1 = QObject::connect(sock_, &QIODevice::readyRead, [ch, dev]{ if(dev) ch->push(dev->readAll()); });
            QObject::connect(sock_, &QAbstractSocket::disconnected, [ch]{ ch->close(); });
            // 避免 check-then-wait 竞态：若数据已就绪(在连接建立前已到达)，立即投递
            if(sock_->bytesAvailable() > 0){ ch->push(sock_->readAll()); }
        }
        a.setOnClose(detail::bind_close(sock_.data(), ch, {c1}));
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
        if(sock_) *c = QObject::connect(sock_, &QIODevice::readyRead, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(sock_.data(), ch, {c}));
        return a;
    }
    /**
     * @brief 等待连接成功（若已连接则立即就绪）
     * @return 连接成功触发一次的 Awaitable<void>
     */
    Awaitable<void> waitForConnected(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(sock_) *c = QObject::connect(sock_, &QAbstractSocket::connected, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(sock_.data(), ch, {c}));
        if(sock_ && sock_->state() == QAbstractSocket::ConnectedState){ ch->push(1); }
        return a;
    }
    /**
     * @brief 等待断开连接（若已断开则立即就绪）
     * @return 断开触发一次的 Awaitable<void>
     */
    Awaitable<void> waitForDisconnected(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(sock_) *c = QObject::connect(sock_, &QAbstractSocket::disconnected, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(sock_.data(), ch, {c}));
        if(sock_ && sock_->state() == QAbstractSocket::UnconnectedState){ ch->push(1); }
        return a;
    }
    /**
     * @brief 发起连接并等待连接成功
     * @param host 目标主机
     * @param port 目标端口
     * @param mode 打开模式
     * @return 连接成功触发一次的 Awaitable<void>
     */
    Awaitable<void> connectToHost(const QString& host, quint16 port,
                                  QIODevice::OpenMode mode = QIODevice::ReadWrite){
        auto a = waitForConnected();
        if(sock_ && sock_->state() != QAbstractSocket::ConnectedState){
            sock_->connectToHost(host, port, mode);
        }
        return a;
    }
};

/**
 * @brief 构造 QAbstractSocket 的协程包装器
 * @param sock 被包装的套接字
 * @return CoroAbstractSocket 包装器
 */
inline CoroAbstractSocket coro(QAbstractSocket* sock){ return CoroAbstractSocket(sock); }

}

#endif // COROSOCKET_HPP
