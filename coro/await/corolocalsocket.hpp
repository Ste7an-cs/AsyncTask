#ifndef COROLOCALSOCKET_HPP
#define COROLOCALSOCKET_HPP

/**
 * @file corolocalsocket.hpp
 * @brief QLocalSocket 的协程包装器：coro(QLocalSocket*).connectToServer()/waitForConnected()。
 */

#include <memory>
#include <QObject>
#include <QPointer>
#include <QIODevice>
#include <QLocalSocket>

#include "awaitable.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

/**
 * @brief QLocalSocket 的协程包装器（方法名镜像原 Qt API）
 */
class CoroLocalSocket{
    QPointer<QLocalSocket> local_;///< 被包装的本地套接字（弱引用）
public:
    /**
     * @brief 构造
     * @param s 被包装的本地套接字
     */
    explicit CoroLocalSocket(QLocalSocket* s): local_(s){}

    /**
     * @brief 等待连接成功（若已连接则立即就绪）
     * @return 连接成功触发一次的 Awaitable<void>
     */
    Awaitable<void> waitForConnected(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(local_) *c = QObject::connect(local_, &QLocalSocket::connected, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(local_.data(), ch, {c}));
        if(local_ && local_->state() == QLocalSocket::ConnectedState){ ch->push(1); }
        return a;
    }
    /**
     * @brief 发起连接到指定服务并等待连接成功
     * @param name 服务名
     * @param mode 打开模式
     * @return 连接成功触发一次的 Awaitable<void>
     */
    Awaitable<void> connectToServer(const QString& name,
                                    QIODevice::OpenMode mode = QIODevice::ReadWrite){
        auto a = waitForConnected();
        if(local_ && local_->state() != QLocalSocket::ConnectedState){
            local_->connectToServer(name, mode);
        }
        return a;
    }
};

/**
 * @brief 构造 QLocalSocket 的协程包装器
 * @param local 被包装的本地套接字
 * @return CoroLocalSocket 包装器
 */
inline CoroLocalSocket coro(QLocalSocket* local){ return CoroLocalSocket(local); }

}

#endif // COROLOCALSOCKET_HPP
