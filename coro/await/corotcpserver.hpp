#ifndef COROTCPSERVER_HPP
#define COROTCPSERVER_HPP

/**
 * @file corotcpserver.hpp
 * @brief QTcpServer 的协程包装器：coro(QTcpServer*).nextConnection() 返回 Awaitable<QTcpSocket*>。
 */

#include <memory>
#include <QObject>
#include <QPointer>
#include <QTcpServer>
#include <QTcpSocket>

#include "awaitable.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

/**
 * @brief QTcpServer 的协程包装器（方法名镜像原 Qt API）
 */
class CoroTcpServer{
    QPointer<QTcpServer> srv_;///< 被包装的服务器（弱引用）
public:
    /**
     * @brief 构造
     * @param s 被包装的服务器
     */
    explicit CoroTcpServer(QTcpServer* s): srv_(s){}

    /**
     * @brief 等待新连接，返回新到的 QTcpSocket*；可 generate 持续接收
     * @return 产出 QTcpSocket* 的 Awaitable
     */
    Awaitable<QTcpSocket*> nextConnection(){
        Awaitable<QTcpSocket*> a;
        auto ch = a.channel();
        QPointer<QTcpServer> srv = srv_;
        auto c1 = std::make_shared<QMetaObject::Connection>();
        if(srv_){
            *c1 = QObject::connect(srv_, &QTcpServer::newConnection, [ch, srv]{
                while(srv && srv->hasPendingConnections()){ ch->push(srv->nextPendingConnection()); }
            });
            while(srv_ && srv_->hasPendingConnections()){ ch->push(srv_->nextPendingConnection()); }
        }
        a.setOnClose(detail::bind_close(srv_.data(), ch, {c1}));
        return a;
    }
};

/**
 * @brief 构造 QTcpServer 的协程包装器
 * @param srv 被包装的服务器
 * @return CoroTcpServer 包装器
 */
inline CoroTcpServer coro(QTcpServer* srv){ return CoroTcpServer(srv); }

}

#endif // COROTCPSERVER_HPP
