#ifndef COROTCPSERVER_HPP
#define COROTCPSERVER_HPP

///
/// \file corotcpserver.hpp
/// \brief QTcpServer 的协程包装器：coro(QTcpServer*).nextConnection() 返回 Awaitable<QTcpSocket*>。
///

#include <memory>
#include <QObject>
#include <QPointer>
#include <QTcpServer>
#include <QTcpSocket>

#include "awaitable.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

///
/// \brief The CoroTcpServer class QTcpServer 的协程包装器。
///
class CoroTcpServer{
    QPointer<QTcpServer> srv_;
public:
    explicit CoroTcpServer(QTcpServer* s): srv_(s){}

    /// 等待新连接，返回新到的 QTcpSocket*；可 generate 持续接收
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

inline CoroTcpServer coro(QTcpServer* srv){ return CoroTcpServer(srv); }

}

#endif // COROTCPSERVER_HPP
