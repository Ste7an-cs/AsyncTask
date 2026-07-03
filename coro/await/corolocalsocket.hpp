#ifndef COROLOCALSOCKET_HPP
#define COROLOCALSOCKET_HPP

///
/// \file corolocalsocket.hpp
/// \brief QLocalSocket 的协程包装器：coro(QLocalSocket*).connectToServer()/waitForConnected()。
///

#include <memory>
#include <QObject>
#include <QPointer>
#include <QIODevice>
#include <QLocalSocket>

#include "awaitable.hpp"
#include "detail/lifecycle.hpp"

namespace Coro {

///
/// \brief The CoroLocalSocket class QLocalSocket 的协程包装器。
///
class CoroLocalSocket{
    QPointer<QLocalSocket> local_;
public:
    explicit CoroLocalSocket(QLocalSocket* s): local_(s){}

    Awaitable<void> waitForConnected(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(local_) *c = QObject::connect(local_, &QLocalSocket::connected, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(local_.data(), ch, {c}));
        if(local_ && local_->state() == QLocalSocket::ConnectedState){ ch->push(1); }
        return a;
    }
    Awaitable<void> connectToServer(const QString& name,
                                    QIODevice::OpenMode mode = QIODevice::ReadWrite){
        auto a = waitForConnected();
        if(local_ && local_->state() != QLocalSocket::ConnectedState){
            local_->connectToServer(name, mode);
        }
        return a;
    }
};

inline CoroLocalSocket coro(QLocalSocket* local){ return CoroLocalSocket(local); }

}

#endif // COROLOCALSOCKET_HPP
