#ifndef CORO_HPP
#define CORO_HPP

///
/// \file coro.hpp
/// \brief 统一的协程等待工厂。参考 qcoro：单一重载入口 coro(...) 返回 Awaitable<T>
///     （信号/future）或镜像原 Qt API 方法名的包装器（IODevice/Socket/Server），
///     包装器方法返回 Awaitable<T>。消费统一用自由函数 await(a)/await(a,timeout)/generate(a)。
///
///     本文件是唯一集中依赖 Qt/QMetaObject 的 await 层文件；Awaitable 本体与 Qt 解耦。
///

#include <future>
#include <thread>
#include <memory>
#include <vector>
#include <functional>

#include <QObject>
#include <QCoreApplication>
#include <QPointer>
#include <QIODevice>
#include <QAbstractSocket>
#include <QTcpServer>
#include <QTcpSocket>
#include <QLocalSocket>
#include <QHostAddress>

#include "awaitable.hpp"
#include "generator.hpp"
#include "detail/signalpack.hpp"

#ifdef ASYNC_HAS_QTCORE
#include <QFuture>
#include <QFutureWatcher>
#endif

namespace Coro {

namespace detail {

///
/// \brief bind_close 为一个 channel 绑定统一的生命周期：
///     来源对象析构、或应用 aboutToQuit 时，关闭 channel（使等待/生成器收敛）。
///     并把主连接与这两条清理连接汇总为一个断连回调（供 Awaitable::setOnClose）。
/// \return 断开所有连接的清理函数
///
template<class ChPtr>
std::function<void()> bind_close(QObject* sender, ChPtr ch,
                                 std::vector<std::shared_ptr<QMetaObject::Connection>> conns){
    if(sender){
        auto cd = std::make_shared<QMetaObject::Connection>();
        *cd = QObject::connect(sender, &QObject::destroyed, [ch]{ ch->close(); });
        conns.push_back(cd);
    }
    if(QCoreApplication::instance()){
        auto cq = std::make_shared<QMetaObject::Connection>();
        *cq = QObject::connect(QCoreApplication::instance(), &QCoreApplication::aboutToQuit, [ch]{ ch->close(); });
        conns.push_back(cq);
    }
    return [conns]{ for(auto& c : conns) QObject::disconnect(*c); };
}

/// 信号 -> Awaitable：连接信号，把参数打包 push 进 channel；析构/来源销毁/退出时收尾。
template<class Obj, class Sig, class... A>
auto coro_signal_impl(Obj* obj, Sig sig, std::tuple<A...>*){
    using PR = pack_result<A...>;
    using R  = typename PR::type;
    Awaitable<R> a;
    auto ch   = a.channel();
    auto conn = std::make_shared<QMetaObject::Connection>();
    *conn = QObject::connect(obj, sig, [ch](A... args){
        if constexpr (std::is_void_v<R>){
            (void)sizeof...(args);
            ch->push(1);
        }else{
            ch->push(PR::make(args...));
        }
    });
    a.setOnClose(bind_close(obj, ch, {conn}));
    return a;
}

/// 信号 -> Awaitable（指定所需类型 Want...，取信号前 K 个参数构造 R）
template<class Obj, class Sig, class... Want, class... A>
auto coro_signal_typed_impl(Obj* obj, Sig sig, std::tuple<Want...>*, std::tuple<A...>*){
    constexpr std::size_t K = sizeof...(Want);
    constexpr std::size_t N = sizeof...(A);
    static_assert(K <= N, "Coro::coro<Types...>(obj, signal): 指定的形参个数超过信号参数个数");
    using R = typename pack_result<std::decay_t<Want>...>::type;
    Awaitable<R> a;
    auto ch   = a.channel();
    auto conn = std::make_shared<QMetaObject::Connection>();
    if constexpr (std::is_void_v<R>){
        *conn = QObject::connect(obj, sig, [ch](A...){ ch->push(1); });
    }else{
        *conn = QObject::connect(obj, sig, [ch](A... args){
            std::tuple<std::decay_t<A>...> all{args...};
            R res = make_typed<R, Want...>(all, std::make_index_sequence<K>{});
            ch->push(res);
        });
    }
    a.setOnClose(bind_close(obj, ch, {conn}));
    return a;
}

} // detail

// ============================ 信号 ============================

///
/// \brief coro 等待一个 Qt 信号，返回 Awaitable。
///     信号无参 -> Awaitable<void>；单参 -> Awaitable<Value>；多参 -> Awaitable<tuple<...>>。
///     用 await(coro(obj,sig)) 取一次，或 generate(coro(obj,sig)) 流式迭代。
///
template<class Obj, class Sig>
auto coro(Obj* obj, Sig sig){
    return detail::coro_signal_impl(
        obj, sig, static_cast<typename detail::signal_args<Sig>::type*>(nullptr));
}

///
/// \brief coro 等待一个 Qt 信号并指定所需类型，如 coro<int>(obj, &Obj::sig2)。
///
template<class W0, class... Wr, class Obj, class Sig>
auto coro(Obj* obj, Sig sig){
    return detail::coro_signal_typed_impl(
        obj, sig,
        static_cast<std::tuple<W0, Wr...>*>(nullptr),
        static_cast<typename detail::signal_args<Sig>::type*>(nullptr));
}

// ============================ future ============================

///
/// \brief coro 等待一个 std::future，返回 Awaitable<T>。
///
template<class T>
Awaitable<T> coro(std::future<T>&& fu){
    Awaitable<T> a;
    auto ch = a.channel();
    auto f  = std::make_shared<std::future<T>>(std::move(fu));
    std::thread([ch, f]{ ch->push(f->get()); }).detach();
    return a;
}

#ifdef ASYNC_HAS_QTCORE
///
/// \brief coro 等待一个 QFuture，返回 Awaitable<T>。
///
template<class T>
Awaitable<T> coro(const QFuture<T>& f){
    Awaitable<T> a;
    auto ch      = a.channel();
    auto watcher = std::make_shared<QFutureWatcher<T>>();
    QObject::connect(watcher.get(), &QFutureWatcherBase::finished, [ch, watcher]{
        ch->push(watcher->result());
    });
    watcher->setFuture(f);
    if(f.isFinished()){
        ch->push(f.result());
    }
    a.setOnClose([watcher]{ /* watcher 随 Awaitable 生命周期一并释放 */ });
    return a;
}
#endif

// ============================ QIODevice ============================

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

// ============================ QAbstractSocket ============================

///
/// \brief The CoroAbstractSocket class QAbstractSocket 的协程包装器。
///
class CoroAbstractSocket{
    QPointer<QAbstractSocket> sock_;
public:
    explicit CoroAbstractSocket(QAbstractSocket* s): sock_(s){}

    /// 等待可读并返回读取的全部数据；socket 断开时结束（用于 generate 流式读取）
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
    Awaitable<void> waitForReadyRead(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(sock_) *c = QObject::connect(sock_, &QIODevice::readyRead, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(sock_.data(), ch, {c}));
        return a;
    }
    /// 等待连接成功（若已连接则立即就绪）
    Awaitable<void> waitForConnected(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(sock_) *c = QObject::connect(sock_, &QAbstractSocket::connected, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(sock_.data(), ch, {c}));
        if(sock_ && sock_->state() == QAbstractSocket::ConnectedState){ ch->push(1); }
        return a;
    }
    /// 等待断开连接（若已断开则立即就绪）
    Awaitable<void> waitForDisconnected(){
        Awaitable<void> a;
        auto ch = a.channel();
        auto c  = std::make_shared<QMetaObject::Connection>();
        if(sock_) *c = QObject::connect(sock_, &QAbstractSocket::disconnected, [ch]{ ch->push(1); });
        a.setOnClose(detail::bind_close(sock_.data(), ch, {c}));
        if(sock_ && sock_->state() == QAbstractSocket::UnconnectedState){ ch->push(1); }
        return a;
    }
    /// 发起连接并等待连接成功
    Awaitable<void> connectToHost(const QString& host, quint16 port,
                                  QIODevice::OpenMode mode = QIODevice::ReadWrite){
        auto a = waitForConnected();
        if(sock_ && sock_->state() != QAbstractSocket::ConnectedState){
            sock_->connectToHost(host, port, mode);
        }
        return a;
    }
};

// ============================ QTcpServer ============================

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

// ============================ QLocalSocket ============================

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

// ============================ coro(obj) 入口重载 ============================

inline CoroIODevice       coro(QIODevice* dev)        { return CoroIODevice(dev); }
inline CoroAbstractSocket coro(QAbstractSocket* sock) { return CoroAbstractSocket(sock); }
inline CoroTcpServer      coro(QTcpServer* srv)       { return CoroTcpServer(srv); }
inline CoroLocalSocket    coro(QLocalSocket* local)   { return CoroLocalSocket(local); }

}

#endif // CORO_HPP
