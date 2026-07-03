#ifndef CORO_LIFECYCLE_HPP
#define CORO_LIFECYCLE_HPP

#include <memory>
#include <vector>
#include <functional>
#include <QObject>
#include <QCoreApplication>

namespace Coro {
namespace detail {

///
/// \brief bind_close 为一个 channel 绑定统一的生命周期：
///     来源对象析构、或应用 aboutToQuit 时，关闭 channel（使等待/生成器收敛）。
///     并把主连接与这两条清理连接汇总为一个断连回调（供 Awaitable::setOnClose）。
/// \param sender 来源 QObject（可为空）
/// \param ch     目标 channel 的 shared_ptr
/// \param conns  已建立的主连接列表
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

} // detail
} // Coro

#endif // CORO_LIFECYCLE_HPP
