#ifndef SOCKETAWAIT_HPP
#define SOCKETAWAIT_HPP

#include <QAbstractSocket>
#include <QLocalSocket>
#include <QTcpServer>
#include "iodeviceawait.hpp"

namespace Coro {

//// Socket


///
/// \brief await 单次等待QAbstractSocket可读，若可读，返回读取的全部值
/// \param dev
/// \return
///
Result<QByteArray> awaitReadAll(QAbstractSocket* dev);

///
/// \brief await_for 单次等待QAbstractSocket可读，最多等待时间为timeout，若可读，返回读取的全部值
/// \param dev
/// \param timeout  超时时间
/// \return
///
Result<QByteArray> awaitForReadAll(QAbstractSocket* dev, int msecs=30000);
///
/// \brief generateReadAll QIODevice生成器，持续等待数据可读
/// \param dev
///
Generator<QByteArray> generateReadAll(QAbstractSocket* dev);

///
/// \brief awaitForReadyRead 协程等待封装的QAbstractSocket::waitForReadyRead(int msecs)
///                         等待至最长时间，直到有数据可读
/// \param dev              QIODevice设备
/// \param msecs            超时时间，如果为-1，则一直等待
/// \return
///
Result<void> awaitForReadyRead(QAbstractSocket* dev, int msecs=30000);

///
/// \brief awaitForBytesWritten 协程等待封装的QIODevice::waitForBytesWritten(int msecs)
///                         等待至最长时间，直到有数据写入
/// \param dev              QIODevice设备
/// \param msecs            超时时间，如果为-1，则一直等待
/// \return
///
Result<void> awaitForBytesWritten(QAbstractSocket* dev, int msecs=30000);

///
/// \brief awaitForConnected 等待socket连接。
/// \param socket           socket指针
/// \param msecs            超时时间，如果为-1，则一直等待
/// \return
///
Result<void> awaitForConnected(QAbstractSocket* socket, int msecs=30000);
///
/// \brief awaitForConnected 等待socket断开连接。
/// \param socket           socket指针
/// \param msecs            超时时间，如果为-1，则一直等待
/// \return
///
Result<void> awaitForDisconnected(QAbstractSocket* socket, int msecs=30000);

///
/// \brief awaitConnectToHost   对socket 调用ConnectToHost，并等待连接成功
/// \param socket               socket指针
/// \param hostName             目标地址
/// \param port                 目标端口
/// \param msecs                超时时间，如果为-1，则一直等待
/// \param openMode
/// \param protocol
/// \return
///
Result<void> awaitConnectToHost(QAbstractSocket* socket, const QString &hostName, quint16 port, int msecs=30000, QIODevice::OpenMode openMode =  QIODevice::ReadWrite, QAbstractSocket::NetworkLayerProtocol protocol =  QAbstractSocket::AnyIPProtocol);

///
/// \brief awaitConnectToHost   对socket 调用ConnectToHost，并等待连接成功
/// \param socket               socket指针
/// \param address              目标地址
/// \param port                 目标端口
/// \param msecs                超时时间，如果为-1，则一直等待
/// \param openMode
/// \return
///
Result<void> awaitConnectToHost(QAbstractSocket* socket, const QHostAddress &address, quint16 port, int msecs=30000, QIODevice::OpenMode openMode = QAbstractSocket::ReadWrite);

/// Local Socket

///
/// \brief awaitConnectToServer 协程等待QLocalSocket与Server连接成功
/// \param local                本地socket指针
/// \param msecs                超时时间，如果为-1，则一直等待
/// \param openMode
/// \return
///
Result<void> awaitConnectToServer(QPointer<QLocalSocket> local, int msecs=30000,  QIODevice::OpenMode openMode = QIODevice::ReadWrite);

///
/// \brief awaitConnectToServer 协程等待QLocalSocket与Server连接成功
/// \param local                本地socket指针
/// \param name                 本地socket名称
/// \param msecs                超时时间，如果为-1，则一直等待
/// \param openMode
/// \return
///
Result<void> awaitConnectToServer(QPointer<QLocalSocket> local, const QString &name, int msecs=30000,  QIODevice::OpenMode openMode = QIODevice::ReadWrite);


/// Tcp Server

///
/// \brief awaitForNewConnection    协程等待有新连接可用
/// \param server                   server指针
/// \param msec                     超时时间，如果为-1，则一直等待
/// \return
///
Result<QTcpSocket*> awaitForNewConnection(QPointer<QTcpServer> server, int msec=30000);

///
/// \brief generateNewConnection    QTcpServer生成器，持续等待有新连接可用
/// \param server
///
Generator<QTcpSocket*> generateNewConnection(QPointer<QTcpServer> server);
}

#endif // SOCKETAWAIT_HPP
