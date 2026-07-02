#ifndef IODEVICEAWAIT_HPP
#define IODEVICEAWAIT_HPP
#include <QIODevice>
#include <QPointer>
#include "signalawait.hpp"

namespace Coro {

///
/// \brief await 单次等待Iodevice可读，若可读，返回读取的全部值
/// \param dev
/// \return
///
Result<QByteArray> awaitReadAll(QPointer<QIODevice> dev);

///
/// \brief await_for 单次等待Iodevice可读，最多等待时间为timeout，若可读，返回读取的全部值
/// \param dev
/// \param timeout  超时时间
/// \return
///
Result<QByteArray> awaitForReadAll(QPointer<QIODevice> dev, int msecs=30000);
///
/// \brief generateReadAll QIODevice生成器，持续等待数据可读
/// \param dev
///
Generator<QByteArray> generateReadAll(QPointer<QIODevice> dev);

///
/// \brief awaitForReadyRead 协程等待封装的QIODevice::waitForReadyRead(int msecs)
///                         等待至最长时间，直到有数据可读
/// \param dev              QIODevice设备
/// \param msecs            超时时间，如果为-1，则一直等待
/// \return
///
Result<void> awaitForReadyRead(QPointer<QIODevice> dev, int msecs=30000);

///
/// \brief awaitForBytesWritten 协程等待封装的QIODevice::waitForBytesWritten(int msecs)
///                         等待至最长时间，直到有数据写入
/// \param dev              QIODevice设备
/// \param msecs            超时时间，如果为-1，则一直等待
/// \return
///
Result<void> awaitForBytesWritten(QPointer<QIODevice> dev, int msecs=30000);

}

#endif // IODEVICEAWAIT_HPP
