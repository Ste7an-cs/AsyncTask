#ifndef QTFIBERTHREAD_H
#define QTFIBERTHREAD_H
#include <QThread>
#include "scheduler/fiberthreadblock.h"

namespace Coro {

///
/// \brief The QtFiberThread class
/// 支持fiber的QThread
///
class QtFiberThread : public QThread
{
public:
    QtFiberThread(QObject *parent = nullptr);
    ~QtFiberThread();
public slots:
    void quit();
protected:
    void run(void) override;
protected:
    FiberThreadBlock block;
};

}

#endif // QTFIBERTHREAD_H
