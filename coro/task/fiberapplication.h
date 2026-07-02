#ifndef FIBERAPPLICATION_H
#define FIBERAPPLICATION_H
#include <QObject>
#include <QCoreApplication>
#include "executor/scheduler/fiberthreadblock.h"
namespace Coro {

class FiberApplication : QObject{
    Q_OBJECT
public:
    static FiberApplication* instance();
    int exec();
    void quit();
protected:
    FiberApplication();
    Coro::FiberThreadBlock block;
};

void installFiberApplication();
int exec();
void quit();

}

#endif // FIBERAPPLICATION_H
