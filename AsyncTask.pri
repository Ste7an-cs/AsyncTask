
!exists( /usr/local/include/boost/){
    error("require boost libraries, run ( sudo bash 3dParty/install.sh) to install boost")
} else {
    message("found boost libraries")
}
unix:!macx: LIBS += -L/usr/local/lib -lboost_fiber
unix:!macx: LIBS += -L/usr/local/lib -lboost_context
unix:!macx: LIBS += -L/usr/local/lib -lboost_thread
unix:!macx: LIBS += -L/usr/local/lib -lboost_chrono
unix:!macx: PRE_TARGETDEPS += /usr/local/lib/libboost_fiber.a
unix:!macx: PRE_TARGETDEPS += /usr/local/lib/libboost_context.a
unix:!macx: PRE_TARGETDEPS += /usr/local/lib/libboost_thread.a
unix:!macx: PRE_TARGETDEPS += /usr/local/lib/libboost_chrono.a
QT += network
INCLUDEPATH += $$PWD/coro/
contains(QT, core){
DEFINES += ASYNC_HAS_QTCORE
HEADERS += \
    $$PWD/coro/executor/scheduler/qtfiberscheduler.h \
    $$PWD/coro/executor/qtfiberthread.h \
    $$PWD/coro/executor/scheduler/qtlocalfiberscheduler.h \
    $$PWD/coro/task/fiberapplication.h \
    $$PWD/coro/await/coro.hpp \
    $$PWD/coro/await/corosignal.hpp \
    $$PWD/coro/await/corofuture.hpp \
    $$PWD/coro/await/coroiodevice.hpp \
    $$PWD/coro/await/corosocket.hpp \
    $$PWD/coro/await/corotcpserver.hpp \
    $$PWD/coro/await/corolocalsocket.hpp \
    $$PWD/coro/await/corolocalserver.hpp \
    $$PWD/coro/await/detail/signalpack.hpp \
    $$PWD/coro/await/detail/lifecycle.hpp \
    $$PWD/coro/await/detail/socketawait.hpp \
    $$PWD/coro/await/detail/socketerror.hpp
SOURCES += \
    $$PWD/coro/executor/scheduler/qtfiberscheduler.cpp \
    $$PWD/coro/executor/qtfiberthread.cpp \
    $$PWD/coro/executor/scheduler/qtlocalfiberscheduler.cpp \
    $$PWD/coro/task/fiberapplication.cpp
}
contains(QT, network){
QT += network
}
contains(QT, widgets){

}

HEADERS += \
    $$PWD/coro/all.hpp \
    $$PWD/coro/await/awaitable.hpp \
    $$PWD/coro/await/generator.hpp \
    $$PWD/coro/detail/fiberchannel.hpp \
    $$PWD/coro/executor/fiberpool.h \
    $$PWD/coro/executor/scheduler/fiberproperty.h \
    $$PWD/coro/executor/scheduler/fiberscheduler.h \
    $$PWD/coro/executor/scheduler/fibertaskqueue.h \
    $$PWD/coro/executor/scheduler/fiberthreadblock.h \
    $$PWD/coro/detail/asyncdefine.h \
    $$PWD/coro/detail/result.hpp \
    $$PWD/coro/task/fibertask.h

SOURCES += \
    $$PWD/coro/executor/fiberpool.cpp \
    $$PWD/coro/executor/scheduler/fiberscheduler.cpp \
    $$PWD/coro/executor/scheduler/fibertaskqueue.cpp \
    $$PWD/coro/executor/scheduler/fiberthreadblock.cpp \
    $$PWD/coro/detail/asyncdefine.cpp \
    $$PWD/coro/task/fibertask.cpp
