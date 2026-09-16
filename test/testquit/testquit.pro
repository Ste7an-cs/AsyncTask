QT += testlib
QT += gui widgets      # helper 的 window 档要真窗口，验证关窗退出

CONFIG += qt console warn_on depend_includepath testcase
CONFIG -= app_bundle

TEMPLATE = app
QMAKE_CXXFLAGS += -std=c++17 #开启c++17
QMAKE_CXXFLAGS += -fsanitize=address -fsanitize-recover=address -fno-omit-frame-pointer#开启anitize
QMAKE_CFLAGS += -fsanitize=address -fsanitize-recover=address -fno-omit-frame-pointer
QMAKE_LFLAGS += -fsanitize=address -static-libasan
include($$PWD/../../AsyncTask.pri)

SOURCES +=  tst_testquit.cpp
