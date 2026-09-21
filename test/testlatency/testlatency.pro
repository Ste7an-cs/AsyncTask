QT += testlib network
QT -= gui

CONFIG += qt console warn_on depend_includepath testcase
CONFIG -= app_bundle

TEMPLATE = app
QMAKE_CXXFLAGS += -std=c++17
# 本测试度量时延，刻意不开 sanitizer：插桩会让往返时间失去参考价值。
include($$PWD/../../AsyncTask.pri)

SOURCES += tst_testlatency.cpp
