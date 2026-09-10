QT += core network
QT -= gui

CONFIG += console c++17
CONFIG -= app_bundle

TEMPLATE = app

QMAKE_CXXFLAGS += -std=c++17

include($$PWD/../../AsyncTask.pri)

SOURCES += main.cpp
