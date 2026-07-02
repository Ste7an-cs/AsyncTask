# 软件使用说明
## 1.软件简述
### 1.1基本信息
Async是一个用户态协程的异步框架，提供一套基于`boost::fiber`异步编程接口。

用户可以将函数封装为可被暂停和回复的协程函数。当协程被暂停，线程不会被阻塞，调用方可继续执行其他的代码，协程将等待条件满足后被唤醒。这使得异步的代码可以像同步代码一样编写，从而更易被阅读和理解。

### 1.2 开发环境
gcc>=9.0,需支持C++17的特性。

若使用Qt，Qt>=5.12

boost>=1.89.0

### 1.3 工程结构
```
---AsyncTask        项目工程文件夹
|---3dParty         第三方库文件夹
|---coro            协程框架代码
|---doc             相关文档
|---skill           当前项目的Skill
|---example         使用例程
|---test            测试用例
|   AsyncTask.pri   工程配置文件
|   ReadMe.md       使用说明
```
### 1.4 第三方依赖清单
| 库名称 | 组件名 |
| ---- | ---- |
| Boost | fiber |
| Boost | context |
| Boost | thread |
| Boost | chrono |
## 2 安装说明
### 2.1 安装环境
gcc g++ qmake boost Qt

### 2.2 安装步骤
1. 进入3dParty/boost路径，打开终端；
2. 使用管理员权限编译并安装boost，可执行命令` sudo bash install.sh`,boost库将安装至`/usr/local/boost`路径下；
3. 使用库时，可在工程配置文件中添加`include($$PWD/../AsyncTask.pri)`，将`AsyncTask.pri`加入工程中（`include(xxx/AsyncTask.pri)`需根据实际路径修改）

AsyncTask会根据项目配置自动使能部分功能，例如在Qt项目中才会启动Qt相关的协程接口和支持Qt时间循环的调度器。在使能Qt network时，才会启用network相关的协程接口。
## 3 使用说明
### 3.1 命名格式
### 3.2 运行示例
### 3.3 使用详解
### 3.4 常见问题与排除方法


