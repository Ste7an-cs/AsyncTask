#include "asyncdefine.h"
#include <atomic>
#include <algorithm>

/**
 * @brief 当前协程休眠指定毫秒数
 * @param mescs 休眠的毫秒数
 */
void Coro::msleep(unsigned long mescs){
    boost::this_fiber::sleep_for(std::chrono::milliseconds(mescs));
}

/**
 * @brief 当前协程休眠指定秒数
 * @param secs 休眠的秒数
 */
void Coro::sleep(unsigned long secs){
    boost::this_fiber::sleep_for(std::chrono::seconds(secs));
}

namespace {
std::atomic_int g_max_event_block_ms{ 10 };
}

/**
 * @brief 设置事件阻塞的安全上限
 */
void Coro::setMaxEventBlockMs(int ms){
    g_max_event_block_ms.store(std::max(1, ms), std::memory_order_release);
}

/**
 * @brief 读取事件阻塞的安全上限
 */
int Coro::maxEventBlockMs(void){
    return g_max_event_block_ms.load(std::memory_order_acquire);
}
