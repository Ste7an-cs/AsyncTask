#include "asyncdefine.h"

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
