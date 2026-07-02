#include "asyncdefine.h"

void Coro::msleep(unsigned long mescs){
    boost::this_fiber::sleep_for(std::chrono::milliseconds(mescs));
}

void Coro::sleep(unsigned long secs){
    boost::this_fiber::sleep_for(std::chrono::seconds(secs));
}
