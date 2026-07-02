#include "fiberthreadblock.h"
#include <boost/fiber/scheduler.hpp>
#include "fiberscheduler.h"

Coro::FiberThreadBlock::FiberThreadBlock()
{

}

void Coro::FiberThreadBlock::schedulerWait()
{
    std::unique_lock<boost::fibers::mutex> lck(mtx_);
    cond_.wait(lck, [this](){return is_closed_;});
    lck.unlock();
}

void Coro::FiberThreadBlock::wait()
{
    this->schedulerWait();
}

void Coro::FiberThreadBlock::close()
{    

    if(true == is_closed_){
        return;
    }
    std::lock_guard<boost::fibers::mutex> lck(mtx_);
    is_closed_ = true;
    cond_.notify_all();
}

bool Coro::FiberThreadBlock::isClosed()
{
    return is_closed_;
}
