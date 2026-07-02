#ifndef ASYNCDEFINE_H
#define ASYNCDEFINE_H
#include <boost/fiber/all.hpp>
#include <executor/scheduler/fiberproperty.h>

namespace Coro {

using boost::fibers::fiber;
using boost::fibers::future;
using boost::fibers::promise;
using boost::fibers::mutex;
using boost::fibers::condition_variable;
using boost::fibers::condition_variable_any;
using namespace boost::this_fiber;
void msleep(unsigned long mescs);

void sleep(unsigned long secs);

template< typename Fn >
fiber launch_properties( Fn && func, Coro::Priority pri, Coro::Affinity affine, std::string name="") {
    boost::fibers::fiber_properties * meta = new Coro::MetaContext(pri, affine, name, nullptr);
    boost::fibers::fiber fiber(meta, func);
    return fiber;
}

}

#endif // ASYNCDEFINE_H
