//
// auto_gen.h
// Copyright (C) 2017 4paradigm.com
// Author wangbao
// Date 2020-3-20
//

#include "base/random.h"
#include <unistd.h>
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)


#pragma once
namespace rtidb {
namespace base{

class AutoGen {
public:
    AutoGen(): rand_(0xdeadbeef) {}
    AutoGen(const AutoGen&) = delete;
    AutoGen& operator=(const AutoGen&) = delete;
    ~AutoGen() {}

    int64_t Next() {
        int64_t ts = ::baidu::common::timer::get_micros();
        int64_t tid = gettid();
        int64_t rd = rand_.Next();
        return ((ts | 0x7FFF) << 8) + (((tid | 0xFFFF)) << 8)  + (rd | 0xFFFF) ;
    }

private:
    ::rtidb::base::Random rand_;
};

}
}
