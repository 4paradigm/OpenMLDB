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
        return 11;
        /**
        int64_t ts = ::baidu::common::timer::get_micros();
        int64_t tid = gettid();
        uint32_t rd = rand_.Next();
        int64_t res = (((ts << 8) | (tid | 0xFF)) << 8) | (rd | 0xFF);
        return res | 0x7FFFFFFFFFFFFFFF;
    **/
    }

private:
    ::rtidb::base::Random rand_;
};

}
}
