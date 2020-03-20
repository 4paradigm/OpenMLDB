//
// id_generator.h  
// Copyright (C) 2017 4paradigm.com
// Author wangbao
// Date 2020-3-20
//

#include "base/random.h"
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>


#pragma once
namespace rtidb {
namespace base{

class IdGenerator {
public:
    IdGenerator(): rand_(0xdeadbeef) {}
    IdGenerator(const IdGenerator&) = delete;
    IdGenerator& operator=(const IdGenerator&) = delete;
    ~IdGenerator() {}

    int64_t Next() {
        int64_t ts = ::baidu::common::timer::get_micros();
        pthread_t tid = pthread_self();
        uint32_t rd = rand_.Next();
        int64_t res = (((ts << 8) | (tid | 0xFF)) << 4) | (rd | 0xFFF);
        return llabs(res);
    }

private:
    ::rtidb::base::Random rand_;
};

}
}
