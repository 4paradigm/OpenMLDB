//
// file_appender_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "replica/mem_log.h"

#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <stdio.h>
#include "proto/tablet.pb.h"
#include "logging.h"
#include "thread_pool.h"
#include <sofa/pbrpc/pbrpc.h>

#include "timer.h"

using ::baidu::common::ThreadPool;
using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::INFO;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace replica {

class MemLogTest : public ::testing::Test {

public:
    MemLogTest() {}

    ~MemLogTest() {}
};

TEST_F(MemLogTest, AppendLog) {
    MemLog* log = new MemLog();
    log->Ref();
    LogEntry* entry1 = new LogEntry();
    LogEntry* entry2 = new LogEntry();
    uint64_t offset = 1;
    log->AppendLog(entry1, offset);
    log->AppendLog(entry2, 2);
    ASSERT_EQ(1, log->GetHeadOffset());
    ASSERT_EQ(2, log->GetTailOffset());
    log->UnRef();
}

}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

