//
// file_appender_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "replica/log_replicator.h"

#include <sched.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <stdio.h>
#include "logging.h"
#include "thread_pool.h"

#include <stdlib.h>
#include <time.h>

using ::baidu::common::ThreadPool;

namespace rtidb {
namespace replica {

bool ReceiveEntry(const ::rtidb::api::LogEntry* entry) {
    if (entry != NULL) {
        return true;
    }
    return false;
}

class LogReplicatorTest : public ::testing::Test {

public:
    LogReplicatorTest() {}

    ~LogReplicatorTest() {}
};

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}

TEST_F(LogReplicatorTest, Init) {
    std::string folder = "/tmp/rtidb/" + GenRand() + "/";
    LogReplicator replicator(folder);
    bool ok = replicator.Init();
    ASSERT_TRUE(ok);
}

TEST_F(LogReplicatorTest, BenchMark) {
    std::string folder = "/tmp/rtidb/" + GenRand() + "/";
    LogReplicator replicator(folder);
    bool ok = replicator.Init();
    ASSERT_TRUE(ok);
}


}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



