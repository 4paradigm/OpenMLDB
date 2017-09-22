//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "gtest/gtest.h"
#include "logging.h"
#include "storage/snapshot.h"
#include "storage/table.h"
#include "storage/ticket.h"
#include "proto/tablet.pb.h"
#include "gflags/gflags.h"
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <time.h>
#include "base/strings.h"

DECLARE_string(db_root_path);

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace storage {

class SnapshotTest : public ::testing::Test {

public:
    SnapshotTest(){}
    ~SnapshotTest() {}
};

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}

TEST_F(SnapshotTest, Recover) {
}

TEST_F(SnapshotTest, RecordOffset) {
    Snapshot snapshot(1, 1);
    snapshot.Init();
    uint64_t offset = 1122;
    uint64_t key_count = 3000;
    ::rtidb::api::LogEntry entry;
    entry.set_log_index(offset);
    std::string buffer;
    entry.SerializeToString(&buffer);
    std::string now_time = ::rtidb::base::GetNowTime();
    std::string snapshot_name = now_time.substr(0, now_time.length() - 2) + ".sdb";
    int ret = snapshot.RecordOffset(buffer, snapshot_name, key_count);
    ASSERT_EQ(0, ret);
    ret = snapshot.RecordOffset(buffer, snapshot_name, key_count);
    ASSERT_EQ(0, ret);
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    FLAGS_db_root_path = "/tmp/" + ::rtidb::storage::GenRand();
    return RUN_ALL_TESTS();
}


