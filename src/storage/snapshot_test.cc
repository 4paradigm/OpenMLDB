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

DECLARE_string(snapshot_root_path);

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


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    FLAGS_snapshot_root_path = "/tmp/" + ::rtidb::storage::GenRand();
    return RUN_ALL_TESTS();
}


