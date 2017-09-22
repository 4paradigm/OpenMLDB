//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "gtest/gtest.h"
#include "logging.h"
#include "base/file_util.h"
#include "storage/snapshot.h"
#include "storage/table.h"
#include "storage/ticket.h"
#include "proto/tablet.pb.h"
#include "log/log_writer.h"
#include "gflags/gflags.h"
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <gflags/gflags.h>

DECLARE_string(db_root_path);

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace log {
class WritableFile;
}
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
    FLAGS_db_root_path = "/tmp/db/" + GenRand();
    std::string snapshot_dir = FLAGS_db_root_path + "/1_1/snapshots";
    ::rtidb::base::MkdirRecur(snapshot_dir);
    {
        std::string snapshot1 = "1.sdb";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        ::rtidb::api::LogEntry entry;
        entry.set_pk("test0");
        entry.set_ts(9527);
        entry.set_value("test1");
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        
        entry.set_pk("test0");
        entry.set_ts(9528);
        entry.set_value("test2");
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());

    }

    {
        std::string snapshot1 = "2.sb";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        ::rtidb::api::LogEntry entry;
        entry.set_pk("test1");
        entry.set_ts(9527);
        entry.set_value("test1");
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry.set_pk("test1");
        entry.set_ts(9528);
        entry.set_value("test2");
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
    }

    {
        std::string snapshot1 = "3.sdb";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        ::rtidb::api::LogEntry entry;
        entry.set_pk("test3");
        entry.set_ts(9527);
        entry.set_value("test1");
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry.set_pk("test3");
        entry.set_ts(9528);
        entry.set_value("test2");
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
    }

    std::vector<std::string> fakes;
    Table* table = new Table("test", 1, 1, 8, 0, true, fakes, true);
    table->Init();
    Snapshot snapshot(1, 1);
    ASSERT_TRUE(snapshot.Init());
    snapshot.Recover(table);
    table->UnRef();
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    return RUN_ALL_TESTS();
}


