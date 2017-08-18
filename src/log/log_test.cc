//
// log_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/atomic.hpp>
#include "base/file_util.h"
#include <iostream>

using ::rtidb::base::Slice;
using ::rtidb::base::Status;

namespace rtidb {
namespace log {

class LogWRTest : public ::testing::Test {

public:
    LogWRTest() {}
    ~LogWRTest () {}
};

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}

TEST_F(LogWRTest, TestWriteAndRead) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::rtidb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(wf);
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    Reader reader(rf, NULL, false, 0);
    Status status = writer.AddRecord("hello");
    ASSERT_TRUE(status.ok());
    std::string scratch;
    Slice value;
    status = reader.ReadRecord(&value, &scratch);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("hello", value.ToString());
    uint64_t last_record_offset = reader.LastRecordOffset();
    std::cout << "last record offset " << last_record_offset << std::endl;
    status = writer.AddRecord("hello1");
    ASSERT_TRUE(status.ok());
    Reader reader2(rf, NULL, false, last_record_offset);
    std::string scratch2;
    Slice value2;
    status = reader2.ReadRecord(&value2, &scratch2);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("hello1", value2.ToString());
}

TEST_F(LogWRTest, TestLogEntry) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::rtidb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(wf);

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
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    std::string scratch2;
    Reader reader(rf, NULL, false, 0);
    {
        Slice value2;
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ::rtidb::api::LogEntry entry2;
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527, entry2.ts());
    }
    status = writer.AddRecord(sval);
    ASSERT_TRUE(status.ok());
    {
        Slice value2;
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ::rtidb::api::LogEntry entry2;
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527, entry2.ts());
        status = reader.ReadRecord(&value2, &scratch2);
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.IsWaitRecord());
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.IsWaitRecord());
    }
}


}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




