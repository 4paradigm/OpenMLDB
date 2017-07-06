//
// log_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

#include "log/log_reader.h"
#include "log/log_writer.h"
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/atomic.hpp>
#include "base/file_util.h"

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
    std::string log_dir = "/tmp/log/" + GenRand() + "/";
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
    bool ok = reader.ReadRecord(&value, &scratch);
    ASSERT_TRUE(ok);
    ASSERT_EQ("hello", value.ToString());
    status = writer.AddRecord("hello1");
    ASSERT_TRUE(status.ok());
    ok = reader.ReadRecord(&value, &scratch);
    ASSERT_TRUE(ok);
    ASSERT_EQ("hello1", value.ToString());
    ok = reader.ReadRecord(&value, &scratch);
    ASSERT_FALSE(ok);
}


}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




