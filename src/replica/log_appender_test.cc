//
// log_appender_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "replica/log_appender.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include "logging.h"

namespace rtidb {
namespace replica {

class LogAppenderTest : public ::testing::Test {

public:
    LogAppenderTest() {}

    ~LogAppenderTest() {}
};


TEST_F(LogAppenderTest, Append) {
    std::string name = "segment.log";
    std::string folder = "/tmp/";
    std::string fullname = folder + name;
    remove(fullname.c_str());
    LogAppender appender(name, folder, 5);
    bool ok = appender.Init();
    ASSERT_TRUE(ok);
    const char* hello = "hello";
    uint32_t size = appender.Append(hello, 5);
    ASSERT_EQ(5, size);
    ok = appender.Flush();
    ASSERT_TRUE(ok);
    appender.Close();
    FILE* fd = fopen(fullname.c_str(), "r");
    int fd_no_ = fileno(fd);
    struct stat sb;
    fstat(fd_no_, &sb);
    ASSERT_EQ(5, sb.st_size);
    fclose(fd);
}

}
}

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



