//
// glog_wapper_test.cc
// Copyright 2017 4paradigm.com

#include "base/glog_wapper.h"

#include <string>
#include <iostream>
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class GlogWapperTest : public ::testing::Test {
 public:
    GlogWapperTest() {}
    ~GlogWapperTest() {}
};

TEST_F(GlogWapperTest, Log) {
    char* path = "/tmp/hello.txt";
    ::google::InitGoogleLogging(path);
    PDLOG(INFO, "hello %d %f", 290, 3.1);
    std::string s = "word";
    PDLOG(INFO, "hello %s", s);
}


}  // namespace base
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
