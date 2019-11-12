//
// file_util_test.cc
// Copyright 2017 4paradigm.com

#include "base/file_util.h"
#include "gtest/gtest.h"
#include <algorithm>

namespace rtidb {
namespace base {
class FileUtilTest : public ::testing::Test {
public:
    FileUtilTest() {}
    ~FileUtilTest() {}
};

TEST_F(FileUtilTest, GetChildFileName) {
    ASSERT_TRUE(MkdirRecur("/tmp/gtest/test/"));
    FILE * f = fopen("/tmp/gtest/test0.txt", "w");
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/test1.txt", "w");
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/test/test2.txt", "w");
    if(f!=nullptr)
        fclose(f);
    std::vector<std::string> file_vec;
    GetChildFileName(std::string("/tmp/gtest"), file_vec);
    ASSERT_EQ(file_vec.size(), 3);
    sort(file_vec.begin(), file_vec.end());
    ASSERT_EQ("/tmp/gtest/test", file_vec[0]);
    ASSERT_EQ("/tmp/gtest/test0.txt", file_vec[1]);
    ASSERT_EQ("/tmp/gtest/test1.txt", file_vec[2]);
}

TEST_F(FileUtilTest, IsFolder) {
    MkdirRecur("/tmp/gtest/test/testdir");
    FILE * f = fopen("/tmp/gtest/test0.txt", "w");
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/test/test1.txt", "w");
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/test/testdir/test2.txt", "w");
    if(f!=nullptr)
        fclose(f);
    ASSERT_TRUE(IsFolder(std::string("/tmp/gtest")));
    ASSERT_FALSE(IsFolder(std::string("/tmp/gtest/test0.txt")));
}

TEST_F(FileUtilTest, RemoveDirRecursive) {
    ASSERT_TRUE(MkdirRecur("/tmp/gtest/test/"));
    FILE * f = fopen("/tmp/gtest/test0.txt", "w");
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/test1.txt", "w");
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/test/test2.txt", "w");
    if(f!=nullptr)
        fclose(f);
    RemoveDirRecursive("/tmp/gtest");
    ASSERT_FALSE(IsExists("/tmp/gtest"));
}

} // namespace base
} // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::rtidb::base::RemoveDirRecursive("/tmp/gtest");
    int ret = RUN_ALL_TESTS();
    ::rtidb::base::RemoveDirRecursive("/tmp/gtest");
    return ret;
}