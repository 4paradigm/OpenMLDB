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
    MkdirRecur("/tmp/gtest/test/testdir/");
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

TEST_F(FileUtilTest, ParseFileNameFromPath) {
    ASSERT_EQ("test.txt", ParseFileNameFromPath("test.txt"));
    ASSERT_EQ("test", ParseFileNameFromPath("/home/rtidb/test"));
    ASSERT_EQ("", ParseFileNameFromPath("/home/rtidb/"));
    ASSERT_EQ("", ParseFileNameFromPath("/"));
}

TEST_F(FileUtilTest, GetDirSizeRecur) {
    ASSERT_TRUE(MkdirRecur("/tmp/gtest/testsize/test/"));
    FILE * f = fopen("/tmp/gtest/testsize/test0.txt", "w");
    for (int i=0;i<1000;++i) {
        fputc('6', f);
    }
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/testsize/test/test1.txt", "w");
    for (int i=0;i<2000;++i) {
        fputc('6', f);
    }
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/testsize/test/test2.txt", "w");
    for (int i=0;i<5000;++i) {
        fputc('6', f);
    }
    if(f!=nullptr)
        fclose(f);
    f = fopen("/tmp/gtest/testsize/test3.txt", "w");
    for (int i=0;i<100;++i) {
        fputc('6', f);
    }
    if(f!=nullptr)
        fclose(f);
    uint64_t size = 0;
    GetDirSizeRecur(std::string("/tmp/gtest/testsize"), size);
    ASSERT_EQ(size, 2000+1000+5000+100+40);
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
