/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/file_util.h"

#include <algorithm>
#include <filesystem>

#include "absl/cleanup/cleanup.h"
#include "gtest/gtest.h"

namespace openmldb {
namespace base {
class FileUtilTest : public ::testing::Test {
 public:
    FileUtilTest() {}
    ~FileUtilTest() {}
};

TEST_F(FileUtilTest, GetChildFileName) {
    ASSERT_TRUE(MkdirRecur("/tmp/gtest/test/"));
    FILE* f = fopen("/tmp/gtest/test0.txt", "w");
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/test1.txt", "w");
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/test/test2.txt", "w");
    if (f != nullptr) fclose(f);
    std::vector<std::string> file_vec;
    GetChildFileName(std::string("/tmp/gtest"), file_vec);
    ASSERT_EQ(file_vec.size(), 3u);
    sort(file_vec.begin(), file_vec.end());
    ASSERT_EQ("/tmp/gtest/test", file_vec[0]);
    ASSERT_EQ("/tmp/gtest/test0.txt", file_vec[1]);
    ASSERT_EQ("/tmp/gtest/test1.txt", file_vec[2]);
}

TEST_F(FileUtilTest, IsFolder) {
    MkdirRecur("/tmp/gtest/test/testdir/");
    FILE* f = fopen("/tmp/gtest/test0.txt", "w");
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/test/test1.txt", "w");
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/test/testdir/test2.txt", "w");
    if (f != nullptr) fclose(f);
    ASSERT_TRUE(IsFolder(std::string("/tmp/gtest")));
    ASSERT_FALSE(IsFolder(std::string("/tmp/gtest/test0.txt")));
}

TEST_F(FileUtilTest, RemoveDirRecursive) {
    ASSERT_TRUE(MkdirRecur("/tmp/gtest/test/"));
    FILE* f = fopen("/tmp/gtest/test0.txt", "w");
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/test1.txt", "w");
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/test/test2.txt", "w");
    if (f != nullptr) fclose(f);
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
    FILE* f = fopen("/tmp/gtest/testsize/test0.txt", "w");
    for (int i = 0; i < 1000; ++i) {
        fputc('6', f);
    }
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/testsize/test/test1.txt", "w");
    for (int i = 0; i < 2000; ++i) {
        fputc('6', f);
    }
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/testsize/test/test2.txt", "w");
    for (int i = 0; i < 5000; ++i) {
        fputc('6', f);
    }
    if (f != nullptr) fclose(f);
    f = fopen("/tmp/gtest/testsize/test3.txt", "w");
    for (int i = 0; i < 100; ++i) {
        fputc('6', f);
    }
    if (f != nullptr) fclose(f);

    struct stat stat_buf;
    lstat("/tmp/gtest/testsize/test/", &stat_buf);
    uint64_t size = 0;
    GetDirSizeRecur(std::string("/tmp/gtest/testsize"), size);
    ASSERT_EQ(size, static_cast<size_t>(2000 + 1000 + 5000 + 100 + stat_buf.st_size));
    RemoveDirRecursive("/tmp/gtest");
}

TEST_F(FileUtilTest, CopyFile) {
    std::string src_file_dir = "/tmp/gtest/testsize/test/";
    ASSERT_TRUE(MkdirRecur(src_file_dir));
    std::string src_file = src_file_dir + "test0.txt";
    FILE* f = fopen(src_file.c_str(), "w+");
    for (int i = 0; i < 1000; ++i) {
        fputc('6', f);
    }
    if (f != nullptr) fclose(f);
    std::string des_file = "/tmp/gtest/testsize/test/test1.txt";
    ASSERT_TRUE(CopyFile(src_file, des_file));
    uint64_t src_size = 0;
    uint64_t des_size = 0;
    GetFileSize(src_file, src_size);
    GetFileSize(des_file, des_size);
    ASSERT_EQ(src_size, des_size);
    RemoveDirRecursive("/tmp/gtest");
}

TEST_F(FileUtilTest, FindFiles) {
    std::filesystem::path tmp_path = std::filesystem::temp_directory_path() / "file_util_test";
    absl::Cleanup clean = [&tmp_path]() { std::filesystem::remove_all(tmp_path); };

    ASSERT_TRUE(MkdirRecur(tmp_path.string()));
    auto file0 = (tmp_path / "test0.csv");
    FILE* f = fopen(file0.c_str(), "w");
    if (f != nullptr) fclose(f);
    auto file1 = (tmp_path / "test1.csv");
    f = fopen(file1.c_str(), "w");
    if (f != nullptr) fclose(f);
    auto file2 = (tmp_path / "test2.csv");
    f = fopen(file2.c_str(), "w");
    if (f != nullptr) fclose(f);

    {
        auto res = FindFiles<false>(tmp_path.string(), "test*");
        ASSERT_EQ(res.size(), 3);
        ASSERT_EQ(res[0], file0);
        ASSERT_EQ(res[1], file1);
        ASSERT_EQ(res[2], file2);
    }

    {
        auto res = FindFiles<false>(tmp_path.string(), "*");
        ASSERT_EQ(res.size(), 3);
        ASSERT_EQ(res[0], file0);
        ASSERT_EQ(res[1], file1);
        ASSERT_EQ(res[2], file2);
    }

    {
        auto res = FindFiles<false>(tmp_path.string(), "*.csv");
        ASSERT_EQ(res.size(), 3);
        ASSERT_EQ(res[0], file0);
        ASSERT_EQ(res[1], file1);
        ASSERT_EQ(res[2], file2);
    }

    {
        auto res = FindFiles<false>(tmp_path.string(), "test1.csv");
        ASSERT_EQ(res.size(), 1);
        ASSERT_EQ(res[0], file1);
    }

    {
        auto res = FindFiles<false>(tmp_path.string(), "test1.csv*");
        ASSERT_EQ(res.size(), 1);
        ASSERT_EQ(res[0], file1);
    }

    {
        auto res = FindFiles<false>(tmp_path.string(), "");
        ASSERT_EQ(res.size(), 0);
    }
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::openmldb::base::SetLogLevel(INFO);
    ::openmldb::base::RemoveDirRecursive("/tmp/gtest");
    int ret = RUN_ALL_TESTS();
    ::openmldb::base::RemoveDirRecursive("/tmp/gtest");
    return ret;
}
