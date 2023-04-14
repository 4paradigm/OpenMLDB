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

#include <gflags/gflags.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <unistd.h>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "gtest/gtest.h"
#include "proto/tablet.pb.h"
#include "storage/disk_table.h"
#include "storage/disk_table_snapshot.h"
#include "test/util.h"

DECLARE_string(hdd_root_path);

using ::openmldb::api::LogEntry;
namespace openmldb {
namespace storage {

class SnapshotTest : public ::testing::Test {
 public:
    SnapshotTest() {}
    ~SnapshotTest() {}
};

int GetManifest(const std::string file, ::openmldb::api::Manifest* manifest) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    google::protobuf::TextFormat::Parse(&fileInput, manifest);
    return 0;
}

TEST_F(SnapshotTest, MakeSnapshot) {
    DiskTableSnapshot snapshot(1, 1, FLAGS_hdd_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::string db_path = FLAGS_hdd_root_path + "/1_1";
    std::string snapshot_path = db_path + "/snapshot/";
    auto table = std::make_shared<DiskTable>(
            "t1", 1, 1, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime,
            ::openmldb::common::StorageMode::kHDD, db_path);
    table->Init();
    int key_num = 100;
    for (int i = 0; i < key_num; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = ::openmldb::test::EncodeKV(key, "value");
        table->Put(key, 1681440343000, value.c_str(), value.size());
    }
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, (int32_t)vec.size());
    std::string full_path = snapshot_path + "MANIFEST";
    auto ParseManifest = ([](const std::string& full_path){
        ::openmldb::api::Manifest manifest;
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
        return manifest;
    });
    auto manifest = ParseManifest(full_path);
    ASSERT_EQ(key_num, (int64_t)manifest.offset());
    ASSERT_EQ(key_num, (int64_t)manifest.count());
    ASSERT_EQ(0, (int64_t)manifest.term());
    ASSERT_TRUE(::openmldb::base::IsExists(snapshot_path + "/" + manifest.name()));
    for (int i = 0; i < key_num; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = ::openmldb::test::EncodeKV(key, "value");
        table->Put(key, 1681440345000, value.c_str(), value.size());
    }
    ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    manifest = ParseManifest(full_path);
    ASSERT_EQ(key_num * 2, (int64_t)manifest.offset());
    ASSERT_EQ(key_num * 2, (int64_t)manifest.count());
    ASSERT_EQ(0, (int64_t)manifest.term());
    ASSERT_TRUE(::openmldb::base::IsExists(snapshot_path + "/" + manifest.name()));
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::test::TempPath tmp_path;
    FLAGS_hdd_root_path = tmp_path.GetTempPath();
    return RUN_ALL_TESTS();
}
