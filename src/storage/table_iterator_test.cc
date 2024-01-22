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

#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "storage/mem_table.h"
#include "storage/disk_table.h"
#include "base/file_util.h"


DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

namespace openmldb {
namespace storage {

std::atomic<int32_t> counter(0);

inline uint32_t GenRand() {
    srand((unsigned)time(NULL));
    return rand() % 10000000 + 1;
}

void RemoveData(const std::string& path) {
    ::openmldb::base::RemoveDir(path + "/data");
    ::openmldb::base::RemoveDir(path);
    ::openmldb::base::RemoveDir(FLAGS_hdd_root_path);
    ::openmldb::base::RemoveDir(FLAGS_ssd_root_path);
}

Table* CreateTable(const std::string& name, uint32_t id, uint32_t pid, uint32_t seg_cnt,
                   const std::map<std::string, uint32_t>& mapping, uint64_t ttl, ::openmldb::type::TTLType ttl_type,
                   const std::string& table_path, ::openmldb::common::StorageMode storage_mode) {
    if (storage_mode == ::openmldb::common::StorageMode::kMemory) {
        return new MemTable(name, id, pid, seg_cnt, mapping, ttl, ttl_type);
    } else {
        return new DiskTable(name, id, pid, mapping, ttl, ttl_type, storage_mode, table_path);
    }
}

Table* CreateTable(const ::openmldb::api::TableMeta& table_meta, const std::string& table_path) {
    if (table_meta.storage_mode() == ::openmldb::common::StorageMode::kMemory) {
        return new MemTable(table_meta);
    } else {
        return new DiskTable(table_meta, table_path);
    }
}

inline std::string GetDBPath(const std::string& root_path, uint32_t tid, uint32_t pid) {
    return root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid);
}

using ::openmldb::codec::SchemaCodec;

class DiskTestEnvironment : public ::testing::Environment{
    virtual void TearDown() {
        for (int i = 1; i <= counter; i++) {
            std::string path = FLAGS_hdd_root_path + "/" + std::to_string(i) +  "_1";
            RemoveData(path);
        }
    }
};

class TableIteratorTest : public ::testing::TestWithParam<::openmldb::common::StorageMode> {
 public:
    TableIteratorTest() {}
    ~TableIteratorTest() {}
};


TEST_P(TableIteratorTest, smoketest) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime,
        table_path, storageMode);
    std::string key = "test";
    std::string value = "test";
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Init();
    table->Put(key, now, value.c_str(), value.size());
    ::hybridse::vm::WindowIterator* it = table->NewWindowIterator(0);
    it->SeekToFirst();
    ASSERT_TRUE(it != NULL);
    ASSERT_TRUE(it->Valid());
    ::hybridse::codec::Row row = it->GetKey();
    ASSERT_EQ(row.ToString(), key);
    std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
    wit->SeekToFirst();
    ASSERT_TRUE(wit->Valid());
    ::hybridse::codec::Row value2 = wit->GetValue();
    ASSERT_EQ(value2.ToString(), value);
    ASSERT_EQ(now, wit->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_P(TableIteratorTest, latest) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_storage_mode(storageMode);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts", ::openmldb::type::kBigInt);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts",
            ::openmldb::type::kLatestTime, 0, 3);

    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(i);
        for (int j = 0; j < 10; j++) {
            std::vector<std::string> row = {key , "mcc", std::to_string(now - j * (60 * 1000))};
            ::openmldb::api::PutRequest request;
            ::openmldb::api::Dimension* dim = request.add_dimensions();
            dim->set_idx(0);
            dim->set_key(key);
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            table->Put(0, value, request.dimensions());
        }
    }
    ::hybridse::vm::WindowIterator* it = table->NewWindowIterator(0);
    it->Seek("card2");
    ASSERT_TRUE(it != NULL);
    ASSERT_TRUE(it->Valid());
    ::hybridse::codec::Row row = it->GetKey();
    ASSERT_EQ(row.ToString(), "card2");
    std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
    wit->SeekToFirst();
    int cnt = 0;
    while (wit->Valid()) {
        cnt++;
        wit->Next();
    }
    ASSERT_EQ(3, cnt);
    wit->Seek(now - 4 * (60 * 1000));
    ASSERT_FALSE(wit->Valid());
    wit->Seek(now - 1000);
    cnt = 0;
    while (wit->Valid()) {
        cnt++;
        wit->Next();
    }
    ASSERT_EQ(2, cnt);
}

TEST_P(TableIteratorTest, smoketest2) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_seg_cnt(1);
    table_meta.set_storage_mode(storageMode);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts", ::openmldb::type::kBigInt);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts",
            ::openmldb::type::kLatestTime, 0, 0);

    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(i);
        for (int j = 0; j < 10; j++) {
            std::vector<std::string> row = {key , std::to_string(now - j * (60 * 1000))};
            ::openmldb::api::PutRequest request;
            ::openmldb::api::Dimension* dim = request.add_dimensions();
            dim->set_idx(0);
            dim->set_key(key);
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            table->Put(0, value, request.dimensions());
        }
    }
    ::hybridse::vm::WindowIterator* it = table->NewWindowIterator(0);
    it->SeekToFirst();
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(it->GetKey().ToString(), "card" + std::to_string(i));

        std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
        wit->SeekToFirst();
        for (int j = 0; j < 10; j++) {
            ASSERT_TRUE(wit->Valid());
            ASSERT_EQ(wit->GetKey(), now - j * (60 * 1000));

            std::string key = "card" + std::to_string(i);
            std::vector<std::string> row = {key, std::to_string(now - j * (60 * 1000))};
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));

            ASSERT_EQ(wit->GetValue().ToString(), value);
            wit->Next();
        }
        ASSERT_FALSE(wit->Valid());

        wit->Seek(now - 5 * (60 * 1000));
        ASSERT_TRUE(wit->Valid());
        ASSERT_EQ(wit->GetKey(), now - 5 * (60 * 1000));
        std::string key = "card" + std::to_string(i);
        std::vector<std::string> row = {key, std::to_string(now - 5 * (60 * 1000))};
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        ASSERT_EQ(wit->GetValue().ToString(), value);

        wit->Next();
        ASSERT_TRUE(wit->Valid());
        ASSERT_EQ(wit->GetKey(), now - 6 * (60 * 1000));
        key = "card" + std::to_string(i);
        row = {key, std::to_string(now - 6 * (60 * 1000))};
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        ASSERT_EQ(wit->GetValue().ToString(), value);

        it->Next();
    }
    ASSERT_FALSE(it->Valid());

    it = table->NewWindowIterator(0);
    it->Seek("card2");
    {
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(it->GetKey().ToString(), "card" + std::to_string(2));

        std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
        wit->SeekToFirst();
        for (int j = 0; j < 10; j++) {
            ASSERT_TRUE(wit->Valid());
            ASSERT_EQ(wit->GetKey(), now - j * (60 * 1000));

            std::string key = "card" + std::to_string(2);
            std::vector<std::string> row = {key, std::to_string(now - j * (60 * 1000))};
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));

            ASSERT_EQ(wit->GetValue().ToString(), value);
            wit->Next();
        }
        ASSERT_FALSE(wit->Valid());

        wit->Seek(now - 5 * (60 * 1000));
        ASSERT_TRUE(wit->Valid());
        ASSERT_EQ(wit->GetKey(), now - 5 * (60 * 1000));
        std::string key = "card" + std::to_string(2);
        std::vector<std::string> row = {key, std::to_string(now - 5 * (60 * 1000))};
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        ASSERT_EQ(wit->GetValue().ToString(), value);

        wit->Next();
        ASSERT_TRUE(wit->Valid());
        ASSERT_EQ(wit->GetKey(), now - 6 * (60 * 1000));
        key = "card" + std::to_string(2);
        row = {key, std::to_string(now - 6 * (60 * 1000))};
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        ASSERT_EQ(wit->GetValue().ToString(), value);
    }
    it->Next();
    {
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(it->GetKey().ToString(), "card" + std::to_string(3));

        std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
        wit->SeekToFirst();
        for (int j = 0; j < 10; j++) {
            ASSERT_TRUE(wit->Valid());
            ASSERT_EQ(wit->GetKey(), now - j * (60 * 1000));

            std::string key = "card" + std::to_string(3);
            std::vector<std::string> row = {key, std::to_string(now - j * (60 * 1000))};
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));

            ASSERT_EQ(wit->GetValue().ToString(), value);
            wit->Next();
        }
        ASSERT_FALSE(wit->Valid());

        wit->Seek(now - 5 * (60 * 1000));
        ASSERT_TRUE(wit->Valid());
        ASSERT_EQ(wit->GetKey(), now - 5 * (60 * 1000));
        std::string key = "card" + std::to_string(3);
        std::vector<std::string> row = {key, std::to_string(now - 5 * (60 * 1000))};
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        ASSERT_EQ(wit->GetValue().ToString(), value);

        wit->Next();
        ASSERT_TRUE(wit->Valid());
        ASSERT_EQ(wit->GetKey(), now - 6 * (60 * 1000));
        key = "card" + std::to_string(3);
        row = {key, std::to_string(now - 6 * (60 * 1000))};
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        ASSERT_EQ(wit->GetValue().ToString(), value);
    }
}

std::unique_ptr<::hybridse::vm::RowIterator> getRowIterator(Table* table) {
    ::hybridse::vm::WindowIterator* it = table->NewWindowIterator(0);
    it->SeekToFirst();
    return it->GetValue();
}

TEST_P(TableIteratorTest, releaseKeyIterator) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_seg_cnt(1);
    table_meta.set_storage_mode(storageMode);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts", ::openmldb::type::kBigInt);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts",
            ::openmldb::type::kLatestTime, 0, 0);

    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 5; i++) {
        std::string key = "card" + std::to_string(i);
        for (int j = 0; j < 10; j++) {
            std::vector<std::string> row = {key , std::to_string(now - j * (60 * 1000))};
            ::openmldb::api::PutRequest request;
            ::openmldb::api::Dimension* dim = request.add_dimensions();
            dim->set_idx(0);
            dim->set_key(key);
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            table->Put(0, value, request.dimensions());
        }
    }

    std::unique_ptr<::hybridse::vm::RowIterator> wit = getRowIterator(table);
    wit->SeekToFirst();
    ASSERT_TRUE(wit->Valid());
    ASSERT_EQ(wit->GetKey(), now);
}

TEST_P(TableIteratorTest, SeekNonExistent) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_seg_cnt(1);
    table_meta.set_storage_mode(storageMode);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    codec::SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts", ::openmldb::type::kBigInt);
    codec::SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts",
            ::openmldb::type::kLatestTime, 0, 0);

    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 5; i += 2) {
        std::string key = "card" + std::to_string(i);
        for (int j = 0; j < 10; j += 2) {
            std::vector<std::string> row = {key , std::to_string(now - j * (60 * 1000))};
            ::openmldb::api::PutRequest request;
            ::openmldb::api::Dimension* dim = request.add_dimensions();
            dim->set_idx(0);
            dim->set_key(key);
            std::string value;
            ASSERT_EQ(0, codec.EncodeRow(row, &value));
            table->Put(0, value, request.dimensions());
        }
    }

    ::hybridse::vm::WindowIterator* it = table->NewWindowIterator(0);
    it->SeekToFirst();

    std::unique_ptr<::hybridse::vm::RowIterator> wit = it->GetValue();
    wit->Seek(now - 3 * (60 * 1000));
    ASSERT_EQ(240000, now - wit->GetKey());

    wit = it->GetValue();
    wit->Seek(now - 50 * (60 * 1000));
    ASSERT_FALSE(wit->Valid());

    wit = it->GetValue();
    wit->Seek(now + 50 * (60 * 1000));
    ASSERT_TRUE(wit->Valid());
    ASSERT_EQ(0, now - wit->GetKey());
}

INSTANTIATE_TEST_SUITE_P(TestMemAndHDD, TableIteratorTest,
                        ::testing::Values(::openmldb::common::kMemory, ::openmldb::common::kHDD));

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::AddGlobalTestEnvironment(new ::openmldb::storage::DiskTestEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    ::openmldb::base::SetLogLevel(INFO);
    FLAGS_hdd_root_path = "/tmp/" + std::to_string(::openmldb::storage::GenRand());
    FLAGS_ssd_root_path = "/tmp/" + std::to_string(::openmldb::storage::GenRand());
    return RUN_ALL_TESTS();
}
