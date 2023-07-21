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
#include <atomic>
#include <iostream>
#include <utility>

#include "base/glog_wrapper.h"
#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "storage/mem_table.h"
#include "storage/ticket.h"
#include "test/util.h"
#include "storage/table.h"
#include "storage/disk_table.h"
#include "base/file_util.h"
#include "storage/iterator.h"

using ::openmldb::codec::SchemaCodec;

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_uint32(max_traverse_cnt);
DECLARE_int32(gc_safe_offset);

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
    return absl::StrCat(root_path, "/", tid, "_", pid);
}

using ::openmldb::codec::SchemaCodec;

class DiskTestEnvironment : public ::testing::Environment{
    virtual void TearDown() {
        for (int i = 1; i <= counter; i++) {
            std::string path = absl::StrCat(FLAGS_hdd_root_path , "/", i, "_1");
            RemoveData(path);
        }
    }
};

class TableTest : public ::testing::TestWithParam<::openmldb::common::StorageMode> {
 public:
    TableTest() {}
    ~TableTest() {}
};

TEST_P(TableTest, Put) {
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
    table->Init();
    ASSERT_TRUE(table->Put("test", 9537, "test", 4));
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    Ticket ticket;
    TableIterator* it = table->NewIterator("test", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9537, (int64_t)it->GetKey());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
}

TEST_P(TableTest, MultiDimissionDelete) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta* table_meta = new ::openmldb::api::TableMeta();
    table_meta->set_name("t0");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta->set_tid(id);
    table_meta->set_pid(1);
    table_meta->set_seg_cnt(1);
    table_meta->set_storage_mode(storageMode);
    ::openmldb::common::ColumnDesc* desc = table_meta->add_column_desc();
    desc->set_name("card");
    desc->set_data_type(::openmldb::type::kString);
    desc = table_meta->add_column_desc();
    desc->set_name("mcc");
    desc->set_data_type(::openmldb::type::kString);
    desc = table_meta->add_column_desc();
    desc->set_name("price");
    desc->set_data_type(::openmldb::type::kBigInt);
    auto column_key = table_meta->add_column_key();
    column_key->set_index_name("card");
    auto ttl = column_key->mutable_ttl();
    ttl->set_abs_ttl(5);
    column_key = table_meta->add_column_key();
    column_key->set_index_name("mcc");
    ttl = column_key->mutable_ttl();
    ttl->set_abs_ttl(5);
    Table* table = CreateTable(*table_meta, table_path);
    table->Init();
    table->DeleteIndex("mcc");
    table->SchedGc();
    table->SchedGc();
    table->SchedGc();
    delete table;
}

TEST_P(TableTest, MultiDimissionPut0) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime,
                                      table_path, storageMode);
    table->Init();
    ASSERT_EQ(3, (int64_t)table->GetIdxCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordCnt());
    Dimensions dimensions;
    ::openmldb::api::Dimension* d0 = dimensions.Add();
    d0->set_key("d0");
    d0->set_idx(0);

    ::openmldb::api::Dimension* d1 = dimensions.Add();
    d1->set_key("d1");
    d1->set_idx(1);

    ::openmldb::api::Dimension* d2 = dimensions.Add();
    d2->set_key("d2");
    d2->set_idx(2);
    auto meta = ::openmldb::test::GetTableMeta({"idx0", "idx1", "idx2"});
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::string result;
    sdk_codec.EncodeRow({"d0", "d1", "d2"}, &result);
    bool ok = table->Put(1, result, dimensions);
    ASSERT_TRUE(ok);
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    }
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    delete table;
}

TEST_P(TableTest, IsExpired) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    // table ttl is 1
    Table* table = CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kAbsoluteTime,
        table_path, storageMode);
    table->Init();
    uint64_t now_time = ::baidu::common::timer::get_micros() / 1000;
    ::openmldb::api::LogEntry entry;
    uint64_t ts_time = now_time;
    entry.set_ts(ts_time);
    ::openmldb::storage::TTLSt ttl(1 * 60 * 1000, 0, ::openmldb::storage::kAbsoluteTime);
    ASSERT_FALSE(entry.ts() < table->GetExpireTime(ttl));

    // ttl_offset_ is 60 * 1000
    ts_time = now_time - 4 * 60 * 1000;
    entry.set_ts(ts_time);
    ASSERT_TRUE(entry.ts() < table->GetExpireTime(ttl));
    delete table;
}

TEST_P(TableTest, Iterator) {
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
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk1", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);
    Ticket ticket;
    TableIterator* it = table->NewIterator("pk", ticket);

    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test0", value_str);
    it->Next();
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test", value2_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
}

TEST_P(TableTest, Iterator_GetSize) {
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
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);
    Ticket ticket;
    TableIterator* it = table->NewIterator("pk", ticket);
    int size = 0;
    it->SeekToFirst();
    while (it->Valid()) {
        it->Next();
        size++;
    }

    // disktable and memtable behave inconsistently when putting duplicate data
    // See issue #1240 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(3, size);
    } else {
        ASSERT_EQ(2, size);
    }
    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test0", value_str);
    it->Next();
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test", value2_str);
    it->Next();

    // disktable and memtable behave inconsistently when putting duplicate data
    // See issue #1240 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_TRUE(it->Valid());
    } else {
        ASSERT_FALSE(it->Valid());
    }
    delete it;
    delete table;
}

TEST_P(TableTest, SchedGcHead) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    // some functions with disktable mode in this test have not been implemented.
    // refer to issue #1238
    if (storageMode == openmldb::common::kHDD) {
        return;
    }
    std::map<std::string, uint32_t> mapping = { {"idx0", 0} };
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    std::unique_ptr<Table> table(CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kLatestTime,
        table_path, storageMode));
    table->Init();
    std::string value = ::openmldb::test::EncodeKV("test", "test1");
    table->Put("test", 2, value.data(), value.size());
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    value = ::openmldb::test::EncodeKV("test", "test2");
    table->Put("test", 1, value.data(), value.size());
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(1, (int64_t)table->GetRecordPkCnt());
    }
    table->SchedGc();
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(1);
        entry.set_value(::openmldb::test::EncodeKV("test", "test2"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(2);
        entry.set_value(::openmldb::test::EncodeKV("test", "test1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());
}

TEST_P(TableTest, SchedGcHead1) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    uint64_t keep_cnt = 500;
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 8, mapping, keep_cnt, ::openmldb::type::kLatestTime,
        table_path, storageMode);
    table->Init();
    uint64_t ts = 0;
    for (int i = 0; i < 10; i++) {
        int count = 5000;
        while (count) {
            ts++;
            table->Put("test", ts, "test1", 5);
            count--;
        }
        table->SchedGc();
        Ticket ticket;
        TableIterator* it = table->NewIterator("test", ticket);

        it->Seek(ts + 1);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt / 2);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt / 4);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt + 1);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt);
        ASSERT_FALSE(it->Valid());
        it->Seek(ts - keep_cnt - 1);
        ASSERT_FALSE(it->Valid());
        delete it;
    }
    table->SchedGc();
    delete table;
}

TEST_P(TableTest, SchedGc) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    // some functions with disktable mode in this test have not been implemented.
    // refer to issue #1238
    if (storageMode == openmldb::common::kHDD) {
        return;
    }
    std::map<std::string, uint32_t> mapping = { {"idx0", 0} };
    std::string table_path;
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    std::unique_ptr<Table> table(CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kLatestTime,
        table_path, storageMode));
    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", now, "tes2", 4);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 9527, "test", 4);
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(1, (int64_t)table->GetRecordPkCnt());
    }
    table->SchedGc();
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    }
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());

    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator("test", ticket));
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("tes2", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_P(TableTest, TableDataCnt) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    // some functions with disktable mode in this test have not been implemented.
    // refer to issue #1238
    if (storageMode == openmldb::common::kHDD) {
        return;
    }
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kAbsoluteTime,
        table_path, storageMode);
    table->Init();
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 0);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 2);
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ((int64_t)table->GetRecordIdxCnt(), 2);
    }
    table->SchedGc();
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(now - 1 * (60 * 1000) - 1);
        entry.set_value(::openmldb::test::EncodeKV("test", "test"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(now);
        entry.set_value(::openmldb::test::EncodeKV("test", "tes2"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 1);
    ASSERT_EQ((int64_t)table->GetRecordIdxCnt(), 1);
    delete table;
}

TEST_P(TableTest, TableUnref) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kAbsoluteTime,
        table_path, storageMode);
    table->Init();
    table->Put("test", 9527, "test", 4);
    delete table;
}

TEST_P(TableTest, TableIteratorRun) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 1, mapping, 0, ::openmldb::type::kAbsoluteTime,
        table_path, storageMode);
    table->Init();

    table->Put("pk", 9527, "test1", 5);
    table->Put("pk1", 9527, "test2", 5);
    table->Put("pk", 9528, "test3", 5);
    table->Put("pk1", 100, "test4", 5);
    table->Put("test", 20, "test5", 5);
    // Ticket ticket;
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9528, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(100, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it->GetKey());
    // it->Next();
    // ASSERT_FALSE(it->Valid());

    it->Seek("none", 11111);

    // disktable and memtable behave inconsistently when seeking with nonexistent pk
    // see issue #1241 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_TRUE(it->Valid());
    } else {
        ASSERT_FALSE(it->Valid());
    }

    it->Seek("test", 30);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it->GetKey());

    it->Seek("test", 20);
    ASSERT_TRUE(it->Valid());

    delete it;
    delete table;

    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table1 = CreateTable("tx_log", id, 1, 1, mapping, 2, ::openmldb::type::kLatestTime,
        table_path, storageMode);
    table1->Init();

    table1->Put("pk", 9527, "test1", 5);
    table1->Put("pk1", 9527, "test2", 5);
    table1->Put("pk", 9528, "test3", 5);
    table1->Put("pk1", 100, "test4", 5);
    table1->Put("test", 20, "test5", 5);

    TableIterator* it1 = table1->NewTraverseIterator(0);

    it1->Seek("pk", 9528);
    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("pk", it1->GetPK().c_str());
    ASSERT_EQ(9528, (int64_t)it1->GetKey());
    it1->Next();

    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("pk", it1->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it1->GetKey());
    it1->Next();
    ASSERT_STREQ("pk1", it1->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it1->GetKey());

    delete it1;
    delete table1;
}

TEST_P(TableTest, TableIteratorNoPk) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping = { {"idx0", 0} };
    std::string table_path;
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    std::unique_ptr<Table> table(CreateTable("tx_log", id, 1, 1, mapping, 0, ::openmldb::type::kAbsoluteTime,
        table_path, storageMode));
    table->Init();

    table->Put("pk10", 9527, "test10", 5);
    table->Put("pk8", 9526, "test8", 5);
    table->Put("pk6", 9525, "test6", 5);
    table->Put("pk4", 9524, "test4", 5);
    table->Put("pk2", 9523, "test2", 5);
    table->Put("pk0", 9522, "test0", 5);
    // Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewTraverseIterator(0));
    it->SeekToFirst();

    ASSERT_STREQ("pk0", it->GetPK().c_str());
    ASSERT_EQ(9522, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk10", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk2", it->GetPK().c_str());
    ASSERT_EQ(9523, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, (int64_t)it->GetKey());

    it.reset(table->NewTraverseIterator(0));
    it->Seek("pk4", 9526);
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, (int64_t)it->GetKey());

    api::LogEntry entry;
    auto dimension = entry.add_dimensions();
    dimension->set_key("pk4");
    dimension->set_idx(0);
    ASSERT_TRUE(table->Delete(entry));
    it.reset(table->NewTraverseIterator(0));
    it->Seek("pk4", 9526);
    ASSERT_TRUE(it->Valid());

    ASSERT_STREQ("pk6", it->GetPK().c_str());
    ASSERT_EQ(9525, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, (int64_t)it->GetKey());
}

TEST_P(TableTest, TableIteratorCount) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 1, mapping, 0, ::openmldb::type::kAbsoluteTime,
        table_path, storageMode);
    table->Init();
    for (int i = 0; i < 100000; i = i + 2) {
        std::string key = "pk" + std::to_string(i);
        std::string value = "test" + std::to_string(i);
        table->Put(key, 9527, value.c_str(), value.size());
        table->Put(key, 9528, value.c_str(), value.size());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(100000, count);
    delete it;
    it = table->NewTraverseIterator(0);
    it->Seek("pk500", 9528);
    ASSERT_STREQ("pk500", it->GetPK().c_str());
    ASSERT_EQ(9528, (int64_t)it->GetKey());
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(55552, count);

    delete it;

    TableIterator* cur_it = table->NewTraverseIterator(0);
    // disktable and memtable behave inconsistently when seeking with nonexistent pk
    // see issue #1241 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        for (int i = 0; i < 99999; i++) {
            std::string key = "pk" + std::to_string(i);
            cur_it->Seek(key, 9528);
            ASSERT_TRUE(cur_it->Valid());
        }
    } else {
        for (int i = 0; i < 100000; i = i + 2) {
            std::string key = "pk" + std::to_string(i);
            cur_it->Seek(key, 9528);
            ASSERT_TRUE(cur_it->Valid());
        }
    }

    delete cur_it;
    delete table;
}

TEST_P(TableTest, TableIteratorTS) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i % 100), "mcc" + std::to_string(i),
            "13", std::to_string(1000 + i), std::to_string(1000 + i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    delete it;

    it = table->NewTraverseIterator(1);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    delete it;

    it = table->NewTraverseIterator(2);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    delete it;

    Ticket ticket;
    TableIterator* iter = table->NewIterator(0, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(10, count);
    delete iter;

    iter = table->NewIterator(1, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(10, count);
    delete iter;

    iter = table->NewIterator(2, "mcc10", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(1, count);
    delete iter;
    iter = table->NewIterator(3, "mcc10", ticket);
    ASSERT_EQ(NULL, iter);
    delete iter;
    delete table;
}

TEST_P(TableTest, TraverseIteratorCount) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);

    Table* table = CreateTable(table_meta, table_path);

    table->Init();
    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i % 100), "mcc" + std::to_string(i),
            "13", std::to_string(1000 + i), std::to_string(10000 + i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);

    // Memtable::GetCount() may return wrong result.
    // refer to issue #1227
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1100, (int64_t)it->GetCount());
    } else {
        ASSERT_EQ(1000, (int64_t)it->GetCount());
    }

    delete it;

    it = table->NewTraverseIterator(2);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }

    ASSERT_EQ(1000, count);
    // Memtable::GetCount() may return wrong result.
    // refer to issue #1227
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(2000, (int64_t)it->GetCount());
    } else {
        ASSERT_EQ(1000, (int64_t)it->GetCount());
    }

    delete it;
    delete table;
}

TEST_P(TableTest, UpdateTTL) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 5, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 10, 0);

    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(20 * 10 * 6000, 0, ::openmldb::storage::kAbsoluteTime));
    table->SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    auto meta = table->GetTableMeta();
    for (const auto& cur_column_key : meta->column_key()) {
        ASSERT_EQ(20, cur_column_key.ttl().abs_ttl());
        ASSERT_EQ(0, cur_column_key.ttl().lat_ttl());
    }
    table->SchedGc();
    ASSERT_EQ(20, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(20, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    delete table;
}

TEST_P(TableTest, AbsAndLatSetGet) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsAndLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsAndLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc1", "mcc", "ts2", ::openmldb::type::kAbsAndLat, 2, 10);
    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 10; i++) {
        std::vector<std::string> row = {"card", "mcc",
            "13", std::to_string(now - i * (60 * 1000)), std::to_string(now - i * (60 * 1000))};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    // test get and set ttl
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 3, ::openmldb::storage::kAbsAndLat), "card");
    table->SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);
    table->SchedGc();
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 9 * (60 * 1000) - 10),
                    std::to_string(now - 1 * (60 * 1000))}, value));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 12 * (60 * 1000)),
                    std::to_string(now - 10 * (60 * 1000))}, value));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    ASSERT_EQ(1, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);
    delete table;
}

TEST_P(TableTest, AbsOrLatSetGet) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsOrLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsOrLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc1", "mcc", "ts2", ::openmldb::type::kAbsOrLat, 2, 10);

    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 10; i++) {
        std::vector<std::string> row = {"card", "mcc",
            "13", std::to_string(now - i * (60 * 1000)), std::to_string(now - i * (60 * 1000))};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    // test get and set ttl
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 3, ::openmldb::storage::kAbsOrLat), "card");
    table->SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);
    table->SchedGc();
    ASSERT_EQ(1, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::test::AddDimension(0, "card", &entry);
        ::openmldb::test::AddDimension(1, "mcc", &entry);
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 9 * (60 * 1000) - 10),
                    std::to_string(now - 1 * (60 * 1000))}, value));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        ::openmldb::test::AddDimension(0, "card", &entry);
        ::openmldb::test::AddDimension(1, "mcc", &entry);
        auto value = entry.mutable_value();
        ASSERT_EQ(0, codec.EncodeRow({"card", "mcc", "12", std::to_string(now - 12 * (60 * 1000)),
                    std::to_string(now - 10 * (60 * 1000))}, value));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    ASSERT_EQ(1, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table->GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table->GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table->GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table->GetIndex(2)->GetTTL()->lat_ttl);

    delete table;
}

TEST_P(TableTest, GcAbsOrLat) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    // some functions with disktable mode in this test have not been implemented.
    // refer to issue #1238
    if (storageMode == openmldb::common::kHDD) {
        return;
    }
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "idx0", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "idx0", "idx0", "", ::openmldb::type::kAbsOrLat, 4, 3);

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;
    std::unique_ptr<Table> table(CreateTable(table_meta, table_path));
    table->Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 2 * (60 * 1000) - 1000, "value2", 6);
    table->Put("test1", now - 1 * (60 * 1000) - 1000, "value3", 6);
    table->Put("test2", now - 4 * (60 * 1000) - 1000, "value4", 6);
    table->Put("test2", now - 2 * (60 * 1000) - 1000, "value5", 6);
    table->Put("test2", now - 1 * (60 * 1000) - 1000, "value6", 6);
    ASSERT_EQ(7, (int64_t)table->GetRecordIdxCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(7, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(3 * 60 * 1000, 0, ::openmldb::storage::kAbsOrLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(5, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(5, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    update_ttl = ::openmldb::storage::UpdateTTLMeta(::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsOrLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(4, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(4, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    update_ttl = ::openmldb::storage::UpdateTTLMeta(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 1, ::openmldb::storage::kAbsOrLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 5 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    table->SchedGc();
    ASSERT_EQ(0, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(0, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    FLAGS_gc_safe_offset = offset;
}

TEST_P(TableTest, GcAbsAndLat) {
    ::openmldb::common::StorageMode storageMode = GetParam();

    // some functions with disktable mode in this test have not been implemented.
    // refer to issue #1238
    if (storageMode == openmldb::common::kHDD) {
        return;
    }
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "idx0", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "idx0", "idx0", "", ::openmldb::type::kAbsAndLat, 3, 3);

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;
    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 2 * (60 * 1000) - 1000, "value2", 6);
    table->Put("test1", now - 1 * (60 * 1000) - 1000, "value3", 6);
    table->Put("test2", now - 4 * (60 * 1000) - 1000, "value4", 6);
    table->Put("test2", now - 3 * (60 * 1000) - 1000, "value5", 6);
    table->Put("test2", now - 2 * (60 * 1000) - 1000, "value6", 6);
    ASSERT_EQ(7, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(7, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 0, ::openmldb::storage::kAbsAndLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(6, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(6, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 4 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    update_ttl = ::openmldb::storage::UpdateTTLMeta(::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsAndLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(6, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(6, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    update_ttl = ::openmldb::storage::UpdateTTLMeta(
        ::openmldb::storage::TTLSt(1 * 60 * 1000, 1, ::openmldb::storage::kAbsAndLat));
    table->SetTTL(update_ttl);
    table->SchedGc();
    ASSERT_EQ(6, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(6, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    table->SchedGc();
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
        ASSERT_EQ(2, (int64_t)table->GetRecordPkCnt());
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_TRUE(table->IsExpire(entry));
        }
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_value(::openmldb::test::EncodeKV("test1", "value1"));
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        // some functions in disk table need to be implemented.
        // refer to issue #1238
        if (storageMode == ::openmldb::common::StorageMode::kMemory) {
            ASSERT_FALSE(table->IsExpire(entry));
        }
    }
    FLAGS_gc_safe_offset = offset;

    delete table;
}

TEST_P(TableTest, TraverseIteratorCountWithLimit) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;

    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);

    Table* table = CreateTable(table_meta, table_path);

    table->Init();
    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i % 100), "mcc" + std::to_string(i),
            "13", std::to_string(1000 + i), std::to_string(10000 + i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }

    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    // disktable and memtable behave inconsistently with max_traverse_cnt
    // refer to issue #1249 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1000, count);
        // Memtable::GetCount() may return wrong result.
        // refer to issue #1227
        ASSERT_EQ(1100, (int64_t)it->GetCount());
    } else {
        ASSERT_EQ(49, count);
        ASSERT_EQ(50, (int64_t)it->GetCount());
    }

    delete it;

    it = table->NewTraverseIterator(2);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    // disktable and memtable behave inconsistently with max_traverse_cnt
    // refer to issue #1249 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1000, count);
        // Memtable::GetCount() may return wrong result.
        // refer to issue #1227
        ASSERT_EQ(2000, (int64_t)it->GetCount());
    } else {
        ASSERT_EQ(49, count);
        ASSERT_EQ(50, (int64_t)it->GetCount());
    }

    delete it;
    delete table;

    FLAGS_max_traverse_cnt = old_max_traverse;
}


// Release function is only in memtable
TEST_F(TableTest, Release) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime);
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->Put("test2", 9537, "test", 4);
    int64_t cnt = table->Release();
    ASSERT_EQ(cnt, 2);
    delete table;
}

TEST_P(TableTest, TSColIDLength) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    for (int i = 2; i <= 300; i++) {
        std::string tsid = "ts" + std::to_string(i);
        SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), tsid, ::openmldb::type::kBigInt);
    }
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts44", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts300", ::openmldb::type::kAbsoluteTime, 0, 0);

    Table* table = CreateTable(table_meta, table_path);

    table->Init();
    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 10; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i), "mcc" + std::to_string(i)};
        for (int j = 2; j <= 300; j++) {
            row.push_back(std::to_string(1000 * i + j));
        }
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        ::openmldb::api::Dimension* dim1 = request.add_dimensions();
        dim1->set_idx(1);
        dim1->set_key(row[0]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }

    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }

    ASSERT_EQ(10, count);
    delete table;
}

TEST_P(TableTest, MultiDimensionPutTS) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(1);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i), "mcc" + std::to_string(i),
            "13", std::to_string(2000 + i), std::to_string(1000 + i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[0]);
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    ASSERT_EQ(2000, it->GetKey());

    TableIterator* it1 = table->NewTraverseIterator(1);
    it1->SeekToFirst();
    ASSERT_EQ(1000, it1->GetKey());

    TableIterator* it2 = table->NewTraverseIterator(2);
    it2->SeekToFirst();
    ASSERT_EQ(2000, it2->GetKey());
}

TEST_P(TableTest, MultiDimensionPutTS1) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(1);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);

    for (int i = 0; i < 1000; i++) {
        std::vector<std::string> row = {"card" + std::to_string(i), "mcc" + std::to_string(i), std::to_string(2000 + i),
                                        std::to_string(1000 + i)};
        ::openmldb::api::PutRequest request;
        ::openmldb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key(row[0]);

        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key(row[1]);
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    ASSERT_EQ(2000, it->GetKey());

    TableIterator* it1 = table->NewTraverseIterator(1);
    it1->SeekToFirst();
    ASSERT_EQ(1000, it1->GetKey());
}

TEST_P(TableTest, MultiDimissionPutTS2) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    Table* table = CreateTable("tx_log", id, 1, 8, mapping, 0, ::openmldb::type::kAbsoluteTime,
                                      table_path, storageMode);
    table->Init();
    Dimensions dimensions;
    ::openmldb::api::Dimension* d0 = dimensions.Add();
    d0->set_key("d0");
    d0->set_idx(0);

    ::openmldb::api::Dimension* d1 = dimensions.Add();
    d1->set_key("d1");
    d1->set_idx(1);

    ::openmldb::api::Dimension* d2 = dimensions.Add();
    d2->set_key("d2");
    d2->set_idx(2);
    auto meta = ::openmldb::test::GetTableMeta({"idx0", "idx1", "idx2"});
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    std::string result;
    sdk_codec.EncodeRow({"d0", "d1", "d2"}, &result);
    bool ok = table->Put(100, result, dimensions);
    ASSERT_TRUE(ok);

    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(100, (int64_t)it->GetKey());

    delete it;
    delete table;
}

TEST_P(TableTest, AbsAndLat) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(1);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "test", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts3", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts4", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts5", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts6", ::openmldb::type::kBigInt);

    SchemaCodec::SetIndex(table_meta.add_column_key(), "index0", "test", "ts1", ::openmldb::type::kAbsAndLat, 100, 10);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "index1", "test", "ts2", ::openmldb::type::kAbsAndLat, 50, 8);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "index2", "test", "ts3", ::openmldb::type::kAbsAndLat, 70, 5);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "index3", "test", "ts4", ::openmldb::type::kAbsAndLat, 0, 5);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "index4", "test", "ts5", ::openmldb::type::kAbsAndLat, 50, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "index5", "test", "ts6", ::openmldb::type::kAbsAndLat, 0, 0);

    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;

    for (int i = 0; i < 100; i++) {
        uint64_t ts = now - (99 - i) * 60 * 1000;
        std::string ts_str = std::to_string(ts);

        std::vector<std::string> row = {
            "test" + std::to_string(i % 10), ts_str, ts_str, ts_str, ts_str, ts_str, ts_str};
        ::openmldb::api::PutRequest request;
        for (int idx = 0; idx <= 5; idx++) {
            auto dim = request.add_dimensions();
            dim->set_idx(idx);
            dim->set_key(row[0]);
        }
        std::string value;
        ASSERT_EQ(0, codec.EncodeRow(row, &value));
        table->Put(0, value, request.dimensions());
    }

    for (int i = 0; i <= 5; i++) {
        TableIterator* it = table->NewTraverseIterator(i);
        it->SeekToFirst();
        int count = 0;
        while (it->Valid()) {
            it->Next();
            count++;
        }

        if (i == 1) {
            ASSERT_EQ(80, count);
        } else if (i == 2) {
            ASSERT_EQ(70, count);
        } else {
            ASSERT_EQ(100, count);
        }
    }

    delete table;
}

TEST_P(TableTest, NegativeTs) {
    ::openmldb::common::StorageMode storageMode = GetParam();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    std::string table_path = "";
    int id = 1;
    if (storageMode == ::openmldb::common::kHDD) {
        id = ++counter;
        table_path = GetDBPath(FLAGS_hdd_root_path, id, 1);
    }
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_format_version(1);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    Table* table = CreateTable(table_meta, table_path);
    table->Init();
    codec::SDKCodec codec(table_meta);
    std::vector<std::string> row = {"card1", "-1224"};
    ::openmldb::api::PutRequest request;
    ::openmldb::api::Dimension* dim = request.add_dimensions();
    dim->set_idx(0);
    dim->set_key(row[0]);
    std::string value;
    ASSERT_EQ(0, codec.EncodeRow(row, &value));
    ASSERT_FALSE(table->Put(0, value, request.dimensions()));
}

INSTANTIATE_TEST_CASE_P(TestMemAndHDD, TableTest,
                        ::testing::Values(::openmldb::common::kMemory, ::openmldb::common::kHDD));

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    FLAGS_max_traverse_cnt = 200000;
    ::testing::AddGlobalTestEnvironment(new ::openmldb::storage::DiskTestEnvironment);
    ::testing::InitGoogleTest(&argc, argv);
    ::openmldb::base::SetLogLevel(INFO);
    FLAGS_hdd_root_path = "/tmp/" + std::to_string(::openmldb::storage::GenRand());
    FLAGS_ssd_root_path = "/tmp/" + std::to_string(::openmldb::storage::GenRand());
    // FLAGS_hdd_root_path = "/tmp/" + std::to_string(1);
    // FLAGS_ssd_root_path = "/tmp/" + std::to_string(1);
    return RUN_ALL_TESTS();
}
