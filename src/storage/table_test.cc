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

#include "base/glog_wapper.h"
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

using ::openmldb::codec::SchemaCodec; 

class DiskTestEnvironment : public ::testing::Environment{
    virtual void TearDown() {
        for (int i = 1; i <= counter; i++) {
            std::string path = FLAGS_hdd_root_path + "/" + std::to_string(i) +  "_1";
            RemoveData(path);
        }
    }
};

class TableTest : public ::testing::Test {
 public:
    TableTest() {}
    ~TableTest() {}
};


void put(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime,
                                      FLAGS_hdd_root_path, storageMode);
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

TEST_F(TableTest, PutMem) {
    put(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, PutDisk) {
    put(::openmldb::common::StorageMode::kHDD);
}

void MultiDimissionDelete(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta* table_meta = new ::openmldb::api::TableMeta();
    table_meta->set_name("t0");
    int id = ++counter;
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
    Table* table = Table::CreateTable(*table_meta, FLAGS_hdd_root_path);
    table->Init();
    table->DeleteIndex("mcc");
    table->SchedGc();
    table->SchedGc();
    table->SchedGc();
    delete table;
}

TEST_F(TableTest, MultiDimissionDeleteMem) {
    MultiDimissionDelete(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, MultiDimissionDeleteDisk) {
    MultiDimissionDelete(::openmldb::common::StorageMode::kHDD);
}

void MultiDimissionPut0(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime,
                                      FLAGS_hdd_root_path, storageMode);
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
        ASSERT_EQ(3, (int64_t)table->GetRecordIdxCnt());
    }
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    delete table;
}

TEST_F(TableTest, MultiDimissionPut0Mem) {
    MultiDimissionPut0(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, MultiDimissionPut0Disk) {
    MultiDimissionPut0(::openmldb::common::StorageMode::kHDD);
}

void Release(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime, 
        FLAGS_hdd_root_path, storageMode);
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->Put("test2", 9537, "test", 4);
    int64_t cnt = table->Release();
    // some functions in disk table need to be implemented.
    // refer to issue #1238
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(cnt, 2);
    }
    delete table;
}

TEST_F(TableTest, ReleaseMem) {
    Release(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, ReleaseDisk) {
    Release(::openmldb::common::StorageMode::kHDD);
}

void IsExpired(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    // table ttl is 1
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
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

TEST_F(TableTest, IsExpiredMem) {
    IsExpired(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, IsExpiredDisk) {
    IsExpired(::openmldb::common::StorageMode::kHDD);
}

void Iterator(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
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

TEST_F(TableTest, IteratorMem) {
    Iterator(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, IteratorDisk) {
    Iterator(::openmldb::common::StorageMode::kHDD);
}

void Iterator_GetSize(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 10, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
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

TEST_F(TableTest, Iterator_GetSizeMem) {
    Iterator_GetSize(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, Iterator_GetSizeDisk) {
    Iterator_GetSize(::openmldb::common::StorageMode::kHDD);
}

void SchedGcHead(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kLatestTime,
        FLAGS_hdd_root_path, storageMode);
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
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());
    delete table;
}

TEST_F(TableTest, SchedGcHeadMem) {
    SchedGcHead(::openmldb::common::StorageMode::kMemory);
}

// some functions in disk table need to be implemented.
// refer to issue #1238
// TEST_F(TableTest, SchedGcHeadDisk) {
//     SchedGcHead(::openmldb::common::StorageMode::kHDD);
// }

void SchedGcHead1(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    uint64_t keep_cnt = 500;
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, keep_cnt, ::openmldb::type::kLatestTime,
        FLAGS_hdd_root_path, storageMode);
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

TEST_F(TableTest, SchedGcHead1Mem) {
    SchedGcHead1(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, SchedGcHead1Disk) {
    SchedGcHead1(::openmldb::common::StorageMode::kHDD);
}

void SchedGc(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kLatestTime,
        FLAGS_hdd_root_path, storageMode);
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
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    }
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());

    Ticket ticket;
    TableIterator* it = table->NewIterator("test", ticket);
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("tes2", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete table;
}

TEST_F(TableTest, SchedGcMem) {
    SchedGc(::openmldb::common::StorageMode::kMemory);
}

// some functions in disk table need to be implemented.
// refer to issue #1238
// TEST_F(TableTest, SchedGcDisk) {
//     SchedGc(::openmldb::common::StorageMode::kHDD);
// }

void TableDataCnt(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
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

TEST_F(TableTest, TableDataCntMem) {
    TableDataCnt(::openmldb::common::StorageMode::kMemory);
}

// some functions in disk table need to be implemented.
// refer to issue #1238
// TEST_F(TableTest, TableDataCntDisk) {
//     TableDataCnt(::openmldb::common::StorageMode::kHDD);
// }

void TableUnref(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 8, mapping, 1, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
    table->Init();
    table->Put("test", 9527, "test", 4);
    delete table;
}

TEST_F(TableTest, TableUnrefMem) {
    TableUnref(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, TableUnrefDisk) {
    TableUnref(::openmldb::common::StorageMode::kHDD);
}

void TableIteratorRun(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 1, mapping, 0, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
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
    ASSERT_FALSE(it->Valid());
    
    delete it;
    delete table;

    id = ++counter;
    Table* table1 = Table::CreateTable("tx_log", id, 1, 1, mapping, 2, ::openmldb::type::kLatestTime,
        FLAGS_hdd_root_path, storageMode);
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
    ASSERT_EQ(9527, (int64_t)it1->GetKey());
    it1->Next();

    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("pk1", it1->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it1->GetKey());
    it1->Next();
    ASSERT_STREQ("pk1", it1->GetPK().c_str());
    ASSERT_EQ(100, (int64_t)it1->GetKey());

    delete it1;
    delete table1;
}

TEST_F(TableTest, TableIteratorMem) {
    TableIteratorRun(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, TableIteratorDisk) {
    TableIteratorRun(::openmldb::common::StorageMode::kHDD);
}

void TableIteratorNoPk(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 1, mapping, 0, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
    table->Init();

    table->Put("pk10", 9527, "test10", 5);
    table->Put("pk8", 9526, "test8", 5);
    table->Put("pk6", 9525, "test6", 5);
    table->Put("pk4", 9524, "test4", 5);
    table->Put("pk2", 9523, "test2", 5);
    table->Put("pk0", 9522, "test0", 5);
    // Ticket ticket;
    TableIterator* it = table->NewTraverseIterator(0);
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
        
    
    delete it;

    it = table->NewTraverseIterator(0);
    it->Seek("pk4", 9526);
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, (int64_t)it->GetKey());
    delete it;

    ASSERT_TRUE(table->Delete("pk4", 0));
    it = table->NewTraverseIterator(0);
    it->Seek("pk4", 9526);
    ASSERT_TRUE(it->Valid());

    ASSERT_STREQ("pk6", it->GetPK().c_str());
    ASSERT_EQ(9525, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, (int64_t)it->GetKey());
    
    delete it;
    delete table;
}

TEST_F(TableTest, TableIteratorNoPkMem) {
    TableIteratorNoPk(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, TableIteratorNoPkDisk) {
    TableIteratorNoPk(::openmldb::common::StorageMode::kHDD);
}

void TableIteratorCount(::openmldb::common::StorageMode storageMode) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    int id = ++counter;
    Table* table = Table::CreateTable("tx_log", id, 1, 1, mapping, 0, ::openmldb::type::kAbsoluteTime,
        FLAGS_hdd_root_path, storageMode);
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
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    // We seek to pk500 9528. The next data is pk500 9527. 
    // Then all these records with pk greater than str(500) and both timestamps are traversed
    // So here the count is 55551
    ASSERT_EQ(55551, count);

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

TEST_F(TableTest, TableIteratorCountMem) {
    TableIteratorCount(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, TableIteratorCountDisk) {
    TableIteratorCount(::openmldb::common::StorageMode::kHDD);
}


void TableIteratorTS(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_storage_mode(storageMode);
    table_meta.set_format_version(1);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);
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
    // disktable and memtable behave inconsistently when putting duplicate data
    // see issue 1240 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1000, count);
    } else {
        ASSERT_EQ(100, count);
    }
    delete it;

    it = table->NewTraverseIterator(1);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    // disktable and memtable behave inconsistently when putting duplicate data
    // see issue 1240 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1000, count);
    } else {
        ASSERT_EQ(100, count);
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
    delete it;

    Ticket ticket;
    TableIterator* iter = table->NewIterator(0, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    // disktable and memtable behave inconsistently when putting duplicate data
    // see issue 1240 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(10, count);
    } else {
        ASSERT_EQ(1, count);
    }
    
    delete iter;
    iter = table->NewIterator(1, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    // disktable and memtable behave inconsistently when putting duplicate data
    // see issue 1240 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(10, count);
    } else {
        ASSERT_EQ(1, count);
    }
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

TEST_F(TableTest, TableIteratorTSMem) {
    TableIteratorTS(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, TableIteratorTSDisk) {
    TableIteratorTS(::openmldb::common::StorageMode::kHDD);
}


void TraverseIteratorCount(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_format_version(1);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);

    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);

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
    // disktable and memtable behave inconsistently when putting duplicate data
    // see issue 1240 for more information
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1000, count);

        // Memtable::GetCount() may return wrong result.
        // refer to issue #1227
        ASSERT_EQ(1100, (int64_t)it->GetCount());
    } else {
        ASSERT_EQ(100, count);
        ASSERT_EQ(100, (int64_t)it->GetCount());
    }
    
    delete it;

    it = table->NewTraverseIterator(2);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }

    // Memtable::GetCount() may return wrong result.
    // refer to issue #1227
    if (storageMode == ::openmldb::common::StorageMode::kMemory) {
        ASSERT_EQ(1000, count);
        ASSERT_EQ(2000, (int64_t)it->GetCount());
    } else {
        ASSERT_EQ(1000, count);
        ASSERT_EQ(1000, (int64_t)it->GetCount());
    }

    delete it;
    delete table;
}

TEST_F(TableTest, TraverseIteratorCountMem) {
    TraverseIteratorCount(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, TraverseIteratorCountDisk) {
    TraverseIteratorCount(::openmldb::common::StorageMode::kHDD);
}

void UpdateTTL(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
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

    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);
    table->Init();
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ::openmldb::storage::UpdateTTLMeta update_ttl(
        ::openmldb::storage::TTLSt(20 * 10 * 6000, 0, ::openmldb::storage::kAbsoluteTime));
    table->SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    table->SchedGc();
    ASSERT_EQ(20, (int64_t)table->GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(20, (int64_t)table->GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    delete table;
}

TEST_F(TableTest, UpdateTTLMem) {
    UpdateTTL(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, UpdateTTLDisk) {
    UpdateTTL(::openmldb::common::StorageMode::kHDD);
}

void AbsAndLatSetGet(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_format_version(1);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsAndLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsAndLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc1", "mcc", "ts2", ::openmldb::type::kAbsAndLat, 2, 10);
    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);
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

TEST_F(TableTest, AbsAndLatSetGetMem) {
    AbsAndLatSetGet(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, AbsAndLatSetGetDisk) {
    AbsAndLatSetGet(::openmldb::common::StorageMode::kHDD);
}

void AbsOrLatSetGet(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_format_version(1);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsOrLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsOrLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc1", "mcc", "ts2", ::openmldb::type::kAbsOrLat, 2, 10);

    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);
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

TEST_F(TableTest, AbsOrLatSetGetMem) {
    AbsOrLatSetGet(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, AbsOrLatSetGetDisk) {
    AbsOrLatSetGet(::openmldb::common::StorageMode::kHDD);
}

void GcAbsOrLat(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_format_version(1);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "idx0", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "idx0", "idx0", "", ::openmldb::type::kAbsOrLat, 4, 3);

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;
    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);
    table->Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table->Put("test1", now - 2 * (60 * 1000) - 1000, "value2", 6);
    table->Put("test1", now - 1 * (60 * 1000) - 1000, "value3", 6);
    table->Put("test2", now - 4 * (60 * 1000) - 1000, "value4", 6);
    table->Put("test2", now - 2 * (60 * 1000) - 1000, "value5", 6);
    table->Put("test2", now - 1 * (60 * 1000) - 1000, "value6", 6);
    ASSERT_EQ(7, (int64_t)table->GetRecordCnt());
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

    delete table;
}

TEST_F(TableTest, GcAbsOrLatMem) {
    GcAbsOrLat(::openmldb::common::StorageMode::kMemory);
}

// some functions in disk table need to be implemented.
// refer to issue #1238
// TEST_F(TableTest, GcAbsOrLatDisk) {
//     GcAbsOrLat(::openmldb::common::StorageMode::kHDD);
// }

void GcAbsAndLat(::openmldb::common::StorageMode storageMode) {
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_format_version(1);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "idx0", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "idx0", "idx0", "", ::openmldb::type::kAbsAndLat, 3, 3);

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;
    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);
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

TEST_F(TableTest, GcAbsAndLatMem) {
    GcAbsAndLat(::openmldb::common::StorageMode::kMemory);
}

// some functions in disk table need to be implemented.
// refer to issue #1238
// TEST_F(TableTest, GcAbsAndLatDisk) {
//     GcAbsAndLat(::openmldb::common::StorageMode::kHDD);
// }

void TraverseIteratorCountWithLimit(::openmldb::common::StorageMode storageMode) {
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;
    
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    int id = ++counter;
    table_meta.set_tid(id);
    table_meta.set_pid(1);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    table_meta.set_format_version(1);
    table_meta.set_storage_mode(storageMode);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);

    Table* table = Table::CreateTable(table_meta, FLAGS_hdd_root_path);

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

TEST_F(TableTest, TraverseIteratorCountWithLimitMem) {
    TraverseIteratorCountWithLimit(::openmldb::common::StorageMode::kMemory);
}

TEST_F(TableTest, TraverseIteratorCountWithLimitDisk) {
    TraverseIteratorCountWithLimit(::openmldb::common::StorageMode::kHDD);
}


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
