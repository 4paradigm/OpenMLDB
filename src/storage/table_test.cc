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
#include "gtest/gtest.h"
#include "base/glog_wapper.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "storage/mem_table.h"
#include "storage/ticket.h"

DECLARE_uint32(max_traverse_cnt);
DECLARE_int32(gc_safe_offset);

namespace fedb {
namespace storage {

using ::fedb::codec::SchemaCodec;

class TableTest : public ::testing::Test {
 public:
    TableTest() {}
    ~TableTest() {}
};

TEST_F(TableTest, Put) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    table->Put("test", 9537, "test", 4);
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

TEST_F(TableTest, MultiDimissionDelete) {
    ::fedb::api::TableMeta* table_meta = new ::fedb::api::TableMeta();
    table_meta->set_name("t0");
    table_meta->set_tid(110);
    table_meta->set_pid(1);
    table_meta->set_seg_cnt(1);
    ::fedb::common::ColumnDesc* desc = table_meta->add_column_desc();
    desc->set_name("card");
    desc->set_data_type(::fedb::type::kString);
    desc = table_meta->add_column_desc();
    desc->set_name("mcc");
    desc->set_data_type(::fedb::type::kString);
    desc = table_meta->add_column_desc();
    desc->set_name("price");
    desc->set_data_type(::fedb::type::kBigInt);
    auto column_key = table_meta->add_column_key();
    column_key->set_index_name("card");
    auto ttl = column_key->mutable_ttl();
    ttl->set_abs_ttl(5);
    column_key = table_meta->add_column_key();
    column_key->set_index_name("mcc");
    ttl = column_key->mutable_ttl();
    ttl->set_abs_ttl(5);
    MemTable* table = new MemTable(*table_meta);
    table->Init();
    table->DeleteIndex("mcc");
    table->SchedGc();
    table->SchedGc();
    table->SchedGc();
}

TEST_F(TableTest, MultiDimissionPut0) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    ASSERT_EQ(3, (int64_t)table->GetIdxCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordCnt());
    Dimensions dimensions;
    ::fedb::api::Dimension* d0 = dimensions.Add();
    d0->set_key("d0");
    d0->set_idx(0);

    ::fedb::api::Dimension* d1 = dimensions.Add();
    d1->set_key("d1");
    d1->set_idx(1);

    ::fedb::api::Dimension* d2 = dimensions.Add();
    d2->set_key("d2");
    d2->set_idx(2);
    bool ok = table->Put(1, "test", dimensions);
    ASSERT_TRUE(ok);
    ASSERT_EQ(3, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
}

TEST_F(TableTest, MultiDimissionPut1) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    ASSERT_EQ(3, (int64_t)table->GetIdxCnt());
    DataBlock* db = new DataBlock(3, "helloworld", 10);
    std::string d1 = "d1";
    ASSERT_FALSE(table->Put(d1, 9527, db, 3));
    ASSERT_TRUE(table->Put(d1, 9527, db, 0));
    std::string d2 = "d2";
    ASSERT_TRUE(table->Put(d2, 9527, db, 1));
    std::string d3 = "d3";
    ASSERT_TRUE(table->Put(d3, 9527, db, 2));
}

TEST_F(TableTest, Release) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->Put("test2", 9537, "test", 4);
    int64_t cnt = table->Release();
    ASSERT_EQ(cnt, 2);
    delete table;
}

TEST_F(TableTest, IsExpired) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    // table ttl is 1
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 1,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    uint64_t now_time = ::baidu::common::timer::get_micros() / 1000;
    ::fedb::api::LogEntry entry;
    uint64_t ts_time = now_time;
    entry.set_ts(ts_time);
    ::fedb::storage::TTLSt ttl(1 * 60 * 1000, 0, ::fedb::storage::kAbsoluteTime);
    ASSERT_FALSE(entry.ts() < table->GetExpireTime(ttl));

    // ttl_offset_ is 60 * 1000
    ts_time = now_time - 4 * 60 * 1000;
    entry.set_ts(ts_time);
    ASSERT_TRUE(entry.ts() < table->GetExpireTime(ttl));
    delete table;
}

TEST_F(TableTest, Iterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10,
                                   ::fedb::type::kAbsoluteTime);
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

TEST_F(TableTest, Iterator_GetSize) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 10,
                                   ::fedb::type::kAbsoluteTime);
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
    ASSERT_EQ(3, size);
    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test0", value_str);
    it->Next();
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test", value2_str);
    it->Next();
    ASSERT_TRUE(it->Valid());
    delete it;
    delete table;
}

TEST_F(TableTest, SchedGcForMultiDimissionTable) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 1,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    ASSERT_EQ(3, (int64_t)table->GetIdxCnt());
    DataBlock* db = new DataBlock(3, "helloworld", 10);
    std::string d1 = "d1";
    ASSERT_FALSE(table->Put(d1, 9527, db, 3));
    ASSERT_TRUE(table->Put(d1, 9527, db, 0));
    std::string d2 = "d2";
    ASSERT_TRUE(table->Put(d2, 9527, db, 1));
    std::string d3 = "d3";
    ASSERT_TRUE(table->Put(d3, 9527, db, 2));
    table->RecordCntIncr(1);
    ASSERT_EQ(3, (int64_t)table->GetRecordIdxCnt());
    table->SchedGc();
    ASSERT_EQ(0, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(0, (int64_t)table->GetRecordIdxCnt());
}

TEST_F(TableTest, SchedGcHead) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 1,
                                   ::fedb::type::kLatestTime);
    table->Init();
    table->Put("test", 2, "test1", 5);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 1, "test2", 5);
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordPkCnt());
    table->SchedGc();
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(1);
        entry.set_value("test2");
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(2);
        entry.set_value("test1");
        ASSERT_FALSE(table->IsExpire(entry));
    }
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());
}

TEST_F(TableTest, SchedGcHead1) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    uint64_t keep_cnt = 500;
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, keep_cnt,
                                   ::fedb::type::kLatestTime);
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
}

TEST_F(TableTest, SchedGc) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 1,
                                   ::fedb::type::kLatestTime);
    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", now, "tes2", 4);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 9527, "test", 4);
    ASSERT_EQ(2, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table->GetRecordIdxCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordPkCnt());

    table->SchedGc();
    ASSERT_EQ(1, (int64_t)table->GetRecordCnt());
    ASSERT_EQ(1, (int64_t)table->GetRecordIdxCnt());
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

TEST_F(TableTest, TableDataCnt) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 1,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 0);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 2);
    ASSERT_EQ((int64_t)table->GetRecordIdxCnt(), 2);
    table->SchedGc();
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(now - 1 * (60 * 1000) - 1);
        entry.set_value("test");
        ASSERT_TRUE(table->IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test");
        entry.set_ts(now);
        entry.set_value("tes2");
        ASSERT_FALSE(table->IsExpire(entry));
    }
    ASSERT_EQ((int64_t)table->GetRecordCnt(), 1);
    ASSERT_EQ((int64_t)table->GetRecordIdxCnt(), 1);
    delete table;
}

TEST_F(TableTest, TableUnref) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 1,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();
    table->Put("test", 9527, "test", 4);
    delete table;
}

TEST_F(TableTest, TableIterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 0,
                                   ::fedb::type::kAbsoluteTime);
    table->Init();

    table->Put("pk", 9527, "test1", 5);
    table->Put("pk1", 9527, "test2", 5);
    table->Put("pk", 9528, "test3", 5);
    table->Put("pk1", 100, "test4", 5);
    table->Put("test", 20, "test5", 5);
    // Ticket ticket;
    TableIterator* it = table->NewTraverseIterator(0);
    // it->Test();
    it->SeekToFirst();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9528, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(100, (int64_t)it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());

    it->Seek("none", 11111);
    ASSERT_TRUE(it->Valid());

    it->Seek("test", 30);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it->GetKey());

    it->Seek("test", 20);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    delete it;
    delete table;

    MemTable* table1 = new MemTable("tx_log", 1, 1, 8, mapping, 2,
                                    ::fedb::type::kLatestTime);
    table1->Init();

    table1->Put("pk", 9527, "test1", 5);
    table1->Put("pk1", 9527, "test2", 5);
    table1->Put("pk", 9528, "test3", 5);
    table1->Put("pk1", 100, "test4", 5);
    table1->Put("test", 20, "test5", 5);
    table1->Put("pk", 200, "test6", 5);
    // Ticket ticket;
    TableIterator* it1 = table1->NewTraverseIterator(0);
    it1->Seek("pk", 9528);
    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("pk", it1->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it1->GetKey());
    it1->Next();
    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("test", it1->GetPK().c_str());
    ASSERT_EQ(20, (int64_t)it1->GetKey());
    it1->Next();
    ASSERT_STREQ("pk1", it1->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it1->GetKey());
}

TEST_F(TableTest, TableIteratorNoPk) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 0,
                                   ::fedb::type::kAbsoluteTime);
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
    ASSERT_STREQ("pk10", it->GetPK().c_str());
    ASSERT_EQ(9527, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk0", it->GetPK().c_str());
    ASSERT_EQ(9522, (int64_t)it->GetKey());
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
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, (int64_t)it->GetKey());
    it->Next();
    ASSERT_STREQ("pk0", it->GetPK().c_str());
    ASSERT_EQ(9522, (int64_t)it->GetKey());
    delete it;
}

TEST_F(TableTest, TableIteratorCount) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    MemTable* table = new MemTable("tx_log", 1, 1, 8, mapping, 0,
                                   ::fedb::type::kAbsoluteTime);
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
    ASSERT_EQ(44471, count);
    delete it;

    for (int i = 0; i < 200000; i++) {
        TableIterator* cur_it = table->NewTraverseIterator(0);
        std::string key = "pk" + std::to_string(i);
        cur_it->Seek(key, 9528);
        ASSERT_TRUE(cur_it->Valid());
        delete cur_it;
    }
}

TEST_F(TableTest, TableIteratorTS) {
    ::fedb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::fedb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::fedb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::fedb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::fedb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::fedb::type::kAbsoluteTime, 0, 0);
    MemTable table(table_meta);
    table.Init();

    for (int i = 0; i < 1000; i++) {
        ::fedb::api::PutRequest request;
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card" + std::to_string(i % 100));
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("card" + std::to_string(i % 100));
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key("mcc" + std::to_string(i));
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(1000 + i);
        ts = request.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(10000 + i);
        std::string value = "value" + std::to_string(i);
        table.Put(request.dimensions(), request.ts_dimensions(), value);
    }
    TableIterator* it = table.NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    delete it;

    it = table.NewTraverseIterator(1);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    delete it;

    Ticket ticket;
    TableIterator* iter = table.NewIterator(0, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(10, count);
    delete iter;
    iter = table.NewIterator(1, "card5", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(10, count);
    delete iter;
    iter = table.NewIterator(2, "mcc10", ticket);
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        count++;
        iter->Next();
    }
    ASSERT_EQ(1, count);
    delete iter;
    iter = table.NewIterator(3, "mcc10", ticket);
    ASSERT_EQ(NULL, iter);
    delete iter;
}

TEST_F(TableTest, TraverseIteratorCount) {
    uint32_t old_max_traverse = FLAGS_max_traverse_cnt;
    FLAGS_max_traverse_cnt = 50;
    ::fedb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::fedb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::fedb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::fedb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::fedb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::fedb::type::kAbsoluteTime, 0, 0);

    MemTable table(table_meta);
    table.Init();

    for (int i = 0; i < 1000; i++) {
        ::fedb::api::PutRequest request;
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card" + std::to_string(i % 100));
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("card" + std::to_string(i % 100));
        dim = request.add_dimensions();
        dim->set_idx(2);
        dim->set_key("mcc" + std::to_string(i));
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(1000 + i);
        ts = request.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(10000 + i);
        std::string value = "value" + std::to_string(i);
        table.Put(request.dimensions(), request.ts_dimensions(), value);
    }
    TableIterator* it = table.NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    ASSERT_EQ(1100, (int64_t)it->GetCount());
    delete it;

    it = table.NewTraverseIterator(1);
    it->SeekToFirst();
    count = 0;
    while (it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(1000, count);
    ASSERT_EQ(1100, (int64_t)it->GetCount());
    delete it;
    FLAGS_max_traverse_cnt = old_max_traverse;
}

TEST_F(TableTest, UpdateTTL) {
    ::fedb::api::TableMeta table_meta;
    table_meta.set_name("table1");
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_seg_cnt(8);
    table_meta.set_mode(::fedb::api::TableMode::kTableLeader);
    table_meta.set_key_entry_max_height(8);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::fedb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::fedb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::fedb::type::kAbsoluteTime, 5, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::fedb::type::kAbsoluteTime, 10, 0);

    MemTable table(table_meta);
    table.Init();
    ASSERT_EQ(10, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table.GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ::fedb::storage::UpdateTTLMeta update_ttl(::fedb::storage::TTLSt(20 * 10 * 6000, 0,
                ::fedb::storage::kAbsoluteTime));
    table.SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(5, (int64_t)table.GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    table.SchedGc();
    ASSERT_EQ(20, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(20, (int64_t)table.GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
}

void BuildTableMeta(::fedb::api::TableMeta* table_meta) {
    table_meta->set_name("table1");
    table_meta->set_tid(1);
    table_meta->set_pid(0);
    table_meta->set_seg_cnt(8);
    table_meta->set_mode(::fedb::api::TableMode::kTableLeader);
    table_meta->set_key_entry_max_height(8);
}

TEST_F(TableTest, AbsAndLatSetGet) {
    ::fedb::api::TableMeta table_meta;
    BuildTableMeta(&table_meta);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::fedb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::fedb::type::kAbsAndLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::fedb::type::kAbsAndLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc1", "mcc", "ts2", ::fedb::type::kAbsAndLat, 2, 10);
    MemTable table(table_meta);
    table.Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 10; i++) {
        ::fedb::api::PutRequest request;
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - i * (60 * 1000));
        ts = request.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - i * (60 * 1000));
        std::string value = "value";
        table.Put(request.dimensions(), request.ts_dimensions(), value);
    }
    // test get and set ttl
    ASSERT_EQ(10, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(0)->GetTTL()->lat_ttl);
    ::fedb::storage::UpdateTTLMeta update_ttl(::fedb::storage::TTLSt(1 * 60 * 1000, 3,
                ::fedb::storage::kAbsAndLat), "card");
    table.SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table.GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table.GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table.GetIndex(2)->GetTTL()->lat_ttl);
    table.SchedGc();
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        ::fedb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        ::fedb::api::TSDimension* ts = entry.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - 10 * (60 * 1000));
        ts = entry.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - 2 * (60 * 1000));
        entry.set_value("value");
        ASSERT_FALSE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        ::fedb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        ::fedb::api::TSDimension* ts = entry.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - 12 * (60 * 1000));
        ts = entry.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - 10 * (60 * 1000));
        entry.set_value("value");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    ASSERT_EQ(1, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table.GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table.GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table.GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table.GetIndex(2)->GetTTL()->lat_ttl);
}

TEST_F(TableTest, AbsOrLatSetGet) {
    ::fedb::api::TableMeta table_meta;
    BuildTableMeta(&table_meta);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "price", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::fedb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::fedb::type::kBigInt);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::fedb::type::kAbsOrLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::fedb::type::kAbsOrLat, 10, 12);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc1", "mcc", "ts2", ::fedb::type::kAbsOrLat, 2, 10);

    MemTable table(table_meta);
    table.Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    for (int i = 0; i < 10; i++) {
        ::fedb::api::PutRequest request;
        ::fedb::api::Dimension* dim = request.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = request.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        ::fedb::api::TSDimension* ts = request.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - i * (60 * 1000));
        ts = request.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - i * (60 * 1000));
        std::string value = "value";
        table.Put(request.dimensions(), request.ts_dimensions(), value);
    }
    // test get and set ttl
    ASSERT_EQ(10, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(0)->GetTTL()->lat_ttl);
    ::fedb::storage::UpdateTTLMeta update_ttl(::fedb::storage::TTLSt(1 * 60 * 1000, 3,
                ::fedb::storage::kAbsOrLat), "card");
    table.SetTTL(update_ttl);
    ASSERT_EQ(10, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table.GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table.GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table.GetIndex(2)->GetTTL()->lat_ttl);
    table.SchedGc();
    ASSERT_EQ(1, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table.GetIndex(0)->GetTTL()->lat_ttl);
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        ::fedb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        ::fedb::api::TSDimension* ts = entry.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - 10 * (60 * 1000));
        ts = entry.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - 2 * (60 * 1000) + 1000);
        entry.set_value("value");
        ASSERT_FALSE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        ::fedb::api::Dimension* dim = entry.add_dimensions();
        dim->set_idx(0);
        dim->set_key("card");
        dim = entry.add_dimensions();
        dim->set_idx(1);
        dim->set_key("mcc");
        ::fedb::api::TSDimension* ts = entry.add_ts_dimensions();
        ts->set_idx(0);
        ts->set_ts(now - 12 * (60 * 1000));
        ts = entry.add_ts_dimensions();
        ts->set_idx(1);
        ts->set_ts(now - 10 * (60 * 1000));
        entry.set_value("value");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    ASSERT_EQ(1, (int64_t)table.GetIndex(0)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(3, (int64_t)table.GetIndex(0)->GetTTL()->lat_ttl);
    ASSERT_EQ(10, (int64_t)table.GetIndex(1)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(12, (int64_t)table.GetIndex(1)->GetTTL()->lat_ttl);
    ASSERT_EQ(2, (int64_t)table.GetIndex(2)->GetTTL()->abs_ttl / (10 * 6000));
    ASSERT_EQ(10, (int64_t)table.GetIndex(2)->GetTTL()->lat_ttl);
}

TEST_F(TableTest, GcAbsOrLat) {
    ::fedb::api::TableMeta table_meta;
    BuildTableMeta(&table_meta);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "idx0", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::fedb::type::kString);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "idx0", "idx0", "", ::fedb::type::kAbsOrLat, 4, 3);

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;
    MemTable table(table_meta);
    table.Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table.Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table.Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table.Put("test1", now - 2 * (60 * 1000) - 1000, "value2", 6);
    table.Put("test1", now - 1 * (60 * 1000) - 1000, "value3", 6);
    table.Put("test2", now - 4 * (60 * 1000) - 1000, "value4", 6);
    table.Put("test2", now - 2 * (60 * 1000) - 1000, "value5", 6);
    table.Put("test2", now - 1 * (60 * 1000) - 1000, "value6", 6);
    ASSERT_EQ(7, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(7, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    ::fedb::storage::UpdateTTLMeta update_ttl(::fedb::storage::TTLSt(3 * 60 * 1000, 0, ::fedb::storage::kAbsOrLat));
    table.SetTTL(update_ttl);
    table.SchedGc();
    ASSERT_EQ(5, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(5, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    update_ttl = ::fedb::storage::UpdateTTLMeta(::fedb::storage::TTLSt(0, 1, ::fedb::storage::kAbsOrLat));
    table.SetTTL(update_ttl);
    table.SchedGc();
    ASSERT_EQ(4, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(4, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    update_ttl = ::fedb::storage::UpdateTTLMeta(::fedb::storage::TTLSt(1 * 60 * 1000, 1,
                ::fedb::storage::kAbsOrLat));
    table.SetTTL(update_ttl);
    table.SchedGc();
    ASSERT_EQ(2, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 5 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    table.SchedGc();
    ASSERT_EQ(0, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(0, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    FLAGS_gc_safe_offset = offset;
}

TEST_F(TableTest, GcAbsAndLat) {
    ::fedb::api::TableMeta table_meta;
    BuildTableMeta(&table_meta);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "idx0", ::fedb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::fedb::type::kString);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "idx0", "idx0", "", ::fedb::type::kAbsAndLat, 3, 3);

    int32_t offset = FLAGS_gc_safe_offset;
    FLAGS_gc_safe_offset = 0;
    MemTable table(table_meta);
    table.Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table.Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table.Put("test1", now - 3 * (60 * 1000) - 1000, "value1", 6);
    table.Put("test1", now - 2 * (60 * 1000) - 1000, "value2", 6);
    table.Put("test1", now - 1 * (60 * 1000) - 1000, "value3", 6);
    table.Put("test2", now - 4 * (60 * 1000) - 1000, "value4", 6);
    table.Put("test2", now - 3 * (60 * 1000) - 1000, "value5", 6);
    table.Put("test2", now - 2 * (60 * 1000) - 1000, "value6", 6);
    ASSERT_EQ(7, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(7, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    ::fedb::storage::UpdateTTLMeta update_ttl(::fedb::storage::TTLSt(1 * 60 * 1000, 0, ::fedb::storage::kAbsAndLat));
    table.SetTTL(update_ttl);
    table.SchedGc();
    ASSERT_EQ(6, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(6, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 4 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_FALSE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_FALSE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_FALSE(table.IsExpire(entry));
    }
    update_ttl = ::fedb::storage::UpdateTTLMeta(::fedb::storage::TTLSt(0, 1, ::fedb::storage::kAbsAndLat));
    table.SetTTL(update_ttl);
    table.SchedGc();
    ASSERT_EQ(6, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(6, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    update_ttl = ::fedb::storage::UpdateTTLMeta(::fedb::storage::TTLSt(1 * 60 * 1000, 1,
                ::fedb::storage::kAbsAndLat));
    table.SetTTL(update_ttl);
    table.SchedGc();
    ASSERT_EQ(6, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(6, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    table.SchedGc();
    ASSERT_EQ(2, (int64_t)table.GetRecordCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordIdxCnt());
    ASSERT_EQ(2, (int64_t)table.GetRecordPkCnt());
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 3 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 2 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_TRUE(table.IsExpire(entry));
    }
    {
        ::fedb::api::LogEntry entry;
        entry.set_log_index(0);
        entry.set_pk("test1");
        entry.set_ts(now - 1 * (60 * 1000) - 1000);
        entry.set_value("value1");
        ASSERT_FALSE(table.IsExpire(entry));
    }
    FLAGS_gc_safe_offset = offset;
}

}  // namespace storage
}  // namespace fedb

int main(int argc, char** argv) {
    FLAGS_max_traverse_cnt = 200000;
    ::testing::InitGoogleTest(&argc, argv);
    ::fedb::base::SetLogLevel(INFO);
    return RUN_ALL_TESTS();
}
