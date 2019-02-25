//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/table.h"
#include "storage/ticket.h"
#include "gtest/gtest.h"
#include "timer.h"
#include "logging.h"

namespace rtidb {
namespace storage {

class TableTest : public ::testing::Test {

public:
    TableTest() {}
    ~TableTest() {}
};

TEST_F(TableTest, Put) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 10);
    table->Init();
    table->Put("test", 9537, "test", 4);
    ASSERT_EQ(1, table->GetRecordCnt());
    Ticket ticket;
    Iterator* it = table->NewIterator("test", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9537, it->GetKey());
    DataBlock* value1 = it->GetValue();
    std::string value_str(value1->data, value1->size);
    ASSERT_EQ("test", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
}

TEST_F(TableTest, MultiDimissionPut0) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 10);
    table->Init();
    ASSERT_EQ(3, table->GetIdxCnt());
    ASSERT_EQ(0, table->GetRecordIdxCnt());
    ASSERT_EQ(0, table->GetRecordCnt());
    Dimensions  dimensions;
    ::rtidb::api::Dimension* d0 = dimensions.Add();
    d0->set_key("d0");
    d0->set_idx(0);

    ::rtidb::api::Dimension* d1 = dimensions.Add();
    d1->set_key("d1");
    d1->set_idx(1);

    ::rtidb::api::Dimension* d2 = dimensions.Add();
    d2->set_key("d2");
    d2->set_idx(2);
    bool ok = table->Put(1, "test", dimensions);
    ASSERT_TRUE(ok);
    ASSERT_EQ(3, table->GetRecordIdxCnt());
    ASSERT_EQ(1, table->GetRecordCnt());
}

TEST_F(TableTest, MultiDimissionPut1) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 10);
    table->Init();
    ASSERT_EQ(3, table->GetIdxCnt());
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
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 10);
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->Put("test2", 9537, "test", 4);
    uint64_t cnt = table->Release();
    ASSERT_EQ(cnt, 2);
    delete table;
}

TEST_F(TableTest, IsExpired) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    // table ttl is 1
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 1);
    table->Init();
    uint64_t now_time = ::baidu::common::timer::get_micros() / 1000;
    ::rtidb::api::LogEntry entry;
    uint64_t ts_time = now_time; 
    entry.set_ts(ts_time);
    ASSERT_FALSE(entry.ts() < table->GetExpireTime());
    
    // ttl_offset_ is 60 * 1000
    ts_time = now_time - 4 * 60 * 1000; 
    entry.set_ts(ts_time);
    ASSERT_TRUE(entry.ts() < table->GetExpireTime());
    delete table;
}

TEST_F(TableTest, Iterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 10);
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk1", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);
    Ticket ticket;
    Iterator* it = table->NewIterator("pk", ticket);

    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    DataBlock* value1 = it->GetValue();
    std::string value_str(value1->data, value1->size);
    ASSERT_EQ("test0", value_str);
    ASSERT_EQ(5, value1->size);
    it->Next();
    DataBlock* value2 = it->GetValue();
    std::string value2_str(value2->data, value2->size);
    ASSERT_EQ("test", value2_str);
    ASSERT_EQ(4, value2->size);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
}

TEST_F(TableTest, Iterator_GetSize) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 10);
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);
    Ticket ticket;
    Iterator* it = table->NewIterator("pk", ticket);
    ASSERT_EQ(3, it->GetSize());
    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    DataBlock* value1 = it->GetValue();
    std::string value_str(value1->data, value1->size);
    ASSERT_EQ("test0", value_str);
    ASSERT_EQ(5, value1->size);
    it->Next();
    DataBlock* value2 = it->GetValue();
    std::string value2_str(value2->data, value2->size);
    ASSERT_EQ("test", value2_str);
    ASSERT_EQ(4, value2->size);
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
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();
    ASSERT_EQ(3, table->GetIdxCnt());
    DataBlock* db = new DataBlock(3, "helloworld", 10);
    std::string d1 = "d1";
    ASSERT_FALSE(table->Put(d1, 9527, db, 3));
    ASSERT_TRUE(table->Put(d1, 9527, db, 0));
    std::string d2 = "d2";
    ASSERT_TRUE(table->Put(d2, 9527, db, 1));
    std::string d3 = "d3";
    ASSERT_TRUE(table->Put(d3, 9527, db, 2));
    table->RecordCntIncr(1);
    ASSERT_EQ(3, table->GetRecordIdxCnt());
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(0, table->GetRecordCnt());
    ASSERT_EQ(0, table->GetRecordIdxCnt());
}

TEST_F(TableTest, SchedGcHead) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->SetTTLType(::rtidb::api::TTLType::kLatestTime);
    table->Init();
    table->Put("test", 2, "test1", 5);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 1, "test2", 5);
    ASSERT_EQ(2, table->GetRecordCnt());
    ASSERT_EQ(2, table->GetRecordIdxCnt());
    ASSERT_EQ(1, table->GetRecordPkCnt());
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(1, table->GetRecordCnt());
    ASSERT_EQ(1, table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());
}

TEST_F(TableTest, SchedGcHead1) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    uint64_t keep_cnt = 500;
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, keep_cnt);
    table->SetTTLType(::rtidb::api::TTLType::kLatestTime);
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
        Iterator* it = table->NewIterator("test", ticket);

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
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", now, "tes2", 4);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 9527, "test", 4);
    ASSERT_EQ(2, table->GetRecordCnt());
    ASSERT_EQ(2, table->GetRecordIdxCnt());
    ASSERT_EQ(1, table->GetRecordPkCnt());

    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(1, table->GetRecordCnt());
    ASSERT_EQ(1, table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());

    Ticket ticket;
    Iterator* it = table->NewIterator("test", ticket);
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue()->data, it->GetValue()->size);
    ASSERT_EQ("tes2", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete table;
}

TEST_F(TableTest, OffSet) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->SetTimeOffset(-60 * 4);
    table->Put("test", now - 10 * 60 * 1000, "test", 4);
    table->Put("test", now - 3 * 60 * 1000, "test", 4);
    table->Put("test", now, "tes2", 4);
    table->Put("test", now + 3 * 60 * 1000, "tes2", 4);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);

    table->SetTimeOffset(0);
    table->SetExpire(false);
    count = table->SchedGc();
    ASSERT_EQ(0, count);
    table->SetExpire(true);
    count = table->SchedGc();
    ASSERT_EQ(1, count);
    {
        Ticket ticket;
        Iterator* it = table->NewIterator("test", ticket);
        it->Seek(now);
        ASSERT_TRUE(it->Valid());
        std::string value_str(it->GetValue()->data, it->GetValue()->size);
        ASSERT_EQ("tes2", value_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
        delete it;
    }
    
    ASSERT_EQ(table->GetRecordCnt(), 2);
    ASSERT_EQ(table->GetRecordIdxCnt(), 2);
    table->SetTimeOffset(120);
    count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetRecordCnt(), 1);
    ASSERT_EQ(table->GetRecordIdxCnt(), 1);
    delete table;
}

TEST_F(TableTest, TableDataCnt) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();
    ASSERT_EQ(table->GetRecordCnt(), 0);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    ASSERT_EQ(table->GetRecordCnt(), 2);
    ASSERT_EQ(table->GetRecordIdxCnt(), 2);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetRecordCnt(), 1);
    ASSERT_EQ(table->GetRecordIdxCnt(), 1);
    delete table;
}

TEST_F(TableTest, TableUnref) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1 ,8 , mapping, 1);
    table->Init();
    table->Put("test", 9527, "test", 4);
    delete table;
}

TEST_F(TableTest, TableIterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 0);
    table->Init();

    table->Put("pk", 9527, "test1", 5);
    table->Put("pk1", 9527, "test2", 5);
    table->Put("pk", 9528, "test3", 5);
    table->Put("pk1", 100, "test4", 5);
    table->Put("test", 20, "test5", 5);
    // Ticket ticket;
    TableIterator* it = table->NewTableIterator(0);
    // it->Test();
    it->SeekToFirst();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9528, it->GetKey());
    it->Next();
    ASSERT_STREQ("pk", it->GetPK().c_str());
    ASSERT_EQ(9527, it->GetKey());
    it->Next();
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, it->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(100, it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());

    it->Seek("none", 11111);
    ASSERT_TRUE(it->Valid());

    it->Seek("test", 30);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("test", it->GetPK().c_str());
    ASSERT_EQ(20, it->GetKey());

    it->Seek("test", 20);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, it->GetKey());
    delete it;
    delete table;

    Table* table1 = new Table("tx_log", 1, 1, 8, mapping, 2);
    table1->Init();
    table1->SetTTLType(::rtidb::api::TTLType::kLatestTime);

    table1->Put("pk", 9527, "test1", 5);
    table1->Put("pk1", 9527, "test2", 5);
    table1->Put("pk", 9528, "test3", 5);
    table1->Put("pk1", 100, "test4", 5);
    table1->Put("test", 20, "test5", 5);
    table1->Put("pk", 200, "test6", 5);
    // Ticket ticket;
    TableIterator* it1 = table1->NewTableIterator(0);
    it1->Seek("pk", 9528);
    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("pk", it1->GetPK().c_str());
    ASSERT_EQ(9527, it1->GetKey());
    it1->Next();
    ASSERT_TRUE(it1->Valid());
    ASSERT_STREQ("test", it1->GetPK().c_str());
    ASSERT_EQ(20, it1->GetKey());
    it->Next();
    ASSERT_STREQ("pk1", it->GetPK().c_str());
    ASSERT_EQ(9527, it->GetKey());

}

TEST_F(TableTest, TableIteratorNoPk) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 0);
    table->Init();

    table->Put("pk10", 9527, "test10", 5);
    table->Put("pk8", 9526, "test8", 5);
    table->Put("pk6", 9525, "test6", 5);
    table->Put("pk4", 9524, "test4", 5);
    table->Put("pk2", 9523, "test2", 5);
    table->Put("pk0", 9522, "test0", 5);
    // Ticket ticket;
    TableIterator* it = table->NewTableIterator(0);
    it->SeekToFirst();
    ASSERT_STREQ("pk10", it->GetPK().c_str());
    ASSERT_EQ(9527, it->GetKey());
    it->Next();
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, it->GetKey());
    it->Next();
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, it->GetKey());
    it->Next();
    ASSERT_STREQ("pk0", it->GetPK().c_str());
    ASSERT_EQ(9522, it->GetKey());
    delete it;
    it = table->NewTableIterator(0);
    it->Seek("pk4", 9526);
    ASSERT_STREQ("pk4", it->GetPK().c_str());
    ASSERT_EQ(9524, it->GetKey());
    delete it;

    ASSERT_TRUE(table->Delete("pk4", 0));
    it = table->NewTableIterator(0);
    it->Seek("pk4", 9526);
    ASSERT_TRUE(it->Valid());
    ASSERT_STREQ("pk8", it->GetPK().c_str());
    ASSERT_EQ(9526, it->GetKey());
    it->Next();
    ASSERT_STREQ("pk0", it->GetPK().c_str());
    ASSERT_EQ(9522, it->GetKey());
    delete it;
}

TEST_F(TableTest, TableIteratorCount) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 0);
    table->Init();
    for (int i = 0; i < 100000; i++) {
        std::string value = "test" + std::to_string(i);
        table->Put("pk" + std::to_string(i), 9527 + i, value.c_str(), value.size());
    }
    TableIterator* it = table->NewTableIterator(0);
    it->SeekToFirst();
    int count = 0;
    while(it->Valid()) {
        count++;
        it->Next();
    }
    ASSERT_EQ(100000, count);
    delete it;
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    return RUN_ALL_TESTS();
}




