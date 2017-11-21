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
    table->RecordCntIncr();
    ASSERT_EQ(1, table->GetRecordCnt());
    Ticket ticket;
    Table::Iterator* it = table->NewIterator("test", ticket);
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

TEST_F(TableTest, MultiDimissionPut) {
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
    ASSERT_FALSE(table->IsExpired(entry, now_time));
    
    // ttl_offset_ is 60 * 1000
    ts_time = now_time - 4 * 60 * 1000; 
    entry.set_ts(ts_time);
    ASSERT_TRUE(table->IsExpired(entry, now_time));
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
    Table::Iterator* it = table->NewIterator("pk", ticket);

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
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);

}

TEST_F(TableTest, SchedGc) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    Ticket ticket;
    Table::Iterator* it = table->NewIterator("test", ticket);
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
    table->RecordCntIncr(4);
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
        Table::Iterator* it = table->NewIterator("test", ticket);
        it->Seek(now);
        ASSERT_TRUE(it->Valid());
        std::string value_str(it->GetValue()->data, it->GetValue()->size);
        ASSERT_EQ("tes2", value_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
        delete it;
    }
    
    ASSERT_EQ(table->GetRecordCnt(), 2);
    table->SetTimeOffset(120);
    count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetRecordCnt(), 1);
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
    table->RecordCntIncr(2);
    ASSERT_EQ(table->GetRecordCnt(), 2);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetRecordCnt(), 1);
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

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    return RUN_ALL_TESTS();
}




