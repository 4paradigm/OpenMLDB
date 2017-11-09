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

namespace rtidb {
namespace storage {

class TableTest : public ::testing::Test {

public:
    TableTest() {}
    ~TableTest() {}
};

TEST_F(TableTest, Put) {
    Table* table = new Table("tx_log", 1, 1, 8, 10);
    table->Init();
    table->Put("test", 9537, "test", 4);
    delete table;
}

TEST_F(TableTest, Release) {
    Table* table = new Table("tx_log", 1, 1, 8, 10);
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->Put("test2", 9537, "test", 4);
    uint64_t size = table->Release();
    ASSERT_EQ(4 + 8 + 4 + 4 + 5 + 8 + 4 + 4, size);
    delete table;
}

TEST_F(TableTest, IsExpired) {
    // table ttl is 1
    Table* table = new Table("tx_log", 1, 1, 8, 1);
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
    Table* table = new Table("tx_log", 1, 1, 8, 10);
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

TEST_F(TableTest, SchedGc) {
    Table* table = new Table("tx_log", 1, 1, 8 , 1);
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
        Table::Iterator* it = table->NewIterator("test", ticket);
        it->Seek(now);
        ASSERT_TRUE(it->Valid());
        std::string value_str(it->GetValue()->data, it->GetValue()->size);
        ASSERT_EQ("tes2", value_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
        delete it;
    }
    
    ASSERT_EQ(table->GetDataCnt(), 2);
    table->SetTimeOffset(120);
    count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetDataCnt(), 1);
    delete table;
}

TEST_F(TableTest, TableDataCnt) {
    Table* table = new Table("tx_log", 1, 1, 8 , 1);
    table->Init();
    ASSERT_EQ(table->GetDataCnt(), 0);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    ASSERT_EQ(table->GetDataCnt(), 2);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetDataCnt(), 1);
    delete table;
}

TEST_F(TableTest, TableUnref) {
    Table* table = new Table("tx_log", 1, 1 ,8 , 1);
    table->Init();
    table->Put("test", 9527, "test", 4);
    delete table;
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




