//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/table.h"
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
    table->Ref();
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->UnRef();
}

TEST_F(TableTest, Iterator) {
    Table* table = new Table("tx_log", 1, 1, 8, 10);
    table->Ref();
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk1", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);

    Table::Iterator* it = table->NewIterator("pk");

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
}

TEST_F(TableTest, SchedGc) {
    Table* table = new Table("tx_log", 1, 1, 8 , 1);
    table->Ref();
    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    Table::Iterator* it = table->NewIterator("test");
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue()->data, it->GetValue()->size);
    ASSERT_EQ("tes2", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(TableTest, TableDataCnt) {
    Table* table = new Table("tx_log", 1, 1, 8 , 1);
    table->Ref();
    table->Init();
    ASSERT_EQ(table->GetDataCnt(), 0);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    ASSERT_EQ(table->GetDataCnt(), 2);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetDataCnt(), 1);
}



}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




