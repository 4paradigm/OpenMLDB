//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/table.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace storage {

class TableTest : public ::testing::Test {

public:
    TableTest() {}
    ~TableTest() {}
};

TEST_F(TableTest, Put) {
    Table* table = new Table("tx_log", 1, 1, 8);
    table->Init();
    table->Put("test", 9537, "test", 4);
}

TEST_F(TableTest, Iterator) {
    Table* table = new Table("tx_log", 1, 1, 8);
    table->Init();

    table->Put("pk", 9527, "test", 4);
    table->Put("pk1", 9527, "test", 4);
    table->Put("pk", 9528, "test0", 5);

    Table::Iterator* it = table->NewIterator("pk");

    it->Seek(9527);
    ASSERT_TRUE(it->Valid());
    DataBlock* value1 = it->GetValue();
    std::string value_str(value1->data, value1->size);
    ASSERT_EQ("test", value_str);
    ASSERT_EQ(4, value1->size);
    it->Next();
    DataBlock* value2 = it->GetValue();
    std::string value2_str(value2->data, value2->size);
    ASSERT_EQ("test0", value2_str);
    ASSERT_EQ(5, value2->size);
    it->Next();
    ASSERT_FALSE(it->Valid());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




