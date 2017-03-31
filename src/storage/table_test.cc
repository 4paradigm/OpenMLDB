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

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




