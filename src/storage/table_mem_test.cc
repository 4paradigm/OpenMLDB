//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/table.h"
#include "gtest/gtest.h"
#include "timer.h"
#ifdef TCMALLOC_ENABLE
#include "gperftools/heap-checker.h"
#endif

namespace rtidb {
namespace storage {

class TableMemTest : public ::testing::Test {

public:
    TableMemTest() {}
    ~TableMemTest() {}
};

TEST_F(TableMemTest, Memory) {

#ifdef TCMALLOC_ENABLE
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    HeapLeakChecker checker("test_mem");
    Table* table = new Table("tx_log", 1, 1, 8, 1);
    table->Init();
    for (uint32_t i = 0; i < 100; i++) {
        table->Put("test", now - i * 60 * 1000, "test", 4);
    }
    uint64_t cnt = table->SchedGc();
    ASSERT_EQ(98, cnt);
    Table::Iterator* it = table->NewIterator("test");
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(now, it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
    ASSERT_EQ(true, checker.NoLeaks());
#endif
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




