//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#include "storage/table.h"
#include <string>
#include "gtest/gtest.h"

namespace fesql {
namespace storage {
class TableTest : public ::testing::Test {
 public:
    TableTest() {}
    ~TableTest() {}
};

TEST_F(TableTest, Iterator) {
    ASSERT_TRUE(true);
    Table table("test", 1, 1, 1);
    table.Init();
    table.Put("key1", 11, "value1", 6);
    table.Put("key1", 22, "value1", 6);
    table.Put("key2", 11, "value2", 6);
    table.Put("key2", 22, "value2", 6);
    TableIterator* iter = table.NewIterator("key2");
    int count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        ASSERT_STREQ("value2", iter->GetValue().ToString().c_str());
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 2);
    delete iter;

    iter = table.NewIterator();
    count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 4);
    delete iter;
}

}  // namespace storage
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
