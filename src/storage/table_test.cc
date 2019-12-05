//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#include "storage/table.h"
#include <string>
#include "gtest/gtest.h"
#include <sys/time.h>

namespace fesql {
namespace storage {
class TableTest : public ::testing::Test {
 public:
    TableTest() {}
    ~TableTest() {}
};

static inline long get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<long>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

TEST_F(TableTest, test) {
    TableDef table_def;
    ::fesql::type::ColumnDef* col = table_def.add_columns();
    col->set_name("col1");
    col->set_type(::fesql::type::kString);
    col = table_def.add_columns();
    col->set_name("col2");
    col->set_type(::fesql::type::kInt64);
    col = table_def.add_columns();
    col->set_name("col3");
    col->set_type(::fesql::type::kString);
    table_def.set_name("table1");
    IndexDef* def = table_def.add_indexes();
    def->set_name("index1");
    def->add_first_keys("col1");
    def->set_second_key("col2");
    RowBuilder builder(table_def.columns());
	uint32_t size = builder.CalTotalLength(9);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    std::string st("key1");
    std::string value("value");
    ASSERT_TRUE(builder.AppendString(st.c_str(), 4));
    ASSERT_TRUE(builder.AppendInt64(1));
    ASSERT_TRUE(builder.AppendString(value.c_str(), 5));
    RowView view(table_def.columns());
	uint64_t start_time = get_micros();
    for (int idx = 0; idx < 10000; idx++) {
        view.Reset((const int8_t*)row.c_str(), row.size());
        char* ch = NULL;
        uint32_t length = 0;
        view.GetString(0, &ch, &length);
    }
	uint64_t end_time = get_micros();
    printf("time uesed: %lu\n", end_time - start_time);
}    

TEST_F(TableTest, Put) {
    TableDef table_def;
    ::fesql::type::ColumnDef* col = table_def.add_columns();
    col->set_name("col1");
    col->set_type(::fesql::type::kString);
    col = table_def.add_columns();
    col->set_name("col2");
    col->set_type(::fesql::type::kInt64);
    col = table_def.add_columns();
    col->set_name("col3");
    col->set_type(::fesql::type::kString);
    table_def.set_name("table1");
    IndexDef* def = table_def.add_indexes();
    def->set_name("index1");
    def->add_first_keys("col1");
    def->set_second_key("col2");
    RowBuilder builder(table_def.columns());
	uint32_t size = builder.CalTotalLength(9);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    std::string st("key1");
    std::string value("value");
    ASSERT_TRUE(builder.AppendString(st.c_str(), 4));
    ASSERT_TRUE(builder.AppendInt64(1));
    ASSERT_TRUE(builder.AppendString(value.c_str(), 5));
    Table table(1, 1, table_def);
    table.Init();
    Table table1(1, 1, table_def);
    table1.Init();
	uint64_t start_time = get_micros();
	for (int idx = 0; idx < 10000; idx++) {
        table.Put(row.c_str(), row.size());
	}
	uint64_t end_time = get_micros();
    printf("time uesed: %lu\n", end_time - start_time);
	start_time = get_micros();
	for (int idx = 0; idx < 10000; idx++) {
        table1.Put(st, 1, row.c_str(), row.size());
	}
	end_time = get_micros();
    printf("time uesed: %lu\n", end_time - start_time);
}    

/*TEST_F(TableTest, Iterator) {
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
}*/

}  // namespace storage
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
