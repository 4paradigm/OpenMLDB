/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mem_catalog_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/25
 *--------------------------------------------------------------------------
 **/
#include "vm/mem_catalog.h"
#include "gtest/gtest.h"
#include "vm/test_base.h"
namespace fesql {
namespace vm {
using fesql::codec::Row;
class MemCataLogTest : public ::testing::Test {
 public:
    MemCataLogTest() {}
    ~MemCataLogTest() {}
};

TEST_F(MemCataLogTest, row_test) {
    std::vector<Row> rows;
    ::fesql::type::TableDef table;
    BuildRows(table, rows);

    codec::RowView row_view(table.columns());
    row_view.Reset(rows[3].buf());
    std::string str = "4444";
    std::string str0 = "0";
    {
        char* s;
        uint32_t size;
        row_view.GetString(0, &s, &size);
        ASSERT_EQ("1", std::string(s, size));
    }
    {
        int32_t value;
        row_view.GetInt32(1, &value);
        ASSERT_EQ(4, value);
    }
    {
        int16_t value;
        row_view.GetInt16(2, &value);
        ASSERT_EQ(55, value);
    }
    {
        float value;
        row_view.GetFloat(3, &value);
        ASSERT_EQ(4.4f, value);
    }
    {
        double value;
        row_view.GetDouble(4, &value);
        ASSERT_EQ(44.4, value);
    }
    {
        int64_t value;
        row_view.GetInt64(5, &value);
        ASSERT_EQ(2, value);
    }
    {
        char* s;
        uint32_t size;
        row_view.GetString(6, &s, &size);
        ASSERT_EQ("4444", std::string(s, size));
    }
}
TEST_F(MemCataLogTest, mem_table_handler_test) {
    std::vector<Row> rows;
    ::fesql::type::TableDef table;
    BuildRows(table, rows);
    vm::MemTableHandler table_handler("t1", "temp", &(table.columns()));
    for (auto row : rows) {
        table_handler.AddRow(row);
    }
}
TEST_F(MemCataLogTest, mem_segment_handler_test) {
    std::vector<Row> rows;
    ::fesql::type::TableDef table;
    BuildRows(table, rows);
    vm::MemTimeTableHandler table_handler("t1", "temp", &(table.columns()));
    for (auto row : rows) {
        table_handler.AddRow(row);
    }
    auto iter = table_handler.GetIterator();

    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[0].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[0].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[1].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[1].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[2].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[2].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[3].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[3].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[4].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[4].size());

    iter->Next();
    ASSERT_FALSE(iter->Valid());
}

TEST_F(MemCataLogTest, mem_table_iterator_test) {
    std::vector<Row> rows;
    ::fesql::type::TableDef table;
    BuildRows(table, rows);
    vm::MemTimeTableHandler table_handler("t1", "temp", &(table.columns()));
    uint64_t ts = 1;
    for (auto row : rows) {
        table_handler.AddRow(ts++, row);
    }

    table_handler.Sort(false);

    auto iter = table_handler.GetIterator();

    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[4].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[4].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[3].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[3].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[2].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[2].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[1].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[1].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[0].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[0].size());

    iter->Next();
    ASSERT_FALSE(iter->Valid());

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[4].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[4].size());

    iter->Seek(3);
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                    iter->GetValue().data())) == rows[2].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[2].size());
}

TEST_F(MemCataLogTest, mem_partition_test) {
    std::vector<Row> rows;
    ::fesql::type::TableDef table;
    BuildRows(table, rows);
    vm::MemPartitionHandler partition_handler("t1", "temp", &(table.columns()));

    uint64_t ts = 1;
    for (auto row : rows) {
        partition_handler.AddRow("group2", ts++, row);
    }

    for (auto row : rows) {
        partition_handler.AddRow("group1", ts++, row);
    }

    partition_handler.Sort(false);
    auto window_iter = partition_handler.GetWindowIterator();

    window_iter->SeekToFirst();
    ASSERT_TRUE(window_iter->Valid());

    {
        auto iter = window_iter->GetValue();
        ASSERT_EQ(Row("group2"), window_iter->GetKey());
        while (iter->Valid()) {
            iter->Next();
        }
        std::cout << std::endl;

        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->GetValue().size(), rows[4].size());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[4].buf());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[3].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[3].size());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[2].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[2].size());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[1].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[1].size());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[0].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[0].size());

        iter->Next();
        ASSERT_FALSE(iter->Valid());

        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[4].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[4].size());

        iter->Seek(3);
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[2].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[2].size());
    }
    window_iter->Next();
    ASSERT_TRUE(window_iter->Valid());
    {
        auto iter = window_iter->GetValue();
        ASSERT_EQ(Row("group1"), window_iter->GetKey());
        while (iter->Valid()) {
            iter->Next();
        }
        std::cout << std::endl;

        iter->Seek(8);
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(reinterpret_cast<int8_t*>(const_cast<char*>(
                        iter->GetValue().data())) == rows[2].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[2].size());
    }
}

TEST_F(MemCataLogTest, mem_row_handler_test) {
    std::vector<Row> rows;
    ::fesql::type::TableDef table;
    BuildRows(table, rows);

    // construct test
    for (auto row : rows) {
        MemRowHandler row_hander(row, &table.columns());
        ASSERT_EQ(row, row_hander.GetValue());
    }
}

}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
