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

void BuildTableDefT1(::fesql::type::TableDef& table) {  // NOLINT
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }
}

void BuildTableDefT2(::fesql::type::TableDef& table) {  // NOLINT
    table.set_name("t1");
    table.set_catalog("db");

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }
}

void BuildRows(::fesql::type::TableDef& table,  // NOLINT
               std::vector<Row>& rows) {        // NOLINT
    BuildTableDefT1(table);
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "1";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(1);
        builder.AppendInt16(5);
        builder.AppendFloat(1.1f);
        builder.AppendDouble(11.1);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), 1);
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "22";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(2);
        builder.AppendInt16(5);
        builder.AppendFloat(2.2f);
        builder.AppendDouble(22.2);
        builder.AppendInt64(2);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "333";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(3);
        builder.AppendInt16(55);
        builder.AppendFloat(3.3f);
        builder.AppendDouble(33.3);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "4444";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(4);
        builder.AppendInt16(55);
        builder.AppendFloat(4.4f);
        builder.AppendDouble(44.4);
        builder.AppendInt64(2);
        builder.AppendString("4444", str.size());
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("2", 1);
        builder.AppendInt32(5);
        builder.AppendInt16(55);
        builder.AppendFloat(5.5f);
        builder.AppendDouble(55.5);
        builder.AppendInt64(3);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(ptr, total_size));
    }
}
void BuildT2Rows(::fesql::type::TableDef& table,  // NOLINT
                 std::vector<Row>& rows) {        // NOLINT
    BuildTableDefT2(table);
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "1";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendString(str.c_str(), 1);
        builder.AppendFloat(1.1f);
        builder.AppendDouble(11.1);
        builder.AppendInt16(5);
        builder.AppendInt32(1);
        builder.AppendInt64(1);
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "22";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendString(str.c_str(), str.size());
        builder.AppendFloat(2.2f);
        builder.AppendDouble(22.2);
        builder.AppendInt16(5);
        builder.AppendInt32(2);
        builder.AppendInt64(2);
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "333";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendString(str.c_str(), str.size());
        builder.AppendFloat(3.3f);
        builder.AppendDouble(33.3);
        builder.AppendInt16(55);
        builder.AppendInt32(3);
        builder.AppendInt64(1);
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "4444";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendString("4444", str.size());
        builder.AppendFloat(4.4f);
        builder.AppendDouble(44.4);
        builder.AppendInt16(55);
        builder.AppendInt32(4);
        builder.AppendInt64(2);
        rows.push_back(Row(ptr, total_size));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("2", 1);
        builder.AppendString(str.c_str(), str.size());
        builder.AppendFloat(5.5f);
        builder.AppendDouble(55.5);
        builder.AppendInt16(55);
        builder.AppendInt32(5);
        builder.AppendInt64(3);
        rows.push_back(Row(ptr, total_size));
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

    std::vector<Row> rows2;
    ::fesql::type::TableDef table2;
    BuildT2Rows(table2, rows2);
    auto table2_handler = std::shared_ptr<MemTableHandler>(
        new MemTableHandler("t2", "temp", &(table2.columns())));
    for (auto row : rows) {
        table2_handler->AddRow(row);
    }
    table_handler.AddOtherTable(table2_handler);
    ASSERT_EQ(1, table_handler.GetOtherTableCnt());


}
TEST_F(MemCataLogTest, mem_segment_handler_test) {
    std::vector<Row> rows;
    ::fesql::type::TableDef table;
    BuildRows(table, rows);
    vm::MemSegmentHandler table_handler("t1", "temp", &(table.columns()));
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
    vm::MemSegmentHandler table_handler("t1", "temp", &(table.columns()));
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

    // Add Row test
    MemRowHandler multi_row_handler(rows[0], &table.columns());
    for (auto row : rows) {
        multi_row_handler.AddOtherRow(row);
    }

    ASSERT_EQ(rows[0], multi_row_handler.GetOtherRow(0));
    ASSERT_EQ(rows[1], multi_row_handler.GetOtherRow(1));
    ASSERT_EQ(rows[2], multi_row_handler.GetOtherRow(2));
    ASSERT_EQ(rows[3], multi_row_handler.GetOtherRow(3));
    ASSERT_EQ(rows[4], multi_row_handler.GetOtherRow(4));
}

}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
