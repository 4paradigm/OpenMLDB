/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/mem_catalog.h"
#include "gtest/gtest.h"
#include "vm/catalog_wrapper.h"
#include "testing/test_base.h"

namespace hybridse {
namespace vm {
using hybridse::codec::Row;
class MemCataLogTest : public ::testing::Test {
 public:
    MemCataLogTest() {}
    ~MemCataLogTest() {}
};

TEST_F(MemCataLogTest, row_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);

    codec::RowView row_view(table.columns());
    row_view.Reset(rows[3].buf());
    std::string str = "4444";
    std::string str0 = "0";
    {
        const char* s;
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
        const char* s;
        uint32_t size;
        row_view.GetString(6, &s, &size);
        ASSERT_EQ("4444", std::string(s, size));
    }
}
TEST_F(MemCataLogTest, mem_table_handler_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);
    vm::MemTableHandler table_handler("t1", "temp", &(table.columns()));
    for (auto row : rows) {
        table_handler.AddRow(row);
    }
}

Row project(const Row& row) {
    type::TableDef table1;
    BuildTableDef(table1);
    codec::RowView row_view(table1.columns());
    row_view.Reset(row.buf());

    type::TableDef table2;
    {
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kInt32);
            column->set_name("c1");
        }
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kFloat);
            column->set_name("c3");
        }
    }
    codec::RowBuilder builder(table2.columns());
    int32_t total_size = builder.CalTotalLength(0);
    int8_t* buf = static_cast<int8_t*>(malloc(total_size));
    builder.SetBuffer(buf, total_size);
    std::string str0 = row_view.GetStringUnsafe(0);
    builder.AppendInt32(row_view.GetInt32Unsafe(1) + 1);
    builder.AppendFloat(row_view.GetFloatUnsafe(3) + 2.0f);
    return Row(base::RefCountedSlice::Create(buf, total_size));
}
const bool predicate(const Row& row) {
    type::TableDef table1;
    BuildTableDef(table1);
    codec::RowView row_view(table1.columns());
    row_view.Reset(row.buf());

    type::TableDef table2;
    {
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kInt32);
            column->set_name("c1");
        }
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kFloat);
            column->set_name("c3");
        }
    }
    return row_view.GetInt32Unsafe(0) > 0;
}

class SimpleWrapperFun : public ProjectFun {
 public:
    SimpleWrapperFun() : ProjectFun() {}
    ~SimpleWrapperFun() {}
    Row operator()(const Row& row) const override { return project(row); }
};

TEST_F(MemCataLogTest, table_hander_wrapper_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);
    std::shared_ptr<MemTableHandler> table_handler =
        std::shared_ptr<MemTableHandler>(
            new vm::MemTableHandler("t1", "temp", &(table.columns())));
    for (auto row : rows) {
        table_handler->AddRow(row);
    }

    SimpleWrapperFun fn;
    vm::TableProjectWrapper wrapper(table_handler, &fn);

    type::TableDef table2;
    {
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kInt32);
            column->set_name("c1");
        }
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kFloat);
            column->set_name("c3");
        }
    }
    codec::RowView row_view(table2.columns());
    auto iter = wrapper.GetIterator();
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    row_view.Reset(iter->GetValue().buf());
    ASSERT_EQ(2, row_view.GetInt32Unsafe(0));
    ASSERT_EQ(3.1f, row_view.GetFloatUnsafe(1));
}

TEST_F(MemCataLogTest, partition_hander_wrapper_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);
    std::shared_ptr<vm::MemPartitionHandler> partition_handler =
        std::shared_ptr<vm::MemPartitionHandler>(
            new vm::MemPartitionHandler("t1", "temp", &(table.columns())));

    uint64_t ts = 1;
    for (auto row : rows) {
        partition_handler->AddRow("group2", ts++, row);
    }

    for (auto row : rows) {
        partition_handler->AddRow("group1", ts++, row);
    }

    partition_handler->Sort(false);

    SimpleWrapperFun fn;
    vm::PartitionProjectWrapper wrapper(partition_handler, &fn);

    type::TableDef table2;
    {
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kInt32);
            column->set_name("c1");
        }
        {
            ::hybridse::type::ColumnDef* column = table2.add_columns();
            column->set_type(::hybridse::type::kFloat);
            column->set_name("c3");
        }
    }
    codec::RowView row_view(table2.columns());
    auto window_iter = wrapper.GetWindowIterator();
    window_iter->SeekToFirst();
    ASSERT_TRUE(window_iter->Valid());

    {
        auto iter = window_iter->GetValue();
        ASSERT_EQ("group2", window_iter->GetKey().ToString());
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        row_view.Reset(iter->GetValue().buf());
        ASSERT_EQ(6, row_view.GetInt32Unsafe(0));
        ASSERT_EQ(7.5f, row_view.GetFloatUnsafe(1));

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        row_view.Reset(iter->GetValue().buf());
        ASSERT_EQ(5, row_view.GetInt32Unsafe(0));
        ASSERT_EQ(6.4f, row_view.GetFloatUnsafe(1));
    }
    window_iter->Next();
    ASSERT_TRUE(window_iter->Valid());
    {
        auto iter = window_iter->GetValue();
        ASSERT_EQ("group1", window_iter->GetKey().ToString());
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        row_view.Reset(iter->GetValue().buf());
        ASSERT_EQ(6, row_view.GetInt32Unsafe(0));
        ASSERT_EQ(7.5f, row_view.GetFloatUnsafe(1));
    }
}
TEST_F(MemCataLogTest, mem_time_table_handler_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);
    vm::MemTimeTableHandler table_handler("t1", "temp", &(table.columns()));
    for (size_t i = 0; i < rows.size(); i++) {
        table_handler.AddRow(i, rows[i]);
    }
    auto iter = table_handler.GetIterator();

    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[0].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[0].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[1].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[1].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[2].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[2].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[3].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[3].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[4].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[4].size());

    iter->Next();
    ASSERT_FALSE(iter->Valid());
}

TEST_F(MemCataLogTest, mem_table_iterator_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);
    vm::MemTimeTableHandler table_handler("t1", "temp", &(table.columns()));
    uint64_t ts = 1;
    for (auto row : rows) {
        table_handler.AddRow(ts++, row);
    }

    table_handler.Sort(false);

    auto iter = table_handler.GetIterator();

    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[4].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[4].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[3].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[3].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[2].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[2].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[1].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[1].size());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[0].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[0].size());

    iter->Next();
    ASSERT_FALSE(iter->Valid());

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[4].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[4].size());

    iter->Seek(3);
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->GetValue().buf() == rows[2].buf());
    ASSERT_EQ(iter->GetValue().size(), rows[2].size());
}

TEST_F(MemCataLogTest, mem_partition_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
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
        ASSERT_EQ("group2", window_iter->GetKey().ToString());
        while (iter->Valid()) {
            iter->Next();
        }
        std::cout << std::endl;

        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->GetValue().size(), rows[4].size());
        ASSERT_TRUE(iter->GetValue().buf() == rows[4].buf());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->GetValue().buf() == rows[3].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[3].size());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->GetValue().buf() == rows[2].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[2].size());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->GetValue().buf() == rows[1].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[1].size());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->GetValue().buf() == rows[0].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[0].size());

        iter->Next();
        ASSERT_FALSE(iter->Valid());

        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->GetValue().buf() == rows[4].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[4].size());

        iter->Seek(3);
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->GetValue().buf() == rows[2].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[2].size());
    }
    window_iter->Next();
    ASSERT_TRUE(window_iter->Valid());
    {
        auto iter = window_iter->GetValue();
        ASSERT_EQ("group1", window_iter->GetKey().ToString());
        while (iter->Valid()) {
            iter->Next();
        }
        std::cout << std::endl;

        iter->Seek(8);
        ASSERT_TRUE(iter->Valid());
        ASSERT_TRUE(iter->GetValue().buf() == rows[2].buf());
        ASSERT_EQ(iter->GetValue().size(), rows[2].size());
    }
}

TEST_F(MemCataLogTest, mem_row_handler_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);

    // construct test
    for (auto row : rows) {
        MemRowHandler row_hander(row, &table.columns());
        ASSERT_EQ(0, row.compare(row_hander.GetValue()));
    }
}
TEST_F(MemCataLogTest, mem_row_comcat_test) {
    std::vector<Row> rows;
    ::hybridse::type::TableDef table;
    BuildRows(table, rows);

    std::vector<Row> rows_right;
    ::hybridse::type::TableDef table_right;
    BuildT2Rows(table_right, rows_right);
    // construct test
    for (size_t i = 0; i < rows.size(); i++) {
        std::shared_ptr<MemRowHandler> row =
            std::shared_ptr<MemRowHandler>(new MemRowHandler(rows[i]));
        std::shared_ptr<MemRowHandler> right_row =
            std::shared_ptr<MemRowHandler>(new MemRowHandler(rows_right[i]));
        // test leftrow concat rightrow
        std::shared_ptr<RowHandler> concat_left_right =
            std::shared_ptr<RowHandler>(
                new RowCombineWrapper(row, 1, right_row, 1));
        Row concat_left_right_row = Row(1, rows[i], 1, rows_right[i]);
        ASSERT_EQ(0,
                  concat_left_right->GetValue().compare(concat_left_right_row));

        // tests left row concat right row concat right row
        std::shared_ptr<RowHandler> concat_left_right_right =
            std::shared_ptr<RowHandler>(
                new RowCombineWrapper(concat_left_right, 2, right_row, 1));
        Row concat_left_right_right_row =
            Row(2, concat_left_right_row, 1, rows_right[i]);
        ASSERT_EQ(0, concat_left_right_right->GetValue().compare(
                         concat_left_right_right_row));
    }
}

}  // namespace vm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
