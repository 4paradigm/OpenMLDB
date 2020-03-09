/*
 * table_iterator_test.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "storage/table_iterator.h"
#include <sys/time.h>
#include <iostream>
#include <string>
#include "gtest/gtest.h"
#include "storage/table.h"

namespace fesql {
namespace storage {

void BuildTableSchema(type::TableDef& table_def) {  // NOLINT
    ::fesql::type::ColumnDef* col = table_def.add_columns();
    col->set_name("col1");
    col->set_type(::fesql::type::kVarchar);
    col = table_def.add_columns();
    col->set_name("col2");
    col->set_type(::fesql::type::kInt64);
    col = table_def.add_columns();
    col->set_name("col3");
    col->set_type(::fesql::type::kVarchar);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->set_second_key("col2");
}

class TableIteratorTest : public ::testing::Test {
 public:
    TableIteratorTest() {}
    ~TableIteratorTest() {}
};

TEST_F(TableIteratorTest, empty_window_table) {
    type::TableDef table_def;
    BuildTableSchema(table_def);
    std::shared_ptr<Table> table(new Table(1, 1, table_def));
    table->Init();
    WindowTableIterator it(table->GetSegments(), table->GetSegCnt(), 0, table);
    ASSERT_FALSE(it.Valid());
}

TEST_F(TableIteratorTest, empty_full_table) {
    type::TableDef table_def;
    BuildTableSchema(table_def);
    std::shared_ptr<Table> table(new Table(1, 1, table_def));
    table->Init();
    FullTableIterator it(table->GetSegments(), table->GetSegCnt(), table);
    ASSERT_FALSE(it.Valid());
}

TEST_F(TableIteratorTest, it_full_table) {
    type::TableDef table_def;
    BuildTableSchema(table_def);
    std::shared_ptr<Table> table(new Table(1, 1, table_def));
    table->Init();
    RowBuilder builder(table_def.columns());
    uint32_t size = builder.CalTotalLength(10);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key1", 4);
    builder.AppendInt64(11);
    builder.AppendString("value1", 6);
    table->Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key2", 4);
    builder.AppendInt64(22);
    builder.AppendString("value2", 6);
    table->Put(row.c_str(), row.length());
    FullTableIterator it(table->GetSegments(), table->GetSegCnt(), table);
    RowView view(table_def.columns());
    ASSERT_TRUE(it.Valid());
    char* ch;
    uint32_t length = 0;
    view.GetValue(reinterpret_cast<const int8_t*>(it.GetValue().data()), 2, &ch,
                  &length);
    ASSERT_STREQ("value1", std::string(ch, length).c_str());
    it.Next();
    ASSERT_TRUE(it.Valid());
    view.GetValue(reinterpret_cast<const int8_t*>(it.GetValue().data()), 2, &ch,
                  &length);
    ASSERT_STREQ("value2", std::string(ch, length).c_str());
    it.Next();
    ASSERT_FALSE(it.Valid());
}

TEST_F(TableIteratorTest, it_window_table) {
    type::TableDef table_def;
    BuildTableSchema(table_def);
    std::shared_ptr<Table> table(new Table(1, 1, table_def));
    table->Init();
    RowBuilder builder(table_def.columns());
    uint32_t size = builder.CalTotalLength(10);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key1", 4);
    builder.AppendInt64(11);
    builder.AppendString("value1", 6);
    table->Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key2", 4);
    builder.AppendInt64(22);
    builder.AppendString("value2", 6);
    table->Put(row.c_str(), row.length());
    WindowTableIterator it(table->GetSegments(), table->GetSegCnt(), 0, table);
    ASSERT_TRUE(it.Valid());
    {
        auto key = it.GetKey();
        base::Slice key_expect("key1");
        ASSERT_EQ(0, key.compare(key_expect));
        auto wit = it.GetValue();
        wit->SeekToFirst();
        ASSERT_TRUE(wit->Valid());
        uint64_t ts = wit->GetKey();
        ASSERT_EQ(11u, ts);
        wit->Next();
        ASSERT_FALSE(wit->Valid());
    }
    it.Next();
    {
        auto key = it.GetKey();
        base::Slice key_expect("key2");
        ASSERT_EQ(0, key.compare(key_expect));
        auto wit = it.GetValue();
        wit->SeekToFirst();
        ASSERT_TRUE(wit->Valid());
        uint64_t ts = wit->GetKey();
        ASSERT_EQ(22u, ts);
        wit->Next();
        ASSERT_FALSE(wit->Valid());
    }
    it.Next();
    ASSERT_FALSE(it.Valid());
}

}  // namespace storage
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
