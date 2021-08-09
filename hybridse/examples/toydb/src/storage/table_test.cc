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

#include <sys/time.h>
#include <string>
#include "codec/fe_row_codec.h"
#include "gtest/gtest.h"
#include "storage/table_impl.h"

namespace hybridse {
namespace storage {
using codec::RowBuilder;
using codec::RowView;

class TableTest : public ::testing::Test {
 public:
    TableTest() {}
    ~TableTest() {}
};

TEST_F(TableTest, SingleIndexIterator) {
    ::hybridse::type::TableDef def;
    ::hybridse::type::ColumnDef* col = def.add_columns();
    col->set_name("col1");
    col->set_type(::hybridse::type::kVarchar);
    col = def.add_columns();
    col->set_name("col2");
    col->set_type(::hybridse::type::kInt64);
    col = def.add_columns();
    col->set_name("col3");
    col->set_type(::hybridse::type::kVarchar);
    ::hybridse::type::IndexDef* index = def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->set_second_key("col2");

    Table table(1, 1, def);
    table.Init();

    RowBuilder builder(def.columns());
    uint32_t size = builder.CalTotalLength(10);

    // test empty table
    std::unique_ptr<TableIterator> iter = table.NewIterator("key2");
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());

    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key1", 4);
    builder.AppendInt64(11);
    builder.AppendString("value1", 6);
    table.Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key1", 4);
    builder.AppendInt64(22);
    builder.AppendString("value1", 6);
    table.Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key1", 4);
    builder.AppendInt64(33);
    builder.AppendString("value2", 6);
    table.Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key2", 4);
    builder.AppendInt64(11);
    builder.AppendString("value2", 6);
    table.Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("key2", 4);
    builder.AppendInt64(22);
    builder.AppendString("value2", 6);
    table.Put(row.c_str(), row.length());

    // test NewIterator(key)
    iter = table.NewIterator("key2");
    int count = 0;
    RowView view(def.columns());
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    while (iter->Valid()) {
        const char* ch = nullptr;
        uint32_t length = 0;
        view.GetValue(reinterpret_cast<const int8_t*>(iter->GetValue().data()),
                      2, &ch, &length);
        ASSERT_STREQ("value2", std::string(ch, length).c_str());
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 2);

    // test NewIterator(key)
    iter = table.NewIterator("key2", "index1");
    count = 0;
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    while (iter->Valid()) {
        const char* ch = nullptr;
        uint32_t length = 0;
        view.GetValue(reinterpret_cast<const int8_t*>(iter->GetValue().data()),
                      2, &ch, &length);
        ASSERT_STREQ("value2", std::string(ch, length).c_str());
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 2);

    // test NewTraverseIterator()
    iter = table.NewTraverseIterator();
    count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 5);

    // test NewTraverseIterator(index_name)
    iter = table.NewTraverseIterator("index1");
    count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 5);

    // test NewIterator(key, ts)
    iter = table.NewIterator("key2", 30);
    count = 0;
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 2);
    iter = table.NewIterator("key2", 11);
    count = 0;
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 1);
    iter = table.NewIterator("key2", 0);
    count = 0;
    ASSERT_FALSE(iter->Valid());

    // test NewTraverseIterator(index_name) with current Ts valid
    iter = table.NewTraverseIterator("index1");
    std::map<std::string, int32_t> key_counters;
    iter->SeekToFirst();

    // iterator 1st segment of pk key1
    while (iter->CurrentTsValid()) {
        iter->NextTs();
        count++;
    }
    ASSERT_EQ(count, 3);
    ASSERT_EQ(iter->GetPK().ToString(), "key1");

    // iterator 2nd segment of pk key1
    count = 0;
    iter->NextTsInPks();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->CurrentTsValid());
    while (iter->CurrentTsValid()) {
        iter->NextTs();
        count++;
    }
    ASSERT_EQ(count, 2);
    ASSERT_EQ(iter->GetPK().ToString(), "key2");
}

TEST_F(TableTest, MultiIndexIterator) {
    ::hybridse::type::TableDef def;
    ::hybridse::type::ColumnDef* col = def.add_columns();
    col->set_name("col1");
    col->set_type(::hybridse::type::kVarchar);
    col = def.add_columns();
    col->set_name("col2");
    col->set_type(::hybridse::type::kInt64);
    col = def.add_columns();
    col->set_name("col3");
    col->set_type(::hybridse::type::kVarchar);
    col = def.add_columns();
    col->set_name("col4");
    col->set_type(::hybridse::type::kInt64);

    ::hybridse::type::IndexDef* index = def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->set_second_key("col2");
    index = def.add_indexes();
    index->set_name("index2");
    index->add_first_keys("col3");
    index->set_second_key("col4");

    Table table(1, 1, def);
    table.Init();

    RowBuilder builder(def.columns());
    uint32_t size = builder.CalTotalLength(10);

    // test empty table
    std::unique_ptr<TableIterator> iter = table.NewIterator("i1_k2");
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());

    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("i1_k1", 5);
    builder.AppendInt64(11);
    builder.AppendString("i2_k1", 5);
    builder.AppendInt64(21);
    table.Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("i1_k1", 5);
    builder.AppendInt64(1);
    builder.AppendString("i2_k2", 5);
    builder.AppendInt64(11);
    table.Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("i1_k2", 5);
    builder.AppendInt64(12);
    builder.AppendString("i2_k1", 5);
    builder.AppendInt64(22);
    table.Put(row.c_str(), row.length());
    row.clear();
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    builder.AppendString("i1_k1", 5);
    builder.AppendInt64(2);
    builder.AppendString("i2_k2", 5);
    builder.AppendInt64(32);
    table.Put(row.c_str(), row.length());
    RowView view(def.columns());

    // test NewIterator(key, index_name)
    iter = table.NewIterator("i2_k2", "index2");
    int count = 0;
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    while (iter->Valid()) {
        const char* ch = nullptr;
        uint32_t length = 0;
        view.GetValue(reinterpret_cast<const int8_t*>(iter->GetValue().data()),
                      0, &ch, &length);
        ASSERT_STREQ("i1_k1", std::string(ch, length).c_str());
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 2);
    iter = table.NewIterator("i1_k2", "index1");
    count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        const char* ch = nullptr;
        uint32_t length = 0;
        view.GetValue(reinterpret_cast<const int8_t*>(iter->GetValue().data()),
                      2, &ch, &length);
        ASSERT_STREQ("i2_k1", std::string(ch, length).c_str());
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 1);

    // test NewIterator(key)
    iter = table.NewIterator("i1_k2");
    count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        const char* ch = nullptr;
        uint32_t length = 0;
        view.GetValue(reinterpret_cast<const int8_t*>(iter->GetValue().data()),
                      2, &ch, &length);
        ASSERT_STREQ("i2_k1", std::string(ch, length).c_str());
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 1);

    // test NewTraverseIterator()
    count = 0;
    iter = table.NewTraverseIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 4);

    // test NewTraverseIterator(index_name)
    count = 0;
    iter = table.NewTraverseIterator("index1");
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 4);

    // test NewTraverseIterator(index_name)
    count = 0;
    iter = table.NewTraverseIterator("index1");
    iter->SeekToFirst();
    ASSERT_EQ(iter->GetPK().ToString(), "i1_k2");
    while (iter->CurrentTsValid()) {
        iter->NextTs();
        count++;
    }
    ASSERT_EQ(count, 1);

    count = 0;
    iter->NextTsInPks();
    ASSERT_EQ(iter->GetPK().ToString(), "i1_k1");
    while (iter->CurrentTsValid()) {
        iter->NextTs();
        count++;
    }
    ASSERT_EQ(count, 3);

    // test NewIterator(key, ts)
    iter = table.NewIterator("i1_k1", 30);
    count = 0;
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 3);
    iter = table.NewIterator("i1_k1", 2);
    count = 0;
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 2);
    iter = table.NewIterator("key2", 0);
    count = 0;
    ASSERT_FALSE(iter->Valid());
}

TEST_F(TableTest, FullTableTest) {
    ::hybridse::type::TableDef def;
    ::hybridse::type::ColumnDef* col = def.add_columns();
    col->set_name("col1");
    col->set_type(::hybridse::type::kVarchar);
    col = def.add_columns();
    col->set_name("col2");
    col->set_type(::hybridse::type::kInt64);
    col = def.add_columns();
    col->set_name("col3");
    col->set_type(::hybridse::type::kVarchar);
    ::hybridse::type::IndexDef* index = def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->set_second_key("col2");

    Table table(1, 1, def);
    table.Init();

    // test empty table
    std::unique_ptr<TableIterator> iter = table.NewIterator("key2");
    int count = 0;
    RowView view(def.columns());
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());

    // test full table
    RowBuilder builder(def.columns());
    uint32_t size = builder.CalTotalLength(14);
    std::string row;
    int entry_count = 1000;
    char key[12];
    char value[12];
    for (int i = 0; i < entry_count; ++i) {
        row.resize(size);
        sprintf(key, "key%03d", i % 10);       // NOLINT
        sprintf(value, "value%03d", i % 100);  // NOLINT
        builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
        builder.AppendString(key, 6);
        builder.AppendInt64(i % 10);
        builder.AppendString(value, 8);
        table.Put(row.c_str(), row.length());
    }
    iter = table.NewTraverseIterator();
    iter->SeekToFirst();
    count = 0;
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 1000);
}
TEST_F(TableTest, DecodeKeysAndTsTest) {
    ::hybridse::type::TableDef def;
    ::hybridse::type::ColumnDef* col = def.add_columns();
    col->set_name("col1");
    col->set_type(::hybridse::type::kVarchar);
    col = def.add_columns();
    col->set_name("col2");
    col->set_type(::hybridse::type::kInt64);
    col = def.add_columns();
    col->set_name("col3");
    col->set_type(::hybridse::type::kVarchar);
    col = def.add_columns();
    col->set_name("col4");
    col->set_type(::hybridse::type::kInt64);

    ::hybridse::type::IndexDef* index = def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->add_first_keys("col4");
    index->set_second_key("col2");

    Table table(1, 1, def);
    table.Init();

    RowBuilder builder(def.columns());
    uint32_t size = builder.CalTotalLength(10);

    // test empty table
    std::unique_ptr<TableIterator> iter = table.NewIterator("i1_k2");
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());

    std::string row;
    {
        row.resize(size);
        builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
        builder.AppendString("i1_k1", 5);
        builder.AppendInt64(11);
        builder.AppendString("i2_k1", 5);
        builder.AppendInt64(21);
    }

    auto index_map = table.GetIndexMap();
    int64_t time = 1;
    std::string key;
    ASSERT_TRUE(table.DecodeKeysAndTs(index_map["index1"], row.c_str(),
                                      row.length(), key, &time));
    ASSERT_EQ("i1_k1|21", key);
    ASSERT_EQ(11L, time);
}
}  // namespace storage
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
