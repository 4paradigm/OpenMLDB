//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#include "storage/table.h"
#include <sys/time.h>
#include <string>
#include "gtest/gtest.h"

namespace fesql {
namespace storage {
class TableTest : public ::testing::Test {
 public:
    TableTest() {}
    ~TableTest() {}
};

TEST_F(TableTest, SingleIndexIterator) {
    ::fesql::type::TableDef def;
    ::fesql::type::ColumnDef* col = def.add_columns();
    col->set_name("col1");
    col->set_type(::fesql::type::kVarchar);
    col = def.add_columns();
    col->set_name("col2");
    col->set_type(::fesql::type::kInt64);
    col = def.add_columns();
    col->set_name("col3");
    col->set_type(::fesql::type::kVarchar);
    ::fesql::type::IndexDef* index = def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col1");
    index->set_second_key("col2");

    Table table(1, 1, def);
    table.Init();

    RowBuilder builder(def.columns());
    uint32_t size = builder.CalTotalLength(10);
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
    std::unique_ptr<TableIterator> iter = table.NewIterator("key2");
    int count = 0;
    RowView view(def.columns());
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    while (iter->Valid()) {
        char* ch;
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
        char* ch;
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
    ASSERT_EQ(count, 4);

    // test NewTraverseIterator(index_name)
    iter = table.NewTraverseIterator("index1");
    count = 0;
    iter->SeekToFirst();
    while (iter->Valid()) {
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 4);

    // test NewTraverseIterator(key, ts)
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
}

TEST_F(TableTest, MultiIndexIterator) {
    ::fesql::type::TableDef def;
    ::fesql::type::ColumnDef* col = def.add_columns();
    col->set_name("col1");
    col->set_type(::fesql::type::kVarchar);
    col = def.add_columns();
    col->set_name("col2");
    col->set_type(::fesql::type::kInt64);
    col = def.add_columns();
    col->set_name("col3");
    col->set_type(::fesql::type::kVarchar);
    col = def.add_columns();
    col->set_name("col4");
    col->set_type(::fesql::type::kInt64);

    ::fesql::type::IndexDef* index = def.add_indexes();
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
    std::unique_ptr<TableIterator> iter = table.NewIterator("i2_k2", "index2");
    int count = 0;
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    while (iter->Valid()) {
        char* ch;
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
        char* ch;
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
        char* ch;
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

}  // namespace storage
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
