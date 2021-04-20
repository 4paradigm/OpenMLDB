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


#include "base/strings.h"
#include "codec/schema_codec.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"

namespace fedb {
namespace codec {

class SchemaCodecTest : public ::testing::Test {
 public:
    SchemaCodecTest() {}
    ~SchemaCodecTest() {}
};

TEST_F(SchemaCodecTest, Encode) {
    std::vector<ColumnDesc> columns;
    ColumnDesc desc1;
    desc1.name = "uname";
    desc1.type = ::fedb::codec::ColType::kString;
    desc1.add_ts_idx = true;
    columns.push_back(desc1);

    ColumnDesc desc2;
    desc2.name = "age";
    desc2.type = ::fedb::codec::ColType::kInt32;
    desc2.add_ts_idx = false;
    columns.push_back(desc2);

    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<ColumnDesc> decoded_columns;
    codec.Decode(buffer, decoded_columns);
    ASSERT_EQ(2, (int64_t)decoded_columns.size());
    ASSERT_EQ(::fedb::codec::ColType::kString, decoded_columns[0].type);
    ASSERT_EQ("uname", decoded_columns[0].name);
    ASSERT_EQ(::fedb::codec::ColType::kInt32, decoded_columns[1].type);
    ASSERT_EQ("age", decoded_columns[1].name);
}

TEST_F(SchemaCodecTest, Timestamp) {
    std::vector<ColumnDesc> columns;
    ColumnDesc desc1;
    desc1.name = "card";
    desc1.type = ::fedb::codec::ColType::kString;
    desc1.add_ts_idx = true;
    columns.push_back(desc1);

    ColumnDesc desc2;
    desc2.name = "ts";
    desc2.type = ::fedb::codec::ColType::kTimestamp;
    desc2.add_ts_idx = false;
    columns.push_back(desc2);

    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<ColumnDesc> decoded_columns;
    codec.Decode(buffer, decoded_columns);

    ASSERT_EQ(2, (int64_t)decoded_columns.size());
    ASSERT_EQ(::fedb::codec::ColType::kString, decoded_columns[0].type);
    ASSERT_EQ("card", decoded_columns[0].name);
    ASSERT_EQ(::fedb::codec::ColType::kTimestamp, decoded_columns[1].type);
    ASSERT_EQ("ts", decoded_columns[1].name);
}

TEST_F(SchemaCodecTest, Int16) {
    std::vector<ColumnDesc> columns;
    ColumnDesc desc1;
    desc1.name = "int16";
    desc1.type = ::fedb::codec::ColType::kInt16;
    desc1.add_ts_idx = false;
    columns.push_back(desc1);
    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<ColumnDesc> decoded_columns;
    codec.Decode(buffer, decoded_columns);
    ASSERT_EQ(1, (int64_t)decoded_columns.size());
    ASSERT_EQ(::fedb::codec::ColType::kInt16, decoded_columns[0].type);
    ASSERT_EQ("int16", decoded_columns[0].name);
}

TEST_F(SchemaCodecTest, UInt16) {
    std::vector<ColumnDesc> columns;
    ColumnDesc desc1;
    desc1.name = "uint16";
    desc1.type = ::fedb::codec::ColType::kUInt16;
    desc1.add_ts_idx = false;
    columns.push_back(desc1);
    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<ColumnDesc> decoded_columns;
    codec.Decode(buffer, decoded_columns);
    ASSERT_EQ(1, (int64_t)decoded_columns.size());
    ASSERT_EQ(::fedb::codec::ColType::kUInt16, decoded_columns[0].type);
    ASSERT_EQ("uint16", decoded_columns[0].name);
}

TEST_F(SchemaCodecTest, Bool) {
    std::vector<ColumnDesc> columns;
    ColumnDesc desc1;
    desc1.name = "bool";
    desc1.type = ::fedb::codec::ColType::kBool;
    desc1.add_ts_idx = false;
    columns.push_back(desc1);
    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<ColumnDesc> decoded_columns;
    codec.Decode(buffer, decoded_columns);
    ASSERT_EQ(1, (int64_t)decoded_columns.size());
    ASSERT_EQ(::fedb::codec::ColType::kBool, decoded_columns[0].type);
    ASSERT_EQ("bool", decoded_columns[0].name);
}

TEST_F(SchemaCodecTest, HasTSCol) {
    std::vector<ColumnDesc> columns;
    ASSERT_FALSE(SchemaCodec::HasTSCol(columns));
    ColumnDesc desc1;
    desc1.is_ts_col = false;
    columns.push_back(desc1);
    ASSERT_FALSE(SchemaCodec::HasTSCol(columns));
    ColumnDesc desc2;
    desc2.is_ts_col = true;
    columns.push_back(desc2);
    ASSERT_TRUE(SchemaCodec::HasTSCol(columns));
}

TEST_F(SchemaCodecTest, ConvertColumnDesc1) {
    std::vector<ColumnDesc> columns;
    ::fedb::nameserver::TableInfo table_info;
    ASSERT_EQ(0, SchemaCodec::ConvertColumnDesc(table_info, columns));
    ASSERT_TRUE(columns.empty());
    ::fedb::common::ColumnDesc* desc = table_info.add_column_desc();
    desc->set_name("col1");
    desc->set_type("notype");
    ASSERT_EQ(-1, SchemaCodec::ConvertColumnDesc(table_info, columns));
    table_info.Clear();
    desc = table_info.add_column_desc();
    desc->set_name("col1");
    desc->set_type("string");
    desc->set_type("string");
    desc->set_add_ts_idx(true);
    desc->set_is_ts_col(false);
    desc = table_info.add_column_desc();
    desc->set_name("col2");
    desc->set_type("int32");
    desc->set_add_ts_idx(false);
    desc->set_is_ts_col(true);
    ASSERT_EQ(0, SchemaCodec::ConvertColumnDesc(table_info, columns));
    ASSERT_EQ(2, (int64_t)columns.size());
    ASSERT_EQ("col1", columns[0].name);
    ASSERT_EQ(::fedb::codec::ColType::kString, columns[0].type);
    ASSERT_TRUE(columns[0].add_ts_idx);
    ASSERT_FALSE(columns[0].is_ts_col);
    ASSERT_FALSE(columns[1].add_ts_idx);
    ASSERT_TRUE(columns[1].is_ts_col);
}

TEST_F(SchemaCodecTest, ConvertColumnDesc2) {
    std::vector<ColumnDesc> columns;
    ::fedb::nameserver::TableInfo table_info;
    ::fedb::nameserver::ColumnDesc* desc = table_info.add_column_desc();
    desc->set_name("col1");
    desc->set_type("col1");
    ASSERT_EQ(-1, SchemaCodec::ConvertColumnDesc(table_info, columns));
    table_info.Clear();
    desc = table_info.add_column_desc();
    desc->set_name("col1");
    desc->set_type("string");
    desc->set_add_ts_idx(true);
    desc = table_info.add_column_desc();
    desc->set_name("col2");
    desc->set_type("uint64");
    desc->set_add_ts_idx(false);
    ASSERT_EQ(0, SchemaCodec::ConvertColumnDesc(table_info, columns));
    ASSERT_EQ(2, (int64_t)columns.size());
    ASSERT_EQ("col1", columns[0].name);
    ASSERT_EQ(::fedb::codec::ColType::kString, columns[0].type);
    ASSERT_TRUE(columns[0].add_ts_idx);
    ASSERT_FALSE(columns[0].is_ts_col);
    ASSERT_FALSE(columns[1].add_ts_idx);
    ASSERT_FALSE(columns[1].is_ts_col);
}

}  // namespace codec
}  // namespace fedb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
