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

#include "gtest/gtest.h"
#include "codec/schema_codec.h"
#include "schema/index_util.h"

namespace openmldb {
namespace schema {

using ::openmldb::codec::SchemaCodec;

class IndexTest : public ::testing::Test {
 public:
    IndexTest() {}
    ~IndexTest() {}
};

TEST_F(IndexTest, CheckUnique) {
    PBIndex indexs;
    auto index = indexs.Add();
    index->set_index_name("index1");
    index->add_col_name("col1");
    index = indexs.Add();
    index->set_index_name("index2");
    index->add_col_name("col2");
    ASSERT_TRUE(IndexUtil::CheckUnique(indexs).OK());
    index = indexs.Add();
    index->set_index_name("index3");
    index->add_col_name("col2");
    ASSERT_FALSE(IndexUtil::CheckUnique(indexs).OK());
}

TEST_F(IndexTest, CheckExist) {
    openmldb::nameserver::TableInfo table_info;
    SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_info.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetIndex(table_info.add_column_key(), "index1", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    auto index2 = table_info.add_column_key();
    SchemaCodec::SetIndex(index2, "index2", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    index2->set_flag(1);

    ::openmldb::common::ColumnKey test_index1;
    SchemaCodec::SetIndex(&test_index1, "test_index1", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    ::openmldb::common::ColumnKey test_index2;
    SchemaCodec::SetIndex(&test_index2, "test_index2", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    ::openmldb::common::ColumnKey test_index3;
    SchemaCodec::SetIndex(&test_index3, "test_index3", "mcc", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    ::openmldb::common::ColumnKey test_index4;
    SchemaCodec::SetIndex(&test_index4, "index1", "aa", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);

    ASSERT_TRUE(IndexUtil::IsExist(test_index1, table_info.column_key()));
    ASSERT_TRUE(IndexUtil::IsExist(test_index2, table_info.column_key()));
    ASSERT_FALSE(IndexUtil::IsExist(test_index3, table_info.column_key()));
    ASSERT_TRUE(IndexUtil::IsExist(test_index4, table_info.column_key()));
}

}  // namespace schema
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
