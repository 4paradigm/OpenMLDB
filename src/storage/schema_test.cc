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

#include "storage/schema.h"

#include <string>

#include "base/glog_wrapper.h"
#include "codec/schema_codec.h"
#include "gtest/gtest.h"

namespace openmldb {
namespace storage {

using ::openmldb::codec::SchemaCodec;

class SchemaTest : public ::testing::Test {};

void AssertIndex(const ::openmldb::storage::IndexDef& index, const std::string& name, const std::string& col,
                 const std::string& ts_col_name, uint32_t ts_index, uint64_t abs_ttl, uint64_t lat_ttl,
                 ::openmldb::storage::TTLType ttl_type) {
    if (!name.empty()) {
        ASSERT_EQ(index.GetName(), name);
    }
    auto ttl = index.GetTTL();
    ASSERT_EQ(ttl->abs_ttl / 60 / 1000, abs_ttl);
    ASSERT_EQ(ttl->ttl_type, ttl_type);
    const auto& ts_col = index.GetTsColumn();
    ASSERT_EQ(ts_col->GetName(), ts_col_name);
    ASSERT_EQ(ts_col->GetId(), ts_index);
}

void AssertInnerIndex(const ::openmldb::storage::InnerIndexSt& inner_index, uint32_t id,
                      const std::vector<std::string>& index_vec, const std::vector<uint32_t>& ts_vec) {
    ASSERT_EQ(inner_index.GetId(), id);
    const auto& indexes = inner_index.GetIndex();
    ASSERT_EQ(indexes.size(), index_vec.size());
    for (size_t i = 0; i < ts_vec.size(); i++) {
        ASSERT_EQ(indexes[i]->GetName(), index_vec[i]);
    }
    const auto& ts_idx = inner_index.GetTsIdx();
    ASSERT_EQ(ts_idx.size(), ts_vec.size());
    for (size_t i = 0; i < ts_vec.size(); i++) {
        ASSERT_EQ(ts_idx[i], ts_vec[i]);
    }
}

TEST_F(SchemaTest, TestNeedGc) {
    ::openmldb::storage::TTLSt ttl_st(0, 0, ::openmldb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(1, 1, ::openmldb::storage::kAbsoluteTime);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(0, 0, ::openmldb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(1, 0, ::openmldb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kLatestTime);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(0, 0, ::openmldb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(1, 0, ::openmldb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(1, 1, ::openmldb::storage::kAbsAndLat);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(0, 0, ::openmldb::storage::kAbsOrLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(1, 0, ::openmldb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::openmldb::storage::TTLSt(1, 1, ::openmldb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.NeedGc());
}

TEST_F(SchemaTest, TestIsExpired) {
    ::openmldb::storage::TTLSt ttl_st(0, 0, ::openmldb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.IsExpired(100, 1));
    ttl_st = ::openmldb::storage::TTLSt(0, 1, ::openmldb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.IsExpired(100, 1));
    ttl_st = ::openmldb::storage::TTLSt(100, 2, ::openmldb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ttl_st = ::openmldb::storage::TTLSt(0, 0, ::openmldb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.IsExpired(200, 1));
    ttl_st = ::openmldb::storage::TTLSt(100, 2, ::openmldb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ASSERT_TRUE(ttl_st.IsExpired(200, 3));
    ttl_st = ::openmldb::storage::TTLSt(0, 0, ::openmldb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ttl_st = ::openmldb::storage::TTLSt(0, 2, ::openmldb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ttl_st = ::openmldb::storage::TTLSt(100, 0, ::openmldb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ttl_st = ::openmldb::storage::TTLSt(100, 2, ::openmldb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ASSERT_FALSE(ttl_st.IsExpired(200, 1));
    ASSERT_FALSE(ttl_st.IsExpired(200, 0));
    ttl_st = ::openmldb::storage::TTLSt(0, 0, ::openmldb::storage::kAbsOrLat);
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ttl_st = ::openmldb::storage::TTLSt(0, 2, ::openmldb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ASSERT_TRUE(ttl_st.IsExpired(0, 3));
    ttl_st = ::openmldb::storage::TTLSt(100, 0, ::openmldb::storage::kAbsOrLat);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_FALSE(ttl_st.IsExpired(200, 0));
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_TRUE(ttl_st.IsExpired(0, 0));
    ASSERT_TRUE(ttl_st.IsExpired(50, 0));
    ttl_st = ::openmldb::storage::TTLSt(100, 2, ::openmldb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_TRUE(ttl_st.IsExpired(50, 1));
    ASSERT_TRUE(ttl_st.IsExpired(50, 0));
    ASSERT_TRUE(ttl_st.IsExpired(200, 3));
    ASSERT_FALSE(ttl_st.IsExpired(200, 1));
    ASSERT_FALSE(ttl_st.IsExpired(200, 0));
}

TEST_F(SchemaTest, ParseEmpty) {
    ::openmldb::api::TableMeta table_meta;
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta), 0);
    auto indexes = table_index.GetAllIndex();
    ASSERT_EQ(indexes.size(), 1u);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "idx0");
}

TEST_F(SchemaTest, ParseOld) {
    ::openmldb::api::TableMeta table_meta;
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 1u);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "idx0");
    index = table_index.GetIndex("idx0");
    auto ttl = index->GetTTL();
    ASSERT_EQ(ttl->abs_ttl / (60 * 1000), 0u);
    ASSERT_EQ(ttl->ttl_type, ::openmldb::storage::kAbsoluteTime);
}

TEST_F(SchemaTest, ColumnKey) {
    ::openmldb::api::TableMeta table_meta;
    for (int i = 0; i < 10; i++) {
        auto column_desc = table_meta.add_column_desc();
        column_desc->set_name("col" + std::to_string(i));
        column_desc->set_data_type(::openmldb::type::kString);
        if (i == 6 || i == 7) {
            column_desc->set_data_type(::openmldb::type::kBigInt);
        }
    }
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key1", "col1", "col6", ::openmldb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key2", "col1", "col7", ::openmldb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key3", "col2", "col6", ::openmldb::type::kAbsoluteTime, 10, 0);
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 3u);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "key1");
    auto aa = table_index.GetIndex("key1");
    AssertIndex(*(table_index.GetIndex("key1")), "key1", "col1", "col6", 6, 10, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key1", 6)), "key1", "col1", "col6", 6, 10, 0,
                ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key2")), "key2", "col1", "col7", 7, 10, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key3")), "key3", "col2", "col6", 6, 10, 0, ::openmldb::storage::kAbsoluteTime);
    auto inner_index = table_index.GetAllInnerIndex();
    ASSERT_EQ(inner_index->size(), 2u);
    std::vector<std::string> index0 = {"key1", "key2"};
    std::vector<uint32_t> ts_vec0 = {6, 7};
    AssertInnerIndex(*(table_index.GetInnerIndex(0)), 0, index0, ts_vec0);
    std::vector<std::string> index1 = {"key3"};
    std::vector<uint32_t> ts_vec1 = {6};
    AssertInnerIndex(*(table_index.GetInnerIndex(1)), 1, index1, ts_vec1);
}

TEST_F(SchemaTest, TsAndDefaultTs) {
    ::openmldb::api::TableMeta table_meta;
    for (int i = 0; i < 10; i++) {
        auto column_desc = table_meta.add_column_desc();
        column_desc->set_name("col" + std::to_string(i));
        column_desc->set_data_type(::openmldb::type::kString);
        if (i == 6 || i == 7) {
            column_desc->set_data_type(::openmldb::type::kBigInt);
        }
    }
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key1", "col1", "col6", ::openmldb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key2", "col1", "col7", ::openmldb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key3", "col2", "col6", ::openmldb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key4", "col2", "", ::openmldb::type::kAbsoluteTime, 10, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key5", "col3", "", ::openmldb::type::kAbsoluteTime, 10, 0);
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 5u);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "key1");
    auto aa = table_index.GetIndex("key1");
    AssertIndex(*(table_index.GetIndex("key1")), "key1", "col1", "col6", 6, 10, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key1", 6)), "key1", "col1", "col6", 6, 10, 0,
                ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key2")), "key2", "col1", "col7", 7, 10, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key3")), "key3", "col2", "col6", 6, 10, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key4")), "key4", "col2", DEFUALT_TS_COL_NAME, DEFUALT_TS_COL_ID,
            10, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key5")), "key5", "col3", DEFUALT_TS_COL_NAME, DEFUALT_TS_COL_ID,
            10, 0, ::openmldb::storage::kAbsoluteTime);
    auto inner_index = table_index.GetAllInnerIndex();
    ASSERT_EQ(inner_index->size(), 3u);
    std::vector<std::string> index0 = {"key1", "key2"};
    std::vector<uint32_t> ts_vec0 = {6, 7};
    AssertInnerIndex(*(table_index.GetInnerIndex(0)), 0, index0, ts_vec0);
    std::vector<std::string> index1 = {"key3", "key4"};
    std::vector<uint32_t> ts_vec1 = {6, DEFUALT_TS_COL_ID};
    AssertInnerIndex(*(table_index.GetInnerIndex(1)), 1, index1, ts_vec1);
    std::vector<std::string> index2 = {"key5"};
    std::vector<uint32_t> ts_vec2 = {DEFUALT_TS_COL_ID};
    AssertInnerIndex(*(table_index.GetInnerIndex(2)), 2, index2, ts_vec2);
}

TEST_F(SchemaTest, ParseMultiTTL) {
    ::openmldb::api::TableMeta table_meta;
    for (int i = 0; i < 10; i++) {
        auto column_desc = table_meta.add_column_desc();
        column_desc->set_name("col" + std::to_string(i));
        column_desc->set_data_type(::openmldb::type::kString);
        if (i > 6) {
            column_desc->set_data_type(::openmldb::type::kBigInt);
        }
    }
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key1", "col0", "col7", ::openmldb::type::kAbsoluteTime, 100, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key2", "col0", "col8", ::openmldb::type::kLatestTime, 0, 1);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key3", "col1", "col9", ::openmldb::type::kAbsAndLat, 100, 1);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key4", "col2", "col9", ::openmldb::type::kAbsOrLat, 200, 1);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key5", "col3", "col7", ::openmldb::type::kAbsoluteTime, 300, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key6", "col1", "col8", ::openmldb::type::kAbsoluteTime, 0, 1);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "key7", "col5", "col8", ::openmldb::type::kAbsOrLat, 400, 2);
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 7u);
    auto index = table_index.GetPkIndex();
    AssertIndex(*index, "key1", "col0", "col7", 7, 100, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key1")), "key1", "col0", "col7", 7, 100, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key2")), "key2", "col0", "col8", 8, 0, 1, ::openmldb::storage::kLatestTime);
    AssertIndex(*(table_index.GetIndex("key3")), "key3", "col1", "col9", 9, 100, 1, ::openmldb::storage::kAbsAndLat);
    AssertIndex(*(table_index.GetIndex("key4")), "key4", "col2", "col9", 9, 200, 0, ::openmldb::storage::kAbsOrLat);
    AssertIndex(*(table_index.GetIndex("key5")), "key5", "col3", "col7", 7, 300, 0, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key6")), "key6", "col1", "col8", 8, 0, 1, ::openmldb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key7")), "key7", "col5", "col8", 8, 400, 2, ::openmldb::storage::kAbsOrLat);
    auto inner_index = table_index.GetAllInnerIndex();
    ASSERT_EQ(inner_index->size(), 5u);
    std::vector<std::string> index0 = {"key1", "key2"};
    std::vector<uint32_t> ts_vec0 = {7, 8};
    AssertInnerIndex(*(table_index.GetInnerIndex(0)), 0, index0, ts_vec0);
    std::vector<std::string> index1 = {"key3", "key6"};
    std::vector<uint32_t> ts_vec1 = {9, 8};
    AssertInnerIndex(*(table_index.GetInnerIndex(1)), 1, index1, ts_vec1);
    std::vector<std::string> index2 = {"key4"};
    std::vector<uint32_t> ts_vec2 = {9};
    AssertInnerIndex(*(table_index.GetInnerIndex(2)), 2, index2, ts_vec2);
    std::vector<std::string> index3 = {"key5"};
    std::vector<uint32_t> ts_vec3 = {7};
    AssertInnerIndex(*(table_index.GetInnerIndex(3)), 3, index3, ts_vec3);
    std::vector<std::string> index4 = {"key7"};
    std::vector<uint32_t> ts_vec4 = {8};
    AssertInnerIndex(*(table_index.GetInnerIndex(4)), 4, index4, ts_vec4);
    ASSERT_EQ(table_index.GetInnerIndexPos(0), 0);
    ASSERT_EQ(table_index.GetInnerIndexPos(1), 0);
    ASSERT_EQ(table_index.GetInnerIndexPos(2), 1);
    ASSERT_EQ(table_index.GetInnerIndexPos(3), 2);
    ASSERT_EQ(table_index.GetInnerIndexPos(4), 3);
    ASSERT_EQ(table_index.GetInnerIndexPos(5), 1);
    ASSERT_EQ(table_index.GetInnerIndexPos(6), 4);
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::openmldb::base::SetLogLevel(INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
