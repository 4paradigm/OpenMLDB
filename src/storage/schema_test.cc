//
// schema_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2021-01-29
//

#include "storage/schema.h"

#include <iostream>
#include <string>

#include "base/glog_wapper.h"
#include "base/slice.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace storage {

class SchemaTest : public ::testing::Test {};

void AddIndex(::rtidb::api::TableMeta* table_meta, const std::string& index_name, const std::string& col,
              const std::string& ts_col, uint64_t abs_ttl, uint64_t lat_ttl, ::rtidb::type::TTLType ttl_type) {
    auto key = table_meta->add_column_key();
    key->set_index_name(index_name);
    key->add_col_name(col);
    if (!ts_col.empty()) {
        key->add_ts_name(ts_col);
    }
    auto ttl = key->mutable_ttl();
    ttl->set_ttl_type(ttl_type);
    ttl->set_abs_ttl(abs_ttl);
    ttl->set_lat_ttl(lat_ttl);
}

void AssertIndex(const ::rtidb::storage::IndexDef& index, const std::string& name, const std::string& col,
                 const std::string& ts_col_name, uint32_t ts_index, uint64_t abs_ttl, uint64_t lat_ttl,
                 ::rtidb::storage::TTLType ttl_type) {
    if (!name.empty()) {
        ASSERT_EQ(index.GetName(), name);
    }
    auto ttl = index.GetTTL();
    ASSERT_EQ(ttl->abs_ttl / (60 * 1000), abs_ttl);
    ASSERT_EQ(ttl->ttl_type, ttl_type);
    if (!ts_col_name.empty()) {
        auto ts_col = index.GetTsColumn();
        ASSERT_EQ(ts_col->GetName(), ts_col_name);
        ASSERT_EQ(ts_col->GetTsIdx(), ts_index);
    }
}

void AssertInnerIndex(const ::rtidb::storage::InnerIndexSt& inner_index, uint32_t id,
                      const std::vector<std::string>& index_vec, const std::vector<uint32_t> ts_vec) {
    ASSERT_EQ(inner_index.GetId(), id);
    const auto& indexs = inner_index.GetIndex();
    ASSERT_EQ(indexs.size(), index_vec.size());
    for (size_t i = 0; i < ts_vec.size(); i++) {
        ASSERT_EQ(indexs[i]->GetName(), index_vec[i]);
    }
    const auto& ts_idx = inner_index.GetTsIdx();
    ASSERT_EQ(ts_idx.size(), ts_vec.size());
    for (size_t i = 0; i < ts_vec.size(); i++) {
        ASSERT_EQ(ts_idx[i], ts_vec[i]);
    }
}

TEST_F(SchemaTest, TestNeedGc) {
    ::rtidb::storage::TTLSt ttl_st(0, 0, ::rtidb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(0, 1, ::rtidb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(1, 1, ::rtidb::storage::kAbsoluteTime);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(0, 0, ::rtidb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(1, 0, ::rtidb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(0, 1, ::rtidb::storage::kLatestTime);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(0, 0, ::rtidb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(1, 0, ::rtidb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(0, 1, ::rtidb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(1, 1, ::rtidb::storage::kAbsAndLat);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(0, 0, ::rtidb::storage::kAbsOrLat);
    ASSERT_FALSE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(1, 0, ::rtidb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(0, 1, ::rtidb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.NeedGc());
    ttl_st = ::rtidb::storage::TTLSt(1, 1, ::rtidb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.NeedGc());
}

TEST_F(SchemaTest, TestIsExpired) {
    ::rtidb::storage::TTLSt ttl_st(0, 0, ::rtidb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.IsExpired(100, 1));
    ttl_st = ::rtidb::storage::TTLSt(0, 1, ::rtidb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.IsExpired(100, 1));
    ttl_st = ::rtidb::storage::TTLSt(100, 2, ::rtidb::storage::kAbsoluteTime);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ttl_st = ::rtidb::storage::TTLSt(0, 0, ::rtidb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.IsExpired(200, 1));
    ttl_st = ::rtidb::storage::TTLSt(100, 2, ::rtidb::storage::kLatestTime);
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ASSERT_TRUE(ttl_st.IsExpired(200, 3));
    ttl_st = ::rtidb::storage::TTLSt(0, 0, ::rtidb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ttl_st = ::rtidb::storage::TTLSt(0, 2, ::rtidb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ttl_st = ::rtidb::storage::TTLSt(100, 0, ::rtidb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ttl_st = ::rtidb::storage::TTLSt(100, 2, ::rtidb::storage::kAbsAndLat);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ASSERT_FALSE(ttl_st.IsExpired(200, 1));
    ASSERT_FALSE(ttl_st.IsExpired(200, 0));
    ttl_st = ::rtidb::storage::TTLSt(0, 0, ::rtidb::storage::kAbsOrLat);
    ASSERT_FALSE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 0));
    ttl_st = ::rtidb::storage::TTLSt(0, 2, ::rtidb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_FALSE(ttl_st.IsExpired(0, 0));
    ASSERT_FALSE(ttl_st.IsExpired(50, 1));
    ASSERT_TRUE(ttl_st.IsExpired(0, 3));
    ttl_st = ::rtidb::storage::TTLSt(100, 0, ::rtidb::storage::kAbsOrLat);
    ASSERT_FALSE(ttl_st.IsExpired(200, 3));
    ASSERT_FALSE(ttl_st.IsExpired(200, 0));
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_TRUE(ttl_st.IsExpired(0, 0));
    ASSERT_TRUE(ttl_st.IsExpired(50, 0));
    ttl_st = ::rtidb::storage::TTLSt(100, 2, ::rtidb::storage::kAbsOrLat);
    ASSERT_TRUE(ttl_st.IsExpired(50, 3));
    ASSERT_TRUE(ttl_st.IsExpired(50, 1));
    ASSERT_TRUE(ttl_st.IsExpired(50, 0));
    ASSERT_TRUE(ttl_st.IsExpired(200, 3));
    ASSERT_FALSE(ttl_st.IsExpired(200, 1));
    ASSERT_FALSE(ttl_st.IsExpired(200, 0));
}

TEST_F(SchemaTest, ParseEmpty) {
    ::rtidb::api::TableMeta table_meta;
    std::map<std::string, uint8_t> ts_mapping;
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta, &ts_mapping), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 1);
    ASSERT_EQ(ts_mapping.size(), 0);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "idx0");
}

TEST_F(SchemaTest, ParseOld) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_ttl(10);
    table_meta.add_dimensions("index0");
    table_meta.add_dimensions("index1");
    table_meta.add_dimensions("index2");
    std::map<std::string, uint8_t> ts_mapping;
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta, &ts_mapping), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 3);
    ASSERT_EQ(ts_mapping.size(), 0);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "index0");
    index = table_index.GetIndex("index1");
    ASSERT_STREQ(index->GetName().c_str(), "index1");
    index = table_index.GetIndex("index2");
    ASSERT_STREQ(index->GetName().c_str(), "index2");
    auto ttl = index->GetTTL();
    ASSERT_EQ(ttl->abs_ttl / (60 * 1000), 10);
    ASSERT_EQ(ttl->ttl_type, ::rtidb::storage::kAbsoluteTime);
}

TEST_F(SchemaTest, ParseColumnDesc) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_ttl(10);
    for (int i = 0; i < 10; i++) {
        auto column_desc = table_meta.add_column_desc();
        column_desc->set_name("col" + std::to_string(i));
        column_desc->set_type("string");
        if (i < 5) {
            column_desc->set_add_ts_idx(true);
        }
    }
    std::map<std::string, uint8_t> ts_mapping;
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta, &ts_mapping), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 5);
    ASSERT_EQ(ts_mapping.size(), 0);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "col0");
    for (int i = 0; i < 5; i++) {
        std::string index_name = "col" + std::to_string(i);
        index = table_index.GetIndex(index_name);
        ASSERT_STREQ(index->GetName().c_str(), index_name.c_str());
        auto ttl = index->GetTTL();
        ASSERT_EQ(ttl->abs_ttl / (60 * 1000), 10);
        ASSERT_EQ(ttl->ttl_type, ::rtidb::storage::kAbsoluteTime);
    }
}

TEST_F(SchemaTest, ParseColumnDescMulTs) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_ttl(10);
    for (int i = 0; i < 10; i++) {
        auto column_desc = table_meta.add_column_desc();
        column_desc->set_name("col" + std::to_string(i));
        column_desc->set_type("string");
        if (i < 5) {
            column_desc->set_add_ts_idx(true);
        } else if (i > 7) {
            column_desc->set_is_ts_col(true);
            column_desc->set_type("uint64");
        }
    }
    std::map<std::string, uint8_t> ts_mapping;
    TableIndex table_index;
    ASSERT_LT(table_index.ParseFromMeta(table_meta, &ts_mapping), 0);
}

TEST_F(SchemaTest, ParseColumnDescTs) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_ttl(10);
    for (int i = 0; i < 10; i++) {
        auto column_desc = table_meta.add_column_desc();
        column_desc->set_name("col" + std::to_string(i));
        column_desc->set_type("string");
        if (i < 5) {
            column_desc->set_add_ts_idx(true);
        } else if (i == 6) {
            column_desc->set_is_ts_col(true);
            column_desc->set_type("uint64");
        }
    }
    std::map<std::string, uint8_t> ts_mapping;
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta, &ts_mapping), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 5);
    ASSERT_EQ(ts_mapping.size(), 1);
    auto index = table_index.GetPkIndex();
    ASSERT_STREQ(index->GetName().c_str(), "col0");
    for (int i = 0; i < 5; i++) {
        std::string index_name = "col" + std::to_string(i);
        index = table_index.GetIndex(index_name);
        ASSERT_STREQ(index->GetName().c_str(), index_name.c_str());
        auto ttl = index->GetTTL();
        ASSERT_EQ(ttl->abs_ttl / (60 * 1000), 10);
        ASSERT_EQ(ttl->ttl_type, ::rtidb::storage::kAbsoluteTime);
        auto ts_col = index->GetTsColumn();
        ASSERT_EQ(ts_col->GetTsIdx(), 0);
    }
}

TEST_F(SchemaTest, ParseMultiTTL) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_ttl(10);
    for (int i = 0; i < 10; i++) {
        auto column_desc = table_meta.add_column_desc();
        column_desc->set_name("col" + std::to_string(i));
        column_desc->set_type("string");
        if (i > 6) {
            column_desc->set_type("uint64");
        }
    }
    AddIndex(&table_meta, "key1", "col0", "col7", 100, 0, ::rtidb::type::kAbsoluteTime);
    AddIndex(&table_meta, "key2", "col0", "col8", 0, 1, ::rtidb::type::kLatestTime);
    AddIndex(&table_meta, "key3", "col1", "col9", 100, 1, ::rtidb::type::kAbsAndLat);
    AddIndex(&table_meta, "key4", "col2", "col9", 200, 1, ::rtidb::type::kAbsOrLat);
    AddIndex(&table_meta, "key5", "col3", "col7", 300, 0, ::rtidb::type::kAbsoluteTime);
    AddIndex(&table_meta, "key6", "col1", "col8", 0, 1, ::rtidb::type::kAbsoluteTime);
    AddIndex(&table_meta, "key7", "col5", "col8", 400, 2, ::rtidb::type::kAbsOrLat);
    std::map<std::string, uint8_t> ts_mapping;
    TableIndex table_index;
    ASSERT_GE(table_index.ParseFromMeta(table_meta, &ts_mapping), 0);
    auto indexs = table_index.GetAllIndex();
    ASSERT_EQ(indexs.size(), 7);
    ASSERT_EQ(ts_mapping.size(), 3);
    auto index = table_index.GetPkIndex();
    AssertIndex(*index, "key1", "col0", "col7", 0, 100, 0, ::rtidb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key1")), "key1", "col0", "col7", 0, 100, 0, ::rtidb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key2")), "key2", "col0", "col8", 1, 0, 1, ::rtidb::storage::kLatestTime);
    AssertIndex(*(table_index.GetIndex("key3")), "key3", "col1", "col9", 2, 100, 1, ::rtidb::storage::kAbsAndLat);
    AssertIndex(*(table_index.GetIndex("key4")), "key4", "col2", "col9", 2, 200, 0, ::rtidb::storage::kAbsOrLat);
    AssertIndex(*(table_index.GetIndex("key5")), "key5", "col3", "col7", 0, 300, 0, ::rtidb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key6")), "key6", "col1", "col8", 1, 0, 1, ::rtidb::storage::kAbsoluteTime);
    AssertIndex(*(table_index.GetIndex("key7")), "key7", "col5", "col8", 1, 400, 2, ::rtidb::storage::kAbsOrLat);
    auto inner_index = table_index.GetAllInnerIndex();
    ASSERT_EQ(inner_index->size(), 5);
    std::vector<std::string> index0 = {"key1", "key2"};
    std::vector<uint32_t> ts_vec0 = {0, 1};
    AssertInnerIndex(*(table_index.GetInnerIndex(0)), 0, index0, ts_vec0);
    std::vector<std::string> index1 = {"key3", "key6"};
    std::vector<uint32_t> ts_vec1 = {2, 1};
    AssertInnerIndex(*(table_index.GetInnerIndex(1)), 1, index1, ts_vec1);
    std::vector<std::string> index2 = {"key4"};
    std::vector<uint32_t> ts_vec2 = {2};
    AssertInnerIndex(*(table_index.GetInnerIndex(2)), 2, index2, ts_vec2);
    std::vector<std::string> index3 = {"key5"};
    std::vector<uint32_t> ts_vec3 = {0};
    AssertInnerIndex(*(table_index.GetInnerIndex(3)), 3, index3, ts_vec3);
    std::vector<std::string> index4 = {"key7"};
    std::vector<uint32_t> ts_vec4 = {1};
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
}  // namespace rtidb

int main(int argc, char** argv) {
    ::rtidb::base::SetLogLevel(INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
