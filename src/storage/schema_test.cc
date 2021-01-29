//
// schema_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2021-01-29
//

#include <iostream>
#include <string>
#include "base/glog_wapper.h"
#include "base/slice.h"
#include "gtest/gtest.h"
#include "storage/schema.h"

namespace rtidb {
namespace storage {

class SchemaTest : public ::testing::Test {};

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

}  // namespace storage
}  // namespace rtidb

int main(int argc, char** argv) {
    ::rtidb::base::SetLogLevel(INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
