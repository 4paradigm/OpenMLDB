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

#include <vector>

#include "absl/strings/str_split.h"
#include "absl/strings/numbers.h"
#include "gtest/gtest.h"
#include "codec/schema_codec.h"
#include "sdk/sdk_util.h"

namespace openmldb {
namespace sdk {

class SDKUtilTest : public ::testing::Test {
 public:
    SDKUtilTest() {}
    ~SDKUtilTest() {}
};

void SetColumnDesc(const std::vector<std::vector<std::string>>& col_vec,
        ::openmldb::nameserver::TableInfo* table_info) {
    table_info->clear_column_desc();
    for (const auto& col : col_vec) {
        auto col_desc = table_info->add_column_desc();
        col_desc->set_name(col[0]);
        col_desc->set_data_type(codec::DATA_TYPE_MAP.find(col[1])->second);
        if (col.size() > 2 && col[2] == "no") {
            col_desc->set_not_null(true);
        }
        if (col.size() > 3 && !col[3].empty()) {
            col_desc->set_default_value(col[3]);
        }
    }
}

void SetIndex(const std::vector<std::vector<std::string>>& index_vec,
        ::openmldb::nameserver::TableInfo* table_info) {
    table_info->clear_column_key();
    for (const auto& index_val : index_vec) {
        auto index = table_info->add_column_key();
        index->set_index_name(index_val[0]);
        std::vector<std::string> list = absl::StrSplit(index_val[1], ",");
        for (const auto& name : list) {
            index->add_col_name(name);
        }
        if (!index_val[2].empty()) {
            index->set_ts_name(index_val[2]);
        }
        auto ttl = index->mutable_ttl();
        int abs_ttl;
        int lat_ttl;
        ASSERT_TRUE(absl::SimpleAtoi(index_val[4], &abs_ttl));
        ASSERT_TRUE(absl::SimpleAtoi(index_val[5], &lat_ttl));
        if (index_val[3] == "absolute") {
            ttl->set_ttl_type(openmldb::type::TTLType::kAbsoluteTime);
            ttl->set_abs_ttl(abs_ttl);
        } else if (index_val[3] == "latest") {
            ttl->set_ttl_type(openmldb::type::TTLType::kLatestTime);
            ttl->set_lat_ttl(lat_ttl);
        } else if (index_val[3] == "absorlat") {
            ttl->set_ttl_type(openmldb::type::TTLType::kAbsAndLat);
            ttl->set_abs_ttl(abs_ttl);
            ttl->set_lat_ttl(lat_ttl);
        } else if (index_val[3] == "absandlat") {
            ttl->set_ttl_type(openmldb::type::TTLType::kAbsOrLat);
            ttl->set_abs_ttl(abs_ttl);
            ttl->set_lat_ttl(lat_ttl);
        }
    }
}

TEST_F(SDKUtilTest, GenCreateTableSQL) {
    ::openmldb::nameserver::TableInfo table_info;
    std::vector<std::vector<std::string>> col = { {"col1", "string"}, {"col2", "int"}, {"col3", "bigint", "no"}};
    std::vector<std::vector<std::string>> index = { {"index1", "col1", "", "absolute", "100", "0"}};
    SetColumnDesc(col, &table_info);
    SetIndex(index, &table_info);
    table_info.set_replica_num(1);
    table_info.set_partition_num(1);
    table_info.set_name("t1");
    std::string exp_ddl = "CREATE TABLE `t1` (\n"
        "`col1` string,\n"
        "`col2` int,\n"
        "`col3` bigInt NOT NULL,\n"
        "INDEX (KEY=`col1`, TTL_TYPE=ABSOLUTE, TTL=100m)\n"
        ") OPTIONS (PARTITIONNUM=1, REPLICANUM=1, STORAGE_MODE='Memory');";
    ASSERT_EQ(SDKUtil::GenCreateTableSQL(table_info), exp_ddl);
    std::vector<std::vector<std::string>> col1 = { {"col1", "string", "no", "aa"},
                                                   {"col2", "int"}, {"col3", "timestamp"}};
    std::vector<std::vector<std::string>> index1 = { {"index1", "col1", "", "absolute", "100", "0"},
                                                     {"index2", "col1,col2", "col3", "absorlat", "100", "10"}};
    SetColumnDesc(col1, &table_info);
    SetIndex(index1, &table_info);
    table_info.set_replica_num(3);
    table_info.set_partition_num(8);
    table_info.set_storage_mode(openmldb::common::StorageMode::kHDD);
    exp_ddl = "CREATE TABLE `t1` (\n"
        "`col1` string DEFAULT 'aa' NOT NULL,\n"
        "`col2` int,\n"
        "`col3` timestamp,\n"
        "INDEX (KEY=`col1`, TTL_TYPE=ABSOLUTE, TTL=100m),\n"
        "INDEX (KEY=(`col1`,`col2`), TS=`col3`, TTL_TYPE=ABSANDLAT, TTL=(100m, 10))\n"
        ") OPTIONS (PARTITIONNUM=8, REPLICANUM=3, STORAGE_MODE='HDD');";
    ASSERT_EQ(SDKUtil::GenCreateTableSQL(table_info), exp_ddl);
}

}  // namespace sdk
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
