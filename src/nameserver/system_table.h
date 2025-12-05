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

#ifndef SRC_NAMESERVER_SYSTEM_TABLE_H_
#define SRC_NAMESERVER_SYSTEM_TABLE_H_

#include <memory>
#include <string>
#include "absl/strings/string_view.h"
#include "base/status.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/name_server.pb.h"

DECLARE_uint32(system_table_replica_num);
DECLARE_bool(skip_grant_tables);

namespace openmldb {
namespace nameserver {

constexpr const char* INTERNAL_DB = "__INTERNAL_DB";
constexpr const char* PRE_AGG_DB = "__PRE_AGG_DB";
constexpr const char* JOB_INFO_NAME = "JOB_INFO";
constexpr const char* PRE_AGG_META_NAME = "PRE_AGG_META_INFO";
constexpr const char* USER_INFO_NAME = "USER";


constexpr const char* INFORMATION_SCHEMA_DB = "INFORMATION_SCHEMA";
// start tables for INFORMATION_SCHEMA
constexpr const char* GLOBAL_VARIABLES = "GLOBAL_VARIABLES";
constexpr const char* DEPLOY_RESPONSE_TIME = "DEPLOY_RESPONSE_TIME";
// end tables for INFORMATION_SCHEMA

enum class SystemTableType {
    kJobInfo = 1,
    kPreAggMetaInfo = 2,
    kGlobalVariable = 3,
    kDeployResponseTime,
    kUser,
};

struct SystemTableInfo {
    SystemTableInfo(absl::string_view db, absl::string_view name)
        : db_(db), name_(name) {}

    // db name
    absl::string_view db_;
    // table name
    absl::string_view name_;
};

inline bool IsHiddenDb(absl::string_view db) {
    return db == INFORMATION_SCHEMA_DB || db == INTERNAL_DB || db == PRE_AGG_DB;
}

const SystemTableInfo* GetSystemTableInfo(SystemTableType type);

absl::string_view GetSystemTableName(SystemTableType type);

class SystemTable {
 public:
    static std::shared_ptr<::openmldb::nameserver::TableInfo> GetTableInfo(SystemTableType table_type) {
        auto table_info = std::make_shared<::openmldb::nameserver::TableInfo>();
        auto info = GetSystemTableInfo(table_type);
        if (info == nullptr) {
            return {};
        }
        table_info->set_db(std::string(info->db_));
        table_info->set_name(std::string(info->name_));
        table_info->set_replica_num(FLAGS_system_table_replica_num);
        table_info->set_partition_num(1);
        switch (table_type) {
            case SystemTableType::kJobInfo: {
                SetColumnDesc("id", openmldb::type::DataType::kInt, table_info->add_column_desc());
                SetColumnDesc("job_type", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("state", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("start_time", openmldb::type::DataType::kTimestamp, table_info->add_column_desc());
                SetColumnDesc("end_time", openmldb::type::DataType::kTimestamp, table_info->add_column_desc());
                SetColumnDesc("parameter", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("cluster", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("application_id", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("error", openmldb::type::DataType::kString, table_info->add_column_desc());
                auto index = table_info->add_column_key();
                index->set_index_name("id");
                index->add_col_name("id");
                auto ttl = index->mutable_ttl();
                ttl->set_ttl_type(::openmldb::type::kLatestTime);
                ttl->set_lat_ttl(1);
                break;
            }
            case SystemTableType::kPreAggMetaInfo: {
                SetColumnDesc("aggr_table", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("aggr_db", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("base_db", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("base_table", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("aggr_func", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("aggr_col", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("partition_cols", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("order_by_col", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("bucket_size", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("filter_col", openmldb::type::DataType::kString, table_info->add_column_desc());
                auto index = table_info->add_column_key();
                index->set_index_name("aggr_table");
                index->add_col_name("aggr_table");
                auto ttl = index->mutable_ttl();
                ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);
                ttl->set_abs_ttl(0);

                auto index2 = table_info->add_column_key();
                index2->set_index_name("unique_key");
                index2->add_col_name("base_db");
                index2->add_col_name("base_table");
                index2->add_col_name("aggr_func");
                index2->add_col_name("aggr_col");
                index2->add_col_name("partition_cols");
                index2->add_col_name("order_by_col");
                index2->add_col_name("filter_col");
                ttl = index2->mutable_ttl();
                ttl->set_ttl_type(::openmldb::type::kAbsoluteTime);
                ttl->set_abs_ttl(0);
                break;
            }
            case SystemTableType::kGlobalVariable: {
                SetColumnDesc("Variable_name", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("Variable_value", openmldb::type::DataType::kString, table_info->add_column_desc());
                auto index = table_info->add_column_key();
                index->set_index_name("Variable_name");
                index->add_col_name("Variable_name");
                auto ttl = index->mutable_ttl();
                ttl->set_ttl_type(::openmldb::type::kLatestTime);
                ttl->set_lat_ttl(1);
                break;
            }
            case SystemTableType::kDeployResponseTime: {
                // DEPLOY_NAME is constructed in the form of {db}_{deploy_procedure_name}
                SetColumnDesc("DEPLOY_NAME", type::DataType::kString, table_info->add_column_desc());
                // upper bound of the bucket, in seconds
                SetColumnDesc("TIME", type::DataType::kString, table_info->add_column_desc());
                // total count of queries in the current bucket
                SetColumnDesc("COUNT", type::DataType::kBigInt, table_info->add_column_desc());
                // sum of queries times in the current bucket, in seconds
                SetColumnDesc("TOTAL", type::DataType::kString, table_info->add_column_desc());
                auto index = table_info->add_column_key();
                index->set_index_name("index");
                index->add_col_name("DEPLOY_NAME");
                index->add_col_name("TIME");
                auto ttl = index->mutable_ttl();
                ttl->set_ttl_type(::openmldb::type::kLatestTime);
                ttl->set_lat_ttl(1);
                break;
            }
            case SystemTableType::kUser: {
                SetColumnDesc("Host", type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("User", type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("authentication_string", type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("password_last_changed", type::DataType::kTimestamp, table_info->add_column_desc());
                SetColumnDesc("password_expired", type::DataType::kTimestamp, table_info->add_column_desc());
                SetColumnDesc("Create_user_priv", type::DataType::kString, table_info->add_column_desc());
                auto index = table_info->add_column_key();
                index->set_index_name("index");
                index->add_col_name("Host");
                index->add_col_name("User");
                auto ttl = index->mutable_ttl();
                ttl->set_ttl_type(::openmldb::type::kLatestTime);
                ttl->set_lat_ttl(1);
                break;
            }
            default:
                return nullptr;
        }
        return table_info;
    }

 private:
    static inline void SetColumnDesc(const std::string& name, openmldb::type::DataType type,
                                     openmldb::common::ColumnDesc* field) {
        if (field != nullptr) {
            field->set_name(name);
            field->set_data_type(type);
        }
    }
};

}  // namespace nameserver
}  // namespace openmldb
#endif  // SRC_NAMESERVER_SYSTEM_TABLE_H_
