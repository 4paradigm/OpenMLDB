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
#include "base/status.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/name_server.pb.h"

DECLARE_uint32(system_table_replica_num);

namespace openmldb {
namespace nameserver {

constexpr const char* INTERNAL_DB = "__INTERNAL_DB";
constexpr const char* JOB_INFO_NAME = "JOB_INFO";

enum class SystemTableType {
    kJobInfo = 1,
};

class SystemTable {
 public:
    static std::shared_ptr<::openmldb::nameserver::TableInfo> GetTableInfo(const std::string& table_name,
            SystemTableType table_type) {
        auto table_info = std::make_shared<::openmldb::nameserver::TableInfo>();
        table_info->set_db(INTERNAL_DB);
        table_info->set_name(table_name);
        table_info->set_replica_num(FLAGS_system_table_replica_num);
        table_info->set_partition_num(1);
        switch (table_type) {
            case SystemTableType::kJobInfo: {
                SetColumnDesc("id", openmldb::type::DataType::kBigInt, table_info->add_column_desc());
                SetColumnDesc("job", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("state", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("start_time", openmldb::type::DataType::kTimestamp, table_info->add_column_desc());
                SetColumnDesc("end_time", openmldb::type::DataType::kTimestamp, table_info->add_column_desc());
                SetColumnDesc("cluster", openmldb::type::DataType::kString, table_info->add_column_desc());
                SetColumnDesc("parameter", openmldb::type::DataType::kString, table_info->add_column_desc());
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
            default:
                return nullptr;
        }
        return table_info;
    }

 private:
    static void SetColumnDesc(const std::string& name, openmldb::type::DataType type,
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
