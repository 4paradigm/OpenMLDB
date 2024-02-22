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

#ifndef SRC_SDK_SQL_CACHE_H_
#define SRC_SDK_SQL_CACHE_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "node/node_manager.h"
#include "proto/name_server.pb.h"
#include "proto/type.pb.h"
#include "sdk/sql_delete_row.h"
#include "vm/router.h"

namespace openmldb {
namespace sdk {

using DefaultValueMap = std::shared_ptr<std::map<uint32_t, std::shared_ptr<::hybridse::node::ConstNode>>>;

class SQLCache {
 public:
    SQLCache(const std::string& db, uint32_t tid, const std::string& table_name)
        : db_(db), tid_(tid), table_name_(table_name) {}
    virtual ~SQLCache() {}
    uint32_t GetTableId() const { return tid_; }
    const std::string& GetTableName() const { return table_name_; }
    const std::string& GetDatabase() const { return db_; }

 private:
    const std::string db_;
    uint32_t tid_;
    const std::string table_name_;
};

class InsertSQLCache : public SQLCache {
 public:
    InsertSQLCache(const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info,
            const std::shared_ptr<::hybridse::sdk::Schema>& column_schema,
            DefaultValueMap default_map,
            uint32_t str_length, std::vector<uint32_t> hole_idx_arr, bool put_if_absent)
        : SQLCache(table_info->db(), table_info->tid(), table_info->name()),
          table_info_(table_info),
          column_schema_(column_schema),
          default_map_(std::move(default_map)),
          str_length_(str_length),
          hole_idx_arr_(std::move(hole_idx_arr)),
          put_if_absent_(put_if_absent) {}

    std::shared_ptr<::openmldb::nameserver::TableInfo> GetTableInfo() { return table_info_; }
    std::shared_ptr<::hybridse::sdk::Schema> GetSchema() const { return column_schema_; }
    uint32_t GetStrLength() const { return str_length_; }
    const DefaultValueMap& GetDefaultValue() const { return default_map_; }
    const std::vector<uint32_t>& GetHoleIdxArr() const { return hole_idx_arr_; }
    const bool IsPutIfAbsent() const { return put_if_absent_; }
 private:
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info_;
    std::shared_ptr<::hybridse::sdk::Schema> column_schema_;
    const DefaultValueMap default_map_;
    const uint32_t str_length_;
    const std::vector<uint32_t> hole_idx_arr_;
    const bool put_if_absent_;
};

class RouterSQLCache : public SQLCache {
 public:
    RouterSQLCache(const std::string& db, uint32_t tid, const std::string& table_name,
            const std::shared_ptr<::hybridse::sdk::Schema>& column_schema,
            const std::shared_ptr<::hybridse::sdk::Schema>& parameter_schema,
            const ::hybridse::vm::Router& router)
    : SQLCache(db, tid, table_name),
    column_schema_(column_schema), parameter_schema_(parameter_schema), router_(router) {}

    std::shared_ptr<::hybridse::sdk::Schema> GetSchema() const { return column_schema_; }
    std::shared_ptr<::hybridse::sdk::Schema> GetParameterSchema() const { return parameter_schema_; }
    const ::hybridse::vm::Router& GetRouter() const { return router_; }

    bool IsCompatibleCache(const std::shared_ptr<::hybridse::sdk::Schema>& other_parameter_schema) const;

 private:
    std::shared_ptr<::hybridse::sdk::Schema> column_schema_;
    std::shared_ptr<::hybridse::sdk::Schema> parameter_schema_;
    ::hybridse::vm::Router router_;
};

class DeleteSQLCache : public SQLCache {
 public:
    DeleteSQLCache(const std::string& db, uint32_t tid, const std::string& table_name,
      const std::vector<Condition>& condition_vec, const std::vector<Condition>& parameter_vec) :
      SQLCache(db, tid, table_name), condition_vec_(condition_vec), parameter_vec_(parameter_vec) {}

    const std::vector<Condition>& GetCondition() const { return parameter_vec_; }
    const std::vector<Condition>& GetDefaultCondition() const {return condition_vec_; }

 private:
    std::vector<Condition> condition_vec_;
    std::vector<Condition> parameter_vec_;
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_SQL_CACHE_H_
