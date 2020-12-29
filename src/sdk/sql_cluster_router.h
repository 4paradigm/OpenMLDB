/*
 * sql_cluster_router.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_SDK_SQL_CLUSTER_ROUTER_H_
#define SRC_SDK_SQL_CLUSTER_ROUTER_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/random.h"
#include "base/spinlock.h"
#include "catalog/schema_adapter.h"
#include "client/tablet_client.h"
#include "parser/parser.h"
#include "sdk/cluster_sdk.h"
#include "sdk/sql_router.h"
#include "boost/compute/detail/lru_cache.hpp"

namespace rtidb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtidbSchema;

static std::shared_ptr<::fesql::sdk::Schema> ConvertToSchema(
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info) {
    ::fesql::vm::Schema schema;
    for (const auto& column_desc : table_info->column_desc_v1()) {
        ::fesql::type::ColumnDef* column_def = schema.Add();
        column_def->set_name(column_desc.name());
        column_def->set_is_not_null(column_desc.not_null());
        column_def->set_type(
            rtidb::codec::SchemaCodec::ConvertType(column_desc.data_type()));
    }
    return std::make_shared<::fesql::sdk::SchemaImpl>(schema);
}

struct SQLCache {
    SQLCache(std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
                DefaultValueMap default_map, uint32_t str_length)
        : table_info(table_info),
          default_map(default_map),
          column_schema(),
          str_length(str_length) {
        column_schema = rtidb::sdk::ConvertToSchema(table_info);
    }

    SQLCache(std::shared_ptr<::fesql::sdk::Schema> column_schema,
            const ::fesql::vm::Router& input_router)
        : table_info(),
          default_map(),
          column_schema(column_schema),
          str_length(0),
          router(input_router) {}

    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    std::shared_ptr<::fesql::sdk::Schema> column_schema;
    uint32_t str_length;
    ::fesql::vm::Router router;
};

class SQLClusterRouter : public SQLRouter {
 public:
    explicit SQLClusterRouter(const SQLRouterOptions& options);
    explicit SQLClusterRouter(ClusterSDK* sdk);

    ~SQLClusterRouter();

    bool Init();

    bool CreateDB(const std::string& db, fesql::sdk::Status* status) override;

    bool DropDB(const std::string& db, fesql::sdk::Status* status) override;

    bool ShowDB(std::vector<std::string>* dbs, fesql::sdk::Status* status) override;

    bool ExecuteDDL(const std::string& db, const std::string& sql,
                    fesql::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql,
                       ::fesql::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql,
                       std::shared_ptr<SQLInsertRow> row,
                       fesql::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql,
                       std::shared_ptr<SQLInsertRows> rows,
                       fesql::sdk::Status* status) override;

    std::shared_ptr<ExplainInfo> Explain(const std::string& db,
                                         const std::string& sql,
                                         ::fesql::sdk::Status* status) override;

    std::shared_ptr<SQLRequestRow> GetRequestRow(const std::string& db,
                                                 const std::string& sql,
                                                 ::fesql::sdk::Status* status) override;

    std::shared_ptr<SQLInsertRow> GetInsertRow(const std::string& db,
                                               const std::string& sql,
                                               ::fesql::sdk::Status* status) override;

    std::shared_ptr<SQLInsertRows> GetInsertRows(const std::string& db,
                                                 const std::string& sql,
                                                 ::fesql::sdk::Status* status) override;

    std::shared_ptr<fesql::sdk::ResultSet> ExecuteSQL(
        const std::string& db, const std::string& sql,
        std::shared_ptr<SQLRequestRow> row, fesql::sdk::Status* status) override;

    std::shared_ptr<fesql::sdk::ResultSet> ExecuteSQL(
        const std::string& db, const std::string& sql,
        ::fesql::sdk::Status* status) override;

    std::shared_ptr<fesql::sdk::ResultSet> ExecuteSQLBatchRequest(
        const std::string& db, const std::string& sql,
        std::shared_ptr<SQLRequestRowBatch> row_batch,
        ::fesql::sdk::Status* status) override;

    bool RefreshCatalog() override;

    std::shared_ptr<fesql::sdk::ResultSet> CallProcedure(
            const std::string& db, const std::string& sp_name,
            std::shared_ptr<SQLRequestRow> row, fesql::sdk::Status* status) override;

    std::shared_ptr<fesql::sdk::ResultSet> CallSQLBatchRequestProcedure(
            const std::string& db, const std::string& sp_name,
            std::shared_ptr<SQLRequestRowBatch> row_batch, fesql::sdk::Status* status) override;

    std::shared_ptr<fesql::sdk::ProcedureInfo> ShowProcedure(
            const std::string& db, const std::string& sp_name, fesql::sdk::Status* status) override;

    std::shared_ptr<fesql::sdk::ProcedureInfo> ShowProcedure(
            const std::string& db, const std::string& sp_name, std::string* msg);

    std::vector<std::shared_ptr<fesql::sdk::ProcedureInfo>> ShowProcedure(std::string* msg);

    std::shared_ptr<rtidb::sdk::QueryFuture> CallProcedure(
            const std::string& db, const std::string& sp_name, int64_t timeout_ms,
            std::shared_ptr<SQLRequestRow> row, fesql::sdk::Status* status);

    std::shared_ptr<rtidb::sdk::QueryFuture> CallSQLBatchRequestProcedure(
            const std::string& db, const std::string& sp_name, int64_t timeout_ms,
            std::shared_ptr<SQLRequestRowBatch> row_batch, fesql::sdk::Status* status);

    std::shared_ptr<::rtidb::client::TabletClient> GetTabletClient(
        const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& row);

 private:
    void GetTables(::fesql::vm::PhysicalOpNode* node,
                   std::set<std::string>* tables);

    bool PutRow(uint32_t tid, const std::shared_ptr<SQLInsertRow>& row,
            const std::vector<std::shared_ptr<::rtidb::catalog::TabletAccessor>>& tablets,
            ::fesql::sdk::Status* status);

    bool IsConstQuery(::fesql::vm::PhysicalOpNode* node);
    std::shared_ptr<SQLCache> GetCache(const std::string& db,
                                          const std::string& sql);

    void SetCache(const std::string& db, const std::string& sql,
                  std::shared_ptr<SQLCache> router_cache);

    bool GetSQLPlan(const std::string& sql, ::fesql::node::NodeManager* nm,
                    ::fesql::node::PlanNodeList* plan);

    bool GetInsertInfo(
        const std::string& db, const std::string& sql,
        ::fesql::sdk::Status* status,
        std::shared_ptr<::rtidb::nameserver::TableInfo>* table_info,
        DefaultValueMap* default_map, uint32_t* str_length);

    std::shared_ptr<fesql::node::ConstNode> GetDefaultMapValue(
        const fesql::node::ConstNode& node, rtidb::type::DataType column_type);

    DefaultValueMap GetDefaultMap(
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
        const std::map<uint32_t, uint32_t>& column_map,
        ::fesql::node::ExprListNode* row, uint32_t* str_length);

    bool HandleSQLCreateProcedure(const fesql::node::NodePointVector& parser_trees,
            const std::string& db, const std::string& sql,
            std::shared_ptr<::rtidb::client::NsClient> ns_ptr,
            fesql::node::NodeManager* node_manager, std::string* msg);

    inline bool CheckParameter(const RtidbSchema& parameter, const RtidbSchema& input_schema);

    std::shared_ptr<rtidb::client::TabletClient> GetTablet(
            const std::string& db, const std::string& sp_name, fesql::sdk::Status* status);

 private:
    SQLRouterOptions options_;
    ClusterSDK* cluster_sdk_;
    std::map<std::string, boost::compute::detail::lru_cache<std::string, std::shared_ptr<SQLCache>>>
        input_lru_cache_;
    ::rtidb::base::SpinMutex mu_;
    ::rtidb::base::Random rand_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_SQL_CLUSTER_ROUTER_H_
