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
#include "sdk/cluster_sdk.h"
#include "sdk/sql_router.h"
#include "boost/compute/detail/lru_cache.hpp"
#include "sdk/table_reader_impl.h"

namespace fedb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::fedb::common::ColumnDesc>
    RtidbSchema;

static std::shared_ptr<::hybridse::sdk::Schema> ConvertToSchema(
    std::shared_ptr<::fedb::nameserver::TableInfo> table_info) {
    ::hybridse::vm::Schema schema;
    for (const auto& column_desc : table_info->column_desc()) {
        ::hybridse::type::ColumnDef* column_def = schema.Add();
        column_def->set_name(column_desc.name());
        column_def->set_is_not_null(column_desc.not_null());
        column_def->set_type(
            fedb::codec::SchemaCodec::ConvertType(column_desc.data_type()));
    }
    return std::make_shared<::hybridse::sdk::SchemaImpl>(schema);
}

struct SQLCache {
    SQLCache(std::shared_ptr<::fedb::nameserver::TableInfo> table_info,
                DefaultValueMap default_map, uint32_t str_length)
        : table_info(table_info),
          default_map(default_map),
          column_schema(),
          str_length(str_length) {
        column_schema = fedb::sdk::ConvertToSchema(table_info);
    }

    SQLCache(std::shared_ptr<::hybridse::sdk::Schema> column_schema,
            const ::hybridse::vm::Router& input_router)
        : table_info(),
          default_map(),
          column_schema(column_schema),
          str_length(0),
          router(input_router) {}

    std::shared_ptr<::fedb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    std::shared_ptr<::hybridse::sdk::Schema> column_schema;
    uint32_t str_length;
    ::hybridse::vm::Router router;
};

class SQLClusterRouter : public SQLRouter {
 public:
    explicit SQLClusterRouter(const SQLRouterOptions& options);
    explicit SQLClusterRouter(ClusterSDK* sdk);

    ~SQLClusterRouter();

    bool Init();

    bool CreateDB(const std::string& db, hybridse::sdk::Status* status) override;

    bool DropDB(const std::string& db, hybridse::sdk::Status* status) override;

    bool ShowDB(std::vector<std::string>* dbs, hybridse::sdk::Status* status) override;

    bool ExecuteDDL(const std::string& db, const std::string& sql,
                    hybridse::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql,
                       ::hybridse::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql,
                       std::shared_ptr<SQLInsertRow> row,
                       hybridse::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql,
                       std::shared_ptr<SQLInsertRows> rows,
                       hybridse::sdk::Status* status) override;

    std::shared_ptr<TableReader> GetTableReader();
    std::shared_ptr<ExplainInfo> Explain(const std::string& db,
                                         const std::string& sql,
                                         ::hybridse::sdk::Status* status) override;

    std::shared_ptr<SQLRequestRow> GetRequestRow(const std::string& db,
                                                 const std::string& sql,
                                                 ::hybridse::sdk::Status* status) override;

    std::shared_ptr<SQLRequestRow> GetRequestRowByProcedure(const std::string& db, const std::string& sp_name,
                                                 ::hybridse::sdk::Status* status) override;


    std::shared_ptr<SQLInsertRow> GetInsertRow(const std::string& db,
                                               const std::string& sql,
                                               ::hybridse::sdk::Status* status) override;

    std::shared_ptr<SQLInsertRows> GetInsertRows(const std::string& db,
                                                 const std::string& sql,
                                                 ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(
        const std::string& db, const std::string& sql,
        std::shared_ptr<SQLRequestRow> row, hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(
        const std::string& db, const std::string& sql,
        ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLBatchRequest(
        const std::string& db, const std::string& sql,
        std::shared_ptr<SQLRequestRowBatch> row_batch,
        ::hybridse::sdk::Status* status) override;

    bool RefreshCatalog() override;

    std::shared_ptr<hybridse::sdk::ResultSet> CallProcedure(
            const std::string& db, const std::string& sp_name,
            std::shared_ptr<SQLRequestRow> row, hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> CallSQLBatchRequestProcedure(
            const std::string& db, const std::string& sp_name,
            std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ProcedureInfo> ShowProcedure(
            const std::string& db, const std::string& sp_name, hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ProcedureInfo> ShowProcedure(
            const std::string& db, const std::string& sp_name, std::string* msg);

    std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> ShowProcedure(std::string* msg);

    std::shared_ptr<fedb::sdk::QueryFuture> CallProcedure(
            const std::string& db, const std::string& sp_name, int64_t timeout_ms,
            std::shared_ptr<SQLRequestRow> row, hybridse::sdk::Status* status);

    std::shared_ptr<fedb::sdk::QueryFuture> CallSQLBatchRequestProcedure(
            const std::string& db, const std::string& sp_name, int64_t timeout_ms,
            std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status);

    std::shared_ptr<::fedb::client::TabletClient> GetTabletClient(
        const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& row);

 private:
    void GetTables(::hybridse::vm::PhysicalOpNode* node,
                   std::set<std::string>* tables);

    bool PutRow(uint32_t tid, const std::shared_ptr<SQLInsertRow>& row,
            const std::vector<std::shared_ptr<::fedb::catalog::TabletAccessor>>& tablets,
            ::hybridse::sdk::Status* status);

    bool IsConstQuery(::hybridse::vm::PhysicalOpNode* node);
    std::shared_ptr<SQLCache> GetCache(const std::string& db,
                                          const std::string& sql);

    void SetCache(const std::string& db, const std::string& sql,
                  std::shared_ptr<SQLCache> router_cache);

    bool GetSQLPlan(const std::string& sql, ::hybridse::node::NodeManager* nm,
                    ::hybridse::node::PlanNodeList* plan);

    bool GetInsertInfo(
        const std::string& db, const std::string& sql,
        ::hybridse::sdk::Status* status,
        std::shared_ptr<::fedb::nameserver::TableInfo>* table_info,
        DefaultValueMap* default_map, uint32_t* str_length);

    std::shared_ptr<hybridse::node::ConstNode> GetDefaultMapValue(
        const hybridse::node::ConstNode& node, fedb::type::DataType column_type);

    DefaultValueMap GetDefaultMap(
        std::shared_ptr<::fedb::nameserver::TableInfo> table_info,
        const std::map<uint32_t, uint32_t>& column_map,
        ::hybridse::node::ExprListNode* row, uint32_t* str_length);

    bool HandleSQLCreateProcedure(hybridse::node::CreateProcedurePlanNode* plan,
            const std::string& db, const std::string& sql,
            std::shared_ptr<::fedb::client::NsClient> ns_ptr,
            hybridse::node::NodeManager* node_manager, std::string* msg);

    inline bool CheckParameter(const RtidbSchema& parameter, const RtidbSchema& input_schema);

    inline bool CheckSQLSyntax(const std::string& sql);

    std::shared_ptr<fedb::client::TabletClient> GetTablet(
            const std::string& db, const std::string& sp_name, hybridse::sdk::Status* status);

 private:
    SQLRouterOptions options_;
    ClusterSDK* cluster_sdk_;
    std::map<std::string, boost::compute::detail::lru_cache<std::string, std::shared_ptr<SQLCache>>>
        input_lru_cache_;
    ::fedb::base::SpinMutex mu_;
    ::fedb::base::Random rand_;
};

}  // namespace sdk
}  // namespace fedb
#endif  // SRC_SDK_SQL_CLUSTER_ROUTER_H_
