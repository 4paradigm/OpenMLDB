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
#include <unordered_set>

#include "base/random.h"
#include "base/spinlock.h"
#include "base/lru_cache.h"
#include "client/tablet_client.h"
#include "sdk/db_sdk.h"
#include "sdk/sql_router.h"
#include "sdk/table_reader_impl.h"

namespace openmldb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> RtidbSchema;

static std::shared_ptr<::hybridse::sdk::Schema> ConvertToSchema(
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info) {
    ::hybridse::vm::Schema schema;
    for (const auto& column_desc : table_info->column_desc()) {
        ::hybridse::type::ColumnDef* column_def = schema.Add();
        column_def->set_name(column_desc.name());
        column_def->set_is_not_null(column_desc.not_null());
        column_def->set_type(openmldb::codec::SchemaCodec::ConvertType(column_desc.data_type()));
    }
    return std::make_shared<::hybridse::sdk::SchemaImpl>(schema);
}

struct SQLCache {
    SQLCache(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info, DefaultValueMap default_map,
             uint32_t str_length, uint32_t limit_cnt = 0)
        : table_info(table_info), default_map(default_map), column_schema(),
          str_length(str_length), limit_cnt(limit_cnt) {
        column_schema = openmldb::sdk::ConvertToSchema(table_info);
    }
    SQLCache(std::shared_ptr<::hybridse::sdk::Schema> column_schema, const ::hybridse::vm::Router& input_router,
             uint32_t limit_cnt = 0)
        : table_info(),
          default_map(),
          column_schema(column_schema),
          parameter_schema(),
          str_length(0),
          limit_cnt(limit_cnt),
          router(input_router) {}
    SQLCache(std::shared_ptr<::hybridse::sdk::Schema> column_schema,
             std::shared_ptr<::hybridse::sdk::Schema> parameter_schema, const ::hybridse::vm::Router& input_router,
             uint32_t limit_cnt = 0)
        : table_info(),
          default_map(),
          column_schema(column_schema),
          parameter_schema(parameter_schema),
          str_length(0),
          limit_cnt(limit_cnt),
          router(input_router) {}
    bool IsCompatibleCache(std::shared_ptr<::hybridse::sdk::Schema> other_parameter_schema) {
        if (!parameter_schema && !other_parameter_schema) {
            return true;
        }
        if (!parameter_schema || !other_parameter_schema) {
            return false;
        }
        if (parameter_schema->GetColumnCnt() != other_parameter_schema->GetColumnCnt()) {
            return false;
        }

        for (int i = 0; i < parameter_schema->GetColumnCnt(); i++) {
            if (parameter_schema->GetColumnType(i) != other_parameter_schema->GetColumnType(i)) {
                return false;
            }
        }
        return true;
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    std::shared_ptr<::hybridse::sdk::Schema> column_schema;
    std::shared_ptr<::hybridse::sdk::Schema> parameter_schema;
    uint32_t str_length;
    uint32_t limit_cnt;
    ::hybridse::vm::Router router;
};

class SQLClusterRouter : public SQLRouter {
 public:
    explicit SQLClusterRouter(const SQLRouterOptions& options);
    explicit SQLClusterRouter(DBSDK* sdk);

    ~SQLClusterRouter() override;

    bool Init();

    bool CreateDB(const std::string& db, hybridse::sdk::Status* status) override;

    bool DropDB(const std::string& db, hybridse::sdk::Status* status) override;

    bool DropTable(const std::string& db, const std::string& table, hybridse::sdk::Status* status);

    bool ShowDB(std::vector<std::string>* dbs, hybridse::sdk::Status* status) override;
    
    std::vector<std::string> GetAllTables() override;

    bool ExecuteDDL(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql, std::shared_ptr<SQLInsertRow> row,
                       hybridse::sdk::Status* status) override;

    bool ExecuteInsert(const std::string& db, const std::string& sql, std::shared_ptr<SQLInsertRows> rows,
                       hybridse::sdk::Status* status) override;

    std::shared_ptr<TableReader> GetTableReader() override;

    std::shared_ptr<ExplainInfo> Explain(const std::string& db, const std::string& sql,
                                         ::hybridse::sdk::Status* status) override;

    std::shared_ptr<SQLRequestRow> GetRequestRow(const std::string& db, const std::string& sql,
                                                 ::hybridse::sdk::Status* status) override;
    std::shared_ptr<SQLRequestRow> GetRequestRowByProcedure(const std::string& db, const std::string& sp_name,
                                                            ::hybridse::sdk::Status* status) override;

    std::shared_ptr<SQLInsertRow> GetInsertRow(const std::string& db, const std::string& sql,
                                               ::hybridse::sdk::Status* status) override;

    std::shared_ptr<SQLInsertRows> GetInsertRows(const std::string& db, const std::string& sql,
                                                 ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLRequest(const std::string& db, const std::string& sql,
                                                                std::shared_ptr<SQLRequestRow> row,
                                                                hybridse::sdk::Status* status) override;
    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                         ::hybridse::sdk::Status* status) override;
    /// Execute batch SQL with parameter row
    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLParameterized(const std::string& db, const std::string& sql,
                                                                      std::shared_ptr<SQLRequestRow> parameter,
                                                                      ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLBatchRequest(const std::string& db, const std::string& sql,
                                                                     std::shared_ptr<SQLRequestRowBatch> row_batch,
                                                                     ::hybridse::sdk::Status* status) override;

    bool RefreshCatalog() override;

    std::shared_ptr<hybridse::sdk::ResultSet> CallProcedure(const std::string& db, const std::string& sp_name,
                                                            std::shared_ptr<SQLRequestRow> row,
                                                            hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, std::shared_ptr<SQLRequestRowBatch> row_batch,
        hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ProcedureInfo> ShowProcedure(const std::string& db, const std::string& sp_name,
                                                                hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ProcedureInfo> ShowProcedure(const std::string& db, const std::string& sp_name,
                                                                std::string* msg);

    std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> ShowProcedure(std::string* msg);

    std::shared_ptr<openmldb::sdk::QueryFuture> CallProcedure(const std::string& db, const std::string& sp_name,
                                                              int64_t timeout_ms, std::shared_ptr<SQLRequestRow> row,
                                                              hybridse::sdk::Status* status);

    std::shared_ptr<openmldb::sdk::QueryFuture> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, int64_t timeout_ms,
        std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) override;

    std::shared_ptr<::openmldb::client::TabletClient> GetTabletClient(const std::string& db, const std::string& sql,
                                                                      const ::hybridse::vm::EngineMode engine_mode,
                                                                      const std::shared_ptr<SQLRequestRow>& row);
    std::shared_ptr<::openmldb::client::TabletClient> GetTabletClient(
        const std::string& db, const std::string& sql, const ::hybridse::vm::EngineMode engine_mode,
        const std::shared_ptr<SQLRequestRow>& row, const std::shared_ptr<SQLRequestRow>& parameter_row);
    std::shared_ptr<SQLCache> GetSQLCache(
        const std::string& db, const std::string& sql, const ::hybridse::vm::EngineMode engine_mode,
        const std::shared_ptr<SQLRequestRow>& parameter_row);
    bool GetTabletClientsForClusterOnlineBatchQuery(
        const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& parameter_row,
        std::unordered_set<std::shared_ptr<::openmldb::client::TabletClient>>& clients); //NOLINT

    std::shared_ptr<hybridse::sdk::Schema> GetTableSchema(const std::string& db,
                                                          const std::string& table_name) override;

    base::Status HandleSQLCreateProcedure(hybridse::node::CreateProcedurePlanNode* plan, const std::string& db,
                                          const std::string& sql, std::shared_ptr<::openmldb::client::NsClient> ns_ptr);

    base::Status HandleSQLCreateTable(hybridse::node::CreatePlanNode* create_node, const std::string& db,
                                      std::shared_ptr<::openmldb::client::NsClient> ns_ptr);

    base::Status HandleSQLCmd(const hybridse::node::CmdPlanNode* cmd_node, const std::string& db,
                              std::shared_ptr<::openmldb::client::NsClient> ns_ptr);

    std::vector<std::string> GetTableNames(const std::string& db) override;

    ::openmldb::nameserver::TableInfo GetTableInfo(const std::string& db, const std::string& table) override;

    bool UpdateOfflineTableInfo(const ::openmldb::nameserver::TableInfo& info) override;

    ::openmldb::base::Status ShowJobs(const bool only_unfinished,
                                      std::vector<::openmldb::taskmanager::JobInfo>& job_infos) override;

    ::openmldb::base::Status ShowJob(const int id, ::openmldb::taskmanager::JobInfo& job_info) override;

    ::openmldb::base::Status StopJob(const int id, ::openmldb::taskmanager::JobInfo& job_info) override;

    ::openmldb::base::Status ExecuteOfflineQuery(const std::string& sql,
                                                 const std::map<std::string, std::string>& config,
                                                 const std::string& default_db,
                                                 ::openmldb::taskmanager::JobInfo& job_info) override;

    ::openmldb::base::Status ImportOnlineData(const std::string& sql,
                                              const std::map<std::string, std::string>& config,
                                              const std::string& default_db,
                                              ::openmldb::taskmanager::JobInfo& job_info) override;

    ::openmldb::base::Status ImportOfflineData(const std::string& sql,
                                               const std::map<std::string, std::string>& config,
                                               const std::string& default_db,
                                               ::openmldb::taskmanager::JobInfo& job_info) override;

    ::openmldb::base::Status ExportOfflineData(const std::string& sql,
                                               const std::map<std::string, std::string>& config,
                                               const std::string& default_db,
                                               ::openmldb::taskmanager::JobInfo& job_info) override;

    bool NotifyTableChange() override;

 private:
    void GetTables(::hybridse::vm::PhysicalOpNode* node, std::set<std::string>* tables);

    bool PutRow(uint32_t tid, const std::shared_ptr<SQLInsertRow>& row,
                const std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>& tablets,
                ::hybridse::sdk::Status* status);

    bool IsConstQuery(::hybridse::vm::PhysicalOpNode* node);
    std::shared_ptr<SQLCache> GetCache(const std::string& db, const std::string& sql,
                                       const hybridse::vm::EngineMode engine_mode);

    void SetCache(const std::string& db, const std::string& sql,
                  const hybridse::vm::EngineMode engine_mode, const std::shared_ptr<SQLCache>& router_cache);

    bool GetSQLPlan(const std::string& sql, ::hybridse::node::NodeManager* nm, ::hybridse::node::PlanNodeList* plan);

    bool GetInsertInfo(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status,
                       std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info, DefaultValueMap* default_map,
                       uint32_t* str_length);
    bool GetMultiRowInsertInfo(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status,
                               std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info,
                               std::vector<DefaultValueMap>* default_maps, std::vector<uint32_t>* str_lengths);

    DefaultValueMap GetDefaultMap(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                                  const std::map<uint32_t, uint32_t>& column_map, ::hybridse::node::ExprListNode* row,
                                  uint32_t* str_length);

    inline bool CheckParameter(const RtidbSchema& parameter, const RtidbSchema& input_schema);

    inline bool CheckSQLSyntax(const std::string& sql);

    std::shared_ptr<openmldb::client::TabletClient> GetTablet(const std::string& db, const std::string& sp_name,
                                                              hybridse::sdk::Status* status);
    bool ExtractDBTypes(std::shared_ptr<hybridse::sdk::Schema> schema,
                        std::vector<openmldb::type::DataType>& parameter_types);  // NOLINT

 private:
    SQLRouterOptions options_;
    DBSDK* cluster_sdk_;
    std::map<std::string,
             std::map<hybridse::vm::EngineMode,
                      base::lru_cache<std::string, std::shared_ptr<SQLCache>>>> input_lru_cache_;
    ::openmldb::base::SpinMutex mu_;
    ::openmldb::base::Random rand_;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_SQL_CLUSTER_ROUTER_H_
