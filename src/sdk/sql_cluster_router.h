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
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/ddl_parser.h"
#include "base/lru_cache.h"
#include "base/random.h"
#include "base/spinlock.h"
#include "client/tablet_client.h"
#include "nameserver/system_table.h"
#include "sdk/db_sdk.h"
#include "sdk/file_option_parser.h"
#include "sdk/sql_cache.h"
#include "sdk/sql_router.h"
#include "sdk/table_reader_impl.h"

namespace openmldb::sdk {

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> PBSchema;

constexpr const char* FORMAT_STRING_KEY = "!%$FORMAT_STRING_KEY";

class SQLClusterRouter : public SQLRouter {
 public:
    explicit SQLClusterRouter(const SQLRouterOptions& options);
    explicit SQLClusterRouter(const StandaloneOptions& options);
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

    bool ExecuteDelete(std::shared_ptr<SQLDeleteRow> row, hybridse::sdk::Status* status) override;

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

    std::shared_ptr<SQLDeleteRow> GetDeleteRow(const std::string& db, const std::string& sql,
                                               ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLRequest(const std::string& db, const std::string& sql,
                                                                std::shared_ptr<SQLRequestRow> row,
                                                                hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& sql,
                                                         ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                         ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                         bool is_online_mode, bool is_sync_job, int offline_job_timeout,
                                                         hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                         std::shared_ptr<SQLRequestRow> parameter, bool is_online_mode,
                                                         bool is_sync_job, int offline_job_timeout,
                                                         hybridse::sdk::Status* status) override;
    /// Execute batch SQL with parameter row
    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLParameterized(const std::string& db, const std::string& sql,
                                                                      std::shared_ptr<SQLRequestRow> parameter,
                                                                      ::hybridse::sdk::Status* status) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLBatchRequest(const std::string& db, const std::string& sql,
                                                                     std::shared_ptr<SQLRequestRowBatch> row_batch,
                                                                     ::hybridse::sdk::Status* status) override;

    /// utility functions to query registered components in the current DBMS
    //
    /// \param status result status, will set status.code to error if error happens
    /// \return ResultSet of components of that type, or empty ResultSet if error happend
    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteShowNameServers(hybridse::sdk::Status* status);

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteShowTablets(hybridse::sdk::Status* status);

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteShowTaskManagers(hybridse::sdk::Status* status);

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteShowApiServers(hybridse::sdk::Status* status);

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
                                                              hybridse::sdk::Status* status) override;

    std::shared_ptr<openmldb::sdk::QueryFuture> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, int64_t timeout_ms,
        std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) override;

    std::shared_ptr<::openmldb::client::TabletClient> GetTabletClient(const std::string& db, const std::string& sql,
                                                                      ::hybridse::vm::EngineMode engine_mode,
                                                                      const std::shared_ptr<SQLRequestRow>& row,
                                                                      hybridse::sdk::Status* status);
    std::shared_ptr<::openmldb::client::TabletClient> GetTabletClient(
        const std::string& db, const std::string& sql, ::hybridse::vm::EngineMode engine_mode,
        const std::shared_ptr<SQLRequestRow>& row, const std::shared_ptr<SQLRequestRow>& parameter_row,
        hybridse::sdk::Status* status);

    std::shared_ptr<SQLCache> GetSQLCache(const std::string& db, const std::string& sql,
                                          ::hybridse::vm::EngineMode engine_mode,
                                          const std::shared_ptr<SQLRequestRow>& parameter_row,
                                          hybridse::sdk::Status* status);

    std::shared_ptr<::openmldb::client::TabletClient> GetTabletClientForBatchQuery(
        const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& parameter_row,
        hybridse::sdk::Status* status);

    std::shared_ptr<hybridse::sdk::Schema> GetTableSchema(const std::string& db,
                                                          const std::string& table_name) override;

    base::Status HandleSQLCreateProcedure(hybridse::node::CreateProcedurePlanNode* plan, const std::string& db,
                                          const std::string& sql, std::shared_ptr<::openmldb::client::NsClient> ns_ptr);

    base::Status HandleSQLCreateTable(hybridse::node::CreatePlanNode* create_node, const std::string& db,
                                      std::shared_ptr<::openmldb::client::NsClient> ns_ptr);

    std::shared_ptr<hybridse::sdk::ResultSet> HandleSQLCmd(const hybridse::node::CmdPlanNode* cmd_node,
                                                           const std::string& db, ::hybridse::sdk::Status* status);

    std::vector<std::string> GetTableNames(const std::string& db) override;

    ::openmldb::nameserver::TableInfo GetTableInfo(const std::string& db, const std::string& table) override;

    bool UpdateOfflineTableInfo(const ::openmldb::nameserver::TableInfo& info) override;

    ::openmldb::base::Status ShowJobs(bool only_unfinished,
                                      std::vector<::openmldb::taskmanager::JobInfo>* job_infos) override;

    ::openmldb::base::Status ShowJob(int id, ::openmldb::taskmanager::JobInfo* job_info) override;

    ::openmldb::base::Status StopJob(int id, ::openmldb::taskmanager::JobInfo* job_info) override;

    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteOfflineQuery(const std::string& db, const std::string& sql,
                                                                  bool is_sync_job, int job_timeout,
                                                                  ::hybridse::sdk::Status* status) override;

    ::openmldb::base::Status ImportOnlineData(const std::string& sql, const std::map<std::string, std::string>& config,
                                              const std::string& default_db, bool sync_job, int job_timeout,
                                              ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status ImportOfflineData(const std::string& sql, const std::map<std::string, std::string>& config,
                                               const std::string& default_db, bool sync_job, int job_timeout,
                                               ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status ExportOfflineData(const std::string& sql, const std::map<std::string, std::string>& config,
                                               const std::string& default_db, bool sync_job, int job_timeout,
                                               ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status CreatePreAggrTable(const std::string& aggr_db, const std::string& aggr_table,
                                                const ::openmldb::base::LongWindowInfo& window_info,
                                                const ::openmldb::nameserver::TableInfo& base_table_info,
                                                std::shared_ptr<::openmldb::client::NsClient> ns_ptr);

    std::string GetJobLog(int id, hybridse::sdk::Status* status) override;

    bool NotifyTableChange() override;

    bool IsOnlineMode() override;
    bool IsEnableTrace();

    std::string GetDatabase();
    void SetDatabase(const std::string& db);
    void SetInteractive(bool value);

    void ReadSparkConfFromFile(std::string conf_file, std::map<std::string, std::string>* config);

    std::shared_ptr<BasicRouterOptions> GetRouterOptions() { return options_; }

 private:
    bool IsSyncJob();
    // get job timeout from the session variables, we will use the timeout when sending requests to the taskmanager
    int GetJobTimeout();

    ::openmldb::base::Status ExecuteOfflineQueryAsync(const std::string& sql,
                                                      const std::map<std::string, std::string>& config,
                                                      const std::string& default_db, int job_timeout,
                                                      ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status ExecuteOfflineQueryGetOutput(const std::string& sql,
                                                          const std::map<std::string, std::string>& config,
                                                          const std::string& default_db, int job_timeout,
                                                          std::string* output);

    void GetTables(::hybridse::vm::PhysicalOpNode* node, std::set<std::string>* tables);

    bool PutRow(uint32_t tid, const std::shared_ptr<SQLInsertRow>& row,
                const std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>& tablets,
                ::hybridse::sdk::Status* status);

    bool IsConstQuery(::hybridse::vm::PhysicalOpNode* node);
    std::shared_ptr<SQLCache> GetCache(const std::string& db, const std::string& sql,
                                       hybridse::vm::EngineMode engine_mode);

    void SetCache(const std::string& db, const std::string& sql, hybridse::vm::EngineMode engine_mode,
                  const std::shared_ptr<SQLCache>& router_cache);

    bool GetSQLPlan(const std::string& sql, ::hybridse::node::NodeManager* nm, ::hybridse::node::PlanNodeList* plan);

    bool GetInsertInfo(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status,
                       std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info, DefaultValueMap* default_map,
                       uint32_t* str_length, std::vector<uint32_t>* stmt_column_idx_in_table);
    bool GetMultiRowInsertInfo(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status,
                               std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info,
                               std::vector<DefaultValueMap>* default_maps, std::vector<uint32_t>* str_lengths);

    DefaultValueMap GetDefaultMap(const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info,
                                  const std::map<uint32_t, uint32_t>& column_map, ::hybridse::node::ExprListNode* row,
                                  uint32_t* str_length);

    inline bool CheckParameter(const PBSchema& parameter, const PBSchema& input_schema);

    inline bool CheckSQLSyntax(const std::string& sql);

    std::shared_ptr<openmldb::client::TabletClient> GetTablet(const std::string& db, const std::string& sp_name,
                                                              hybridse::sdk::Status* status);

    bool ExtractDBTypes(const std::shared_ptr<hybridse::sdk::Schema>& schema,
                        std::vector<openmldb::type::DataType>* parameter_types);

    ::hybridse::sdk::Status SetVariable(hybridse::node::SetPlanNode* node);

    ::hybridse::sdk::Status ParseNamesFromArgs(const std::string& db, const std::vector<std::string>& args,
                                               std::string* db_name, std::string* sp_name);

    bool CheckAnswerIfInteractive(const std::string& drop_type, const std::string& name);

    ::openmldb::base::Status SaveResultSet(const std::string& file_path,
                                           const std::shared_ptr<hybridse::node::OptionsMap>& options_map,
                                           ::hybridse::sdk::ResultSet* result_set);

    hybridse::sdk::Status HandleLoadDataInfile(const std::string& database, const std::string& table,
                                               const std::string& file_path,
                                               const openmldb::sdk::ReadFileOptionsParser& options_parser);

    hybridse::sdk::Status LoadDataMultipleFile(int id, int step, const std::string& database,
                                               const std::string& table, const std::vector<std::string>& file_list,
                                               const openmldb::sdk::ReadFileOptionsParser& options_parser,
                                               uint64_t* count);

    hybridse::sdk::Status LoadDataSingleFile(int id, int step, const std::string& database,
                                             const std::string& table, const std::string& file_path,
                                             const openmldb::sdk::ReadFileOptionsParser& options_parser,
                                             uint64_t* count);

    hybridse::sdk::Status InsertOneRow(const std::string& database, const std::string& insert_placeholder,
                                       const std::vector<int>& str_col_idx, const std::string& null_value,
                                       const std::vector<std::string>& cols);

    hybridse::sdk::Status HandleDeploy(const std::string& db, const hybridse::node::DeployPlanNode* deploy_node);

    hybridse::sdk::Status HandleDelete(const std::string& db, const std::string& table_name,
                                       const hybridse::node::ExprNode* condition);

    hybridse::sdk::Status HandleIndex(const std::string& db,
                                      const std::set<std::pair<std::string, std::string>>& table_pair,
                                      const std::string& select_sql);

    hybridse::sdk::Status GetNewIndex(
        const std::map<std::string, ::openmldb::nameserver::TableInfo>& table_map,
        const std::map<std::string, std::vector<::openmldb::common::ColumnKey>>& index_map,
        std::map<std::string, std::vector<::openmldb::common::ColumnKey>>* new_index_map);

    hybridse::sdk::Status AddNewIndex(
        const std::string& db, const std::map<std::string, ::openmldb::nameserver::TableInfo>& table_map,
        const std::map<std::string, std::vector<::openmldb::common::ColumnKey>>& new_index_map);

    hybridse::sdk::Status HandleCreateFunction(const hybridse::node::CreateFunctionPlanNode* node);

    hybridse::sdk::Status HandleLongWindows(const hybridse::node::DeployPlanNode* deploy_node,
                                            const std::set<std::pair<std::string, std::string>>& table_pair,
                                            const std::string& select_sql);

    bool CheckPreAggrTableExist(const std::string& base_table, const std::string& base_db,
                                const openmldb::base::LongWindowInfo& lw, ::hybridse::sdk::Status* status);

    ///
    /// \brief Query all registered components, aka tablet, nameserver, task manager,
    /// which is required by `SHOW COMPONENTS` statement
    ///
    /// \param status result status
    /// \return ResultSet represent all components, each row represent one component
    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteShowComponents(hybridse::sdk::Status* status);

    /// internal implementation for SQL 'SHOW TABLE STATUS'
    std::shared_ptr<hybridse::sdk::ResultSet> ExecuteShowTableStatus(const std::string& db,
                                                                     hybridse::sdk::Status* status, bool all = true);

    bool CheckTableStatus(const std::string& db, const std::string& table_name, uint32_t tid, uint32_t pid,
                          std::string* msg);

 private:
    std::shared_ptr<BasicRouterOptions> options_;
    std::string db_;
    std::map<std::string, std::string> session_variables_;
    bool is_cluster_mode_;
    bool interactive_;
    DBSDK* cluster_sdk_;
    std::map<std::string, std::map<hybridse::vm::EngineMode, base::lru_cache<std::string, std::shared_ptr<SQLCache>>>>
        input_lru_cache_;
    ::openmldb::base::SpinMutex mu_;
    ::openmldb::base::Random rand_;
};

}  // namespace openmldb::sdk
#endif  // SRC_SDK_SQL_CLUSTER_ROUTER_H_
