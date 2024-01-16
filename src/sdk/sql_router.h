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

#ifndef SRC_SDK_SQL_ROUTER_H_
#define SRC_SDK_SQL_ROUTER_H_

#include <base/status.h>
#include <proto/taskmanager.pb.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "sdk/base.h"
#include "sdk/options.h"
#include "sdk/result_set.h"
#include "sdk/sql_delete_row.h"
#include "sdk/sql_insert_row.h"
#include "sdk/sql_request_row.h"
#include "sdk/table_reader.h"
#include "vm/catalog.h"

namespace openmldb {
namespace sdk {

typedef char* ByteArrayPtr;

class ExplainInfo {
 public:
    ExplainInfo() {}
    virtual ~ExplainInfo() {}
    virtual const ::hybridse::sdk::Schema& GetInputSchema() = 0;
    virtual const ::hybridse::sdk::Schema& GetOutputSchema() = 0;
    virtual const std::string& GetLogicalPlan() = 0;
    virtual const std::string& GetPhysicalPlan() = 0;
    virtual const std::string& GetIR() = 0;
    virtual const std::string& GetRequestName() = 0;
    virtual const std::string& GetRequestDbName() = 0;
};

struct DAGNode {
    DAGNode(absl::string_view name, absl::string_view sql) : name(name), sql(sql) {}
    DAGNode(absl::string_view name, absl::string_view sql, const std::vector<std::shared_ptr<DAGNode>>& producers)
        : name(name), sql(sql), producers(producers) {}

    std::string name;
    std::string sql;
    std::vector<std::shared_ptr<DAGNode>> producers;

    bool operator==(const DAGNode& op) const noexcept;

    std::string DebugString() const;

    friend std::ostream& operator<<(std::ostream& os, const DAGNode& obj);
};

class QueryFuture {
 public:
    QueryFuture() {}
    virtual ~QueryFuture() {}

    virtual std::shared_ptr<hybridse::sdk::ResultSet> GetResultSet(hybridse::sdk::Status* status) = 0;
    virtual bool IsDone() const = 0;
};

class SQLRouter {
 public:
    SQLRouter() {}
    virtual ~SQLRouter() {}

    virtual bool ShowDB(std::vector<std::string>* dbs, hybridse::sdk::Status* status) = 0;

    virtual std::vector<std::string> GetAllTables() = 0;

    virtual bool CreateDB(const std::string& db, hybridse::sdk::Status* status) = 0;

    virtual bool DropDB(const std::string& db, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteDDL(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteInsert(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteInsert(const std::string& db, const std::string& sql,
                               std::shared_ptr<openmldb::sdk::SQLInsertRow> row, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteInsert(const std::string& db, const std::string& sql,
                               std::shared_ptr<openmldb::sdk::SQLInsertRows> row, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteInsert(const std::string& db, const std::string& name, int tid, int partition_num,
                hybridse::sdk::ByteArrayPtr dimension, int dimension_len,
                hybridse::sdk::ByteArrayPtr value, int len, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteDelete(std::shared_ptr<openmldb::sdk::SQLDeleteRow> row, hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::TableReader> GetTableReader() = 0;

    virtual std::shared_ptr<ExplainInfo> Explain(const std::string& db, const std::string& sql,
                                                 ::hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::SQLRequestRow> GetRequestRow(const std::string& db, const std::string& sql,
                                                                        hybridse::sdk::Status* status) = 0;
    virtual std::shared_ptr<openmldb::sdk::SQLRequestRow> GetRequestRowByProcedure(const std::string& db,
                                                                                   const std::string& sp_name,
                                                                                   ::hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::SQLInsertRow> GetInsertRow(const std::string& db, const std::string& sql,
                                                                      ::hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::SQLInsertRows> GetInsertRows(const std::string& db, const std::string& sql,
                                                                        ::hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::SQLDeleteRow> GetDeleteRow(const std::string& db, const std::string& sql,
                                                                      ::hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLRequest(
        const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRow> row,
        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                                 hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& sql,
                                                                 hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                                 bool is_online_mode, bool is_sync_job,
                                                                 int offline_job_timeout,
                                                                 hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(
        const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter,
        bool is_online_mode, bool is_sync_job, int offline_job_timeout, hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLParameterized(
        const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter,
        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLBatchRequest(
        const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRowBatch> row_batch,
        ::hybridse::sdk::Status* status) = 0;

    virtual bool RefreshCatalog() = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> CallProcedure(const std::string& db, const std::string& sp_name,
                                                                    std::shared_ptr<openmldb::sdk::SQLRequestRow> row,
                                                                    hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> CallProcedure(const std::string& db, const std::string& sp_name,
            hybridse::sdk::ByteArrayPtr buf, int len, const std::string& router_col,
            hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, std::shared_ptr<openmldb::sdk::SQLRequestRowBatch> row_batch,
        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, hybridse::sdk::ByteArrayPtr meta, int meta_len,
        hybridse::sdk::ByteArrayPtr buf, int len,
        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ProcedureInfo> ShowProcedure(const std::string& db,
                                                                        const std::string& sp_name,
                                                                        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::QueryFuture> CallProcedure(const std::string& db, const std::string& sp_name,
                                                                      int64_t timeout_ms,
                                                                      std::shared_ptr<openmldb::sdk::SQLRequestRow> row,
                                                                      hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::QueryFuture> CallProcedure(const std::string& db, const std::string& sp_name,
            int64_t timeout_ms, hybridse::sdk::ByteArrayPtr buf, int len,
            const std::string& router_col, hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::QueryFuture> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, int64_t timeout_ms,
        std::shared_ptr<openmldb::sdk::SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::QueryFuture> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, int64_t timeout_ms,
        hybridse::sdk::ByteArrayPtr meta, int meta_len,
        hybridse::sdk::ByteArrayPtr buf, int len,
        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::Schema> GetTableSchema(const std::string& db,
                                                                  const std::string& table_name) = 0;

    virtual std::vector<std::string> GetTableNames(const std::string& db) = 0;

    virtual ::openmldb::nameserver::TableInfo GetTableInfo(const std::string& db, const std::string& table) = 0;

    virtual bool UpdateOfflineTableInfo(const ::openmldb::nameserver::TableInfo& info) = 0;

    virtual ::openmldb::base::Status ShowJobs(const bool only_unfinished,
                                              std::vector<::openmldb::taskmanager::JobInfo>* job_infos) = 0;

    virtual ::openmldb::base::Status ShowJob(const int id, ::openmldb::taskmanager::JobInfo* job_info) = 0;

    virtual ::openmldb::base::Status StopJob(const int id, ::openmldb::taskmanager::JobInfo* job_info) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteOfflineQuery(const std::string& db, const std::string& sql,
                                                                          bool is_sync_job, int job_timeout,
                                                                          ::hybridse::sdk::Status* status) = 0;

    virtual std::string GetJobLog(int id, hybridse::sdk::Status* status) = 0;

    virtual bool NotifyTableChange() = 0;

    virtual bool IsOnlineMode() = 0;

    virtual std::string GetDatabase() = 0;

    // parse SQL query into DAG representation
    //
    // Optional CONFIG clause from SQL query statement is skipped in output DAG
    std::shared_ptr<DAGNode> SQLToDAG(const std::string& query, hybridse::sdk::Status* status);
};

std::shared_ptr<SQLRouter> NewClusterSQLRouter(const SQLRouterOptions& options);

std::shared_ptr<SQLRouter> NewStandaloneSQLRouter(const StandaloneOptions& options);

/*
 * return ddl statements
 * schemas example:
 * {
 *  "table1" : [
 *      {
 *          "col1": "kTypeString"
 *      }
 *      {
 *          "col2": "kTypeInt64"
 *      }
 *  ],
 *  "table2": [
 *      {
 *          "col1": "kTypeString"
 *      },
 *      {
 *          "col2": "kTypeInt64"
 *      }
 *  ]
 * }
 *
 * enum ColumnType: hybridse::sdk::DataType
 *
 * return:
 *      [
 *          "CREATE TABLE IF NOT EXISTS table1(
 *              col1 string,
 *              col2 bigint,
 *              index(key=col1, ttl=60)
 *          )",
 *          "CREATE TABLE IF NOT EXISTS table2(
 *              col1 string,
 *              col2 bigint,
 *              index(key=col1, ttl=60)
 *          )"
 *      ]
 */
// TODO(hw): support multi db
// All types should be convertible in swig, so we use vector&pair, not map
std::vector<std::string> GenDDL(
    const std::string& sql,
    const std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>& schemas);

// support multi db, input schema: vector<db, vector<table, vector<column, type>>>
// if using db is empty, use the first db in schemas
std::shared_ptr<hybridse::sdk::Schema> GenOutputSchema(
    const std::string& sql, const std::string& db,
    const std::vector<
        std::pair<std::string,
                  std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>>>&
        schemas);

std::vector<std::string> ValidateSQLInBatch(
    const std::string& sql, const std::string& db,
    const std::vector<
        std::pair<std::string,
                  std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>>>&
        schemas);

std::vector<std::string> ValidateSQLInRequest(
    const std::string& sql, const std::string& db,
    const std::vector<
        std::pair<std::string,
                  std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>>>&
        schemas);

// vector[0] is the main db.table
std::vector<std::pair<std::string, std::string>> GetDependentTables(
    const std::string& sql, const std::string& db,
    const std::vector<
        std::pair<std::string,
                  std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>>>>&
        schemas);

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_SQL_ROUTER_H_
