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

#include <memory>
#include <string>
#include <vector>

#include "sdk/base.h"
#include "sdk/result_set.h"
#include "sdk/sql_insert_row.h"
#include "sdk/sql_request_row.h"
#include "sdk/table_reader.h"

namespace openmldb {
namespace sdk {

struct SQLRouterOptions {
    std::string zk_cluster;
    std::string zk_path;
    bool enable_debug = false;
    uint32_t session_timeout = 2000;
    uint32_t max_sql_cache_size = 10;
    uint32_t request_timeout = 60000;
};

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
    virtual bool CreateDB(const std::string& db, hybridse::sdk::Status* status) = 0;

    virtual bool DropDB(const std::string& db, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteDDL(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteInsert(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteInsert(const std::string& db, const std::string& sql,
                               std::shared_ptr<openmldb::sdk::SQLInsertRow> row, hybridse::sdk::Status* status) = 0;

    virtual bool ExecuteInsert(const std::string& db, const std::string& sql,
                               std::shared_ptr<openmldb::sdk::SQLInsertRows> row, hybridse::sdk::Status* status) = 0;

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

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                                 std::shared_ptr<openmldb::sdk::SQLRequestRow> row,
                                                                 hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQL(const std::string& db, const std::string& sql,
                                                                 hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> ExecuteSQLBatchRequest(
        const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRowBatch> row_batch,
        ::hybridse::sdk::Status* status) = 0;

    virtual bool RefreshCatalog() = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> CallProcedure(const std::string& db, const std::string& sp_name,
                                                                    std::shared_ptr<openmldb::sdk::SQLRequestRow> row,
                                                                    hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ResultSet> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, std::shared_ptr<openmldb::sdk::SQLRequestRowBatch> row_batch,
        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<hybridse::sdk::ProcedureInfo> ShowProcedure(const std::string& db,
                                                                        const std::string& sp_name,
                                                                        hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::QueryFuture> CallProcedure(const std::string& db, const std::string& sp_name,
                                                                      int64_t timeout_ms,
                                                                      std::shared_ptr<openmldb::sdk::SQLRequestRow> row,
                                                                      hybridse::sdk::Status* status) = 0;

    virtual std::shared_ptr<openmldb::sdk::QueryFuture> CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, int64_t timeout_ms,
        std::shared_ptr<openmldb::sdk::SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) = 0;
};

std::shared_ptr<SQLRouter> NewClusterSQLRouter(const SQLRouterOptions& options);

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_SQL_ROUTER_H_
