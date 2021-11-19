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

#include "sdk/sql_sdk_test.h"

#include <sched.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "test/base_test.h"
#include "vm/catalog.h"

namespace openmldb {
namespace sdk {

MiniCluster* mc_ = nullptr;
std::shared_ptr<SQLRouter> router_ = std::shared_ptr<SQLRouter>();

void SQLSDKTest::CreateDB(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                          std::shared_ptr<SQLRouter> router) {
    std::unordered_set<std::string> input_dbs;
    if (sql_case.db().empty()) {
        sql_case.db_ =
            hybridse::sqlcase::SqlCase::GenRand("auto_db") + std::to_string(static_cast<int64_t>(time(NULL)));
    }
    input_dbs.insert(sql_case.db_);

    for (size_t i = 0; i < sql_case.inputs_.size(); i++) {
        if (!sql_case.inputs_[i].db_.empty()) {
            input_dbs.insert(sql_case.inputs_[i].db_);
        }
    }
    hybridse::sdk::Status status;
    std::vector<std::string> dbs;
    ASSERT_TRUE(router->ShowDB(&dbs, &status));

    // create db if not exist
    std::set<std::string> db_set(dbs.begin(), dbs.end());
    for (auto iter = input_dbs.begin(); iter != input_dbs.end(); iter++) {
        auto db_name = *iter;
        DLOG(INFO) << "Create DB " << db_name << " BEGIN";
        if (db_set.find(db_name) == db_set.cend()) {
            ASSERT_TRUE(router->CreateDB(db_name, &status));
        }
        DLOG(INFO) << "Create DB DONE!";
    }
}

void SQLSDKTest::CreateTables(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router, int partition_num) {
    DLOG(INFO) << "Create Tables BEGIN";
    hybridse::sdk::Status status;
    // create and insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(
                hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL))), i);
        }
        std::string input_db_name = sql_case.inputs_[i].db_.empty() ? sql_case.db() : sql_case.inputs_[i].db_;
        // create table
        std::string create;
        if (sql_case.BuildCreateSqlFromInput(i, &create, partition_num) && !create.empty()) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            boost::replace_all(create, placeholder, sql_case.inputs()[i].name_);
            LOG(INFO) << create;
            router->ExecuteDDL(input_db_name, create, &status);
            ASSERT_TRUE(router->RefreshCatalog());
            ASSERT_TRUE(status.code == 0) << status.msg;
        }
    }
    DLOG(INFO) << "Create Tables DONE";
}

void SQLSDKTest::DropTables(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                            std::shared_ptr<SQLRouter> router) {
    hybridse::sdk::Status status;

    // create and insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            LOG(WARNING) << "Skip drop table with empty table name";
            continue;
        }
        // create table
        std::string db_name = sql_case.inputs_[i].db_.empty() ? sql_case.db_ : sql_case.inputs_[i].db_;
        std::string drop = "drop table " + sql_case.inputs()[i].name_ + ";";
        LOG(INFO) << drop;
        if (!drop.empty()) {
            router->ExecuteDDL(db_name, drop, &status);
            ASSERT_TRUE(router->RefreshCatalog());
        }
    }
}

void SQLSDKTest::CreateProcedure(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router, bool is_batch) {
    DLOG(INFO) << "Create Procedure BEGIN";
    hybridse::sdk::Status status;
    if (sql_case.inputs()[0].name_.empty()) {
        sql_case.set_input_name(
            hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL))), 0);
    }
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(
        sql, "{auto}",
        hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL))));
    boost::trim(sql);
    LOG(INFO) << sql;
    sql_case.sp_name_ =
        hybridse::sqlcase::SqlCase::GenRand("auto_sp") + std::to_string(static_cast<int64_t>(time(NULL)));
    std::string create_sp;
    if (is_batch) {
        hybridse::type::TableDef batch_request_schema;
        ASSERT_TRUE(sql_case.ExtractTableDef(sql_case.batch_request().columns_, sql_case.batch_request().indexs_,
                                             batch_request_schema));
        ASSERT_TRUE(sql_case.BuildCreateSpSqlFromSchema(batch_request_schema, sql,
                                                        sql_case.batch_request().common_column_indices_, &create_sp));
    } else {
        std::set<size_t> common_idx;
        ASSERT_TRUE(sql_case.BuildCreateSpSqlFromInput(0, sql, common_idx, &create_sp));
    }

    for (size_t i = 0; i < sql_case.inputs_.size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(create_sp, placeholder, sql_case.inputs()[i].name_);
    }
    LOG(INFO) << create_sp;
    if (!create_sp.empty()) {
        router->ExecuteDDL(sql_case.db(), create_sp, &status);
        ASSERT_TRUE(router->RefreshCatalog());
    }
    DLOG(INFO) << "Create Procedure DONE";
    if (!sql_case.expect().success_) {
        ASSERT_NE(0, status.code) << status.msg;
        return;
    }
    ASSERT_NO_FATAL_FAILURE(ASSERT_TRUE(0 == status.code)) << status.msg;
    auto sp_info = router->ShowProcedure(sql_case.db(), sql_case.sp_name_, &status);
    for (int try_n = 0; try_n < 5; try_n++) {
        if (sp_info && status.code == 0) {
            break;
        }
        ASSERT_TRUE(router->RefreshCatalog());
        sp_info = router->ShowProcedure(sql_case.db(), sql_case.sp_name_, &status);
        LOG(WARNING) << "Procedure not found, try " << try_n << " times";
        sleep(try_n + 1);
    }
    ASSERT_TRUE(sp_info && status.code == 0) << status.msg;
    if (is_batch) {
        std::set<size_t> input_common_indices;
        for (size_t idx : sql_case.batch_request().common_column_indices_) {
            input_common_indices.insert(idx);
        }
        for (int i = 0; i < sp_info->GetInputSchema().GetColumnCnt(); ++i) {
            auto is_const = input_common_indices.find(i) != input_common_indices.end();
            ASSERT_EQ(is_const, sp_info->GetInputSchema().IsConstant(i)) << "At input column " << i;
        }
        if (!sql_case.expect().common_column_indices_.empty()) {
            std::set<size_t> output_common_indices;
            for (size_t idx : sql_case.expect().common_column_indices_) {
                output_common_indices.insert(idx);
            }
            for (int i = 0; i < sp_info->GetOutputSchema().GetColumnCnt(); ++i) {
                auto is_const = output_common_indices.find(i) != output_common_indices.end();
                ASSERT_EQ(is_const, sp_info->GetOutputSchema().IsConstant(i)) << "At output column " << i;
            }
        }
    }
}

void SQLSDKTest::DropProcedure(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                               std::shared_ptr<SQLRouter> router) {
    hybridse::sdk::Status status;
    if (sql_case.sp_name_.empty()) {
        LOG(WARNING) << "fail to drop procedure, sp name is empty";
        return;
    }
    std::string drop = "drop procedure " + sql_case.sp_name_ + ";";
    LOG(INFO) << drop;
    if (!drop.empty()) {
        router->ExecuteDDL(sql_case.db(), drop, &status);
        ASSERT_TRUE(router->RefreshCatalog());
    }
}

void SQLSDKTest::InsertTables(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router, InsertRule insert_rule) {
    DLOG(INFO) << "Insert Tables BEGIN";
    hybridse::sdk::Status status;
    // insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (0 == i && kNotInsertFirstInput == insert_rule) {
            continue;
        }

        std::string db_name = sql_case.inputs_[i].db_.empty() ? sql_case.db_ : sql_case.inputs_[i].db_;
        // insert into table
        std::vector<std::string> inserts;
        ASSERT_TRUE(sql_case.BuildInsertSqlListFromInput(i, &inserts));
        for (size_t row_idx = 0; row_idx < inserts.size(); row_idx++) {
            if (0 == i && row_idx == inserts.size() - 1 && kNotInsertLastRowOfFirstInput == insert_rule) {
                continue;
            }
            auto insert = inserts[row_idx];
            std::string placeholder = "{" + std::to_string(i) + "}";
            boost::replace_all(insert, placeholder, sql_case.inputs()[i].name_);
            DLOG(INFO) << insert;
            if (!insert.empty()) {
                for (int j = 0; j < sql_case.inputs()[i].repeat_; j++) {
                    ASSERT_TRUE(router->ExecuteInsert(db_name, insert, &status)) << status.msg;
                    ASSERT_TRUE(router->RefreshCatalog());
                }
            }
        }
    }
    ASSERT_TRUE(router->RefreshCatalog());
    DLOG(INFO) << "Insert Tables DONE";
}

void SQLSDKTest::CovertHybridSERowToRequestRow(hybridse::codec::RowView* row_view,
                                               std::shared_ptr<openmldb::sdk::SQLRequestRow> request_row) {
    ASSERT_EQ(row_view->GetSchema()->size(), request_row->GetSchema()->GetColumnCnt());

    int32_t init_size = 0;
    for (int i = 0; i < row_view->GetSchema()->size(); i++) {
        if (hybridse::type::Type::kVarchar == row_view->GetSchema()->Get(i).type()) {
            init_size += row_view->GetStringUnsafe(i).size();
        }
    }
    DLOG(INFO) << "Build Request Row: init string size " << init_size;
    request_row->Init(init_size);
    for (int i = 0; i < row_view->GetSchema()->size(); i++) {
        if (row_view->IsNULL(i)) {
            request_row->AppendNULL();
            continue;
        }
        switch (row_view->GetSchema()->Get(i).type()) {
            case hybridse::type::kBool:
                ASSERT_TRUE(request_row->AppendBool(row_view->GetBoolUnsafe(i)));
                break;
            case hybridse::type::kInt16:
                ASSERT_TRUE(request_row->AppendInt16(row_view->GetInt16Unsafe(i)));
                break;
            case hybridse::type::kInt32:
                ASSERT_TRUE(request_row->AppendInt32(row_view->GetInt32Unsafe(i)));
                break;
            case hybridse::type::kInt64:
                ASSERT_TRUE(request_row->AppendInt64(row_view->GetInt64Unsafe(i)));
                break;
            case hybridse::type::kFloat:
                ASSERT_TRUE(request_row->AppendFloat(row_view->GetFloatUnsafe(i)));
                break;
            case hybridse::type::kDouble:
                ASSERT_TRUE(request_row->AppendDouble(row_view->GetDoubleUnsafe(i)));
                break;
            case hybridse::type::kTimestamp:
                ASSERT_TRUE(request_row->AppendTimestamp(row_view->GetTimestampUnsafe(i)));
                break;
            case hybridse::type::kDate:
                ASSERT_TRUE(request_row->AppendDate(row_view->GetDateUnsafe(i)));
                break;
            case hybridse::type::kVarchar:
                ASSERT_TRUE(request_row->AppendString(row_view->GetStringUnsafe(i)));
                break;
            default: {
                FAIL() << "Fail conver hybridse row to fedb sdk request row";
                return;
            }
        }
    }
    ASSERT_TRUE(request_row->Build());
}
void SQLSDKTest::BatchExecuteSQL(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints) {
    DLOG(INFO) << "BatchExecuteSQL BEGIN";
    hybridse::sdk::Status status;
    DLOG(INFO) << "format sql begin";
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    DLOG(INFO) << "format sql 1";
    boost::replace_all(
        sql, "{auto}",
        hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL))));
    for (size_t endpoint_id = 0; endpoint_id < tbEndpoints.size(); endpoint_id++) {
        boost::replace_all(sql, "{tb_endpoint_" + std::to_string(endpoint_id) + "}", tbEndpoints.at(endpoint_id));
    }
    DLOG(INFO) << "format sql done";
    LOG(INFO) << sql;
    std::string lower_sql = boost::to_lower_copy(sql);
    if (boost::algorithm::starts_with(lower_sql, "select")) {
        std::shared_ptr<hybridse::sdk::ResultSet> rs;
        // parameterized batch query
        if (!sql_case.parameters().columns_.empty()) {
            auto parameter_schema = sql_case.ExtractParameterTypes();
            std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter_row =
                std::make_shared<openmldb::sdk::SQLRequestRow>(
                    std::make_shared<hybridse::sdk::SchemaImpl>(parameter_schema), std::set<std::string>());
            hybridse::codec::RowView row_view(parameter_schema);
            std::vector<hybridse::codec::Row> parameter_rows;
            sql_case.ExtractRows(parameter_schema, sql_case.parameters().rows_, parameter_rows);
            if (parameter_rows.empty()) {
                FAIL() << "sql case parameter rows extract fail";
                return;
            }
            row_view.Reset(parameter_rows[0].buf());
            CovertHybridSERowToRequestRow(&row_view, parameter_row);

            rs = router->ExecuteSQLParameterized(sql_case.db(), sql, parameter_row, &status);
        } else {
            rs = router->ExecuteSQL(sql_case.db(), sql, &status);
        }
        if (!sql_case.expect().success_) {
            if ((rs)) {
                FAIL() << "sql case expect success == false";
            }
            return;
        }

        if (!rs) FAIL() << "sql case expect success == true" << status.msg;
        std::vector<hybridse::codec::Row> rows;
        hybridse::type::TableDef output_table;
        if (!sql_case.expect().schema_.empty() || !sql_case.expect().columns_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
            CheckSchema(output_table.columns(), *(rs->GetSchema()));
        }

        if (!sql_case.expect().data_.empty() || !sql_case.expect().rows_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputData(rows));
            CheckRows(output_table.columns(), sql_case.expect().order_, rows, rs);
        }

        if (sql_case.expect().count_ > 0) {
            ASSERT_EQ(sql_case.expect().count_, static_cast<int64_t>(rs->Size()));
        }
    } else if (boost::algorithm::starts_with(lower_sql, "create")) {
        bool ok = router->ExecuteDDL(sql_case.db(), sql, &status);
        router->RefreshCatalog();
        ASSERT_EQ(sql_case.expect().success_, ok) << status.msg;
    } else if (boost::algorithm::starts_with(lower_sql, "insert")) {
        bool ok = router->ExecuteInsert(sql_case.db(), sql, &status);
        ASSERT_EQ(sql_case.expect().success_, ok) << status.msg;
    } else {
        FAIL() << "sql not support in request mode";
    }
    DLOG(INFO) << "BatchExecuteSQL DONE";
}

void SQLSDKTest::RunBatchModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router,
                                 const std::vector<std::string>& tbEndpoints) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, kInsertAllInputs);
    BatchExecuteSQL(sql_case, router, tbEndpoints);
    DropTables(sql_case, router);
    LOG(INFO) << "RunBatchModeSDK ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKQueryTest::RequestExecuteSQL(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                        std::shared_ptr<SQLRouter> router, bool has_batch_request, bool is_procedure,
                                        bool is_asyn) {
    hybridse::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", hybridse::sqlcase::SqlCase::GenRand("auto_t") +
            std::to_string(static_cast<int64_t>(time(NULL))));
    LOG(INFO) << sql;
    std::string lower_sql = sql;
    boost::to_lower(lower_sql);
    if (boost::algorithm::starts_with(lower_sql, "select")) {
        LOG(INFO) << "GetRequestRow start";
        auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);
        LOG(INFO) << "GetRequestRow done";
        // success check
        LOG(INFO) << "sql case success check: required success " << (sql_case.expect().success_ ? "true" : "false");
        if (!sql_case.expect().success_) {
            if ((request_row)) {
                FAIL() << "sql case expect success == false";
            }
            LOG(INFO) << "sql case success false check done!";
            return;
        }
        if (!request_row) {
            FAIL() << "sql case expect success == true" << status.msg;
        }

        hybridse::type::TableDef insert_table;
        std::vector<hybridse::codec::Row> insert_rows;
        std::vector<std::string> inserts;
        if (!has_batch_request) {
            ASSERT_TRUE(sql_case.ExtractInputTableDef(insert_table, 0));
            ASSERT_TRUE(sql_case.ExtractInputData(insert_rows, 0));
            sql_case.BuildInsertSqlListFromInput(0, &inserts);
        } else {
            ASSERT_TRUE(sql_case.ExtractInputTableDef(sql_case.batch_request_, insert_table));
            ASSERT_TRUE(sql_case.ExtractInputData(sql_case.batch_request_, insert_rows));
        }
        CheckSchema(insert_table.columns(), *(request_row->GetSchema().get()));
        LOG(INFO) << "Request Row:\n";
        PrintRows(insert_table.columns(), insert_rows);

        hybridse::codec::RowView row_view(insert_table.columns());
        std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results;
        LOG(INFO) << "Request execute sql start!";
        for (size_t i = 0; i < insert_rows.size(); i++) {
            row_view.Reset(insert_rows[i].buf());
            CovertHybridSERowToRequestRow(&row_view, request_row);
            std::shared_ptr<hybridse::sdk::ResultSet> rs;
            if (is_procedure) {
                if (is_asyn) {
                    LOG(INFO) << "-------asyn procedure----------";
                    auto future = router->CallProcedure(sql_case.db(), sql_case.sp_name_, 1000, request_row, &status);
                    if (!future || status.code != 0) FAIL() << "sql case expect success == true" << status.msg;
                    rs = future->GetResultSet(&status);
                } else {
                    LOG(INFO) << "--------syn procedure----------";
                    rs = router->CallProcedure(sql_case.db(), sql_case.sp_name_, request_row, &status);
                }
            } else {
                rs = router->ExecuteSQLRequest(sql_case.db(), sql, request_row, &status);
            }
            if (!rs || status.code != 0) FAIL() << "sql case expect success == true" << status.msg;
            results.push_back(rs);
            if (!has_batch_request) {
                LOG(INFO) << "insert request: \n" << inserts[i];
                bool ok = router->ExecuteInsert(insert_table.catalog(), inserts[i], &status);
                ASSERT_TRUE(ok);
            }
        }
        LOG(INFO) << "Request execute sql done!";
        ASSERT_FALSE(results.empty());
        std::vector<hybridse::codec::Row> rows;
        hybridse::type::TableDef output_table;
        if (!sql_case.expect().schema_.empty() || !sql_case.expect().columns_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
            CheckSchema(output_table.columns(), *(results[0]->GetSchema()));
        }

        if (!sql_case.expect().data_.empty() || !sql_case.expect().rows_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputData(rows));
            CheckRows(output_table.columns(), sql_case.expect().order_, rows, results);
        }

        if (sql_case.expect().count_ > 0) {
            ASSERT_EQ(sql_case.expect().count_, static_cast<int64_t>(results.size()));
        }
    } else if (boost::algorithm::starts_with(lower_sql, "create")) {
        FAIL() << "create sql not support in request mode";
    } else if (boost::algorithm::starts_with(lower_sql, "insert")) {
        FAIL() << "insert sql not support in request mode";
    } else {
        FAIL() << "sql not support in request mode";
    }
}

void SQLSDKQueryTest::BatchRequestExecuteSQL(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                             std::shared_ptr<SQLRouter> router, bool has_batch_request,
                                             bool is_procedure, bool is_asyn) {
    if (has_batch_request) {
        BatchRequestExecuteSQLWithCommonColumnIndices(sql_case, router, sql_case.batch_request().common_column_indices_,
                                                      is_procedure, is_asyn);
    } else if (!sql_case.inputs().empty()) {
        // set different common column conf
        size_t schema_size = sql_case.inputs()[0].columns_.size();
        std::set<size_t> common_column_indices;

        // empty
        BatchRequestExecuteSQLWithCommonColumnIndices(sql_case, router, common_column_indices, is_procedure, is_asyn);

        // full
        for (size_t i = 0; i < schema_size; ++i) {
            common_column_indices.insert(i);
        }
        BatchRequestExecuteSQLWithCommonColumnIndices(sql_case, router, common_column_indices, is_procedure, is_asyn);

        common_column_indices.clear();

        // partial
        // 0, 2, 4, ...
        for (size_t i = 0; i < schema_size; i += 2) {
            common_column_indices.insert(i);
        }
        BatchRequestExecuteSQLWithCommonColumnIndices(sql_case, router, common_column_indices, is_procedure, is_asyn);

        common_column_indices.clear();
        return;
        // 1, 3, 5, ...
        for (size_t i = 1; i < schema_size; i += 2) {
            common_column_indices.insert(i);
        }
        BatchRequestExecuteSQLWithCommonColumnIndices(sql_case, router, common_column_indices, is_procedure, is_asyn);
    }
}
void SQLSDKQueryTest::BatchRequestExecuteSQLWithCommonColumnIndices(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                                    std::shared_ptr<SQLRouter> router,
                                                                    const std::set<size_t>& common_indices,
                                                                    bool is_procedure, bool is_asyn) {
    std::ostringstream oss;
    if (common_indices.empty()) {
        oss << "empty";
    } else {
        for (size_t index : common_indices) {
            oss << index << ",";
        }
    }

    LOG(INFO) << "BatchRequestExecuteSQLWithCommonColumnIndices: " << oss.str();
    hybridse::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", hybridse::sqlcase::SqlCase::GenRand("auto_t") +
            std::to_string(static_cast<int64_t>(time(NULL))));
    LOG(INFO) << sql;
    std::string lower_sql = sql;
    boost::to_lower(lower_sql);
    if (!boost::algorithm::starts_with(lower_sql, "select")) {
        FAIL() << "sql not support in request mode";
    }

    auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);
    LOG(INFO) << "sql case success check: required success " << (sql_case.expect().success_ ? "true" : "false");
    if (!sql_case.expect().success_) {
        if ((request_row)) {
            FAIL() << "sql case expect success == false";
        }
        LOG(INFO) << "sql case success false check done!";
        return;
    }
    if (!request_row) {
        FAIL() << "sql case expect success == true" << status.msg;
    }

    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    hybridse::type::TableDef batch_request_table;
    std::vector<::hybridse::codec::Row> request_rows;
    auto common_column_indices = std::make_shared<ColumnIndicesSet>(request_row->GetSchema());
    if (has_batch_request) {
        auto& batch_request = sql_case.batch_request();
        ASSERT_TRUE(sql_case.ExtractTableDef(batch_request.columns_, batch_request.indexs_, batch_request_table));
        ASSERT_TRUE(sql_case.ExtractRows(batch_request_table.columns(), batch_request.rows_, request_rows));

    } else {
        ASSERT_TRUE(sql_case.ExtractInputTableDef(sql_case.inputs_[0], batch_request_table));
        std::vector<hybridse::codec::Row> rows;
        ASSERT_TRUE(sql_case.ExtractInputData(sql_case.inputs_[0], rows));
        request_rows.push_back(rows.back());
    }
    CheckSchema(batch_request_table.columns(), *(request_row->GetSchema().get()));
    LOG(INFO) << "Request Row:\n";
    PrintRows(batch_request_table.columns(), request_rows);

    for (size_t idx : common_indices) {
        common_column_indices->AddCommonColumnIdx(idx);
    }
    hybridse::codec::RowView row_view(batch_request_table.columns());

    auto row_batch = std::make_shared<SQLRequestRowBatch>(request_row->GetSchema(), common_column_indices);

    LOG(INFO) << "Batch Request execute sql start!";
    for (size_t i = 0; i < request_rows.size(); i++) {
        row_view.Reset(request_rows[i].buf());
        CovertHybridSERowToRequestRow(&row_view, request_row);
        ASSERT_TRUE(row_batch->AddRow(request_row));
    }
    std::shared_ptr<hybridse::sdk::ResultSet> results;
    if (is_procedure) {
        if (is_asyn) {
            LOG(INFO) << "-------asyn procedure----------";
            auto future =
                router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.sp_name_, 1000, row_batch, &status);
            if (!future || status.code != 0) FAIL() << "sql case expect success == true" << status.msg;
            results = future->GetResultSet(&status);
        } else {
            LOG(INFO) << "--------syn procedure----------";
            results = router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.sp_name_, row_batch, &status);
            if (status.code != 0) FAIL() << "sql case expect success == true" << status.msg;
        }
    } else {
        results = router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status);
        if (status.code != 0) FAIL() << "sql case expect success == true" << status.msg;
    }
    if (!results) {
        FAIL() << "sql case expect success == true";
    }
    ASSERT_NO_FATAL_FAILURE(ASSERT_EQ(0, status.code)) << status.msg;
    LOG(INFO) << "Batch request execute sql done!";
    ASSERT_GT(results->Size(), 0);
    std::vector<hybridse::codec::Row> rows;
    hybridse::type::TableDef output_table;
    if (!sql_case.expect().schema_.empty() || !sql_case.expect().columns_.empty()) {
        ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
        CheckSchema(output_table.columns(), *(results->GetSchema()));
    }
    if (!sql_case.expect().data_.empty() || !sql_case.expect().rows_.empty()) {
        ASSERT_TRUE(sql_case.ExtractOutputData(rows));
        // for batch request mode, trivally compare last result
        if (!has_batch_request) {
            if (!rows.empty()) {
                rows = {rows.back()};
            }
        }
        CheckRows(output_table.columns(), sql_case.expect().order_, rows, results);
    }
    if (sql_case.expect().count_ > 0) {
        ASSERT_EQ(sql_case.expect().count_, static_cast<int64_t>(results->Size()));
    }
}

void SQLSDKQueryTest::RunRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                        std::shared_ptr<SQLRouter> router) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertFirstInput);
    RequestExecuteSQL(sql_case, router, has_batch_request);
    DropTables(sql_case, router);
    LOG(INFO) << "RequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}
void SQLSDKQueryTest::DistributeRunRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                  std::shared_ptr<SQLRouter> router, int32_t partition_num) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertFirstInput);
    RequestExecuteSQL(sql_case, router, has_batch_request);
    DropTables(sql_case, router);
    LOG(INFO) << "DistributeRunRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}
void SQLSDKQueryTest::RunBatchRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                             std::shared_ptr<SQLRouter> router) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertLastRowOfFirstInput);
    BatchRequestExecuteSQL(sql_case, router, has_batch_request, false, false);
    DropTables(sql_case, router);
    LOG(INFO) << "BatchRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKQueryTest::DistributeRunBatchRequestModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                       std::shared_ptr<SQLRouter> router, int32_t partition_num) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertLastRowOfFirstInput);
    BatchRequestExecuteSQL(sql_case, router, has_batch_request, false, false);

    DropTables(sql_case, router);
    LOG(INFO) << "DistributeRunBatchRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc()
              << " done!";
}

void SQLSDKQueryTest::RunRequestProcedureModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                 std::shared_ptr<SQLRouter> router, bool is_asyn) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertFirstInput);
    CreateProcedure(sql_case, router);
    RequestExecuteSQL(sql_case, router, has_batch_request, true, is_asyn);
    DropProcedure(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "RequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKBatchRequestQueryTest::RunBatchRequestProcedureModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                                  std::shared_ptr<SQLRouter> router, bool is_asyn) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    CreateProcedure(sql_case, router, true);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertLastRowOfFirstInput);
    BatchRequestExecuteSQL(sql_case, router, has_batch_request, true, is_asyn);
    DropProcedure(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "BatchRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKQueryTest::DistributeRunRequestProcedureModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                           std::shared_ptr<SQLRouter> router, int32_t partition_num,
                                                           bool is_asyn) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertFirstInput);
    CreateProcedure(sql_case, router);
    RequestExecuteSQL(sql_case, router, has_batch_request, true, is_asyn);
    DropProcedure(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "RequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKBatchRequestQueryTest::DistributeRunBatchRequestProcedureModeSDK(
    hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
    std::shared_ptr<SQLRouter> router, int32_t partition_num, bool is_asyn) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    CreateProcedure(sql_case, router, true);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertLastRowOfFirstInput);
    BatchRequestExecuteSQL(sql_case, router, has_batch_request, true, is_asyn);
    DropProcedure(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "BatchRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

static std::shared_ptr<SQLRouter> GetNewSQLRouter() {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.session_timeout = 60000;
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    return NewClusterSQLRouter(sql_opt);
}
static bool IsRequestSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos ||
        mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
            mode.find("request-unsupport") != std::string::npos
        || mode.find("standalone-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
static bool IsBatchRequestSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos ||
        mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("batch-request-unsupport") != std::string::npos ||
        mode.find("request-unsupport") != std::string::npos
        || mode.find("standalone-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
static bool IsBatchSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos ||
        mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("batch-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("standalone-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
TEST_P(SQLSDKTest, sql_sdk_batch_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsBatchSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    SQLRouterOptions sql_opt;
    sql_opt.session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = sql_case.debug() || hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
        return;
    }
    SQLSDKTest::RunBatchModeSDK(sql_case, router, mc_->GetTbEndpoint());
}

TEST_P(SQLSDKQueryTest, sql_sdk_request_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunRequestModeSDK(sql_case, router_);
    LOG(INFO) << "Finish sql_sdk_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKQueryTest, sql_sdk_batch_request_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunBatchRequestModeSDK(sql_case, router_);
    LOG(INFO) << "Finish sql_sdk_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, sql_sdk_batch_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsBatchSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunBatchModeSDK(sql_case, router_, mc_->GetTbEndpoint());
}

TEST_P(SQLSDKQueryTest, sql_sdk_request_procedure_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunRequestProcedureModeSDK(sql_case, router_, false);
    LOG(INFO) << "Finish sql_sdk_request_procedure_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, sql_sdk_request_procedure_asyn_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunRequestProcedureModeSDK(sql_case, router_, true);
    LOG(INFO) << "Finish sql_sdk_request_procedure_asyn_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_batch_request_test) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunBatchRequestModeSDK(sql_case, router_);
    LOG(INFO) << "Finish sql_sdk_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_batch_request_procedure_test) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunBatchRequestProcedureModeSDK(sql_case, router_, false);
    LOG(INFO) << "Finish sql_sdk_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_batch_request_procedure_asyn_test) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunBatchRequestProcedureModeSDK(sql_case, router_, true);
    LOG(INFO) << "Finish sql_sdk_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_F(SQLSDKQueryTest, execute_where_test) {
    std::string ddl =
        "create table trans(c_sk_seq string,\n"
        "                   cust_no string,\n"
        "                   pay_cust_name string,\n"
        "                   pay_card_no string,\n"
        "                   payee_card_no string,\n"
        "                   card_type string,\n"
        "                   merch_id string,\n"
        "                   txn_datetime string,\n"
        "                   txn_amt double,\n"
        "                   txn_curr string,\n"
        "                   card_balance double,\n"
        "                   day_openbuy double,\n"
        "                   credit double,\n"
        "                   remainning_credit double,\n"
        "                   indi_openbuy double,\n"
        "                   lgn_ip string,\n"
        "                   IEMI string,\n"
        "                   client_mac string,\n"
        "                   chnl_type int32,\n"
        "                   cust_idt int32,\n"
        "                   cust_idt_no string,\n"
        "                   province string,\n"
        "                   city string,\n"
        "                   latitudeandlongitude string,\n"
        "                   txn_time timestamp,\n"
        "                   index(key=pay_card_no, ts=txn_time),\n"
        "                   index(key=merch_id, ts=txn_time));";
    SQLRouterOptions sql_opt;
    sql_opt.session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "sql_where_test";
    hybridse::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));
    ASSERT_TRUE(router->RefreshCatalog());
    int64_t ts = 1594800959827;
    char buffer[4096];
    sprintf(buffer,  // NOLINT
            "insert into trans "
            "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
            "payee_card_no0','card_type0','mc_%d','2020-"
            "10-20 "
            "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
            ",'client_mac0',10,20,'cust_idt_no0','"
            "province0',"
            "'city0', 'longitude', %s);",
            0, 0, std::to_string(ts++).c_str());  // NOLINT
    std::string insert_sql = std::string(buffer, strlen(buffer));
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    std::string where_exist = "select * from trans where merch_id='mc_0';";
    auto rs = router->ExecuteSQL(db, where_exist, &status);
    if (!rs) {
        FAIL() << "fail to execute sql";
    }
    ASSERT_EQ(rs->Size(), 1);
    std::string where_not_exist = "select * from trans where merch_id='mc_1';";
    rs = router->ExecuteSQL(db, where_not_exist, &status);
    if (!rs) {
        FAIL() << "fail to execute sql";
    }
    ASSERT_EQ(rs->Size(), 0);
}

TEST_F(SQLSDKQueryTest, execute_insert_loops_test) {
    std::string ddl =
        "create table trans(c_sk_seq string,\n"
        "                   cust_no string,\n"
        "                   pay_cust_name string,\n"
        "                   pay_card_no string,\n"
        "                   payee_card_no string,\n"
        "                   card_type string,\n"
        "                   merch_id string,\n"
        "                   txn_datetime string,\n"
        "                   txn_amt double,\n"
        "                   txn_curr string,\n"
        "                   card_balance double,\n"
        "                   day_openbuy double,\n"
        "                   credit double,\n"
        "                   remainning_credit double,\n"
        "                   indi_openbuy double,\n"
        "                   lgn_ip string,\n"
        "                   IEMI string,\n"
        "                   client_mac string,\n"
        "                   chnl_type int32,\n"
        "                   cust_idt int32,\n"
        "                   cust_idt_no string,\n"
        "                   province string,\n"
        "                   city string,\n"
        "                   latitudeandlongitude string,\n"
        "                   txn_time timestamp,\n"
        "                   index(key=pay_card_no, ts=txn_time),\n"
        "                   index(key=merch_id, ts=txn_time));";
    int64_t ts = 1594800959827;
    int card = 0;
    int mc = 0;
    int64_t error_cnt = 0;
    int64_t cnt = 0;
    SQLRouterOptions sql_opt;
    sql_opt.session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "leak_test";
    hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    router->ExecuteDDL(db, "drop table trans;", &status);
    ASSERT_TRUE(router->RefreshCatalog());
    if (!router->ExecuteDDL(db, ddl, &status)) {
        ASSERT_TRUE(router->RefreshCatalog());
        LOG(WARNING) << "fail to create table";
        return;
    }
    ASSERT_TRUE(router->RefreshCatalog());
    while (true) {
        char buffer[4096];
        sprintf(buffer,  // NOLINT
                "insert into trans "
                "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
                "payee_card_no0','card_type0','mc_%d','2020-"
                "10-20 "
                "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
                ",'client_mac0',10,20,'cust_idt_no0','"
                "province0',"
                "'city0', 'longitude', %s);",
                card++, mc++, std::to_string(ts++).c_str());  // NOLINT
        std::string insert_sql = std::string(buffer, strlen(buffer));
        //        LOG(INFO) << insert_sql;
        hybridse::sdk::Status status;
        if (!router->ExecuteInsert(db, insert_sql, &status)) {
            error_cnt += 1;
        }

        if (cnt % 10000 == 0) {
            LOG(INFO) << "process ...... " << cnt << " error: " << error_cnt;
        }
        cnt++;
        break;
    }
}

TEST_F(SQLSDKQueryTest, create_no_ts) {
    std::string ddl =
        "create table t1(c1 string,\n"
        "                c2 bigint,\n"
        "                index(key=c1, ttl=14400m, ttl_type=absolute));";
    SQLRouterOptions sql_opt;
    sql_opt.session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "create_no_ts";
    hybridse::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));
    ASSERT_TRUE(router->RefreshCatalog());
    std::string insert_sql = "insert into t1 values('c1x', 1234);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    std::string where_exist = "select * from t1 where c1='c1x';";
    auto rs = router->ExecuteSQL(db, where_exist, &status);
    if (!rs) {
        FAIL() << "fail to execute sql";
    }
    ASSERT_EQ(rs->Size(), 1);
    std::string where_not_exist = "select * from t1 where c1='mc_1';";
    rs = router->ExecuteSQL(db, where_not_exist, &status);
    if (!rs) {
        FAIL() << "fail to execute sql";
    }
    ASSERT_EQ(rs->Size(), 0);
}

TEST_F(SQLSDKQueryTest, request_procedure_test) {
    // create table
    std::string ddl =
        "create table trans(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7));";
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.session_timeout = 30000;
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "test";
    hybridse::sdk::Status status;
    router->CreateDB(db, &status);
    router->ExecuteDDL(db, "drop table trans;", &status);
    ASSERT_TRUE(router->RefreshCatalog());
    if (!router->ExecuteDDL(db, ddl, &status)) {
        FAIL() << "fail to create table";
    }
    ASSERT_TRUE(router->RefreshCatalog());
    // insert
    std::string insert_sql = "insert into trans values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp";
    std::string sql =
        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS"
        " (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl =
        "create procedure " + sp_name +
        " (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, const c7 timestamp, c8 date" + ")" +
        " begin " + sql + " end;";
    if (!router->ExecuteDDL(db, sp_ddl, &status)) {
        FAIL() << "fail to create procedure";
    }
    // call procedure
    ASSERT_TRUE(router->RefreshCatalog());
    auto request_row = router->GetRequestRow(db, sql, &status);
    ASSERT_TRUE(request_row);
    request_row->Init(2);
    ASSERT_TRUE(request_row->AppendString("bb"));
    ASSERT_TRUE(request_row->AppendInt32(23));
    ASSERT_TRUE(request_row->AppendInt64(33));
    ASSERT_TRUE(request_row->AppendFloat(1.5f));
    ASSERT_TRUE(request_row->AppendDouble(2.5));
    ASSERT_TRUE(request_row->AppendTimestamp(1590738994000));
    ASSERT_TRUE(request_row->AppendDate(1234));
    ASSERT_TRUE(request_row->Build());
    auto rs = router->CallProcedure(db, sp_name, request_row, &status);
    if (!rs) FAIL() << "call procedure failed";
    auto schema = rs->GetSchema();
    ASSERT_EQ(schema->GetColumnCnt(), 3);
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(rs->GetStringUnsafe(0), "bb");
    ASSERT_EQ(rs->GetInt32Unsafe(1), 23);
    ASSERT_EQ(rs->GetInt64Unsafe(2), 67);
    ASSERT_FALSE(rs->Next());
    // show procedure
    std::string msg;
    auto sp_info = router->ShowProcedure(db, sp_name, &status);
    ASSERT_TRUE(sp_info);
    ASSERT_EQ(sp_info->GetDbName(), db);
    ASSERT_EQ(sp_info->GetSpName(), sp_name);
    ASSERT_EQ(sp_info->GetMainTable(), "trans");
    ASSERT_EQ(sp_info->GetMainDb(), db);
    ASSERT_EQ(sp_info->GetDbs().size(), 1u);
    ASSERT_EQ(sp_info->GetDbs().at(0), db);
    ASSERT_EQ(sp_info->GetTables().size(), 1u);
    ASSERT_EQ(sp_info->GetTables().at(0), "trans");
    auto& input_schema = sp_info->GetInputSchema();
    ASSERT_EQ(input_schema.GetColumnCnt(), 7);
    ASSERT_EQ(input_schema.GetColumnName(0), "c1");
    ASSERT_EQ(input_schema.GetColumnName(1), "c3");
    ASSERT_EQ(input_schema.GetColumnName(2), "c4");
    ASSERT_EQ(input_schema.GetColumnName(3), "c5");
    ASSERT_EQ(input_schema.GetColumnName(4), "c6");
    ASSERT_EQ(input_schema.GetColumnName(5), "c7");
    ASSERT_EQ(input_schema.GetColumnName(6), "c8");
    ASSERT_EQ(input_schema.GetColumnType(0), hybridse::sdk::kTypeString);
    ASSERT_EQ(input_schema.GetColumnType(1), hybridse::sdk::kTypeInt32);
    ASSERT_EQ(input_schema.GetColumnType(2), hybridse::sdk::kTypeInt64);
    ASSERT_EQ(input_schema.GetColumnType(3), hybridse::sdk::kTypeFloat);
    ASSERT_EQ(input_schema.GetColumnType(4), hybridse::sdk::kTypeDouble);
    ASSERT_EQ(input_schema.GetColumnType(5), hybridse::sdk::kTypeTimestamp);
    ASSERT_EQ(input_schema.GetColumnType(6), hybridse::sdk::kTypeDate);
    ASSERT_TRUE(input_schema.IsConstant(0));
    ASSERT_TRUE(input_schema.IsConstant(1));
    ASSERT_TRUE(!input_schema.IsConstant(2));

    auto& output_schema = sp_info->GetOutputSchema();
    ASSERT_EQ(output_schema.GetColumnCnt(), 3);
    ASSERT_EQ(output_schema.GetColumnName(0), "c1");
    ASSERT_EQ(output_schema.GetColumnName(1), "c3");
    ASSERT_EQ(output_schema.GetColumnName(2), "w1_c4_sum");
    ASSERT_EQ(output_schema.GetColumnType(0), hybridse::sdk::kTypeString);
    ASSERT_EQ(output_schema.GetColumnType(1), hybridse::sdk::kTypeInt32);
    ASSERT_EQ(output_schema.GetColumnType(2), hybridse::sdk::kTypeInt64);
    ASSERT_TRUE(output_schema.IsConstant(0));
    ASSERT_TRUE(output_schema.IsConstant(1));
    ASSERT_TRUE(!output_schema.IsConstant(2));

    // fail to drop table before drop all associated procedures
    ASSERT_FALSE(router->ExecuteDDL(db, "drop table trans;", &status));

    // drop procedure
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(router->ExecuteDDL(db, drop_sp_sql, &status));
    // success drop table after drop all associated procedures
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table trans;", &status));
}


TEST_F(SQLSDKQueryTest, drop_table_with_procedure_test) {
    // create table trans
    std::string ddl =
        "create table trans(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7));";
    // create table trans2
    std::string ddl2 =
        "create table trans2(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7));";
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.session_timeout = 30000;
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "test_db1";
    std::string db2 = "test_db2";
    hybridse::sdk::Status status;

    // create table test_db1.trans
    router->CreateDB(db, &status);
    router->ExecuteDDL(db, "drop table trans;", &status);
    ASSERT_TRUE(router->RefreshCatalog());
    if (!router->ExecuteDDL(db, ddl, &status)) {
        FAIL() << "fail to create table";
    }
    ASSERT_TRUE(router->RefreshCatalog());

    // create table test_db1.trans2
    router->CreateDB(db, &status);
    router->ExecuteDDL(db, "drop table trans2;", &status);
    ASSERT_TRUE(router->RefreshCatalog());
    if (!router->ExecuteDDL(db, ddl2, &status)) {
        FAIL() << "fail to create table";
    }
    ASSERT_TRUE(router->RefreshCatalog());

    // create table test_db2.trans
    router->CreateDB(db2, &status);
    router->ExecuteDDL(db2, "drop table trans;", &status);
    ASSERT_TRUE(router->RefreshCatalog());
    if (!router->ExecuteDDL(db2, ddl, &status)) {
        FAIL() << "fail to create table";
    }
    ASSERT_TRUE(router->RefreshCatalog());
    // insert
    std::string insert_sql = "insert into trans values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp";
    std::string sql =
        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS"
        " (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl =
        "create procedure " + sp_name +
        " (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, const c7 timestamp, c8 date" + ")" +
        " begin " + sql + " end;";
    if (!router->ExecuteDDL(db, sp_ddl, &status)) {
        FAIL() << "fail to create procedure";
    }
    // fail to drop table test_db1.trans before drop all associated procedures
    ASSERT_FALSE(router->ExecuteDDL(db, "drop table trans;", &status));
    // it's ok to drop test_db1.trans2 since there is no associated procedures
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table trans2;", &status));
    // it's ok to drop test_db2.trans since there is no associated procedures
    ASSERT_TRUE(router->ExecuteDDL(db2, "drop table trans;", &status));

    // drop procedure
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(router->ExecuteDDL(db, drop_sp_sql, &status));
    // success drop table test_db1.trans after drop all associated procedures
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table trans;", &status));
}
TEST_F(SQLSDKTest, table_reader_scan) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string db = GenRand("db");
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    for (int i = 0; i < 2; i++) {
        std::string name = "test" + std::to_string(i);
        std::string ddl = "create table " + name +
                          "("
                          "col1 string, col2 bigint,"
                          "index(key=col1, ts=col2));";
        ok = router->ExecuteDDL(db, ddl, &status);
        ASSERT_TRUE(ok);
    }
    ASSERT_TRUE(router->RefreshCatalog());
    std::string insert = "insert into test0 values('key1', 1609212669000L);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &status));
    auto table_reader = router->GetTableReader();
    ScanOption so;
    auto rs = table_reader->Scan(db, "test0", "key1", 1609212679000l, 0, so, &status);
    ASSERT_TRUE(rs);
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(1609212669000l, rs->GetInt64Unsafe(1));
    ASSERT_FALSE(rs->Next());
}

TEST_F(SQLSDKTest, table_reader_async_scan) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string db = GenRand("db");
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    for (int i = 0; i < 2; i++) {
        std::string name = "test" + std::to_string(i);
        std::string ddl = "create table " + name +
                          "("
                          "col1 string, col2 bigint,"
                          "index(key=col1, ts=col2));";
        ok = router->ExecuteDDL(db, ddl, &status);
        ASSERT_TRUE(ok);
    }
    ASSERT_TRUE(router->RefreshCatalog());
    std::string insert = "insert into test0 values('key1', 1609212669000L);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &status));
    auto table_reader = router->GetTableReader();
    ScanOption so;
    auto future = table_reader->AsyncScan(db, "test0", "key1", 1609212679000l, 0, so, 10, &status);
    ASSERT_TRUE(future);
    auto rs = future->GetResultSet(&status);
    ASSERT_TRUE(rs);
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(1609212669000l, rs->GetInt64Unsafe(1));
    ASSERT_FALSE(rs->Next());
}
TEST_F(SQLSDKTest, create_table) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string db = GenRand("db");
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    for (int i = 0; i < 2; i++) {
        std::string name = "test" + std::to_string(i);
        std::string ddl = "create table " + name +
                          "("
                          "col1 string, col2 bigint,"
                          "index(key=col1, ts=col2));";
        ok = router->ExecuteDDL(db, ddl, &status);
        ASSERT_TRUE(ok);
    }
    ASSERT_TRUE(router->RefreshCatalog());
    auto ns_client = mc_->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    ASSERT_TRUE(ns_client->ShowTable("", db, false, tables, msg));
    ASSERT_TRUE(!tables.empty());
    std::map<std::string, int> pid_map;
    for (const auto& table : tables) {
        for (const auto& partition : table.table_partition()) {
            for (const auto& meta : partition.partition_meta()) {
                if (pid_map.find(meta.endpoint()) == pid_map.end()) {
                    pid_map.emplace(meta.endpoint(), 0);
                }
                pid_map[meta.endpoint()]++;
            }
        }
    }
    ASSERT_EQ(pid_map.size(), 1u);
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test0;", &status));
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test1;", &status));
    ASSERT_TRUE(router->DropDB(db, &status));
}

TEST_F(SQLSDKQueryTest, execute_where_with_parameter) {
    std::string ddl =
        "create table trans(c_sk_seq string,\n"
        "                   cust_no string,\n"
        "                   pay_cust_name string,\n"
        "                   pay_card_no string,\n"
        "                   payee_card_no string,\n"
        "                   card_type string,\n"
        "                   merch_id string,\n"
        "                   txn_datetime string,\n"
        "                   txn_amt double,\n"
        "                   txn_curr string,\n"
        "                   card_balance double,\n"
        "                   day_openbuy double,\n"
        "                   credit double,\n"
        "                   remainning_credit double,\n"
        "                   indi_openbuy double,\n"
        "                   lgn_ip string,\n"
        "                   IEMI string,\n"
        "                   client_mac string,\n"
        "                   chnl_type int32,\n"
        "                   cust_idt int32,\n"
        "                   cust_idt_no string,\n"
        "                   province string,\n"
        "                   city string,\n"
        "                   latitudeandlongitude string,\n"
        "                   txn_time int64,\n"
        "                   index(key=pay_card_no, ts=txn_time),\n"
        "                   index(key=merch_id, ts=txn_time));";
    SQLRouterOptions sql_opt;
    sql_opt.session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "execute_where_with_parameter";
    hybridse::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));
    ASSERT_TRUE(router->RefreshCatalog());
    int64_t ts = 1594800959827;
    // Insert 3 rows into table trans
    {
        char buffer[4096];
        sprintf(buffer,  // NOLINT
                "insert into trans "
                "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
                "payee_card_no0','card_type0','mc_%d','2020-"
                "10-20 "
                "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
                ",'client_mac0',10,20,'cust_idt_no0','"
                "province0',"
                "'city0', 'longitude', %s);",
                0, 0, std::to_string(ts++).c_str());  // NOLINT
        std::string insert_sql = std::string(buffer, strlen(buffer));
        ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    }
    {
        char buffer[4096];
        sprintf(buffer,  // NOLINT
                "insert into trans "
                "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
                "payee_card_no0','card_type0','mc_%d','2020-"
                "10-20 "
                "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
                ",'client_mac0',10,20,'cust_idt_no0','"
                "province0',"
                "'city0', 'longitude', %s);",
                0, 0, std::to_string(ts++).c_str());  // NOLINT
        std::string insert_sql = std::string(buffer, strlen(buffer));
        ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    }
    {
        char buffer[4096];
        sprintf(buffer,  // NOLINT
                "insert into trans "
                "values('c_sk_seq0','cust_no0','pay_cust_name0','card_%d','"
                "payee_card_no0','card_type0','mc_%d','2020-"
                "10-20 "
                "10:23:50',1.0,'txn_curr',2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0'"
                ",'client_mac0',10,20,'cust_idt_no0','"
                "province0',"
                "'city0', 'longitude', %s);",
                0, 0, std::to_string(ts++).c_str());  // NOLINT
        std::string insert_sql = std::string(buffer, strlen(buffer));
        ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    }

    auto parameter_types = std::make_shared<hybridse::sdk::ColumnTypes>();
    parameter_types->AddColumnType(::hybridse::sdk::kTypeString);
    parameter_types->AddColumnType(::hybridse::sdk::kTypeInt64);

    std::string where_exist = "select * from trans where merch_id = ? and txn_time < ?;";
    // parameterized query
    auto parameter_row = SQLRequestRow::CreateSQLRequestRowFromColumnTypes(parameter_types);
    {
        ASSERT_EQ(2, parameter_row->GetSchema()->GetColumnCnt());
        ASSERT_TRUE(parameter_row->Init(4));
        ASSERT_TRUE(parameter_row->AppendString("mc_0"));
        ASSERT_TRUE(parameter_row->AppendInt64(1594800959830));
        ASSERT_TRUE(parameter_row->Build());

        auto rs = router->ExecuteSQLParameterized(db, where_exist, parameter_row, &status);

        if (!rs) {
            FAIL() << "fail to execute sql";
        }
        ASSERT_EQ(rs->Size(), 3);
    }
    {
        ASSERT_TRUE(parameter_row->Init(4));
        ASSERT_TRUE(parameter_row->AppendString("mc_0"));
        ASSERT_TRUE(parameter_row->AppendInt64(1594800959828));
        ASSERT_TRUE(parameter_row->Build());
        auto rs = router->ExecuteSQLParameterized(db, where_exist, parameter_row, &status);
        if (!rs) {
            FAIL() << "fail to execute sql";
        }
        ASSERT_EQ(rs->Size(), 1);
    }
    {
        parameter_row->Init(4);
        parameter_row->AppendString("mc_0");
        parameter_row->AppendInt64(1594800959827);
        parameter_row->Build();
        auto rs = router->ExecuteSQLParameterized(db, where_exist, parameter_row, &status);
        if (!rs) {
            FAIL() << "fail to execute sql";
        }
        ASSERT_EQ(rs->Size(), 0);
    }
    {
        parameter_row->Init(4);
        parameter_row->AppendString("mc_1");
        parameter_row->AppendInt64(1594800959830);
        parameter_row->Build();
        auto rs = router->ExecuteSQLParameterized(db, where_exist, parameter_row, &status);
        if (!rs) {
            FAIL() << "fail to execute sql";
        }
        ASSERT_EQ(rs->Size(), 0);
    }
}
}  // namespace sdk
}  // namespace openmldb

int main(int argc, char** argv) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    FLAGS_zk_session_timeout = 100000;
    ::openmldb::sdk::MiniCluster mc(6181);
    ::openmldb::sdk::mc_ = &mc;
    int ok = ::openmldb::sdk::mc_->SetUp(3);
    sleep(1);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::sdk::router_ = ::openmldb::sdk::GetNewSQLRouter();
    if (nullptr == ::openmldb::sdk::router_) {
        LOG(ERROR) << "Fail Test with NULL SQL router";
        return -1;
    }
    ok = RUN_ALL_TESTS();
    ::openmldb::sdk::mc_->Close();
    return ok;
}
