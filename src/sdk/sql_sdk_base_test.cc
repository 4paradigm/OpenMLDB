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

#include "sdk/sql_sdk_base_test.h"

#include "absl/random/random.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "sdk/base_impl.h"
#include "test/util.h"

namespace openmldb {
namespace sdk {
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
            create = absl::StrReplaceAll(create, {{placeholder, sql_case.inputs()[i].name_}});
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
        sql = absl::StrReplaceAll(sql, {{placeholder, sql_case.inputs()[i].name_}});
    }
    sql = absl::StrReplaceAll(
        sql, {{"{auto}",
        hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL)))}});
    absl::RemoveExtraAsciiWhitespace(&sql);
    LOG(INFO) << sql;
    sql_case.sp_name_ =
        hybridse::sqlcase::SqlCase::GenRand("auto_sp") + std::to_string(static_cast<int64_t>(time(NULL)));
    std::string create_sp;
    if (is_batch) {
        hybridse::type::TableDef batch_request_schema;
        ASSERT_TRUE(sql_case.ExtractTableDef(sql_case.batch_request().columns_, sql_case.batch_request().indexs_,
                                             batch_request_schema));
        auto s = sql_case.BuildCreateSpSqlFromSchema(batch_request_schema, sql,
                                                     sql_case.batch_request().common_column_indices_);
        ASSERT_TRUE(s.ok()) << s.status();
        create_sp = s.value();
    } else {
        std::set<size_t> common_idx;
        auto s = sql_case.BuildCreateSpSqlFromInput(0, sql, common_idx);
        ASSERT_TRUE(s.ok()) << s.status();
        create_sp = s.value();
    }

    for (size_t i = 0; i < sql_case.inputs_.size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        create_sp = absl::StrReplaceAll(create_sp, {{placeholder, sql_case.inputs()[i].name_}});
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
            insert = absl::StrReplaceAll(insert, {{placeholder, sql_case.inputs()[i].name_}});
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
                FAIL() << "Fail conver hybridse row to OpenMLDB sdk request row";
                return;
            }
        }
    }
    ASSERT_TRUE(request_row->Build());
}
void SQLSDKTest::BatchExecuteSQL(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints) {
    hybridse::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        sql = absl::StrReplaceAll(sql, {{placeholder, sql_case.inputs()[i].name_}});
    }
    sql = absl::StrReplaceAll(
        sql, {{"{auto}",
        hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL)))}});
    for (size_t endpoint_id = 0; endpoint_id < tbEndpoints.size(); endpoint_id++) {
        sql = absl::StrReplaceAll(sql, {{"{tb_endpoint_" + std::to_string(endpoint_id) + "}", tbEndpoints.at(endpoint_id)}});
    }
    std::string lower_sql = sql;
    absl::AsciiStrToLower(lower_sql);
    if (absl::StartsWith(lower_sql, "select") || absl::StartsWith(lower_sql, "with")) {
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
            if (rs) {
                FAIL() << "sql case expect success == false";
            }
            return;
        }

        if (!rs) FAIL() << "sql case expect success == true: " << status.msg;
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
    } else if (absl::StartsWith(lower_sql, "create")) {
        bool ok = router->ExecuteDDL(sql_case.db(), sql, &status);
        router->RefreshCatalog();
        ASSERT_EQ(sql_case.expect().success_, ok) << status.msg;
    } else if (absl::StartsWith(lower_sql, "insert")) {
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
        sql = absl::StrReplaceAll(sql, {{placeholder, sql_case.inputs()[i].name_}});
    }
    sql = absl::StrReplaceAll(sql, {{"{auto}", hybridse::sqlcase::SqlCase::GenRand("auto_t") +
            std::to_string(static_cast<int64_t>(time(NULL)))}});
    std::string lower_sql = sql;
    absl::AsciiStrToLower(lower_sql);
    if (absl::StartsWith(lower_sql, "select") || absl::StartsWith(lower_sql, "with")) {
        auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);
        // success check
        LOG(INFO) << "sql case success check: required success " << (sql_case.expect().success_ ? "true" : "false");
        if (!sql_case.expect().success_) {
            if ((request_row)) {
                FAIL() << "sql case expect success == false";
            }
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
    } else if (absl::StartsWith(lower_sql, "create")) {
        FAIL() << "create sql not support in request mode";
    } else if (absl::StartsWith(lower_sql, "insert")) {
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
        sql = absl::StrReplaceAll(sql, {{placeholder, sql_case.inputs()[i].name_}});
    }
    sql = absl::StrReplaceAll(sql, {{"{auto}", hybridse::sqlcase::SqlCase::GenRand("auto_t") +
            std::to_string(static_cast<int64_t>(time(NULL)))}});
    LOG(INFO) << sql;
    std::string lower_sql = sql;
    absl::AsciiStrToLower(lower_sql);
    if (!(absl::StartsWith(lower_sql, "select") || absl::StartsWith(lower_sql, "with"))) {
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
void SQLSDKClusterOnlineBatchQueryTest::DistributeRunBatchModeSDK(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                                                                  std::shared_ptr<SQLRouter> router,
                                                                  const std::vector<std::string>& tbEndpoints,
                                                                  const int partition_num) {
    hybridse::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    InsertTables(sql_case, router, kInsertAllInputs);
    BatchExecuteSQL(sql_case, router, tbEndpoints);
    DropTables(sql_case, router);
    LOG(INFO) << "DistributeRunBatchModeSDK ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

DeploymentEnv::DeploymentEnv(std::shared_ptr<sdk::SQLRouter> sr, hybridse::sqlcase::SqlCase* sqlcase)
    : sr_(sr), sql_case_(sqlcase) {
    if (sqlcase->deployment_.name_.empty()) {
        dp_name_ = absl::StrCat("dp_", ::openmldb::test::GenRand());
    } else {
        dp_name_ = sqlcase->deployment_.name_;
    }
}

void DeploymentEnv::SetUp(bool skip_data) {
    hybridse::sdk::Status status;
    SQLSDKTest::CreateDB(*sql_case_, sr_);
    SQLSDKTest::CreateTables(*sql_case_, sr_, 0);
    if (!skip_data) {
        SQLSDKTest::InsertTables(*sql_case_, sr_, kNotInsertFirstInput);
    }

    if (sql_case_->inputs()[0].name_.empty()) {
        sql_case_->set_input_name(
            hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL))), 0);
    }

    sql_str_ = sql_case_->sql_str_;
    for (size_t i = 0; i < sql_case_->inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        absl::StrReplaceAll({{placeholder, sql_case_->inputs_[i].name_}}, &sql_str_);
    }
    absl::StrReplaceAll(
        {{"{auto}", hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL)))}},
        &sql_str_);
    absl::StripAsciiWhitespace(&sql_str_);

    ::openmldb::test::ProcessSQLs(
        sr_.get(), {absl::StrCat("use ", sql_case_->db_), absl::StrCat("deploy ", dp_name_, " ", sql_str_)});
}

void DeploymentEnv::CallDeployProcedure() const {
    hybridse::sdk::Status s;
    auto request_row = sr_->GetRequestRowByProcedure(sql_case_->db_, dp_name_, &s);
    ASSERT_TRUE(s.IsOK());

    hybridse::type::TableDef insert_table;
    std::vector<hybridse::codec::Row> insert_rows;
    std::vector<std::string> inserts;
    ASSERT_TRUE(sql_case_->ExtractInputTableDef(insert_table, 0));
    ASSERT_TRUE(sql_case_->ExtractInputData(insert_rows, 0));
    sql_case_->BuildInsertSqlListFromInput(0, &inserts);
    test::SQLCaseTest::CheckSchema(insert_table.columns(), *(request_row->GetSchema().get()));
    LOG(INFO) << "Request Row:\n";
    test::SQLCaseTest::PrintRows(insert_table.columns(), insert_rows);

    hybridse::codec::RowView row_view(insert_table.columns());
    std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results;
    LOG(INFO) << "Request execute sql start!";
    for (size_t i = 0; i < insert_rows.size(); i++) {
        row_view.Reset(insert_rows[i].buf());
        SQLSDKTest::CovertHybridSERowToRequestRow(&row_view, request_row);
        std::shared_ptr<hybridse::sdk::ResultSet> rs;
        rs = sr_->CallProcedure(sql_case_->db_, dp_name_, request_row, &s);
        if (!rs || s.code != 0) {
            FAIL() << "sql case expect success == true" << s.msg;
        }
        results.push_back(rs);

        if (!pure_deploy_) {
            LOG(INFO) << "insert request: \n" << inserts[i];
            bool ok = sr_->ExecuteInsert(insert_table.catalog(), inserts[i], &s);
            ASSERT_TRUE(ok);
        }
    }
    ASSERT_FALSE(results.empty());
    std::vector<hybridse::codec::Row> rows;
    hybridse::type::TableDef output_table;
    if (!sql_case_->expect().schema_.empty() || !sql_case_->expect().columns_.empty()) {
        ASSERT_TRUE(sql_case_->ExtractOutputSchema(output_table));
        SQLSDKQueryTest::CheckSchema(output_table.columns(), *(results[0]->GetSchema()));
    }

    if (!sql_case_->expect().data_.empty() || !sql_case_->expect().rows_.empty()) {
        ASSERT_TRUE(sql_case_->ExtractOutputData(rows));
        SQLSDKQueryTest::CheckRows(output_table.columns(), sql_case_->expect().order_, rows, results);
    }

    if (sql_case_->expect().count_ > 0) {
        ASSERT_EQ(sql_case_->expect().count_, static_cast<int64_t>(results.size()));
    }
}

void DeploymentEnv::CallDeployProcedureTiny() const {
    hybridse::sdk::Status s;
    auto request_row = sr_->GetRequestRowByProcedure(sql_case_->db_, dp_name_, &s);
    ASSERT_TRUE(s.IsOK());

    hybridse::type::TableDef insert_table;
    std::vector<hybridse::codec::Row> insert_rows;
    ASSERT_TRUE(sql_case_->ExtractInputTableDef(insert_table, 0));
    ASSERT_TRUE(sql_case_->ExtractInputData(insert_rows, 0));

    hybridse::codec::RowView row_view(insert_table.columns());
    for (size_t i = 0; i < insert_rows.size(); i++) {
        row_view.Reset(insert_rows[i].buf());
        SQLSDKTest::CovertHybridSERowToRequestRow(&row_view, request_row);
        std::shared_ptr<hybridse::sdk::ResultSet> rs;
        rs = sr_->CallProcedure(sql_case_->db_, dp_name_, request_row, &s);
        if (!rs || s.code != 0) {
            FAIL() << "sql case expect success == true" << s.msg;
        }
    }
}

void DeploymentEnv::TearDown() {
    if (cleanup_) {
        ::openmldb::test::ProcessSQLs(sr_.get(), {
                                                     absl::StrCat("drop deployment ", dp_name_),
                                                 });
        SQLSDKTest::DropTables(*sql_case_, sr_);
        ::openmldb::test::ProcessSQLs(sr_.get(), {
                                                     absl::StrCat("drop database ", sql_case_->db_),
                                                 });
    }
}

}  // namespace sdk
}  // namespace openmldb
