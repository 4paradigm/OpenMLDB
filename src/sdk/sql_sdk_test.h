/*
 * sql_sdk_test.cc
 * Copyright (C) 4paradigm.com 2020 chenjing <chenjing@4paradigm.com>
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
#ifndef SRC_SDK_SQL_SDK_TEST_H_
#define SRC_SDK_SQL_SDK_TEST_H_

#include <sched.h>
#include <timer.h>
#include <unistd.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wapper.h"  // NOLINT
#include "boost/algorithm/string.hpp"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "gflags/gflags.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "test/base_test.h"
#include "vm/catalog.h"
namespace rtidb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc> RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey> RtiDBIndex;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}
enum InsertRule {
    kNotInsertFirstInput,
    kNotInsertLastRowOfFirstInput,
    kInsertAllInputs,
};
class SQLSDKTest : public rtidb::test::SQLCaseTest {
 public:
    SQLSDKTest() : rtidb::test::SQLCaseTest() {}
    ~SQLSDKTest() {}
    void SetUp() { LOG(INFO) << "SQLSDKTest TearDown"; }
    void TearDown() { LOG(INFO) << "SQLSDKTest TearDown"; }

    static void CreateDB(const fesql::sqlcase::SQLCase& sql_case, std::shared_ptr<SQLRouter> router);
    static void CreateTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router, int partition_num = 1);

    static void DropTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                           std::shared_ptr<SQLRouter> router);
    static void InsertTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router, InsertRule insert_rule);

    static void CovertFesqlRowToRequestRow(fesql::codec::RowView* row_view,
                                           std::shared_ptr<rtidb::sdk::SQLRequestRow> request_row);
    static void BatchExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints);
    static void RunBatchModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints);
    static void CreateProcedure(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router, bool is_batch = false);
    static void DropProcedure(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router);
};

INSTANTIATE_TEST_SUITE_P(SQLSDKTestCreate, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("/cases/integration/v1/test_create.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestInsert, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("/cases/integration/v1/test_insert.yaml")));

class SQLSDKQueryTest : public SQLSDKTest {
 public:
    SQLSDKQueryTest() : SQLSDKTest() {}
    ~SQLSDKQueryTest() {}
    static void RequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router, bool is_procedure = false, bool is_asyn = false);
    static void RunRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router);
    static void DistributeRunRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                            std::shared_ptr<SQLRouter> router, int32_t partition_num = 8);
    void RunRequestProcedureModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                    std::shared_ptr<SQLRouter> router, bool is_asyn);
    void DistributeRunRequestProcedureModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                              std::shared_ptr<SQLRouter> router, int32_t partition_num, bool is_asyn);

    static void BatchRequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                       std::shared_ptr<SQLRouter> router, bool has_batch_request,
                                       bool is_procedure = false, bool is_asyn = false);
    static void BatchRequestExecuteSQLWithCommonColumnIndices(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                              std::shared_ptr<SQLRouter> router,
                                                              const std::set<size_t>& common_column_indices,
                                                              bool is_procedure = false, bool is_asyn = false);
    static void RunBatchRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                       std::shared_ptr<SQLRouter> router);
    static void DistributeRunBatchRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                 std::shared_ptr<SQLRouter> router, int32_t partition_num = 8);
};

class SQLSDKBatchRequestQueryTest : public SQLSDKQueryTest {
 public:
    SQLSDKBatchRequestQueryTest() : SQLSDKQueryTest() {}
    ~SQLSDKBatchRequestQueryTest() {}

    static void DistributeRunBatchRequestProcedureModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                          std::shared_ptr<SQLRouter> router, int32_t partition_num,
                                                          bool is_asyn);
    static void RunBatchRequestProcedureModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                std::shared_ptr<SQLRouter> router, bool is_asyn);
};

void SQLSDKTest::CreateDB(const fesql::sqlcase::SQLCase& sql_case, std::shared_ptr<SQLRouter> router) {
    DLOG(INFO) << "Create DB BEGIN";
    fesql::sdk::Status status;
    std::vector<std::string> dbs;
    ASSERT_TRUE(router->ShowDB(&dbs, &status));

    // create db if not exist
    std::set<std::string> db_set(dbs.begin(), dbs.end());
    if (db_set.find(sql_case.db()) == db_set.cend()) {
        ASSERT_TRUE(router->CreateDB(sql_case.db(), &status));
    }
    DLOG(INFO) << "Create DB DONE!";
}

void SQLSDKTest::CreateTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router, int partition_num) {
    DLOG(INFO) << "Create Tables BEGIN";
    fesql::sdk::Status status;
    // create and insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(fesql::sqlcase::SQLCase::GenRand("auto_t"), i);
        }
        // create table
        std::string create;
        ASSERT_TRUE(sql_case.BuildCreateSQLFromInput(i, &create, partition_num));
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(create, placeholder, sql_case.inputs()[i].name_);
        LOG(INFO) << create;
        if (!create.empty()) {
            router->ExecuteDDL(sql_case.db(), create, &status);
            ASSERT_TRUE(router->RefreshCatalog());
        }
    }
    DLOG(INFO) << "Create Tables DONE";
}

void SQLSDKTest::DropTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                            std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;

    // create and insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(fesql::sqlcase::SQLCase::GenRand("auto_t"), i);
        }
        // create table
        std::string drop = "drop table " + sql_case.inputs()[i].name_ + ";";
        LOG(INFO) << drop;
        if (!drop.empty()) {
            router->ExecuteDDL(sql_case.db(), drop, &status);
            ASSERT_TRUE(router->RefreshCatalog());
        }
    }
}

void SQLSDKTest::CreateProcedure(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router, bool is_batch) {
    DLOG(INFO) << "Create Procedure BEGIN";
    fesql::sdk::Status status;
    if (sql_case.inputs()[0].name_.empty()) {
        sql_case.set_input_name(fesql::sqlcase::SQLCase::GenRand("auto_t"), 0);
    }
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", fesql::sqlcase::SQLCase::GenRand("auto_t"));
    boost::trim(sql);
    LOG(INFO) << sql;
    sql_case.sp_name_ = fesql::sqlcase::SQLCase::GenRand("auto_sp");
    std::string create_sp;
    if (is_batch) {
        ASSERT_TRUE(
            sql_case.BuildCreateSpSQLFromInput(0, sql, sql_case.batch_request().common_column_indices_, &create_sp));
    } else {
        std::set<size_t> common_idx;
        ASSERT_TRUE(sql_case.BuildCreateSpSQLFromInput(0, sql, common_idx, &create_sp));
    }

    for (int i = 0; i < sql_case.inputs_.size(); i++) {
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
        return;
    }
    auto sp_info = router->ShowProcedure(sql_case.db(), sql_case.sp_name_, &status);
    ASSERT_TRUE(sp_info && status.code == 0) << status.msg;
    if (is_batch) {
        std::set<size_t> input_common_indices;
        for (size_t idx : sql_case.batch_request().common_column_indices_) {
            input_common_indices.insert(idx);
        }
        for (size_t i = 0; i < sp_info->GetInputSchema().GetColumnCnt(); ++i) {
            auto is_const = input_common_indices.find(i) != input_common_indices.end();
            ASSERT_EQ(is_const, sp_info->GetInputSchema().IsConstant(i)) << "At input column " << i;
        }
        if (!sql_case.expect().common_column_indices_.empty()) {
            std::set<size_t> output_common_indices;
            for (size_t idx : sql_case.expect().common_column_indices_) {
                output_common_indices.insert(idx);
            }
            for (size_t i = 0; i < sp_info->GetOutputSchema().GetColumnCnt(); ++i) {
                auto is_const = output_common_indices.find(i) != output_common_indices.end();
                ASSERT_EQ(is_const, sp_info->GetOutputSchema().IsConstant(i)) << "At output column " << i;
            }
        }
    }
}

void SQLSDKTest::DropProcedure(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                               std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
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

void SQLSDKTest::InsertTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router, InsertRule insert_rule) {
    DLOG(INFO) << "Insert Tables BEGIN";
    fesql::sdk::Status status;
    // insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (0 == i && kNotInsertFirstInput == insert_rule) {
            continue;
        }

        // insert into table
        std::vector<std::string> inserts;
        ASSERT_TRUE(sql_case.BuildInsertSQLListFromInput(i, &inserts));
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
                    ASSERT_TRUE(router->ExecuteInsert(sql_case.db(), insert, &status));
                }
            }
        }
    }
    ASSERT_TRUE(router->RefreshCatalog());
    DLOG(INFO) << "Insert Tables DONE";
}

void SQLSDKTest::CovertFesqlRowToRequestRow(fesql::codec::RowView* row_view,
                                            std::shared_ptr<rtidb::sdk::SQLRequestRow> request_row) {
    ASSERT_EQ(row_view->GetSchema()->size(), request_row->GetSchema()->GetColumnCnt());

    int32_t init_size = 0;
    for (int i = 0; i < row_view->GetSchema()->size(); i++) {
        if (fesql::type::Type::kVarchar == row_view->GetSchema()->Get(i).type()) {
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
            case fesql::type::kBool:
                ASSERT_TRUE(request_row->AppendBool(row_view->GetBoolUnsafe(i)));
                break;
            case fesql::type::kInt16:
                ASSERT_TRUE(request_row->AppendInt16(row_view->GetInt16Unsafe(i)));
                break;
            case fesql::type::kInt32:
                ASSERT_TRUE(request_row->AppendInt32(row_view->GetInt32Unsafe(i)));
                break;
            case fesql::type::kInt64:
                ASSERT_TRUE(request_row->AppendInt64(row_view->GetInt64Unsafe(i)));
                break;
            case fesql::type::kFloat:
                ASSERT_TRUE(request_row->AppendFloat(row_view->GetFloatUnsafe(i)));
                break;
            case fesql::type::kDouble:
                ASSERT_TRUE(request_row->AppendDouble(row_view->GetDoubleUnsafe(i)));
                break;
            case fesql::type::kTimestamp:
                ASSERT_TRUE(request_row->AppendTimestamp(row_view->GetTimestampUnsafe(i)));
                break;
            case fesql::type::kDate:
                ASSERT_TRUE(request_row->AppendDate(row_view->GetDateUnsafe(i)));
                break;
            case fesql::type::kVarchar:
                ASSERT_TRUE(request_row->AppendString(row_view->GetStringUnsafe(i)));
                break;
            default: {
                FAIL() << "Fail conver fesql row to rtidb sdk request row";
                return;
            }
        }
    }
    ASSERT_TRUE(request_row->Build());
}

void SQLSDKTest::BatchExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints) {
    DLOG(INFO) << "BatchExecuteSQL BEGIN";
    fesql::sdk::Status status;
    DLOG(INFO) << "format sql begin";
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    DLOG(INFO) << "format sql 1";
    boost::replace_all(sql, "{auto}", fesql::sqlcase::SQLCase::GenRand("auto_t"));
    if (tbEndpoints.size() > 0) {
        boost::replace_all(sql, "{tb_endpoint_0}", tbEndpoints.at(0));
    }
    if (tbEndpoints.size() > 1) {
        boost::replace_all(sql, "{tb_endpoint_1}", tbEndpoints.at(1));
    }
    DLOG(INFO) << "format sql done";
    LOG(INFO) << sql;
    std::string lower_sql = sql;
    boost::to_lower(lower_sql);
    if (boost::algorithm::starts_with(lower_sql, "select")) {
        auto rs = router->ExecuteSQL(sql_case.db(), sql, &status);
        if (!sql_case.expect().success_) {
            if ((rs)) {
                FAIL() << "sql case expect success == false";
            }
            return;
        }

        if (!rs) FAIL() << "sql case expect success == true";
        std::vector<fesql::codec::Row> rows;
        fesql::type::TableDef output_table;
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
        ASSERT_EQ(sql_case.expect().success_, ok);
    } else if (boost::algorithm::starts_with(lower_sql, "insert")) {
        bool ok = router->ExecuteInsert(sql_case.db(), sql, &status);
        ASSERT_EQ(sql_case.expect().success_, ok);
    } else {
        FAIL() << "sql not support in request mode";
    }
    DLOG(INFO) << "BatchExecuteSQL DONE";
}

void SQLSDKTest::RunBatchModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router, const std::vector<std::string>& tbEndpoints) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, kInsertAllInputs);
    BatchExecuteSQL(sql_case, router, tbEndpoints);
    DropTables(sql_case, router);
    LOG(INFO) << "RunBatchModeSDK ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKQueryTest::RequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                        std::shared_ptr<SQLRouter> router, bool is_procedure, bool is_asyn) {
    fesql::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", fesql::sqlcase::SQLCase::GenRand("auto_t"));
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
            FAIL() << "sql case expect success == true";
        }

        fesql::type::TableDef insert_table;
        ASSERT_TRUE(sql_case.ExtractInputTableDef(insert_table, 0));

        std::vector<fesql::codec::Row> insert_rows;
        ASSERT_TRUE(sql_case.ExtractInputData(insert_rows, 0));

        CheckSchema(insert_table.columns(), *(request_row->GetSchema().get()));

        LOG(INFO) << "Request Row:\n";
        PrintRows(insert_table.columns(), insert_rows);

        std::vector<std::string> inserts;
        sql_case.BuildInsertSQLListFromInput(0, &inserts);
        fesql::codec::RowView row_view(insert_table.columns());
        std::vector<std::shared_ptr<fesql::sdk::ResultSet>> results;
        LOG(INFO) << "Request execute sql start!";
        for (size_t i = 0; i < insert_rows.size(); i++) {
            row_view.Reset(insert_rows[i].buf());
            CovertFesqlRowToRequestRow(&row_view, request_row);
            std::shared_ptr<fesql::sdk::ResultSet> rs;
            if (is_procedure) {
                if (is_asyn) {
                    LOG(INFO) << "-------asyn procedure----------";
                    auto future = router->CallProcedure(sql_case.db(), sql_case.sp_name_, 1000, request_row, &status);
                    if (!future || status.code != 0) FAIL() << "sql case expect success == true";
                    rs = future->GetResultSet(&status);
                } else {
                    LOG(INFO) << "--------syn procedure----------";
                    rs = router->CallProcedure(sql_case.db(), sql_case.sp_name_, request_row, &status);
                }
            } else {
                rs = router->ExecuteSQL(sql_case.db(), sql, request_row, &status);
            }
            if (!rs || status.code != 0) FAIL() << "sql case expect success == true";
            results.push_back(rs);
            std::string insert_request;
            LOG(INFO) << "insert request: \n" << inserts[i];
            bool ok = router->ExecuteInsert(sql_case.db(), inserts[i], &status);
            ASSERT_TRUE(ok);
        }
        LOG(INFO) << "Request execute sql done!";
        ASSERT_FALSE(results.empty());
        std::vector<fesql::codec::Row> rows;
        fesql::type::TableDef output_table;
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

void SQLSDKQueryTest::BatchRequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
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
void SQLSDKQueryTest::BatchRequestExecuteSQLWithCommonColumnIndices(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
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
    fesql::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", fesql::sqlcase::SQLCase::GenRand("auto_t"));
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
        FAIL() << "sql case expect success == true";
    }

    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    fesql::type::TableDef batch_request_table;
    std::vector<::fesql::codec::Row> request_rows;
    auto common_column_indices = std::make_shared<ColumnIndicesSet>(request_row->GetSchema());
    if (has_batch_request) {
        auto& batch_request = sql_case.batch_request();
        ASSERT_TRUE(sql_case.ExtractTableDef(batch_request.columns_, batch_request.indexs_, batch_request_table));
        ASSERT_TRUE(sql_case.ExtractRows(batch_request_table.columns(), batch_request.rows_, request_rows));

    } else {
        ASSERT_TRUE(sql_case.ExtractInputTableDef(sql_case.inputs_[0], batch_request_table));
        std::vector<fesql::codec::Row> rows;
        ASSERT_TRUE(sql_case.ExtractInputData(sql_case.inputs_[0], rows));
        request_rows.push_back(rows.back());
    }
    CheckSchema(batch_request_table.columns(), *(request_row->GetSchema().get()));
    LOG(INFO) << "Request Row:\n";
    PrintRows(batch_request_table.columns(), request_rows);

    for (size_t idx : common_indices) {
        common_column_indices->AddCommonColumnIdx(idx);
    }
    fesql::codec::RowView row_view(batch_request_table.columns());

    auto row_batch = std::make_shared<SQLRequestRowBatch>(request_row->GetSchema(), common_column_indices);

    LOG(INFO) << "Batch Request execute sql start!";
    for (size_t i = 0; i < request_rows.size(); i++) {
        row_view.Reset(request_rows[i].buf());
        CovertFesqlRowToRequestRow(&row_view, request_row);
        ASSERT_TRUE(row_batch->AddRow(request_row));
    }
    std::shared_ptr<fesql::sdk::ResultSet> results;
    if (is_procedure) {
        if (is_asyn) {
            LOG(INFO) << "-------asyn procedure----------";
            auto future =
                router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.sp_name_, 1000, row_batch, &status);
            if (!future || status.code != 0) FAIL() << "sql case expect success == true";
            results = future->GetResultSet(&status);
        } else {
            LOG(INFO) << "--------syn procedure----------";
            results = router->CallSQLBatchRequestProcedure(sql_case.db(), sql_case.sp_name_, row_batch, &status);
        }
    } else {
        results = router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status);
    }
    if (!results) {
        FAIL() << "sql case expect success == true";
    }
    ASSERT_NO_FATAL_FAILURE(ASSERT_EQ(0, status.code));
    LOG(INFO) << "Batch request execute sql done!";
    ASSERT_GT(results->Size(), 0);
    std::vector<fesql::codec::Row> rows;
    fesql::type::TableDef output_table;
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

void SQLSDKQueryTest::RunRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                        std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, kNotInsertFirstInput);
    RequestExecuteSQL(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "RequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}
void SQLSDKQueryTest::DistributeRunRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                  std::shared_ptr<SQLRouter> router, int32_t partition_num) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    InsertTables(sql_case, router, kNotInsertFirstInput);
    RequestExecuteSQL(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "DistributeRunRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}
void SQLSDKQueryTest::RunBatchRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                             std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertLastRowOfFirstInput);
    BatchRequestExecuteSQL(sql_case, router, has_batch_request);
    DropTables(sql_case, router);
    LOG(INFO) << "BatchRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKQueryTest::DistributeRunBatchRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                       std::shared_ptr<SQLRouter> router, int32_t partition_num) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    bool has_batch_request = !sql_case.batch_request().columns_.empty();
    InsertTables(sql_case, router, has_batch_request ? kInsertAllInputs : kNotInsertLastRowOfFirstInput);
    BatchRequestExecuteSQL(sql_case, router, has_batch_request);

    DropTables(sql_case, router);
    LOG(INFO) << "DistributeRunBatchRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc()
              << " done!";
}

void SQLSDKQueryTest::RunRequestProcedureModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                 std::shared_ptr<SQLRouter> router, bool is_asyn) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, kNotInsertFirstInput);
    CreateProcedure(sql_case, router);
    RequestExecuteSQL(sql_case, router, true, is_asyn);
    DropProcedure(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "RequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKBatchRequestQueryTest::RunBatchRequestProcedureModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                                  std::shared_ptr<SQLRouter> router, bool is_asyn) {
    fesql::sdk::Status status;
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

void SQLSDKQueryTest::DistributeRunRequestProcedureModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                           std::shared_ptr<SQLRouter> router, int32_t partition_num,
                                                           bool is_asyn) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router, partition_num);
    InsertTables(sql_case, router, kNotInsertFirstInput);
    CreateProcedure(sql_case, router);
    RequestExecuteSQL(sql_case, router, true, is_asyn);
    DropProcedure(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "RequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKBatchRequestQueryTest::DistributeRunBatchRequestProcedureModeSDK(
    fesql::sqlcase::SQLCase& sql_case,  // NOLINT
    std::shared_ptr<SQLRouter> router, int32_t partition_num, bool is_asyn) {
    fesql::sdk::Status status;
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

INSTANTIATE_TEST_SUITE_P(SQLSDKTestConstsSelect, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/query/const_query.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKLastJoinWindowQuery, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSelectSample, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/select/test_select_sample.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestArithmetic, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/expression/test_arithmetic.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCompare, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/expression/test_compare.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCondition, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/expression/test_condition.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLogic, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/expression/test_logic.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestType, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/expression/test_type.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowRow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/window/test_window_row.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowRowRange, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/window/test_window_row_range.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowMaxSize, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/window/test_maxsize.yaml")));


INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowUnion, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/window/test_window_union.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLast_Join, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/join/test_last_join.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestLastJoin, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/join/test_lastjoin.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSubSelect, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/select/test_sub_select.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestUDAFFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/function/test_udaf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestUDFFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/function/test_udf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestStringFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/function/test_string.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestDateFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/function/test_date.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestCalulateFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/function/test_calculate.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestWhere, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/select/test_where.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestFZFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_feature_zero_function.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestBatchRequest, SQLSDKBatchRequestQueryTest,
    testing::ValuesIn(SQLSDKBatchRequestQueryTest::InitCases("/cases/integration/v1/test_batch_request.yaml")));
INSTANTIATE_TEST_CASE_P(
    SQLSDKClusterCaseWindowAndLastJoin, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/cluster/window_and_lastjoin.yaml")));
INSTANTIATE_TEST_CASE_P(
    SQLSDKClusterCaseWindowRow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_CASE_P(
    SQLSDKClusterCaseWindowRowRange, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/cluster/test_window_row_range.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestErrorWindow, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/window/error_window.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestIndexOptimized, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_index_optimized.yaml")));
INSTANTIATE_TEST_CASE_P(SQLSDKTestDebugIssues, SQLSDKQueryTest,
                        testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/debug/issues_case.yaml")));
}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_SQL_SDK_TEST_H_
