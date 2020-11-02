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

#include <sched.h>
#include <timer.h>
#include <unistd.h>

#include <memory>
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

MiniCluster* mc_ = nullptr;

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc> RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey> RtiDBIndex;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class SQLSDKTest : public rtidb::test::SQLCaseTest {
 public:
    SQLSDKTest() : rtidb::test::SQLCaseTest() {}
    ~SQLSDKTest() {}
    void SetUp() { LOG(INFO) << "SQLSDKTest TearDown"; }
    void TearDown() { LOG(INFO) << "SQLSDKTest TearDown"; }

    static void CreateDB(const fesql::sqlcase::SQLCase& sql_case, std::shared_ptr<SQLRouter> router);
    static void CreateTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router);

    static void DropTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                           std::shared_ptr<SQLRouter> router);
    static void InsertTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router, bool is_bath);

    static void CovertFesqlRowToRequestRow(fesql::codec::RowView* row_view,
                                           std::shared_ptr<rtidb::sdk::SQLRequestRow> request_row);
    static void BatchExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router);
    static void RunBatchModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                std::shared_ptr<SQLRouter> router);
};

TEST_P(SQLSDKTest, sql_sdk_batch_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = sql_case.debug() || fesql::sqlcase::SQLCase::IS_DEBUG();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
        return;
    }
    SQLSDKTest::RunBatchModeSDK(sql_case, router);
}

INSTANTIATE_TEST_SUITE_P(SQLSDKTestCreate, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("/cases/integration/v1/test_create.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestInsert, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases("/cases/integration/v1/test_insert.yaml")));

class SQLSDKQueryTest : public SQLSDKTest {
 public:
    SQLSDKQueryTest() : SQLSDKTest() {}
    ~SQLSDKQueryTest() {}
    static void RequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router);
    static void RunRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router);
};

class SQLSDKBatchRequestQueryTest : public SQLSDKTest {
 public:
    SQLSDKBatchRequestQueryTest() : SQLSDKTest() {}
    ~SQLSDKBatchRequestQueryTest() {}
    static void BatchRequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                       std::shared_ptr<SQLRouter> router);
    static void RunBatchRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                       std::shared_ptr<SQLRouter> router);
};

void SQLSDKTest::CreateDB(const fesql::sqlcase::SQLCase& sql_case, std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    std::vector<std::string> dbs;
    ASSERT_TRUE(router->ShowDB(&dbs, &status));

    // create db if not exist
    std::set<std::string> db_set(dbs.begin(), dbs.end());
    if (db_set.find(sql_case.db()) == db_set.cend()) {
        ASSERT_TRUE(router->CreateDB(sql_case.db(), &status));
    }
}

void SQLSDKTest::CreateTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    // create and insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(fesql::sqlcase::SQLCase::GenRand("auto_t"), i);
        }
        // create table
        std::string create;
        ASSERT_TRUE(sql_case.BuildCreateSQLFromInput(i, &create));
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(create, placeholder, sql_case.inputs()[i].name_);
        LOG(INFO) << create;
        if (!create.empty()) {
            router->ExecuteDDL(sql_case.db(), create, &status);
            ASSERT_TRUE(router->RefreshCatalog());
        }
    }
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

void SQLSDKTest::InsertTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router, bool is_bath) {
    fesql::sdk::Status status;
    // insert inputs
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        if (0 == i && !is_bath) {
            continue;
        }
        // insert into table
        std::vector<std::string> inserts;
        ASSERT_TRUE(sql_case.BuildInsertSQLListFromInput(i, &inserts));
        for (auto insert : inserts) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            boost::replace_all(insert, placeholder, sql_case.inputs()[i].name_);
            LOG(INFO) << insert;
            if (!insert.empty()) {
                ASSERT_TRUE(router->ExecuteInsert(sql_case.db(), insert, &status));
            }
        }
    }
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
    LOG(INFO) << "Build Request Row: init string size " << init_size;
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
                                 std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", fesql::sqlcase::SQLCase::GenRand("auto_t"));
    boost::replace_all(sql, "{tb_endpoint_0}", mc_->GetTbEndpoint().at(0));
    boost::replace_all(sql, "{tb_endpoint_1}", mc_->GetTbEndpoint().at(1));
    LOG(INFO) << sql;
    boost::to_lower(sql);
    if (boost::algorithm::starts_with(sql, "select")) {
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
    } else if (boost::algorithm::starts_with(sql, "create")) {
        bool ok = router->ExecuteDDL(sql_case.db(), sql, &status);
        router->RefreshCatalog();
        ASSERT_EQ(sql_case.expect().success_, ok);
    } else if (boost::algorithm::starts_with(sql, "insert")) {
        bool ok = router->ExecuteInsert(sql_case.db(), sql, &status);
        ASSERT_EQ(sql_case.expect().success_, ok);
    } else {
        FAIL() << "sql not support in request mode";
    }
}

void SQLSDKTest::RunBatchModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, true);
    BatchExecuteSQL(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "RunBatchModeSDK ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKQueryTest::RequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                        std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", fesql::sqlcase::SQLCase::GenRand("auto_t"));
    boost::to_lower(sql);
    LOG(INFO) << sql;
    if (boost::algorithm::starts_with(sql, "select")) {
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
            auto rs = router->ExecuteSQL(sql_case.db(), sql, request_row, &status);
            if (!rs) FAIL() << "sql case expect success == true";
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
    } else if (boost::algorithm::starts_with(sql, "create")) {
        FAIL() << "create sql not support in request mode";
    } else if (boost::algorithm::starts_with(sql, "insert")) {
        FAIL() << "insert sql not support in request mode";
    } else {
        FAIL() << "sql not support in request mode";
    }
}

void SQLSDKBatchRequestQueryTest::BatchRequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                         std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (size_t i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}", fesql::sqlcase::SQLCase::GenRand("auto_t"));
    boost::to_lower(sql);
    LOG(INFO) << sql;
    if (!boost::algorithm::starts_with(sql, "select")) {
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

    auto& batch_request = sql_case.batch_request();
    fesql::type::TableDef batch_request_table;
    std::vector<::fesql::codec::Row> request_rows;
    ASSERT_TRUE(sql_case.ExtractTableDef(
        batch_request.columns_, batch_request.indexs_, batch_request_table));
    ASSERT_TRUE(sql_case.ExtractRows(
        batch_request_table.columns(), batch_request.rows_, request_rows));

    CheckSchema(batch_request_table.columns(), *(request_row->GetSchema().get()));

    LOG(INFO) << "Request Row:\n";
    PrintRows(batch_request_table.columns(), request_rows);

    fesql::codec::RowView row_view(batch_request_table.columns());
    auto common_column_indices = std::make_shared<ColumnIndicesSet>(
        request_row->GetSchema());
    for (size_t idx : batch_request.common_column_indices_) {
        common_column_indices->AddCommonColumnIdx(idx);
    }
    auto row_batch = std::make_shared<SQLRequestRowBatch>(
        request_row->GetSchema(), common_column_indices);

    LOG(INFO) << "Request execute sql start!";
    for (size_t i = 0; i < request_rows.size(); i++) {
        row_view.Reset(request_rows[i].buf());
        CovertFesqlRowToRequestRow(&row_view, request_row);
        ASSERT_TRUE(row_batch->AddRow(request_row));
    }
    auto results = router->ExecuteSQLBatchRequest(sql_case.db(), sql, row_batch, &status);
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
        CheckRows(output_table.columns(), sql_case.expect().order_, rows, results);
    }
    if (sql_case.expect().count_ > 0) {
        ASSERT_EQ(sql_case.expect().count_,
                  static_cast<int64_t>(results->Size()));
    }
}

void SQLSDKQueryTest::RunRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                        std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, false);
    RequestExecuteSQL(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "RequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

void SQLSDKBatchRequestQueryTest::RunBatchRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                                         std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, true);
    BatchRequestExecuteSQL(sql_case, router);
    DropTables(sql_case, router);
    LOG(INFO) << "BatchRequestExecuteSQL ID: " << sql_case.id() << ", DESC: " << sql_case.desc() << " done!";
}

INSTANTIATE_TEST_SUITE_P(SQLSDKTestConstsSelect, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/query/const_query.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestSelectSample, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_select_sample.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestExpression, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_expression.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestWindowRow, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_window_row.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestWindowRowRange, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_window_row_range.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestWindowUnion, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_window_union.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestLastJoin, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_last_join.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestSubSelect, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_sub_select.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestUDAFFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_udaf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestUDFFunction, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_udf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(SQLSDKTestWhere, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_where.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestFZFunction, SQLSDKQueryTest,
    testing::ValuesIn(SQLSDKQueryTest::InitCases("/cases/integration/v1/test_feature_zero_function.yaml")));

INSTANTIATE_TEST_SUITE_P(
    SQLSDKTestBatchRequest, SQLSDKBatchRequestQueryTest,
    testing::ValuesIn(SQLSDKBatchRequestQueryTest::InitCases("/cases/integration/v1/test_batch_request.yaml")));

static std::shared_ptr<SQLRouter> GetNewSQLRouter(const fesql::sqlcase::SQLCase& sql_case) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = sql_case.debug() || fesql::sqlcase::SQLCase::IS_DEBUG();
    return NewClusterSQLRouter(sql_opt);
}

TEST_P(SQLSDKQueryTest, sql_sdk_request_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    auto router = GetNewSQLRouter(sql_case);
    ASSERT_TRUE(router != nullptr) << "Fail new cluster sql router";
    RunRequestModeSDK(sql_case, router);
    LOG(INFO) << "Finish sql_sdk_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_batch_request_test) {
    auto sql_case = GetParam();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    auto router = GetNewSQLRouter(sql_case);
    ASSERT_TRUE(router != nullptr) << "Fail new cluster sql router";
    RunBatchRequestModeSDK(sql_case, router);
    LOG(INFO) << "Finish sql_sdk_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, sql_sdk_batch_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-batch-unsupport") ||
        boost::contains(sql_case.mode(), "batch-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    auto router = GetNewSQLRouter(sql_case);
    ASSERT_TRUE(router != nullptr) << "Fail new cluster sql router";
    RunBatchModeSDK(sql_case, router);
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
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = fesql::sqlcase::SQLCase::IS_DEBUG();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "sql_where_test";
    fesql::sdk::Status status;
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
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = fesql::sqlcase::SQLCase::IS_DEBUG();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "leak_test";
    fesql::sdk::Status status;
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
        fesql::sdk::Status status;
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
}  // namespace sdk
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    srand(time(NULL));
    FLAGS_zk_session_timeout = 100000;
    ::rtidb::sdk::MiniCluster mc(6181);
    ::rtidb::sdk::mc_ = &mc;
    int ok = ::rtidb::sdk::mc_->SetUp();
    sleep(1);
    ok = RUN_ALL_TESTS();
    ::rtidb::sdk::mc_->Close();
    return ok;
}
