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

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>
    RtiDBIndex;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class SQLSDKTest : public rtidb::test::SQLCaseTest {
 public:
    SQLSDKTest() : rtidb::test::SQLCaseTest() {}
    ~SQLSDKTest() {}
    // Per-test-suite set-up.
    // Called before the first test in this test suite.
    // Can be omitted if not needed.
    static void SetUpTestCase() {}

    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestCase() {}
    void SetUp() {}
    void TearDown() {}

    static void CreateDB(const fesql::sqlcase::SQLCase& sql_case,
                         std::shared_ptr<SQLRouter> router);
    static void CreateTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router);

    static void InsertTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                             std::shared_ptr<SQLRouter> router, bool is_bath);

    static void CovertFesqlRowToRequestRow(
        fesql::codec::RowView* row_view,
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
    sql_opt.enbale_debug = true;
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SQLSDKTest::RunBatchModeSDK(sql_case, router);
}
INSTANTIATE_TEST_SUITE_P(SQLSDKTestCreate, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases(
                             "/cases/integration/v1/test_create.yaml")));

INSTANTIATE_TEST_SUITE_P(SQLSDKTestInsert, SQLSDKTest,
                         testing::ValuesIn(SQLSDKTest::InitCases(
                             "/cases/integration/v1/test_insert.yaml")));

class SQLSDKQueryTest : public SQLSDKTest {
 public:
    SQLSDKQueryTest() : SQLSDKTest() {}
    ~SQLSDKQueryTest() {}
    static void RequestExecuteSQL(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router);
    static void RunRequestModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                  std::shared_ptr<SQLRouter> router);
    // Per-test-suite set-up.
    // Called before the first test in this test suite.
    // Can be omitted if not needed.
    static void SetUpTestCase() {
        LOG(INFO) << "SetUpTestCase cluster init >>";
        query_mc_ = new MiniCluster(6181);
        query_mc_->SetUp();
    }

    // Per-test-suite tear-down.
    // Called after the last test in this test suite.
    // Can be omitted if not needed.
    static void TearDownTestCase() {
        query_mc_->Close();
        delete query_mc_;
        LOG(INFO) << "TearDownTestCase cluster close>>";
    }

 public:
    static MiniCluster* query_mc_;
};
MiniCluster* SQLSDKQueryTest::query_mc_ = nullptr;

void SQLSDKTest::CreateDB(const fesql::sqlcase::SQLCase& sql_case,
                          std::shared_ptr<SQLRouter> router) {
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
    for (auto i = 0; i < sql_case.inputs().size(); i++) {
        if (sql_case.inputs()[i].name_.empty()) {
            sql_case.set_input_name(fesql::sqlcase::SQLCase::GenRand("auto_t"),
                                    i);
        }
        // create table
        std::string create;
        ASSERT_TRUE(sql_case.BuildCreateSQLFromInput(i, &create));
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(create, placeholder, sql_case.inputs()[i].name_);
        LOG(INFO) << create;
        if (!create.empty()) {
            ASSERT_TRUE(router->ExecuteDDL(sql_case.db(), create, &status));
        }
        ASSERT_TRUE(router->RefreshCatalog());
    }
}

void SQLSDKTest::InsertTables(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                              std::shared_ptr<SQLRouter> router, bool is_bath) {
    fesql::sdk::Status status;
    // insert inputs
    for (auto i = 0; i < sql_case.inputs().size(); i++) {
        if (0 == i && !is_bath) {
            continue;
        }
        // insert into table
        std::string insert;
        ASSERT_TRUE(sql_case.BuildInsertSQLFromInput(i, &insert));
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(insert, placeholder, sql_case.inputs()[i].name_);
        LOG(INFO) << insert;
        if (!insert.empty()) {
            ASSERT_TRUE(router->ExecuteInsert(sql_case.db(), insert, &status));
            ASSERT_TRUE(router->RefreshCatalog());
        }
        ASSERT_TRUE(router->RefreshCatalog());
    }
}

void SQLSDKTest::CovertFesqlRowToRequestRow(
    fesql::codec::RowView* row_view,
    std::shared_ptr<rtidb::sdk::SQLRequestRow> request_row) {
    ASSERT_EQ(row_view->GetSchema()->size(),
              request_row->GetSchema()->GetColumnCnt());

    int32_t init_size = 0;
    for (int i = 0; i < row_view->GetSchema()->size(); i++) {
        if (fesql::type::Type::kVarchar ==
            row_view->GetSchema()->Get(i).type()) {
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
            case fesql::type::kInt16:
                ASSERT_TRUE(
                    request_row->AppendInt16(row_view->GetInt16Unsafe(i)));
                break;
            case fesql::type::kInt32:
                ASSERT_TRUE(
                    request_row->AppendInt32(row_view->GetInt32Unsafe(i)));
                break;
            case fesql::type::kInt64:
                ASSERT_TRUE(
                    request_row->AppendInt64(row_view->GetInt64Unsafe(i)));
                break;
            case fesql::type::kFloat:
                ASSERT_TRUE(
                    request_row->AppendFloat(row_view->GetFloatUnsafe(i)));
                break;
            case fesql::type::kDouble:
                ASSERT_TRUE(
                    request_row->AppendDouble(row_view->GetDoubleUnsafe(i)));
                break;
            case fesql::type::kTimestamp:
                ASSERT_TRUE(request_row->AppendTimestamp(
                    row_view->GetTimestampUnsafe(i)));
                break;
            case fesql::type::kDate:
                ASSERT_TRUE(
                    request_row->AppendDate(row_view->GetDateUnsafe(i)));
                break;
            case fesql::type::kVarchar:
                ASSERT_TRUE(
                    request_row->AppendString(row_view->GetStringUnsafe(i)));
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
    boost::replace_all(sql, "{auto}",
                       fesql::sqlcase::SQLCase::GenRand("auto_t"));
    LOG(INFO) << sql;

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
        if (!sql_case.expect().schema_.empty() ||
            !sql_case.expect().columns_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
            CheckSchema(output_table.columns(), *(rs->GetSchema()));
        }

        if (!sql_case.expect().data_.empty() ||
            !sql_case.expect().rows_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputData(rows));
            CheckRows(output_table.columns(), sql_case.expect().order_, rows,
                      rs);
        }

        if (!sql_case.expect().count_ > 0) {
            ASSERT_EQ(sql_case.expect().count_,
                      static_cast<int64_t>(rs->Size()));
        }
    } else if (boost::algorithm::starts_with(sql, "create")) {
        bool ok = router->ExecuteDDL(sql_case.db(), sql, &status);
        router->RefreshCatalog();
        ASSERT_EQ(sql_case.expect().success_, ok);
    } else if (boost::algorithm::starts_with(sql, "insert")) {
        bool ok = router->ExecuteInsert(sql_case.db(), sql, &status);
        router->RefreshCatalog();
        ASSERT_EQ(sql_case.expect().success_, ok);
        router->RefreshCatalog();
    }
}

void SQLSDKTest::RunBatchModeSDK(fesql::sqlcase::SQLCase& sql_case,  // NOLINT
                                 std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, true);
    BatchExecuteSQL(sql_case, router);
}

void SQLSDKQueryTest::RequestExecuteSQL(
    fesql::sqlcase::SQLCase& sql_case,  // NOLINT
    std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    // execute SQL
    std::string sql = sql_case.sql_str();
    for (auto i = 0; i < sql_case.inputs().size(); i++) {
        std::string placeholder = "{" + std::to_string(i) + "}";
        boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
    }
    boost::replace_all(sql, "{auto}",
                       fesql::sqlcase::SQLCase::GenRand("auto_t"));
    LOG(INFO) << sql;

    if (boost::algorithm::starts_with(sql, "select")) {
        auto request_row = router->GetRequestRow(sql_case.db(), sql, &status);

        // success check
        if (!sql_case.expect().success_) {
            if ((request_row)) {
                FAIL() << "sql case expect success == false";
            }
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
        for (size_t i = 0; i < insert_rows.size(); i++) {
            row_view.Reset(insert_rows[i].buf());
            CovertFesqlRowToRequestRow(&row_view, request_row);
            auto rs =
                router->ExecuteSQL(sql_case.db(), sql, request_row, &status);
            if (!rs) FAIL() << "sql case expect success == true";
            results.push_back(rs);
            std::string insert_request;
            bool ok = router->ExecuteInsert(sql_case.db(), inserts[i], &status);
            router->RefreshCatalog();
            ASSERT_TRUE(ok);
        }
        ASSERT_FALSE(results.empty());
        std::vector<fesql::codec::Row> rows;
        fesql::type::TableDef output_table;
        if (!sql_case.expect().schema_.empty() ||
            !sql_case.expect().columns_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputSchema(output_table));
            CheckSchema(output_table.columns(), *(results[0]->GetSchema()));
        }

        if (!sql_case.expect().data_.empty() ||
            !sql_case.expect().rows_.empty()) {
            ASSERT_TRUE(sql_case.ExtractOutputData(rows));
            CheckRows(output_table.columns(), sql_case.expect().order_, rows,
                      results);
        }

        if (!sql_case.expect().count_ > 0) {
            ASSERT_EQ(sql_case.expect().count_,
                      static_cast<int64_t>(results.size()));
        }
    } else if (boost::algorithm::starts_with(sql, "create")) {
        FAIL() << "create sql not support in request mode";
    } else if (boost::algorithm::starts_with(sql, "insert")) {
        FAIL() << "insert sql not support in request mode";
    }
}

void SQLSDKQueryTest::RunRequestModeSDK(
    fesql::sqlcase::SQLCase& sql_case,  // NOLINT
    std::shared_ptr<SQLRouter> router) {
    fesql::sdk::Status status;
    CreateDB(sql_case, router);
    CreateTables(sql_case, router);
    InsertTables(sql_case, router, false);
    RequestExecuteSQL(sql_case, router);
}

INSTANTIATE_TEST_SUITE_P(SQLSDKTestSelectSample, SQLSDKQueryTest,
                         testing::ValuesIn(SQLSDKQueryTest::InitCases(
                             "/cases/integration/v1/test_select_sample.yaml")));
TEST_P(SQLSDKQueryTest, sql_sdk_request_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = query_mc_->GetZkCluster();
    sql_opt.zk_path = query_mc_->GetZkPath();
    sql_opt.enbale_debug = true;
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    RunRequestModeSDK(sql_case, router);
}
TEST_P(SQLSDKQueryTest, sql_sdk_batch_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = query_mc_->GetZkCluster();
    sql_opt.zk_path = query_mc_->GetZkPath();
    sql_opt.enbale_debug = true;
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    RunBatchModeSDK(sql_case, router);
}

}  // namespace sdk
}  // namespace rtidb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::rtidb::sdk::MiniCluster mc(6181);
    ::rtidb::sdk::mc_ = &mc;
    int ok = ::rtidb::sdk::mc_->SetUp();
    sleep(1);
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ok = RUN_ALL_TESTS();
    ::rtidb::sdk::mc_->Close();
    return ok;
}
