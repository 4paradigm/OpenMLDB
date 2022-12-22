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

#include "absl/random/random.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_replace.h"
#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "codec/fe_row_codec.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "test/base_test.h"
#include "test/util.h"
#include "vm/catalog.h"

namespace openmldb {
namespace sdk {

MiniCluster* mc_ = nullptr;
std::shared_ptr<SQLRouter> router_ = std::shared_ptr<SQLRouter>();

static void SetOnlineMode(std::shared_ptr<SQLRouter> router) {
    ::hybridse::sdk::Status status;
    router->ExecuteSQL("SET @@execute_mode='online';", &status);
}

static std::shared_ptr<SQLRouter> GetNewSQLRouter() {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.zk_session_timeout = 60000;
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    SetOnlineMode(router);
    return router;
}

static bool IsRequestSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos || mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("request-unsupport") != std::string::npos || mode.find("standalone-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
static bool IsBatchRequestSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos || mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("batch-request-unsupport") != std::string::npos ||
        mode.find("request-unsupport") != std::string::npos || mode.find("standalone-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
static bool IsBatchSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos || mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("batch-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("standalone-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
TEST_P(SQLSDKTest, SqlSdkBatchTest) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsBatchSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    SQLRouterOptions sql_opt;
    sql_opt.zk_session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = sql_case.debug() || hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
        return;
    }
    SetOnlineMode(router);
    SQLSDKTest::RunBatchModeSDK(sql_case, router, mc_->GetTbEndpoint());
}

TEST_P(SQLSDKQueryTest, SqlSdkRequestTest) {
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
TEST_P(SQLSDKQueryTest, SqlSdkBatchRequestTest) {
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

TEST_P(SQLSDKQueryTest, SqlSdkBatchTest) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsBatchSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    RunBatchModeSDK(sql_case, router_, mc_->GetTbEndpoint());
}

TEST_P(SQLSDKQueryTest, SqlSdkRequestProcedureTest) {
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

TEST_P(SQLSDKQueryTest, SqlSdkRequestProcedureAsynTest) {
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

struct DeploymentEnv {
    explicit DeploymentEnv(std::shared_ptr<sdk::SQLRouter> sr, hybridse::sqlcase::SqlCase* sqlcase)
        : sr_(sr), sql_case_(sqlcase) {
        dp_name_ = absl::StrCat("dp_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));
    }

    virtual ~DeploymentEnv() { TearDown(); }

    void SetUp() {
        hybridse::sdk::Status status;
        SQLSDKTest::CreateDB(*sql_case_, sr_);
        SQLSDKTest::CreateTables(*sql_case_, sr_);
        SQLSDKTest::InsertTables(*sql_case_, sr_, kNotInsertFirstInput);

        if (sql_case_->inputs()[0].name_.empty()) {
            sql_case_->set_input_name(
                hybridse::sqlcase::SqlCase::GenRand("auto_t") + std::to_string(static_cast<int64_t>(time(NULL))), 0);
        }

        sql_str_ = sql_case_->sql_str_;
        for (size_t i = 0; i < sql_case_->inputs().size(); i++) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            absl::StrReplaceAll({{placeholder, sql_case_->inputs_[i].name_}}, &sql_str_);
        }
        absl::StrReplaceAll({{"{auto}", hybridse::sqlcase::SqlCase::GenRand("auto_t") +
                                            std::to_string(static_cast<int64_t>(time(NULL)))}},
                            &sql_str_);
        absl::StripAsciiWhitespace(&sql_str_);

        ::openmldb::test::ProcessSQLs(
            sr_.get(), {absl::StrCat("use ", sql_case_->db_), absl::StrCat("deploy ", dp_name_, " ", sql_str_)});
    }

    void TearDown() {
        ::openmldb::test::ProcessSQLs(sr_.get(), {
                                                     absl::StrCat("drop deployment ", dp_name_),
                                                 });
        SQLSDKTest::DropTables(*sql_case_, sr_);
        ::openmldb::test::ProcessSQLs(sr_.get(), {
                                                     absl::StrCat("drop database ", sql_case_->db_),
                                                 });
    }

    void CallDeployProcedure() {
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

            LOG(INFO) << "insert request: \n" << inserts[i];
            bool ok = sr_->ExecuteInsert(insert_table.catalog(), inserts[i], &s);
            ASSERT_TRUE(ok);
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

    std::shared_ptr<sdk::SQLRouter> sr_;
    absl::BitGen gen_;
    std::string dp_name_;
    hybridse::sqlcase::SqlCase* sql_case_;
    std::string sql_str_;
};

TEST_P(SQLSDKQueryTest, SqlSdkDeployTest) {
    auto sql_case = GetParam();
    if (!sql_case.deployable_) {
        LOG(INFO) << "SKIPPED";
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DeploymentEnv env(router_, &sql_case);
    env.SetUp();
    env.CallDeployProcedure();
}

TEST_P(SQLSDKBatchRequestQueryTest, SqlSdkBatchRequestTest) {
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
TEST_P(SQLSDKBatchRequestQueryTest, SqlSdkBatchRequestProcedureTest) {
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

TEST_P(SQLSDKBatchRequestQueryTest, SqlSdkBatchRequestProcedureAsynTest) {
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

TEST_F(SQLSDKQueryTest, ExecuteLatestWhereTest) {
    std::string ddl =
        "create table latest_table(c1 string, c2 int, c3 timestamp, c4 timestamp, "
        "index(key=(c1),ts=c4,ttl_type=latest, ttl=2));";
    SQLRouterOptions sql_opt;
    sql_opt.zk_session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
    std::string db = "sql_latest_where_test";
    hybridse::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));
    ASSERT_TRUE(router->RefreshCatalog());
    std::string insert_sql = "insert into latest_table values('aa',1,1590738990000,1637047962096);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    insert_sql = "insert into latest_table values('aa',1,1590738990000,1637047962097);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    insert_sql = "insert into latest_table values('aa',1,1590738990000,1637047962098);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    std::string select = "select * from latest_table;";
    auto rs = router->ExecuteSQL(db, select, &status);
    if (!rs) {
        FAIL() << "fail to execute sql";
    }
    ASSERT_EQ(rs->Size(), 2);
    std::string select_where = "select * from latest_table where c1='aa';";
    rs = router->ExecuteSQL(db, select_where, &status);
    if (!rs) {
        FAIL() << "fail to execute sql";
    }
    ASSERT_EQ(rs->Size(), 2);
}

TEST_F(SQLSDKQueryTest, ExecuteWhereTest) {
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
    sql_opt.zk_session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
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

TEST_F(SQLSDKQueryTest, ExecuteInsertLoopsTest) {
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
    sql_opt.zk_session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
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

TEST_F(SQLSDKQueryTest, CreateNoTs) {
    std::string ddl =
        "create table t1(c1 string,\n"
        "                c2 bigint,\n"
        "                index(key=c1, ttl=14400m, ttl_type=absolute));";
    SQLRouterOptions sql_opt;
    sql_opt.zk_session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
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

TEST_F(SQLSDKQueryTest, RequestProcedureTest) {
    // create table
    std::string ddl =
        "create table trans(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7)) OPTIONS(replicanum=1, partitionnum=1);";
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.zk_session_timeout = 30000;
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
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

TEST_F(SQLSDKQueryTest, DropTableWithProcedureTest) {
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
    sql_opt.zk_session_timeout = 30000;
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
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
TEST_F(SQLSDKTest, TableReaderScan) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
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

TEST_F(SQLSDKTest, TableReaderAsyncScan) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
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
TEST_F(SQLSDKTest, CreateTable) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
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
    ASSERT_EQ(pid_map.size(), 3u);
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test0;", &status));
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test1;", &status));
    ASSERT_TRUE(router->DropDB(db, &status));
}

TEST_F(SQLSDKQueryTest, ExecuteWhereWithParameter) {
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
    sql_opt.zk_session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
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
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::openmldb::base::SetupGlog(true);

    srand(time(NULL));
    FLAGS_zk_session_timeout = 100000;
    ::openmldb::sdk::MiniCluster mc(6181);
    ::openmldb::sdk::mc_ = &mc;
    int ok = ::openmldb::sdk::mc_->SetUp(3);
    sleep(5);
    ::openmldb::sdk::router_ = ::openmldb::sdk::GetNewSQLRouter();
    if (nullptr == ::openmldb::sdk::router_) {
        LOG(ERROR) << "Fail Test with NULL SQL router";
        return -1;
    }
    ok = RUN_ALL_TESTS();
    ::openmldb::sdk::mc_->Close();
    return ok;
}
