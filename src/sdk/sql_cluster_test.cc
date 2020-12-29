/*
 * sql_router_test.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "sdk/sql_cluster_router.h"
#include "sdk/sql_sdk_test.h"
#include "timer.h"  // NOLINT
#include "vm/catalog.h"

namespace rtidb {
namespace sdk {

::rtidb::sdk::MiniCluster* mc_;

class SQLClusterTest : public ::testing::Test {
 public:
    SQLClusterTest() {}
    ~SQLClusterTest() {}
    void SetUp() {}
    void TearDown() {}
};

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

TEST_F(SQLClusterTest, cluster_insert) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2)) partitionnum=8;";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::map<uint32_t, std::vector<std::string>> key_map;
    for (int i = 0; i < 100; i++) {
        std::string key = "hello" + std::to_string(i);
        std::string insert = "insert into " + name + " values('" + key + "', 1590);";
        ok = router->ExecuteInsert(db, insert, &status);
        ASSERT_TRUE(ok);
        uint32_t pid = (uint32_t)(::rtidb::base::hash64(key) % 8);
        key_map[pid].push_back(key);
    }
    auto endpoints = mc_->GetTbEndpoint();
    uint32_t count = 0;
    for (const auto& endpoint : endpoints) {
        ::rtidb::tablet::TabletImpl* tb1 = mc_->GetTablet(endpoint);
        ::rtidb::api::GetTableStatusRequest request;
        ::rtidb::api::GetTableStatusResponse response;
        MockClosure closure;
        tb1->GetTableStatus(NULL, &request, &response, &closure);
        for (const auto& table_status : response.all_table_status()) {
            count += table_status.record_cnt();
            auto iter = key_map.find(table_status.pid());
            ASSERT_EQ(iter->second.size(), table_status.record_cnt());
        }
    }
    ASSERT_EQ(100u, count);
    ok = router->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLSDKQueryTest, GetTabletClient) {
    std::string ddl =
        "create table t1(col0 string,\n"
        "                col1 bigint,\n"
        "                col2 string,\n"
        "                col3 bigint,\n"
        "                index(key=col2, ts=col3)) partitionnum=2;";
    SQLRouterOptions sql_opt;
    sql_opt.session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = fesql::sqlcase::SQLCase::IS_DEBUG();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "gettabletclient;";
    fesql::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));
    ASSERT_TRUE(router->RefreshCatalog());
    std::string sql = "select col2, sum(col1) over w1 from t1 \n"
                      "window w1 as (partition by col2 \n"
                      "order by col3 rows between 3 preceding and current row);";
    auto ns_client = mc_->GetNsClient();
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    ASSERT_TRUE(ns_client->ShowTable("t1", db, false, tables, msg));
    for (int i = 0; i < 10; i++) {
        std::string pk = "pk" + std::to_string(i);
        auto request_row = router->GetRequestRow(db, sql, &status);
        request_row->Init(4 + pk.size());
        request_row->AppendString("col0");
        request_row->AppendInt64(1);
        request_row->AppendString(pk);
        request_row->AppendInt64(3);
        ASSERT_TRUE(request_row->Build());
        auto sql_cluster_router = std::dynamic_pointer_cast<SQLClusterRouter>(router);
        auto client = sql_cluster_router->GetTabletClient(db, sql, request_row);
        int pid = ::rtidb::base::hash64(pk) % 2;
        ASSERT_EQ(client->GetEndpoint(), tables[0].table_partition(pid).partition_meta(0).endpoint());
    }
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table t1;", &status));
    ASSERT_TRUE(router->DropDB(db, &status));
}


static std::shared_ptr<SQLRouter> GetNewSQLRouter(const fesql::sqlcase::SQLCase& sql_case) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = sql_case.debug() || fesql::sqlcase::SQLCase::IS_DEBUG();
    return NewClusterSQLRouter(sql_opt);
}

TEST_P(SQLSDKQueryTest, sql_sdk_distribute_batch_request_test) {
    auto sql_case = GetParam();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
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
    DistributeRunBatchRequestModeSDK(sql_case, router);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_distribute_batch_request_test) {
    auto sql_case = GetParam();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
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
    DistributeRunBatchRequestModeSDK(sql_case, router);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, sql_sdk_distribute_request_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    auto router = GetNewSQLRouter(sql_case);
    ASSERT_TRUE(router != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestModeSDK(sql_case, router);
    LOG(INFO) << "Finish sql_sdk_distribute_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKQueryTest, sql_sdk_distribute_batch_request_single_partition_test) {
    auto sql_case = GetParam();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
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
    DistributeRunBatchRequestModeSDK(sql_case, router, 1);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_single_partition_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_distribute_batch_request_single_partition_test) {
    auto sql_case = GetParam();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
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
    DistributeRunBatchRequestModeSDK(sql_case, router, 1);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_single_partition_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, sql_sdk_distribute_request_single_partition_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    auto router = GetNewSQLRouter(sql_case);
    ASSERT_TRUE(router != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestModeSDK(sql_case, router, 1);
    LOG(INFO) << "Finish sql_sdk_distribute_request_single_partition_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_distribute_batch_request_procedure_test) {
    auto sql_case = GetParam();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
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
    DistributeRunBatchRequestProcedureModeSDK(sql_case, router, 8, false);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_procedure_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, sql_sdk_distribute_request_procedure_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    auto router = GetNewSQLRouter(sql_case);
    ASSERT_TRUE(router != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestProcedureModeSDK(sql_case, router, 8, false);
    LOG(INFO) << "Finish sql_sdk_distribute_request_procedure_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, sql_sdk_distribute_batch_request_procedure_async_test) {
    auto sql_case = GetParam();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
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
    DistributeRunBatchRequestProcedureModeSDK(sql_case, router, 8, true);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_procedure_async_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, sql_sdk_distribute_request_procedure_async_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    auto router = GetNewSQLRouter(sql_case);
    ASSERT_TRUE(router != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestProcedureModeSDK(sql_case, router, 8, true);
    LOG(INFO) << "Finish sql_sdk_distribute_request_procedure_async_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_F(SQLClusterTest, create_table) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    for (int i = 0; i < 2; i++) {
        std::string name = "test" + std::to_string(i);
        std::string ddl = "create table " + name +
                          "("
                          "col1 string, col2 bigint,"
                          "index(key=col1, ts=col2)) partitionnum=3;";
        ok = router->ExecuteDDL(db, ddl, &status);
        ASSERT_TRUE(ok);
    }
    ASSERT_TRUE(router->RefreshCatalog());
    auto ns_client = mc_->GetNsClient();
    std::vector<::rtidb::nameserver::TableInfo> tables;
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
    ASSERT_EQ(pid_map.size(), 2);
    ASSERT_EQ(pid_map.begin()->second, pid_map.rbegin()->second);
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test0;", &status));
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test1;", &status));
    ASSERT_TRUE(router->DropDB(db, &status));
}

}  // namespace sdk
}  // namespace rtidb

int main(int argc, char** argv) {
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    FLAGS_zk_session_timeout = 100000;
    ::rtidb::sdk::MiniCluster mc(6181);
    ::rtidb::sdk::mc_ = &mc;
    FLAGS_enable_distsql = true;
    int ok = ::rtidb::sdk::mc_->SetUp(2);
    sleep(1);
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ok = RUN_ALL_TESTS();
    ::rtidb::sdk::mc_->Close();
    return ok;
}
