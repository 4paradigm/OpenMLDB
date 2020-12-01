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


static std::shared_ptr<SQLRouter> GetNewSQLRouter(const fesql::sqlcase::SQLCase& sql_case) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = sql_case.debug() || fesql::sqlcase::SQLCase::IS_DEBUG();
    return NewClusterSQLRouter(sql_opt);
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
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
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
    LOG(INFO) << "Finish sql_sdk_distribute_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

}  // namespace sdk
}  // namespace rtidb

int main(int argc, char** argv) {
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
