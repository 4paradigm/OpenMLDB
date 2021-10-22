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

#include "base/file_util.h"
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>

#include "base/glog_wapper.h"
#include "client/ns_client.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "rpc/rpc_client.h"
#include "tablet/tablet_impl.h"
#include "sdk/db_sdk.h"
#include "sdk/node_adapter.h"
#include "sdk/sql_cluster_router.h"
#include "plan/plan_api.h"
#include "test/util.h"
#include "cmd/sql_cmd.h"


DECLARE_string(db_root_path);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_bool(interactive);
DECLARE_string(cmd);
DEFINE_bool(interactive, true, "Set the interactive");
DEFINE_string(cmd, "", "Set the cmd");

namespace openmldb {
namespace cmd {
class StandaloneSQLTest : public ::testing::Test {
 public:
    StandaloneSQLTest() {}
    ~StandaloneSQLTest() {}
};

static void ExecuteSelectInto(const std::string& db, const std::string& sql, ::openmldb::sdk::SQLClusterRouter *router) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    ASSERT_EQ(sql_status.code, 0);

    hybridse::node::PlanNode *node = plan_trees[0];
    auto *select_into_plan_node = dynamic_cast<hybridse::node::SelectIntoPlanNode *>(node);
    const std::string& query_sql = select_into_plan_node->QueryStr();
    const std::string& file_path = select_into_plan_node->OutFile();
    const std::shared_ptr<hybridse::node::OptionsMap> options_map = select_into_plan_node->Options();
    ASSERT_TRUE(!db.empty());
    ::hybridse::sdk::Status status;
    auto rs = router->ExecuteSQL(db, query_sql, &status);
    try {
        openmldb::cmd::SaveResultSet(rs.get(), file_path, options_map);
    } catch (const char* errorMsg) {
        throw errorMsg;
    }
}

TEST_F(StandaloneSQLTest, smoketest) {
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();
    brpc::Server tablet;
    ASSERT_TRUE(::openmldb::test::StartTablet("127.0.0.1:9530", &tablet));

    brpc::Server server;
    ASSERT_TRUE(::openmldb::test::StartNS("127.0.0.1:9631", "127.0.0.1:9530", &server));
    ::openmldb::client::NsClient client("127.0.0.1:9631", "");
    ASSERT_EQ(client.Init(), 0);

    ::openmldb::sdk::DBSDK *sdk = new ::openmldb::sdk::StandAloneSDK("127.0.0.1", 9631);
    bool ok = sdk->Init();
    ASSERT_TRUE(ok);
    ::openmldb::sdk::SQLClusterRouter *router = new ::openmldb::sdk::SQLClusterRouter(sdk);
    ASSERT_TRUE(router->Init());
    const std::string db = "db";
    hybridse::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));

    const std::string ddl = "create table t1(col1 string, col2 int);";
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));

    const std::string insert = "insert into t1 values('key1', 1);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &status));

    int flag = 0;
    // True
    std::string select_into_sql = "select * from t1 into outfile '/tmp/data.csv'";
    try {
        openmldb::cmd::ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // True
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'overwrite')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // True
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'append')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // Faile - File exists
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'error_if_exists')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        flag = 1;
    }
    ASSERT_TRUE(flag == 1);
    flag = 0;

    // Fail - Mode un-supported
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'error')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        flag = 1;
    }
    ASSERT_TRUE(flag == 1);
    flag = 0;

    // False - Format un-supported
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'overwrite', format = 'parquet')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        flag = 1;
    }
    ASSERT_TRUE(flag == 1);
    flag = 0;

    // False - File path error
    select_into_sql = "select * from t1 into outfile 'file:////tmp/data.csv'";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        flag = 1;
    }
    ASSERT_TRUE(flag == 1);
    flag = 0;

    // False - Option un-supported
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'overwrite', test = 'null')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        flag = 1;
    }
    ASSERT_TRUE(flag == 1);
}
}  // namespace cmd
}  // namespace openmldb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::openmldb::test::GenRand();
    return RUN_ALL_TESTS();
}
