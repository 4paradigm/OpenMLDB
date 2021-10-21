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

namespace openmldb {
namespace cmd {
::openmldb::sdk::DBSDK *cs = nullptr;
::openmldb::sdk::SQLClusterRouter *sr = nullptr;

class StandaloneSQLTest : public ::testing::Test {
 public:
    StandaloneSQLTest() {}
    ~StandaloneSQLTest() {}
};

static void ExecuteSelectInto(std::string& sql) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    ASSERT_EQ(sql_status.code, 0);

    hybridse::node::PlanNode *node = plan_trees[0];
    auto *select_into_plan_node = dynamic_cast<hybridse::node::SelectIntoPlanNode *>(node);
    const std::string& query_sql = selectIntoPlanNode->QueryStr();
    const std::string& file_path = selectIntoPlanNode->OutFile();
    const std::shared_ptr<hybridse::node::OptionsMap> options_map = select_into_plan_node->Options();
    ASSERT_TRUE(!openmldb::cmd::db.empty());
    ::hybridse::sdk::Status status;
    bool rs = sr->ExecuteSQL(db, query_sql, &status);
    try {
        openmldb::cmd::SaveResultSet(rs.get(), file_path, options_map);
    } catch (const char* errorMsg) {
        throw errorMsg;
    }
}

TEST_F(StandaloneSQLTest, smoketest) {
    openmldb::cmd::db = GenRand("db");
    hybridse::sdk::Status status;
    ASSERT_TRUE(sr->CreateDB(openmldb::cmd::db, &status));

    hybridse::sdk::Status status;
    std::string ddl = "create table t1(col1 string, col2 int);";
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));

    std::string insert = "insert into t1 values('key1', 1);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &status));

    ASSERT_TRUE(MkdirRecur("/tmp/"));

    // True
    std::string select_into_sql = "select * from t1 into outfile '/tmp/data.csv'";
    try {
        openmldb::cmd::ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // True
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'overwrite')";
    try {
        ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // True
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'append')";
    try {
        ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }
    
    // Faile - File exists
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'error_if_exists')";
    try {
        ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(!(std::string)errorMsg.empty());
    }

    // Fail - Mode un-supported
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'error')";
    try {
        ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(!(std::string)errorMsg.empty());
    }

    // False - Format un-supported
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'overwrite', format = 'parquet')";
    try {
        ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(!(std::string)errorMsg.empty());
    }

    // False - File path error
    select_into_sql = "select * from t1 into outfile 'file:////tmp/data.csv'";
    try {
        ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(!(std::string)errorMsg.empty());
    }

    // False - Option un-supported
    select_into_sql = "select * from t1 into outfile '/tmp/data.csv' options (mode = 'overwrite', test = 'option_test')";
    try {
        ExecuteSelectInto(select_into_sql);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(!(std::string)errorMsg.empty());
    }
}
}  // namespace cmd
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    brpc::Server tablet;
    ASSERT_TRUE(::openmldb::test::StartTablet("127.0.0.1:9530", &tablet));
    brpc::Server server;
    ASSERT_TRUE(::openmldb::test::StartNS("127.0.0.1:9631", "127.0.0.1:9530", &server));
    ::openmldb::client::NsClient client("127.0.0.1:9631", "");
    ASSERT_EQ(client.Init(), 0);
    cs = new ::openmldb::sdk::StandAloneSDK("127.0.0.1", "9631");
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    return RUN_ALL_TESTS();
}
