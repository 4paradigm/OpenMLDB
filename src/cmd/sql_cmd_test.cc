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

#include "cmd/sql_cmd.h"

#include <sched.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "case/sql_case.h"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "vm/catalog.h"

DEFINE_bool(interactive, true, "Set interactive");
DEFINE_string(cmd, "", "Set cmd");
DECLARE_string(host);
DECLARE_int32(port);

::openmldb::sdk::StandaloneEnv env;

namespace openmldb {
namespace cmd {

::openmldb::sdk::MiniCluster* mc_;
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}
class SqlCmdTest : public ::testing::Test {
 public:
    SqlCmdTest() {}
    ~SqlCmdTest() {}
};

static void ExecuteSelectInto(const std::string& db, const std::string& sql, std::shared_ptr<sdk::SQLRouter> router,
                              ::openmldb::base::Status* openmldb_base_status) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status hybridse_base_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, hybridse_base_status);
    ASSERT_EQ(hybridse_base_status.code, 0);
    hybridse::node::PlanNode* node = plan_trees[0];
    auto* select_into_plan_node = dynamic_cast<hybridse::node::SelectIntoPlanNode*>(node);
    const std::string& query_sql = select_into_plan_node->QueryStr();
    const std::string& file_path = select_into_plan_node->OutFile();
    const std::shared_ptr<hybridse::node::OptionsMap> options_map = select_into_plan_node->Options();
    ASSERT_TRUE(!db.empty());
    hybridse::sdk::Status hybridse_sdk_status;
    auto rs = router->ExecuteSQL(db, query_sql, &hybridse_sdk_status);
    ASSERT_EQ(hybridse_sdk_status.code, 0);
    openmldb::cmd::SaveResultSet(rs.get(), file_path, options_map, openmldb_base_status);
}

TEST_F(SqlCmdTest, select_into_outfile) {
    sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    std::string file_path = "/tmp/data" + GenRand() + ".csv";
    ::hybridse::sdk::Status hybridse_sdk_status;
    bool ok = router->CreateDB(db, &hybridse_sdk_status);
    ASSERT_TRUE(ok);
    auto endpoints = mc_->GetTbEndpoint();
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 int);";
    ok = router->ExecuteDDL(db, ddl, &hybridse_sdk_status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    std::string insert = "insert into " + name + " (col1) " + " values('key1');";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &hybridse_sdk_status));
    ASSERT_TRUE(router->RefreshCatalog());

    ::openmldb::base::Status openmldb_base_status;
    // True
    std::string select_into_sql = "select * from " + name + " into outfile '" + file_path + "'";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(openmldb_base_status.OK());
    // Check file
    std::ifstream file;
    file.open(file_path);
    file.seekg(0, file.end);
    int length = file.tellg();
    file.seekg(0, file.beg);
    char* data = new char[length + 1];
    data[length] = '\0';
    file.read(data, length);
    ASSERT_EQ(strcmp(data, "col1,col2\nkey1,null"), 0);
    delete [] data;
    file.close();

    // True
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'overwrite')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(openmldb_base_status.OK());

    // True
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'append')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(openmldb_base_status.OK());

    file.open(file_path);
    file.seekg(0, file.end);
    int append_length = file.tellg();
    file.seekg(0, file.beg);
    char* append_data = new char[append_length + 1];
    append_data[append_length] = '\0';
    file.read(append_data, append_length);
    ASSERT_EQ(strcmp(append_data, "col1,col2\nkey1,null\ncol1,col2\nkey1,null"), 0);
    delete [] append_data;
    file.close();

    // Fail - File exists
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'error_if_exists')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // Fail - Mode un-supported
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'error')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - Format un-supported
    select_into_sql =
        "select * from " + name + " into outfile '" + file_path + "' options (mode = 'overwrite', format = 'parquet')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - File path error
    select_into_sql = "select * from " + name + " into outfile 'file:////tmp/data.csv'";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - Option un-supported
    select_into_sql =
        "select * from " + name + " into outfile '" + file_path + "' options (mode = 'overwrite', test = 'null')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - Type un-supproted
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 1)";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    remove(file_path.c_str());
}

TEST_F(SqlCmdTest, deploy) {
    HandleSQL("create database test1;");
    HandleSQL("use test1;");
    std::string create_sql = "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
                             "c8 date, index(key=c3, ts=c7, abs_ttl=0, ttl_type=absolute));";
    HandleSQL(create_sql);
    HandleSQL("insert into trans values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\");");

    std::string deploy_sql = "deploy demo SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(deploy_sql, plan_trees, &node_manager, sql_status);
    ASSERT_EQ(0, sql_status.code);
    hybridse::node::PlanNode *node = plan_trees[0];
    auto status = HandleDeploy(dynamic_cast<hybridse::node::DeployPlanNode*>(node));
    ASSERT_TRUE(status.OK());
    std::string msg;
    ASSERT_FALSE(cs->GetNsClient()->DropTable("test1", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test1", "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test1", "trans", msg));
}

TEST_F(SqlCmdTest, create_without_index_col) {
    HandleSQL("create database test2;");
    HandleSQL("use test2;");
    std::string create_sql = "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
                             "c8 date, index(ts=c7));";
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(create_sql, plan_trees, &node_manager, sql_status);
    ASSERT_EQ(0, sql_status.code);
    hybridse::node::PlanNode *node = plan_trees[0];
    auto status = sr->HandleSQLCreateTable(dynamic_cast<hybridse::node::CreatePlanNode*>(node),
            "test2", cs->GetNsClient());
    ASSERT_TRUE(status.OK());
    std::string msg;
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "trans", msg));
}

}  // namespace cmd
}  // namespace openmldb

int main(int argc, char** argv) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_zk_session_timeout = 100000;
    ::openmldb::sdk::MiniCluster mc(6181);
    ::openmldb::cmd::mc_ = &mc;
    int ok = ::openmldb::cmd::mc_->SetUp(1);
    sleep(1);
    srand(time(NULL));
    env.SetUp();
    FLAGS_host = "127.0.0.1";
    FLAGS_port = env.GetNsPort();
    ::openmldb::cmd::StandAloneInit();

    ok = RUN_ALL_TESTS();
    ::openmldb::cmd::mc_->Close();
    return ok;
}
