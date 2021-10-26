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

#include <sched.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "cmd/sql_cmd.h"

#include "sdk/sql_router.h"
#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "case/sql_case.h"
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "vm/catalog.h"

DEFINE_bool(interactive, true, "Set interactive");
DEFINE_string(cmd, "", "Set cmd");

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

static void ExecuteSelectInto(const std::string& db, const std::string& sql,
    std::shared_ptr<sdk::SQLRouter> router, ::openmldb::base::ResultMsg* openmldb_base_status) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status hybridse_base_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, hybridse_base_status);
    ASSERT_EQ(hybridse_base_status.code, 0);
    hybridse::node::PlanNode *node = plan_trees[0];
    auto *select_into_plan_node = dynamic_cast<hybridse::node::SelectIntoPlanNode *>(node);
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
    std::string file_path =  "/tmp/data" + GenRand() + ".csv";
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

    std::string insert = "insert into "+ name +" values('key1', 1);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &hybridse_sdk_status));
    ASSERT_TRUE(router->RefreshCatalog());


    ::openmldb::base::ResultMsg openmldb_base_status;
    // True
    std::string select_into_sql = "select * from "+ name +" into outfile '" + file_path + "'";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(openmldb_base_status.OK());
    // Check file
    std::ifstream file;
    file.open(file_path);
    std::string line;
    std::string data;
    while (!file.eof()) {
        getline(file, line);
        data.append(line);
    }
    ASSERT_EQ(data, "col1,col2key1,1");
    file.close();

    // True
    select_into_sql = "select * from "+ name +" into outfile '" + file_path + "' options (mode = 'overwrite')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(openmldb_base_status.OK());

    // True
    select_into_sql = "select * from "+ name +" into outfile '" + file_path + "' options (mode = 'append')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(openmldb_base_status.OK());

    file.open(file_path);
    std::string data_append;
    while (!file.eof()) {
        getline(file, line);
        data_append.append(line);
    }
    ASSERT_EQ(data_append, "col1,col2key1,1col1,col2key1,1");
    file.close();

    // Fail - File exists
    select_into_sql = "select * from "+ name +" into outfile '" + file_path + "' options (mode = 'error_if_exists')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // Fail - Mode un-supported
    select_into_sql = "select * from "+ name +" into outfile '" + file_path + "' options (mode = 'error')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - Format un-supported
    select_into_sql = "select * from "+ name +" into outfile '" + file_path
     + "' options (mode = 'overwrite', format = 'parquet')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - File path error
    select_into_sql = "select * from "+ name +" into outfile 'file:////tmp/data.csv'";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - Option un-supported
    select_into_sql = "select * from "+ name +" into outfile '" + file_path
     + "' options (mode = 'overwrite', test = 'null')";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    // False - Type un-supproted
    select_into_sql = "select * from "+ name +" into outfile '" + file_path + "' options (mode = 1)";
    ExecuteSelectInto(db, select_into_sql, router, &openmldb_base_status);
    ASSERT_TRUE(!openmldb_base_status.OK());

    remove(file_path.c_str());
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
    ok = RUN_ALL_TESTS();
    ::openmldb::cmd::mc_->Close();
    return ok;
}
