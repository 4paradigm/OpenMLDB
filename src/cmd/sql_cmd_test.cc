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

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> RtiDBIndex;

::openmldb::sdk::MiniCluster* mc_;
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}
class StandaloneSQLTest : public ::testing::Test {
 public:
    StandaloneSQLTest() {}
    ~StandaloneSQLTest() {}
    void SetUp() {}
    void TearDown() {}
};

static void ExecuteSelectInto(const std::string& db, const std::string& sql, std::shared_ptr<sdk::SQLRouter> router) {
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
    sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    auto endpoints = mc_->GetTbEndpoint();
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 int);";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    std::string insert = "insert into "+ name +" values('key1', 1);";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &status));
    ASSERT_TRUE(router->RefreshCatalog());
    
    // True
    std::string select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv'";
    try {
        openmldb::cmd::ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // True
    select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv' options (mode = 'overwrite')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // True
    select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv' options (mode = 'append')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // Faile - File exists
    select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv' options (mode = 'error_if_exists')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
        ASSERT_TRUE(true);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // Fail - Mode un-supported
    select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv' options (mode = 'error')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
        ASSERT_TRUE(true);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // False - Format un-supported
    select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv' options (mode = 'overwrite', format = 'parquet')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
        ASSERT_TRUE(true);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // False - File path error
    select_into_sql = "select * from "+ name +" into outfile 'file:////tmp/data.csv'";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
        ASSERT_TRUE(true);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // False - Option un-supported
    select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv' options (mode = 'overwrite', test = 'null')";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
        ASSERT_TRUE(true);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

    // False - Type un-supproted
    select_into_sql = "select * from "+ name +" into outfile '/tmp/data.csv' options (mode = 1)";
    try {
        ExecuteSelectInto(db, select_into_sql, router);
        ASSERT_TRUE(true);
    } catch (const char* errorMsg) {
        ASSERT_TRUE(false);
    }

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
