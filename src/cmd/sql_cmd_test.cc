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

#include <unistd.h>

#include <memory>
#include <string>

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "vm/catalog.h"

DECLARE_bool(interactive);
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

struct CLI {
    ::openmldb::sdk::DBSDK* cs = nullptr;
    ::openmldb::sdk::SQLClusterRouter* sr = nullptr;
};

CLI standalone_cli;
CLI cluster_cli;

class SqlCmdTest : public ::testing::Test {
 public:
    SqlCmdTest() {}
    ~SqlCmdTest() {}
};

class DBSDKTest : public ::testing::TestWithParam<CLI*> {};

TEST_F(SqlCmdTest, select_into_outfile) {
    sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    std::string file_path = "/tmp/data" + GenRand() + ".csv";
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    router->ExecuteSQL("use " + db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << "error msg: " + status.msg;
    router->ExecuteSQL("SET @@execute_mode='online';", &status);
    ASSERT_TRUE(status.IsOK()) << "error msg: " + status.msg;
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 int);";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    std::string insert = "insert into " + name + " (col1) " + " values('key1');";
    ASSERT_TRUE(router->ExecuteInsert(db, insert, &status));
    ASSERT_TRUE(router->RefreshCatalog());

    // True
    std::string select_into_sql = "select * from " + name + " into outfile '" + file_path + "'";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_TRUE(status.IsOK()) << "error msg: " + status.msg;
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
    delete[] data;
    file.close();

    // True
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'overwrite')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_TRUE(status.IsOK());

    // True
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'append')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_TRUE(status.IsOK());

    file.open(file_path);
    file.seekg(0, file.end);
    int append_length = file.tellg();
    file.seekg(0, file.beg);
    char* append_data = new char[append_length + 1];
    append_data[append_length] = '\0';
    file.read(append_data, append_length);
    ASSERT_EQ(strcmp(append_data, "col1,col2\nkey1,null\ncol1,col2\nkey1,null"), 0);
    delete[] append_data;
    file.close();

    // Fail - File exists
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'error_if_exists')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // Fail - Mode un-supported
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 'error')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // False - Format un-supported
    select_into_sql =
        "select * from " + name + " into outfile '" + file_path + "' options (mode = 'overwrite', format = 'parquet')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // False - File path error
    select_into_sql = "select * from " + name + " into outfile 'file:////tmp/data.csv'";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // False - Option un-supported
    select_into_sql =
        "select * from " + name + " into outfile '" + file_path + "' options (mode = 'overwrite', test = 'null')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // False - Type un-supproted
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (mode = 1)";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // False - Type un-supproted
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (quote = '__')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // False - Type un-supproted
    select_into_sql = "select * from " + name + " into outfile '" + file_path + "' options (delimiter = '')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    // False - Delimiter can't include quote
    select_into_sql =
        "select * from " + name + " into outfile '" + file_path + "' options (quote = '_', delimiter = '__')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_FALSE(status.IsOK());

    remove(file_path.c_str());
}

TEST_P(DBSDKTest, deploy) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("create database test1;");
    HandleSQL("use test1;");
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c3, ts=c7, abs_ttl=0, ttl_type=absolute));";
    HandleSQL(create_sql);
    if (!cs->IsClusterMode()) {
        HandleSQL("insert into trans values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\");");
    }

    std::string deploy_sql =
        "deploy demo SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

    hybridse::sdk::Status status;
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK());
    std::string msg;
    ASSERT_FALSE(cs->GetNsClient()->DropTable("test1", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test1", "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test1", "trans", msg));

    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_FALSE(status.IsOK());
}

TEST_P(DBSDKTest, deploy_col) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("create database test2;");
    HandleSQL("use test2;");
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c1, ts=c4, abs_ttl=0, ttl_type=absolute));";
    HandleSQL(create_sql);
    if (!cs->IsClusterMode()) {
        HandleSQL("insert into trans values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\");");
    }

    std::string deploy_sql =
        "deploy demo SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK());
    std::string msg;
    ASSERT_FALSE(cs->GetNsClient()->DropTable("test2", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "trans", msg));
}

TEST_P(DBSDKTest, deploy_options) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleCmd("show global variables;");
    HandleSQL("create database test2;");
    HandleSQL("use test2;");
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c1, ts=c4, abs_ttl=0, ttl_type=absolute));";
    HandleSQL(create_sql);
    if (!cs->IsClusterMode()) {
        HandleSQL("insert into trans values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\");");
    }

    std::string deploy_sql =
        "deploy demo OPTIONS(long_windows='w1:100') SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK());
    std::string msg;
    ASSERT_FALSE(cs->GetNsClient()->DropTable("test2", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "trans", msg));
}

TEST_P(DBSDKTest, create_without_index_col) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("create database test2;");
    HandleSQL("use test2;");
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(ts=c7));";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(create_sql, &status);
    ASSERT_TRUE(status.IsOK());
    std::string msg;
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "trans", msg));
}

/* TODO: Only run test in standalone mode
TEST_P(DBSDKTest, load_data) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    std::string read_file_path = "/tmp/data" + GenRand() + ".csv";
    std::string write_file_path = "/tmp/data" + GenRand() + ".csv";
    std::ofstream ofile;
    std::ifstream ifile;
    ofile.open(read_file_path);
    ofile << "1 ---345---567" << std::endl;
    ofile << "1 ---\"3 4 5\"---567" << std::endl;
    ofile << "1 --- -- - --- abc" << std::endl;
    ofile << "1 --- - - --- abc" << std::endl;
    ofile << "1 --- - A --- A--" << std::endl;
    ofile << "1 --- --- -" << std::endl;
    ofile << "1 --- \" --- \" --- A" << std::endl;

    ExecuteSQL("create database test1;");
    ExecuteSQL("use test1;");

    std::string create_sql = "create table t1 (c1 string, c2 string, c3 string);";
    ExecuteSQL(create_sql);

    ExecuteSQL("load data infile '" + read_file_path +
              "' into table t1 OPTIONS( header = false, delimiter = '---', quote = '\"');");
    ExecuteSQL("select * from t1 into outfile '" + write_file_path + "';");

    ifile.open(write_file_path);
    ifile.seekg(0, ifile.end);
    int length = ifile.tellg();
    ifile.seekg(0, ifile.beg);
    char* data = new char[length + 1];
    data[length] = '\0';

    ifile.read(data, length);
    ASSERT_EQ(strcmp(data, "c1,c2,c3\n1, --- ,A\n1,,-\n1,- A,A--\n1,- -,abc\n1,-- -,abc\n1,3 4 5,567\n1,345,567"), 0);
    delete[] data;
    ifile.close();
    ofile.close();
}
*/

INSTANTIATE_TEST_SUITE_P(DBSDK, DBSDKTest, testing::Values(&standalone_cli, &cluster_cli));

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
    ::openmldb::sdk::ClusterOptions copt;
    copt.zk_cluster = mc.GetZkCluster();
    copt.zk_path = mc.GetZkPath();
    ::openmldb::cmd::cluster_cli.cs = new ::openmldb::sdk::ClusterSDK(copt);
    ::openmldb::cmd::cluster_cli.cs->Init();
    ::openmldb::cmd::cluster_cli.sr = new ::openmldb::sdk::SQLClusterRouter(::openmldb::cmd::cluster_cli.cs);
    env.SetUp();
    FLAGS_host = "127.0.0.1";
    FLAGS_port = env.GetNsPort();

    ::openmldb::cmd::standalone_cli.cs = new ::openmldb::sdk::StandAloneSDK(FLAGS_host, FLAGS_port);
    ::openmldb::cmd::standalone_cli.cs->Init();
    ::openmldb::cmd::standalone_cli.sr = new ::openmldb::sdk::SQLClusterRouter(::openmldb::cmd::standalone_cli.cs);

    ok = RUN_ALL_TESTS();
    ::openmldb::cmd::mc_->Close();
    return ok;
}
