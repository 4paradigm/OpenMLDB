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


#include <unistd.h>
#include <limits>
#include <memory>
#include <string>

#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "cmd/sql_cmd.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "test/util.h"
#include "vm/catalog.h"

::openmldb::sdk::StandaloneEnv env;
::openmldb::sdk::MiniCluster mc(6181);

namespace openmldb {
namespace cmd {

using test::GenRand;
using test::ProcessSQLs;
using test::ExpectResultSetStrEq;

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

#if defined(__linux__)
TEST_P(DBSDKTest, CreateFunction) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::openmldb::sdk::SQLClusterRouter* sr_2 = nullptr;
    if (cs->IsClusterMode()) {
    ::openmldb::sdk::ClusterOptions copt;
        copt.zk_cluster = mc.GetZkCluster();
        copt.zk_path = mc.GetZkPath();
        auto cur_cs = new ::openmldb::sdk::ClusterSDK(copt);
        cur_cs->Init();
        sr_2 = new ::openmldb::sdk::SQLClusterRouter(cur_cs);
        sr_2->Init();
        ProcessSQLs(sr_2, {"set @@execute_mode = 'online'"});
    }
    hybridse::sdk::Status status;
    std::string so_path = openmldb::test::GetParentDir(openmldb::test::GetExeDir()) + "/libtest_udf.so";
    std::string cut2_sql = absl::StrCat("CREATE FUNCTION cut2(x STRING) RETURNS STRING "
                            "OPTIONS (FILE='", so_path, "');");
    std::string strlength_sql = absl::StrCat("CREATE FUNCTION strlength(x STRING) RETURNS INT "
                            "OPTIONS (FILE='", so_path, "');");
    std::string int2str_sql = absl::StrCat("CREATE FUNCTION int2str(x INT) RETURNS STRING "
                            "OPTIONS (FILE='", so_path, "');");
    std::string db_name = "test" + GenRand();
    std::string tb_name = "t1";
    ProcessSQLs(sr,
                {
                    "set @@execute_mode = 'online'",
                    absl::StrCat("create database ", db_name, ";"),
                    absl::StrCat("use ", db_name, ";"),
                    absl::StrCat("create table ", tb_name, " (c1 string, c2 int, c3 double);"),
                    absl::StrCat("insert into ", tb_name, " values ('aab', 11, 1.2);"),
                    cut2_sql,
                    strlength_sql,
                    int2str_sql
                });
    auto result = sr->ExecuteSQL("show functions", &status);
    ExpectResultSetStrEq({{"Name", "Return_type", "Arg_type", "Is_aggregate", "File"},
                          {"cut2", "Varchar", "Varchar", "false", so_path},
                          {"int2str", "Varchar", "Int", "false", so_path},
                          {"strlength", "Int", "Varchar", "false", so_path}},
                         result.get());
    result = sr->ExecuteSQL("select cut2(c1), strlength(c1), int2str(c2) from t1;", &status);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(1, result->Size());
    result->Next();
    std::string str;
    result->GetString(0, &str);
    ASSERT_EQ(str, "aa");
    int value = 0;
    result->GetInt32(1, &value);
    ASSERT_EQ(value, 3);
    str.clear();
    result->GetString(2, &str);
    ASSERT_EQ(str, "11");
    if (cs->IsClusterMode()) {
        ProcessSQLs(sr_2, {"set @@execute_mode = 'online'", absl::StrCat("use ", db_name, ";")});
        // check function in another sdk
        result = sr_2->ExecuteSQL("select cut2(c1), strlength(c1), int2str(c2) from t1;", &status);
        ASSERT_TRUE(status.IsOK()) << status.msg;
        ASSERT_EQ(1, result->Size());
    }
    ProcessSQLs(sr, {"DROP FUNCTION cut2;"});
    result = sr->ExecuteSQL("select cut2(c1) from t1;", &status);
    ASSERT_FALSE(status.IsOK());
    ProcessSQLs(sr,
                {
                    "DROP FUNCTION strlength;",
                    "DROP FUNCTION int2str;",
                    absl::StrCat("drop table ", tb_name, ";"),
                    absl::StrCat("drop database ", db_name, ";"),
                });
}

TEST_P(DBSDKTest, CreateUdafFunction) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    std::unique_ptr<::openmldb::sdk::SQLClusterRouter> sr_2;
    if (cs->IsClusterMode()) {
    ::openmldb::sdk::ClusterOptions copt;
        copt.zk_cluster = mc.GetZkCluster();
        copt.zk_path = mc.GetZkPath();
        auto cur_cs = new ::openmldb::sdk::ClusterSDK(copt);
        cur_cs->Init();
        sr_2 = std::make_unique<::openmldb::sdk::SQLClusterRouter>(cur_cs);
        sr_2->Init();
        ProcessSQLs(sr_2.get(), {"set @@execute_mode = 'online'"});
    }
    hybridse::sdk::Status status;
    std::string so_path = openmldb::test::GetParentDir(openmldb::test::GetExeDir()) + "/libtest_udf.so";
    std::string agg_fun_str = absl::StrCat("CREATE AGGREGATE FUNCTION special_sum(x BIGINT) RETURNS BIGINT "
                            "OPTIONS (FILE='", so_path, "');");
    std::string agg_fun_str1 = absl::StrCat("CREATE AGGREGATE FUNCTION count_null(x STRING) RETURNS BIGINT "
                            "OPTIONS (FILE='", so_path, "', ARG_NULLABLE=true);");
    std::string db_name = "test" + GenRand();
    std::string tb_name = "t1";
    ProcessSQLs(sr,
                {
                    "set @@execute_mode = 'online'",
                    absl::StrCat("create database ", db_name, ";"),
                    absl::StrCat("use ", db_name, ";"),
                    absl::StrCat("create table ", tb_name, " (c1 string, c2 bigint, c3 double);"),
                    absl::StrCat("insert into ", tb_name, " values ('aab', 11, 1.2);"),
                    absl::StrCat("insert into ", tb_name, " values ('aac', null, 1.2);"),
                    absl::StrCat("insert into ", tb_name, " values (null, 12, 1.2);"),
                    agg_fun_str,
                    agg_fun_str1
                });
    auto result = sr->ExecuteSQL("show functions", &status);
    ExpectResultSetStrEq({{"Name", "Return_type", "Arg_type", "Is_aggregate", "File",
                                "Return_nullable", "Arg_nullable"},
                          {"count_null", "BigInt", "Varchar", "true", so_path, "false", "true"},
                          {"special_sum", "BigInt", "BigInt", "true", so_path, "false", "false"}},
                         result.get());
    std::string select_sql = "select special_sum(c2) as sumc2, count_null(c1) as c1_null from t1;";
    result = sr->ExecuteSQL(select_sql, &status);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(1, result->Size());
    result->Next();
    int64_t value = 0;
    result->GetInt64(0, &value);
    ASSERT_EQ(value, 38);
    result->GetInt64(1, &value);
    ASSERT_EQ(value, 1);
    if (cs->IsClusterMode()) {
        ProcessSQLs(sr_2.get(), {"set @@execute_mode = 'online'", absl::StrCat("use ", db_name, ";")});
        // check function in another sdk
        result = sr_2->ExecuteSQL(select_sql, &status);
        ASSERT_TRUE(status.IsOK()) << status.msg;
        ASSERT_EQ(1, result->Size());
        result->Next();
        int64_t value = 0;
        result->GetInt64(0, &value);
        ASSERT_EQ(value, 38);
        result->GetInt64(1, &value);
        ASSERT_EQ(value, 1);
    }
    ProcessSQLs(sr, {"DROP FUNCTION special_sum;", "DROP FUNCTION count_null;"});
    result = sr->ExecuteSQL(select_sql, &status);
    ASSERT_FALSE(status.IsOK());
}
#endif

INSTANTIATE_TEST_SUITE_P(DBSDK, DBSDKTest, testing::Values(&standalone_cli, &cluster_cli));

}  // namespace cmd
}  // namespace openmldb

int main(int argc, char** argv) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_zk_session_timeout = 100000;
    FLAGS_enable_distsql = true;
    mc.SetUp(1);
    sleep(5);
    srand(time(NULL));
    ::openmldb::sdk::ClusterOptions copt;
    copt.zk_cluster = mc.GetZkCluster();
    copt.zk_path = mc.GetZkPath();
    ::openmldb::cmd::cluster_cli.cs = new ::openmldb::sdk::ClusterSDK(copt);
    ::openmldb::cmd::cluster_cli.cs->Init();
    ::openmldb::cmd::cluster_cli.sr = new ::openmldb::sdk::SQLClusterRouter(::openmldb::cmd::cluster_cli.cs);
    ::openmldb::cmd::cluster_cli.sr->Init();

    env.SetUp();
    ::openmldb::cmd::standalone_cli.cs = new ::openmldb::sdk::StandAloneSDK("127.0.0.1", env.GetNsPort());
    ::openmldb::cmd::standalone_cli.cs->Init();
    ::openmldb::cmd::standalone_cli.sr = new ::openmldb::sdk::SQLClusterRouter(::openmldb::cmd::standalone_cli.cs);
    ::openmldb::cmd::standalone_cli.sr->Init();
    sleep(3);

    bool ok = RUN_ALL_TESTS();

    // sr owns relative cs
    delete openmldb::cmd::cluster_cli.sr;
    delete openmldb::cmd::standalone_cli.sr;

    mc.Close();
    env.Close();

    return ok;
}
