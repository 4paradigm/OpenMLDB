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

#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "absl/algorithm/container.h"
#include "absl/cleanup/cleanup.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_router.h"
#include "test/util.h"
#include "vm/catalog.h"

DECLARE_string(host);
DECLARE_int32(port);
DECLARE_uint32(traverse_cnt_limit);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(recycle_bin_ssd_root_path);
DECLARE_string(recycle_bin_hdd_root_path);
DECLARE_uint32(get_table_status_interval);

::openmldb::sdk::StandaloneEnv env;

namespace openmldb {
namespace cmd {

::openmldb::sdk::MiniCluster* mc_;

using test::ExpectResultSetStrEq;
using test::GenRand;
using test::ProcessSQLs;

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

bool EmptyDB(std::shared_ptr<openmldb::client::NsClient> ns_client, std::string db) {
    std::vector<nameserver::TableInfo> tables;
    auto ret = ns_client->ShowDBTable(db, &tables);
    if (!ret.OK()) {
        LOG(INFO) << "show table failed: " << ret.GetMsg();
        return false;
    }
    if (!tables.empty()) {
        LOG(INFO) << "db " << db << " is not empty:";
        for (auto& table : tables) {
            LOG(INFO) << "table " << table.name();
        }
        return false;
    }
    // if no table, it can't have deployments, but it's better to check for debug
    std::vector<api::ProcedureInfo> procedures;
    std::string msg;
    auto ok = ns_client->ShowProcedure(db, "", &procedures, &msg);
    if (!ok) {
        LOG(INFO) << "show procedure failed: " << msg;
        return false;
    }
    if (!procedures.empty()) {
        LOG(INFO) << "db " << db << " is not empty:";
        for (auto& procedure : procedures) {
            LOG(INFO) << "procedure " << procedure.sp_name();
        }
        return false;
    }
    return true;
}

TEST_F(SqlCmdTest, showDeployment) {
    auto cli = cluster_cli;
    auto sr = cli.sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("show deployment aa", &status);
    ASSERT_FALSE(status.IsOK());
    ASSERT_EQ(status.msg, "Please enter database first");
}

TEST_F(SqlCmdTest, SelectIntoOutfile) {
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

    // True - Option un-supported will be ignored
    select_into_sql =
        "select * from " + name + " into outfile '" + file_path + "' options (mode = 'overwrite', test = 'null')";
    router->ExecuteSQL(select_into_sql, &status);
    ASSERT_TRUE(status.IsOK());

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

    router->ExecuteSQL("drop table " + name, &status);
    router->DropDB(db, &status);
    ASSERT_TRUE(status.IsOK());
    remove(file_path.c_str());
}

TEST_P(DBSDKTest, CreateDatabase) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    hybridse::sdk::Status status;
    auto db1 = absl::StrCat("db_", GenRand());
    auto db2 = absl::StrCat("db_", GenRand());
    ProcessSQLs(sr, {
                        absl::StrCat("CREATE DATABASE ", db1),
                        absl::StrCat("CREATE DATABASE ", db2),
                        absl::StrCat("CREATE DATABASE IF NOT EXISTS ", db1),
                    });
    sr->ExecuteSQL(absl::StrCat("CREATE DATABASE ", db1), &status);
    EXPECT_FALSE(status.IsOK());
    auto rs = sr->ExecuteSQL("SHOW DATABASES", &status);
    std::set<std::string> dbs;
    while (rs->Next()) {
        std::string val;
        rs->GetString(0, &val);
        dbs.insert(val);
    }
    ASSERT_EQ(dbs.count(db1), 1);
    ASSERT_EQ(dbs.count(db2), 1);
    ProcessSQLs(sr, {absl::StrCat("DROP DATABASE ", db1), absl::StrCat("DROP DATABASE ", db2)});
}

TEST_P(DBSDKTest, CreateAndShowTable) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    auto db = absl::StrCat("db_", GenRand());
    auto t1 = absl::StrCat("table_", GenRand());
    auto t2 = absl::StrCat("table_", GenRand());
    ProcessSQLs(sr, {
                        absl::StrCat("CREATE DATABASE ", db),
                        absl::StrCat("USE ", db),
                        absl::StrCat("CREATE TABLE ", t1, " (col1 string);"),
                        absl::StrCat("CREATE TABLE ", t2, " (col1 string);"),
                    });
    std::set<std::string> dbs = {t1, t2};
    hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("SHOW TABLES", &status);
    rs->Next();
    std::string val;
    rs->GetString(0, &val);
    ASSERT_EQ(dbs.count(val), 1);
    rs->Next();
    val.clear();
    rs->GetString(0, &val);
    ASSERT_EQ(dbs.count(val), 1);
    ASSERT_FALSE(rs->Next());
    ProcessSQLs(sr, {absl::StrCat("DROP TABLE ", t1)});
    rs = sr->ExecuteSQL("SHOW TABLES", &status);
    rs->Next();
    val.clear();
    rs->GetString(0, &val);
    ASSERT_EQ(val, t2);
    ASSERT_FALSE(rs->Next());
    ProcessSQLs(sr, {absl::StrCat("DROP TABLE ", t2), absl::StrCat("DROP DATABASE ", db)});
}

TEST_P(DBSDKTest, Select) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    hybridse::sdk::Status status;
    if (cs->IsClusterMode()) {
        sr->ExecuteSQL("SET @@execute_mode='online';", &status);
        ASSERT_TRUE(status.IsOK()) << "error msg: " + status.msg;
    }
    std::string db = "db" + GenRand();
    sr->ExecuteSQL("create database " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
    sr->ExecuteSQL("use " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c3, ts=c7, abs_ttl=0, ttl_type=absolute));";
    sr->ExecuteSQL(create_sql, &status);
    ASSERT_TRUE(status.IsOK());
    std::string insert_sql = "insert into trans values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\");";
    sr->ExecuteSQL(insert_sql, &status);
    ASSERT_TRUE(status.IsOK());
    insert_sql = "insert into trans values ('aaa', 11, 22, 1.2, 1.3, -123, \"2021-05-20\");";
    sr->ExecuteSQL(insert_sql, &status);
    ASSERT_FALSE(status.IsOK());
    auto rs = sr->ExecuteSQL("select * from trans", &status);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(1, rs->Size());
    sr->ExecuteSQL("drop table trans;", &status);
    ASSERT_TRUE(status.IsOK());
    sr->ExecuteSQL("drop database " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
}

TEST_P(DBSDKTest, SelectSnappy) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    hybridse::sdk::Status status;
    if (cs->IsClusterMode()) {
        sr->ExecuteSQL("SET @@execute_mode='online';", &status);
        ASSERT_TRUE(status.IsOK()) << "error msg: " + status.msg;
    }
    std::string db = "db" + GenRand();
    sr->ExecuteSQL("create database " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
    sr->ExecuteSQL("use " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
    std::string create_sql =
        "create table trans (c1 string, c2 bigint, c3 date,"
        "index(key=c1, ts=c2, abs_ttl=0, ttl_type=absolute)) options (compress_type='snappy');";
    sr->ExecuteSQL(create_sql, &status);
    ASSERT_TRUE(status.IsOK());
    int insert_num = 100;
    for (int i = 0; i < insert_num; i++) {
        auto insert_sql = absl::StrCat("insert into trans values ('aaa", i, "', 1635247427000, \"2021-05-20\");");
        sr->ExecuteSQL(insert_sql, &status);
        ASSERT_TRUE(status.IsOK());
    }
    auto rs = sr->ExecuteSQL("select * from trans", &status);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(insert_num, rs->Size());
    int count = 0;
    while (rs->Next()) {
        count++;
    }
    EXPECT_EQ(count, insert_num);
    sr->ExecuteSQL("drop table trans;", &status);
    ASSERT_TRUE(status.IsOK());
    sr->ExecuteSQL("drop database " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
}

TEST_F(SqlCmdTest, SelectMultiPartition) {
    auto sr = cluster_cli.sr;
    std::string db_name = "test" + GenRand();
    std::string name = "table" + GenRand();
    std::string ddl = "create table " + name +
                      "("
                      "col1 int not null,"
                      "col2 bigint default 112 not null,"
                      "col4 string default 'test4' not null,"
                      "col5 date default '2000-01-01' not null,"
                      "col6 timestamp default 10000 not null,"
                      "index(key=col1, ts=col2)) options(partitionnum=8);";
    ProcessSQLs(sr, {"set @@execute_mode = 'online'", absl::StrCat("create database ", db_name, ";"),
                     absl::StrCat("use ", db_name, ";"), ddl});
    std::string sql;
    int expect = 1000;
    hybridse::sdk::Status status;
    for (int i = 0; i < expect; i++) {
        sql = "insert into " + name + " values(" + std::to_string(i) + ", 1, '1', '2021-01-01', 1);";
        ASSERT_TRUE(sr->ExecuteInsert(db_name, sql, &status));
    }
    auto res = sr->ExecuteSQL(db_name, "select * from " + name, &status);
    ASSERT_TRUE(res);
    int count = 0;
    while (res->Next()) {
        count++;
    }
    EXPECT_EQ(count, expect);
    ProcessSQLs(sr, {absl::StrCat("drop table ", name, ";"), absl::StrCat("drop database ", db_name, ";")});
}

TEST_F(SqlCmdTest, ShowNameserverJob) {
    sr = cluster_cli.sr;
    std::string db_name = "test" + GenRand();
    std::string name = "table" + GenRand();
    std::string ddl = "create table " + name +
                      "(col1 string, col2 string, col3 bigint, index(key=col1, ts=col3, TTL_TYPE=absolute)) "
                      "options (partitionnum=2, replicanum=1)";
    ProcessSQLs(sr, {"set @@execute_mode = 'online'", absl::StrCat("create database ", db_name, ";"),
                     absl::StrCat("use ", db_name, ";"), ddl});
    absl::Cleanup clean = [&]() {
        ProcessSQLs(sr, {absl::StrCat("drop table ", name, ";"), absl::StrCat("drop database ", db_name, ";")});
    };
    hybridse::sdk::Status status;
    std::string sql;
    for (int i = 0; i < 10; i++) {
        sql = absl::StrCat("insert into ", name, " values('", i, "', '", i, "', 1635247427000);");
        ASSERT_TRUE(sr->ExecuteInsert(db_name, sql, &status));
    }
    sql = absl::StrCat("create index index2 on ", name, " (col2) options(ts=col3);");
    sr->ExecuteSQL(db_name, sql, &status);
    ASSERT_TRUE(status.IsOK());
    auto rs = sr->ExecuteSQL(db_name, "show jobs from nameserver;", &status);
    ASSERT_TRUE(status.IsOK());
    ASSERT_GT(rs->Size(), 0);
}

TEST_F(SqlCmdTest, TableReader) {
    auto sr = cluster_cli.sr;
    std::string db_name = "test" + GenRand();
    std::string name = "table" + GenRand();
    std::string ddl = "create table " + name +
                      "("
                      "col1 string not null,"
                      "col2 bigint default 112 not null,"
                      "col4 string default 'test4' not null,"
                      "col5 date default '2000-01-01' not null,"
                      "col6 int default 10000 not null,"
                      "index(key=col1, ts=col2));";
    ProcessSQLs(sr, {"set @@execute_mode = 'online'", absl::StrCat("create database ", db_name, ";"),
                     absl::StrCat("use ", db_name, ";"), ddl});
    hybridse::sdk::Status status;
    std::string sql = "insert into " + name + " values('key1', 1, '1', '2021-01-01', 10);";
    ASSERT_TRUE(sr->ExecuteInsert(db_name, sql, &status));
    auto reader = sr->GetTableReader();
    openmldb::sdk::ScanOption option;
    option.projection = {"col1", "col2", "col6"};
    auto result_set = reader->Scan(db_name, name, "key1", 0, 0, option, &status);
    ASSERT_TRUE(status.IsOK());
    result_set->Next();
    std::string val;
    result_set->GetString(0, &val);
    ASSERT_EQ("key1", val);
    int64_t val1 = 0;
    result_set->GetInt64(1, &val1);
    ASSERT_EQ(1, val1);
    int32_t val2 = 0;
    result_set->GetInt32(2, &val2);
    ASSERT_EQ(10, val2);
    ProcessSQLs(sr, {absl::StrCat("drop table ", name, ";"), absl::StrCat("drop database ", db_name, ";")});
}

TEST_P(DBSDKTest, Desc) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    hybridse::sdk::Status status;
    if (cs->IsClusterMode()) {
        sr->ExecuteSQL("SET @@execute_mode='online';", &status);
        ASSERT_TRUE(status.IsOK()) << "error msg: " + status.msg;
    }
    std::string db = "db" + GenRand();
    sr->ExecuteSQL("create database " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
    sr->ExecuteSQL("use " + db + ";", &status);
    ASSERT_TRUE(status.IsOK());
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date) options(storage_mode='Memory');";
    sr->ExecuteSQL(create_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    std::string desc_sql = "desc trans;";
    auto rs = sr->ExecuteSQL(desc_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(3, rs->Size());

    std::string expect_schema =
        " --- ------- ----------- ------ --------- \n"
        "  #   Field   Type        Null   Default  \n"
        " --- ------- ----------- ------ --------- \n"
        "  1   c1      Varchar     YES             \n"
        "  2   c3      Int         YES             \n"
        "  3   c4      BigInt      YES             \n"
        "  4   c5      Float       YES             \n"
        "  5   c6      Double      YES             \n"
        "  6   c7      Timestamp   YES             \n"
        "  7   c8      Date        YES             \n"
        " --- ------- ----------- ------ --------- \n";

    std::string expect_options =
        " --------------- -------------- \n"
        "  compress_type   storage_mode  \n"
        " --------------- -------------- \n"
        "  NoCompress      Memory        \n"
        " --------------- -------------- \n\n";

    // index name is dynamically assigned. do not check here
    std::vector<std::string> expect = {expect_schema, "", expect_options};
    int count = 0;
    while (rs->Next()) {
        std::string val;
        rs->GetString(0, &val);
        if (!expect[count].empty()) {
            EXPECT_EQ(expect[count], val);
        }
        count++;
    }
    rs = sr->ExecuteSQL(absl::StrCat("desc ", db, ".trans;"), &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(3, rs->Size());
    sr->ExecuteSQL("drop table trans;", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL("drop database " + db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
}

TEST_F(SqlCmdTest, InsertWithDB) {
    sr = standalone_cli.sr;
    ProcessSQLs(
        sr, {"create database test1;", "create database test2;", "use test1;",
             "create table trans (c1 string, c2 int);", "use test2;", "insert into test1.trans values ('aaa', 123);"});

    auto cur_cs = new ::openmldb::sdk::StandAloneSDK(FLAGS_host, FLAGS_port);
    cur_cs->Init();
    auto cur_sr = std::make_unique<::openmldb::sdk::SQLClusterRouter>(cur_cs);
    cur_sr->Init();
    ProcessSQLs(cur_sr.get(), {"insert into test1.trans values ('bbb', 123);"});
    ProcessSQLs(sr, {"drop table test1.trans;", "drop database test1;", "drop database test2;"});
}

TEST_P(DBSDKTest, LoadDataMultipleFiles) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("SET @@execute_mode='online';");
    HandleSQL("create database test1;");
    HandleSQL("use test1;");
    std::string create_sql = "create table trans (c1 string, c2 int);";
    HandleSQL(create_sql);
    int file_num = 2;
    std::filesystem::path tmp_path = std::filesystem::temp_directory_path() / "load_test";
    ASSERT_TRUE(base::MkdirRecur(tmp_path.string()));
    absl::Cleanup clean = [&tmp_path]() { std::filesystem::remove_all(tmp_path); };
    std::vector<std::string> data;

    for (int j = 0; j < file_num; j++) {
        std::string file_name = tmp_path / absl::StrCat("myfile-", j, ".csv");
        std::ofstream ofile;
        ofile.open(file_name);
        ofile << "c1,c2" << std::endl;
        for (int i = 0; i < 10; i++) {
            std::string row = absl::StrCat("aa-", j, ",", i);
            data.push_back(row);
            ofile << row << std::endl;
        }
        ofile.close();
    }
    std::string load_sql = "LOAD DATA INFILE 'file://" + (tmp_path / "myfile*").string() +
                           "' INTO TABLE trans options(load_mode='local', thread=10);";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(load_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(status.msg, "Load 20 rows");
    auto result = sr->ExecuteSQL("select * from trans;", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(20, result->Size());
    while (result->Next()) {
        std::string col1 = result->GetStringUnsafe(0);
        int col2 = result->GetInt32Unsafe(1);
        ASSERT_EQ(col1.substr(0, 3), "aa-");
        ASSERT_TRUE(col2 >= 0 && col2 < 10);
    }
    HandleSQL("drop table trans;");
    HandleSQL("drop database test1;");
}

TEST_P(DBSDKTest, LoadData) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("SET @@execute_mode='online';");
    HandleSQL("create database test1;");
    HandleSQL("use test1;");
    std::string create_sql = "create table trans (c1 string, c2 int);";
    HandleSQL(create_sql);
    std::string file_name = "./myfile.csv";
    std::ofstream ofile;
    ofile.open(file_name);
    ofile << "c1,c2" << std::endl;
    for (int i = 0; i < 10; i++) {
        ofile << "aa" << i << "," << i << std::endl;
    }
    ofile.close();
    // deep_copy is ignored but can still pass
    std::string load_sql = "LOAD DATA INFILE '" + file_name +
                           "' INTO TABLE trans options(deep_copy=true, mode='append', load_mode='local', thread=60);";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(load_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(status.msg, "Load 10 rows");
    auto result = sr->ExecuteSQL("select * from trans;", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(10, result->Size());
    HandleSQL("drop table trans;");
    HandleSQL("drop database test1;");
    unlink(file_name.c_str());
}

TEST_P(DBSDKTest, LoadDataError) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("SET @@execute_mode='online';");
    HandleSQL("create database test1;");
    HandleSQL("use test1;");
    std::string create_sql = "create table trans (c1 string, c2 int);";
    HandleSQL(create_sql);
    std::string file_name = "./myfile.csv";
    std::ofstream ofile;
    ofile.open(file_name);
    ofile << "c1,c2" << std::endl;
    for (int i = 0; i < 10; i++) {
        ofile << "aa" << i << "," << i << std::endl;
    }
    ofile.close();

    hybridse::sdk::Status status;
    std::string load_sql =
        "LOAD DATA INFILE 'not_exist.csv' INTO TABLE trans options(mode='append', load_mode='local', thread=60);";
    sr->ExecuteSQL(load_sql, &status);
    ASSERT_FALSE(status.IsOK()) << status.msg;
    ASSERT_EQ(status.msg, "file not exist");

    load_sql =
        "LOAD DATA INFILE 'not_exist.csv' INTO TABLE trans options(mode='overwrite', load_mode='local', thread=60);";
    sr->ExecuteSQL(load_sql, &status);
    ASSERT_FALSE(status.IsOK()) << status.msg;
    ASSERT_EQ(status.msg, "INVALID_ARGUMENT: local load mode must be append\n");

    load_sql =
        "LOAD DATA INFILE 'not_exist.csv' INTO TABLE trans options(format='parquet', load_mode='local', thread=60);";
    sr->ExecuteSQL(load_sql, &status);
    ASSERT_FALSE(status.IsOK()) << status.msg;
    ASSERT_EQ(status.msg, "local data load only supports 'csv' format");

    load_sql = "LOAD DATA INFILE 'not_exist.csv' INTO TABLE trans options(load_mode='local', thread=0);";
    sr->ExecuteSQL(load_sql, &status);
    ASSERT_FALSE(status.IsOK()) << status.msg;
    ASSERT_EQ(status.msg, "ERROR: parse option thread failed");

    HandleSQL("SET @@execute_mode='offline';");
    load_sql =
        "LOAD DATA INFILE '" + file_name + "' INTO TABLE trans options(mode='append', load_mode='local', thread=60);";
    sr->ExecuteSQL(load_sql, &status);
    if (cs->IsClusterMode()) {
        ASSERT_FALSE(status.IsOK()) << status.msg;
        ASSERT_EQ(status.msg, "local load only supports loading data to online storage");

        load_sql = "LOAD DATA INFILE 'not_exist.csv' INTO TABLE trans options(format='parquet', load_mode='cluster');";
        sr->ExecuteSQL(load_sql, &status);
        ASSERT_FALSE(status.IsOK()) << status.msg;
        ASSERT_TRUE(status.msg.find("Fail to get TaskManager client") != std::string::npos);
    } else {
        ASSERT_TRUE(status.IsOK()) << status.msg;
    }

    HandleSQL("SET @@execute_mode='online';");
    auto result = sr->ExecuteSQL("select * from trans;", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    if (cs->IsClusterMode()) {
        ASSERT_EQ(0, result->Size());
    } else {
        ASSERT_EQ(10, result->Size());
    }
    HandleSQL("drop table trans;");
    HandleSQL("drop database test1;");
    unlink(file_name.c_str());
}

TEST_P(DBSDKTest, LoadDataMultipleThread) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("SET @@execute_mode='online';");
    HandleSQL("create database test1;");
    HandleSQL("use test1;");
    std::string create_sql = "create table trans (c1 string, c2 int);";
    HandleSQL(create_sql);
    int file_num = 10;
    int rows_per_file = 100;
    std::filesystem::path tmp_path = std::filesystem::temp_directory_path() / "load_test_thread";
    ASSERT_TRUE(base::MkdirRecur(tmp_path.string()));
    absl::Cleanup clean = [&tmp_path]() { std::filesystem::remove_all(tmp_path); };
    std::vector<std::string> data;

    for (int j = 0; j < file_num; j++) {
        std::string file_name = tmp_path / absl::StrCat("myfile-", j, ".csv");
        std::ofstream ofile;
        ofile.open(file_name);
        ofile << "c1,c2" << std::endl;
        for (int i = 0; i < rows_per_file; i++) {
            std::string row = absl::StrCat("aa-", j, ",", i);
            data.push_back(row);
            ofile << row << std::endl;
        }
        ofile.close();
    }

    int num_thread = 20;
    hybridse::sdk::Status status;
    for (int i = 0; i < num_thread; i++) {
        std::string load_sql = absl::StrCat("LOAD DATA INFILE '", (tmp_path / "myfile*").string(),
                                            "' INTO TABLE trans options(load_mode='local', thread=", num_thread, ");");
        sr->ExecuteSQL(load_sql, &status);
        ASSERT_TRUE(status.IsOK()) << status.msg;
        ASSERT_EQ(status.msg, absl::StrCat("Load ", file_num * rows_per_file, " rows"));
    }
    auto result = sr->ExecuteSQL("select * from trans;", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(rows_per_file * file_num * num_thread, result->Size());
    std::unordered_map<std::string, std::unordered_set<int>> stats;
    while (result->Next()) {
        std::string col1 = result->GetStringUnsafe(0);
        int col2 = result->GetInt32Unsafe(1);
        stats[col1].insert(col2);
        ASSERT_EQ(col1.substr(0, 3), "aa-");
        ASSERT_TRUE(col2 >= 0 && col2 < rows_per_file);
    }
    ASSERT_EQ(stats.size(), file_num);
    for (int j = 0; j < file_num; j++) {
        const auto& stat = stats[absl::StrCat("aa-", j)];
        ASSERT_EQ(stat.size(), rows_per_file);
    }
    HandleSQL("drop table trans;");
    HandleSQL("drop database test1;");
}

TEST_P(DBSDKTest, Deploy) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("set @@execute_mode = 'online';");
    HandleSQL("create database test1;");
    HandleSQL("use test1;");
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, `index` bigint, index(key=c3, ts=c7, abs_ttl=0, ttl_type=absolute));";
    HandleSQL(create_sql);
    if (!cs->IsClusterMode()) {
        HandleSQL("insert into trans values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\", 113);");
    }

    std::string deploy_sql =
        "deploy demo SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";

    hybridse::sdk::Status status;

    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    std::string deploy_sql1 =
        "deploy demo1 SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 4 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(deploy_sql1, &status);
    ASSERT_TRUE(status.IsOK());
    std::string msg;
    ASSERT_FALSE(cs->GetNsClient()->DropTable("test1", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test1", "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test1", "demo1", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test1", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test1", msg));

    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_FALSE(status.IsOK());
}

TEST_P(DBSDKTest, DeployWithSameIndex) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("set @@execute_mode = 'online';");
    auto db = "db" + GenRand();
    HandleSQL("create database " + db);
    HandleSQL("use " + db);
    // ensure no table and deployment in db
    ASSERT_TRUE(EmptyDB(cs->GetNsClient(), db));
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c1, ts=c7, ttl=1, ttl_type=latest));";

    HandleSQL(create_sql);
    if (!cs->IsClusterMode()) {
        HandleSQL("insert into trans values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\");");
    }

    // origin index
    std::string msg;
    auto ns_client = cs->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
    ASSERT_TRUE(ns_client->ShowTable("trans", db, false, tables, msg));
    ::openmldb::nameserver::TableInfo table = tables[0];

    ASSERT_EQ(table.column_key_size(), 1);
    ::openmldb::common::ColumnKey column_key = table.column_key(0);
    ASSERT_EQ(column_key.col_name_size(), 1);
    ASSERT_EQ(column_key.col_name(0), "c1");
    ASSERT_EQ(column_key.ts_name(), "c7");
    ASSERT_TRUE(column_key.has_ttl());
    ASSERT_EQ(column_key.ttl().ttl_type(), ::openmldb::type::TTLType::kLatestTime);
    ASSERT_EQ(column_key.ttl().lat_ttl(), 1);

    std::string deploy_sql =
        "deploy demo SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans "
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    // new index, update ttl
    tables.clear();
    ASSERT_TRUE(ns_client->ShowTable("trans", db, false, tables, msg));
    table = tables[0];

    ASSERT_EQ(table.column_key_size(), 1);
    column_key = table.column_key(0);
    ASSERT_EQ(column_key.col_name_size(), 1);
    ASSERT_EQ(column_key.col_name(0), "c1");
    ASSERT_EQ(column_key.ts_name(), "c7");
    ASSERT_TRUE(column_key.has_ttl());
    ASSERT_EQ(column_key.ttl().ttl_type(), ::openmldb::type::TTLType::kLatestTime);
    ASSERT_EQ(column_key.ttl().lat_ttl(), 2);

    // type mismatch case, still ok
    create_sql =
        "create table trans1 (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c1, ts=c7, ttl=1m, ttl_type=absolute));";
    HandleSQL(create_sql);
    if (!cs->IsClusterMode()) {
        HandleSQL("insert into trans1 values ('aaa', 11, 22, 1.2, 1.3, 1635247427000, \"2021-05-20\");");
    }
    deploy_sql =
        "deploy demo1 SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans1 "
        " WINDOW w1 AS (PARTITION BY trans1.c1 ORDER BY trans1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    ASSERT_FALSE(cs->GetNsClient()->DropTable(db, "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(db, "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(db, "demo1", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable(db, "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable(db, "trans1", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase(db, msg));
}

TEST_P(DBSDKTest, DeployCol) {
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
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test2", msg));
}

TEST_P(DBSDKTest, DeploySkipIndexCheck) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    std::string ddl1 =
        "create table if not exists t1 (col1 string, col2 string, col3 bigint, "
        "index(key=col1, ts=col3, TTL_TYPE=absolute));";
    std::string ddl2 =
        "create table if not exists t2 (col1 string, col2 string, col3 bigint, "
        "index(key=col1, ts=col3, TTL_TYPE=absolute));";
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online';",
                        "create database test2;",
                        "use test2;",
                        ddl1,
                        ddl2,
                    });
    std::string deploy_sql = "deploy demo SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;";
    hybridse::sdk::Status status;
    std::string msg;
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg)) << msg;
    deploy_sql =
        "deploy demo OPTIONS (skip_index_check=\"false\") "
        "SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg)) << msg;
    deploy_sql =
        "deploy demo OPTIONS (skip_index_check=\"false\") "
        "SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg)) << msg;
    // skip index check won't update the existing index of table TODO(hw): check index?
    deploy_sql =
        "deploy demo OPTIONS (skip_index_check=\"true\") "
        "SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg)) << msg;
    deploy_sql =
        "deploy demo OPTIONS (SKIP_INDEX_CHECK=\"TRUE\") "
        "SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "t1", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "t2", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test2", msg));
}

TEST_P(DBSDKTest, DeployWithData) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    std::string ddl1 =
        "create table if not exists t1 (col1 string, col2 string, col3 bigint, col4 int, "
        "index(key=col1, ts=col3, TTL_TYPE=absolute)) options (partitionnum=2, replicanum=1);";
    std::string ddl2 =
        "create table if not exists t2 (col1 string, col2 string, col3 bigint, "
        "index(key=col1, ts=col3, TTL_TYPE=absolute)) options (partitionnum=2, replicanum=1);";
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online';",
                        "create database test2;",
                        "use test2;",
                        ddl1,
                        ddl2,
                    });
    hybridse::sdk::Status status;
    for (int i = 0; i < 100; i++) {
        std::string key1 = absl::StrCat("col1", i);
        std::string key2 = absl::StrCat("col2", i);
        sr->ExecuteSQL(absl::StrCat("insert into t1 values ('", key1, "', '", key2, "', 1635247427000, 5);"), &status);
        sr->ExecuteSQL(absl::StrCat("insert into t2 values ('", key1, "', '", key2, "', 1635247427000);"), &status);
    }
    sleep(2);
    std::string deploy_sql =
        "deploy demo SELECT t1.col1, t2.col2, sum(col4) OVER w1 as w1_col4_sum FROM t1 "
        "LAST JOIN t2 ORDER BY t2.col3 ON t1.col2 = t2.col2 "
        "WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK());
    std::string msg;
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "t1", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "t2", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test2", msg));
}

TEST_P(DBSDKTest, DeployWithBias) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    std::string db = "test_bias";
    std::string ddl1 =
        "create table if not exists t1 (col1 string, col2 string, col3 bigint, col4 int, "
        "index(key=col1, ts=col3, TTL_TYPE=absolute)) options (partitionnum=2, replicanum=1);";
    std::string ddl2 =
        "create table if not exists t2 (col1 string, col2 string, col3 bigint, "
        "index(key=col1, ts=col3, TTL_TYPE=absolute)) options (partitionnum=2, replicanum=1);";
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online';",
                        "create database " + db ,
                        "use " + db,
                        ddl1,
                        ddl2,
                    });
    hybridse::sdk::Status status;
    for (int i = 0; i < 100; i++) {
        std::string key1 = absl::StrCat("col1", i);
        std::string key2 = absl::StrCat("col2", i);
        sr->ExecuteSQL(absl::StrCat("insert into t1 values ('", key1, "', '", key2, "', 1635247427000, 5);"), &status);
        sr->ExecuteSQL(absl::StrCat("insert into t2 values ('", key1, "', '", key2, "', 1635247427000);"), &status);
    }
    sleep(2);

    std::string rows_deployment_part =
        "SELECT t1.col1, t2.col2, sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 "
        "LAST JOIN t2 ORDER BY t2.col3 ON t1.col2 = t2.col2 "
        "WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    auto range_deployment_part =
        "SELECT t1.col1, t2.col2, sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 "
        "LAST JOIN t2 ORDER BY t2.col3 ON t1.col2 = t2.col2 "
        "WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col3 ROWS_RANGE BETWEEN 2 PRECEDING AND CURRENT ROW);";

    // test rows bias
    auto i = 0;
    auto rows_test = [&](std::string option, bool expect = true) {
        sr->ExecuteSQL(absl::StrCat("DEPLOY d", i++, " OPTIONS(", option, ") ", rows_deployment_part), &status);
        if (expect)
            EXPECT_TRUE(status.IsOK());
        else
            EXPECT_FALSE(status.IsOK());
        // check table index
        auto info = sr->GetTableInfo(db, "t1");
        return info.column_key().Get(1);
    };

    auto index_res = rows_test("rows_bias=0");
    ASSERT_EQ(index_res.ttl().lat_ttl(), 2);
    // range bias won't work cuz no new abs index in deploy
    index_res = rows_test("rows_bias=20, range_bias='inf'");
    ASSERT_EQ(index_res.ttl().lat_ttl(), 22);
    rows_test("rows_bias=20s", false);
    i--;  // last one is failed, reset the num

    // test range bias
    auto range_test = [&](std::string option, bool expect = true) {
        sr->ExecuteSQL(absl::StrCat("DEPLOY d", i++, " OPTIONS(", option, ") ", range_deployment_part), &status);
        if (expect)
            EXPECT_TRUE(status.IsOK());
        else
            EXPECT_FALSE(status.IsOK());
        // check table index
        auto info = sr->GetTableInfo(db, "t1");
        return info.column_key().Get(1);
    };
    index_res = range_test("range_bias=0");
    ASSERT_EQ(index_res.ttl().abs_ttl(), 1);
    index_res = range_test("range_bias=20");
    ASSERT_EQ(index_res.ttl().abs_ttl(), 2);
    // rows bias won't work cuz no **new** lat index in deploy, just new abs index + the old index
    index_res = range_test("range_bias=1d, rows_bias=100");
    ASSERT_EQ(index_res.ttl().abs_ttl(), 1441);

    // set inf in the end, if not, all bias + inf = inf
    index_res = rows_test("range_bias='inf'");
    ASSERT_EQ(index_res.ttl().abs_ttl(), 0);
    index_res = rows_test("rows_bias='inf'");
    ASSERT_EQ(index_res.ttl().lat_ttl(), 0);

    // sp in tablet may be stored a bit late, wait
    sleep(3);
    std::string msg;
    for (int j = 0; j < i; j++) {
        ASSERT_TRUE(cs->GetNsClient()->DropProcedure(db, "d" + std::to_string(j), msg));
    }
    ASSERT_TRUE(cs->GetNsClient()->DropTable(db, "t1", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable(db, "t2", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase(db, msg));
}

TEST_P(DBSDKTest, Truncate) {
    auto cli = GetParam();
    sr = cli->sr;
    std::string db_name = "test2";
    std::string table_name = "test1";
    std::string ddl = "create table test1 (c1 string, c2 int, c3 bigint, INDEX(KEY=c1, ts=c3));";
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online'",
                        absl::StrCat("create database ", db_name, ";"),
                        absl::StrCat("use ", db_name, ";"),
                        ddl,
                    });
    hybridse::sdk::Status status;
    sr->ExecuteSQL(absl::StrCat("truncate table ", table_name, ";"), &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    auto res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 0);
    for (int i = 0; i < 10; i++) {
        std::string key = absl::StrCat("key", i);
        for (int j = 0; j < 10; j++) {
            uint64_t ts = 1000 + j;
            sr->ExecuteSQL(absl::StrCat("insert into ", table_name, " values ('", key, "', 11, ", ts, ");"), &status);
        }
    }

    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 100);
    sr->ExecuteSQL(absl::StrCat("truncate table ", table_name, ";"), &status);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 0);
    sr->ExecuteSQL(absl::StrCat("insert into ", table_name, " values ('aa', 11, 100);"), &status);
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 1);
    ProcessSQLs(sr, {
                        absl::StrCat("use ", db_name, ";"),
                        absl::StrCat("drop table ", table_name),
                        absl::StrCat("drop database ", db_name),
                    });
}

TEST_P(DBSDKTest, DeletetRange) {
    auto cli = GetParam();
    sr = cli->sr;
    std::string db_name = "test2";
    std::string table_name = "test1";
    std::string ddl = "create table test1 (c1 string, c2 int, c3 bigint, INDEX(KEY=c1, ts=c3));";
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online'",
                        absl::StrCat("create database ", db_name, ";"),
                        absl::StrCat("use ", db_name, ";"),
                        ddl,
                    });
    hybridse::sdk::Status status;
    for (int i = 0; i < 10; i++) {
        std::string key = absl::StrCat("key", i);
        for (int j = 0; j < 10; j++) {
            uint64_t ts = 1000 + j;
            sr->ExecuteSQL(absl::StrCat("insert into ", table_name, " values ('", key, "', 11, ", ts, ");"), &status);
        }
    }

    auto res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 100);
    ProcessSQLs(sr, {absl::StrCat("delete from ", table_name, " where c1 = 'key2';")});
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 90);
    ProcessSQLs(sr, {absl::StrCat("delete from ", table_name, " where c1 = 'key3' and c3 = 1001;")});
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 89);
    ProcessSQLs(sr, {absl::StrCat("delete from ", table_name, " where c1 = 'key4' and c3 > 1005;")});
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 85);
    ProcessSQLs(sr, {absl::StrCat("delete from ", table_name, " where c1 = 'key4' and c3 > 1002 and c3 < 1006;")});
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 82);
    ProcessSQLs(sr, {absl::StrCat("delete from ", table_name, " where c3 > 1007;")});
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 66);
    ProcessSQLs(sr, {
                        absl::StrCat("use ", db_name, ";"),
                        absl::StrCat("drop table ", table_name),
                        absl::StrCat("drop database ", db_name),
                    });
}

TEST_P(DBSDKTest, DeletetSameColIndex) {
    auto cli = GetParam();
    sr = cli->sr;
    std::string db_name = "test2";
    std::string table_name = "test1";
    std::string ddl =
        "create table test1 (c1 string, c2 int, c3 bigint, c4 bigint, "
        "INDEX(KEY=c1, ts=c3), INDEX(KEY=c1, ts=c4));";
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online'",
                        absl::StrCat("create database ", db_name, ";"),
                        absl::StrCat("use ", db_name, ";"),
                        ddl,
                    });
    hybridse::sdk::Status status;
    for (int i = 0; i < 10; i++) {
        std::string key = absl::StrCat("key", i);
        for (int j = 0; j < 10; j++) {
            uint64_t ts = 1000 + j;
            sr->ExecuteSQL(absl::StrCat("insert into ", table_name, " values ('", key, "', 11, ", ts, ",", ts, ");"),
                           &status);
        }
    }

    auto res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 100);
    ProcessSQLs(sr, {absl::StrCat("delete from ", table_name, " where c1 = 'key2';")});
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 90);
    ProcessSQLs(sr, {
                        absl::StrCat("drop table ", table_name),
                        absl::StrCat("drop database ", db_name),
                    });
}

TEST_P(DBSDKTest, SQLDeletetRow) {
    auto cli = GetParam();
    sr = cli->sr;
    std::string db_name = "test2";
    std::string table_name = "test1";
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online'",
                        absl::StrCat("create database ", db_name, ";"),
                        absl::StrCat("use ", db_name, ";"),
                        absl::StrCat("create table ", table_name, "(c1 string, c2 int, c3 bigint);"),
                        absl::StrCat("insert into ", table_name, " values ('key1', 11, 22);"),
                        absl::StrCat("insert into ", table_name, " values ('key2', 11, 22);"),
                        absl::StrCat("insert into ", table_name, " values ('key3', 11, 22);"),
                        absl::StrCat("insert into ", table_name, " values ('key4', 11, 22);"),
                    });

    hybridse::sdk::Status status;
    auto res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 4);
    ProcessSQLs(sr, {absl::StrCat("delete from ", table_name, " where c1 = 'key2';")});
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 3);
    std::string delete_sql = "delete from " + table_name + " where c1 = ?;";
    auto insert_row = sr->GetDeleteRow(db_name, delete_sql, &status);
    ASSERT_TRUE(status.IsOK());
    insert_row->SetString(1, "key3");
    ASSERT_TRUE(insert_row->Build());
    sr->ExecuteDelete(insert_row, &status);
    ASSERT_TRUE(status.IsOK());
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 2);
    insert_row->Reset();
    insert_row->SetString(1, "key100");
    ASSERT_TRUE(insert_row->Build());
    sr->ExecuteDelete(insert_row, &status);
    ASSERT_TRUE(status.IsOK());
    res = sr->ExecuteSQL(absl::StrCat("select * from ", table_name, ";"), &status);
    ASSERT_EQ(res->Size(), 2);

    ProcessSQLs(sr, {
                        absl::StrCat("use ", db_name, ";"),
                        absl::StrCat("drop table ", table_name),
                        absl::StrCat("drop database ", db_name),
                    });
}

TEST_P(DBSDKTest, DeployLongWindows) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("SET @@execute_mode='online';");
    HandleSQL("create database test2;");
    HandleSQL("use test2;");
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c1, ts=c4, ttl=0, ttl_type=latest));";
    HandleSQL(create_sql);

    std::string deploy_sql =
        "deploy demo1 OPTIONS(long_windows='w1:1d,w2') SELECT c1, sum(c4) OVER w1 as w1_c4_sum,"
        " sum(c4) OVER w1 as w1_c4_sum2, max(c5) over w2 as w2_max_c5 FROM trans"
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),"
        " w2 AS (PARTITION BY trans.c1 ORDER BY trans.c4 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    std::string result_sql = "select * from __INTERNAL_DB.PRE_AGG_META_INFO;";
    auto rs = sr->ExecuteSQL("", result_sql, &status);
    ASSERT_EQ(2, rs->Size());

    // deploy another deployment with same long window meta but different bucket
    // it will not create a new aggregator/pre-aggr table, but re-use the existing one
    deploy_sql =
        "deploy demo2 OPTIONS(long_windows='w1:2d,w2') SELECT c1, sum(c4) OVER w1 as w1_c4_sum,"
        " sum(c4) OVER w1 as w1_c4_sum2, max(c5) over w2 as w2_max_c5 FROM trans"
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),"
        " w2 AS (PARTITION BY trans.c1 ORDER BY trans.c4 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    rs = sr->ExecuteSQL("", result_sql, &status);
    ASSERT_EQ(2, rs->Size());

    // deploy another deployment with different long window meta will create a new aggregator/pre-agg table
    deploy_sql =
        "deploy demo3 OPTIONS(long_windows='w1:2d') SELECT c1, count_where(c4, c3=1) over w1,"
        " count_where(c4, c3=2) over w1 FROM trans"
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    rs = sr->ExecuteSQL("", result_sql, &status);
    ASSERT_EQ(3, rs->Size());

    std::string msg;
    auto ok = sr->ExecuteDDL(openmldb::nameserver::PRE_AGG_DB, "drop table pre_test2_demo1_w1_sum_c4;", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(openmldb::nameserver::PRE_AGG_DB, "drop table pre_test2_demo1_w2_max_c5;", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(openmldb::nameserver::PRE_AGG_DB, "drop table pre_test2_demo3_w1_count_where_c4_c3;", &status);
    ASSERT_TRUE(ok);
    ASSERT_FALSE(cs->GetNsClient()->DropTable("test2", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo1", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo2", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo3", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "trans", msg));
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test2", msg));
}

void CreateDBTableForLongWindow(const std::string& base_db, const std::string& base_table) {
    ::hybridse::sdk::Status status;
    bool ok = sr->CreateDB(base_db, &status);
    ASSERT_TRUE(ok) << status.msg;
    std::string ddl = "create table " + base_table +
                      "(col1 string, col2 string, col3 timestamp, i64_col bigint, i16_col smallint, i32_col int, f_col "
                      "float, d_col double, t_col timestamp, s_col string, date_col date, filter int, "
                      "index(key=(col1,col2), ts=col3, abs_ttl=0, ttl_type=absolute)) "
                      "options(partitionnum=8);";
    ok = sr->ExecuteDDL(base_db, ddl, &status);
    ASSERT_TRUE(ok) << status.msg;
    ASSERT_TRUE(sr->RefreshCatalog());

    auto ns_client = cs->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    ASSERT_TRUE(ns_client->ShowTable(base_table, base_db, false, tables, msg));
    ASSERT_EQ(tables.size(), 1) << msg;
}

// -----------------------------------------------------------------------------------
// col1 col2 col3 i64_col i16_col i32_col f_col d_col t_col  s_col  date_col   filter
// str1 str2  i     i       i       i       i     i     i      i    1900-01-i  i % 2
//
// where i in [1 .. 11]
// -----------------------------------------------------------------------------------
void PrepareDataForLongWindow(const std::string& base_db, const std::string& base_table) {
    ::hybridse::sdk::Status status;
    for (int i = 1; i <= 11; i++) {
        std::string val = std::to_string(i);
        std::string filter_val = std::to_string(i % 2);
        std::string date;
        if (i < 10) {
            date = absl::StrCat("1900-01-0", std::to_string(i));
        } else {
            date = absl::StrCat("1900-01-", std::to_string(i));
        }
        std::string insert =
            absl::StrCat("insert into ", base_table, " values('str1', 'str2', ", val, ", ", val, ", ", val, ", ", val,
                         ", ", val, ", ", val, ", ", val, ", '", val, "', '", date, "', ", filter_val, ");");
        bool ok = sr->ExecuteInsert(base_db, insert, &status);
        ASSERT_TRUE(ok) << status.msg;
    }
}

void PrepareRequestRowForLongWindow(const std::string& base_db, const std::string& sp_name,
                                    std::shared_ptr<sdk::SQLRequestRow>& req) {  // NOLINT
    ::hybridse::sdk::Status status;
    req = sr->GetRequestRowByProcedure(base_db, sp_name, &status);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(req->Init(strlen("str1") + strlen("str2") + strlen("11")));
    ASSERT_TRUE(req->AppendString("str1"));
    ASSERT_TRUE(req->AppendString("str2"));
    ASSERT_TRUE(req->AppendTimestamp(11));
    ASSERT_TRUE(req->AppendInt64(11));
    ASSERT_TRUE(req->AppendInt16(11));
    ASSERT_TRUE(req->AppendInt32(11));
    ASSERT_TRUE(req->AppendFloat(11));
    ASSERT_TRUE(req->AppendDouble(11));
    ASSERT_TRUE(req->AppendTimestamp(11));
    ASSERT_TRUE(req->AppendString("11"));
    ASSERT_TRUE(req->AppendDate(11));
    // filter = null
    req->AppendNULL();
    ASSERT_TRUE(req->Build());
}

// TODO(ace): create instance of DeployLongWindowEnv with template
class DeployLongWindowEnv {
 public:
    explicit DeployLongWindowEnv(sdk::SQLClusterRouter* sr) : sr_(sr) {}

    virtual ~DeployLongWindowEnv() {}

    void SetUp() {
        db_ = absl::StrCat("db_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));
        table_ = absl::StrCat("tb_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));
        dp_ = absl::StrCat("dp_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));

        PrepareSchema();

        ASSERT_TRUE(sr_->RefreshCatalog());

        Deploy();

        PrepareData();
    }

    void TearDown() {
        TearDownPreAggTables();
        ProcessSQLs(sr_, {
                             absl::StrCat("drop table ", table_),
                             absl::StrCat("drop database ", db_),
                         });
    }

    void CallDeploy(std::shared_ptr<hybridse::sdk::ResultSet>* rs) {
        hybridse::sdk::Status status;
        std::shared_ptr<sdk::SQLRequestRow> rr = std::make_shared<sdk::SQLRequestRow>();
        GetRequestRow(&rr, dp_);
        auto res = sr_->CallProcedure(db_, dp_, rr, &status);
        ASSERT_TRUE(status.IsOK()) << status.msg << "\n" << status.trace;
        *rs = std::move(res);
    }

 private:
    virtual void PrepareSchema() {
        ProcessSQLs(
            sr_, {"SET @@execute_mode='online';", absl::StrCat("create database ", db_), absl::StrCat("use ", db_),
                  absl::StrCat(
                      "create table ", table_,
                      "(col1 string, col2 string, col3 timestamp, i64_col bigint, i16_col smallint, i32_col int, f_col "
                      "float, d_col double, t_col timestamp, s_col string, date_col date, filter int, "
                      "index(key=(col1,col2), ts=col3, abs_ttl=0, ttl_type=absolute)) "
                      "options(partitionnum=8);")});
    }

    virtual void PrepareData() {
        // prepare data
        // -----------------------------------------------------------------------------------
        // col1 col2  col3       i64_col i16_col i32_col f_col d_col t_col  s_col  date_col   filter
        // str1 str2  i * 1000     i       i       i       i     i     i      i    1900-01-i  i % 2
        //
        // where i in [1 .. 11]
        // -----------------------------------------------------------------------------------
        for (int i = 1; i <= 11; i++) {
            std::string val = std::to_string(i);
            std::string filter_val = std::to_string(i % 2);
            std::string date;
            if (i < 10) {
                date = absl::StrCat("1900-01-0", std::to_string(i));
            } else {
                date = absl::StrCat("1900-01-", std::to_string(i));
            }
            std::string insert =
                absl::StrCat("insert into ", table_, " values('str1', 'str2', ", i * 1000, ", ", val, ", ", val, ", ",
                             val, ", ", val, ", ", val, ", ", val, ", '", val, "', '", date, "', ", filter_val, ");");
            ::hybridse::sdk::Status s;
            bool ok = sr_->ExecuteInsert(db_, insert, &s);
            ASSERT_TRUE(ok && s.IsOK()) << s.msg << "\n" << s.trace;
        }
    }

    virtual void Deploy() = 0;

    virtual void TearDownPreAggTables() = 0;

    void GetRequestRow(std::shared_ptr<sdk::SQLRequestRow>* rs, const std::string& name) {  // NOLINT
        ::hybridse::sdk::Status status;
        auto req = sr_->GetRequestRowByProcedure(db_, dp_, &status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(req->Init(strlen("str1") + strlen("str2") + strlen("11")));
        ASSERT_TRUE(req->AppendString("str1"));
        ASSERT_TRUE(req->AppendString("str2"));
        ASSERT_TRUE(req->AppendTimestamp(11000));
        ASSERT_TRUE(req->AppendInt64(11));
        ASSERT_TRUE(req->AppendInt16(11));
        ASSERT_TRUE(req->AppendInt32(11));
        ASSERT_TRUE(req->AppendFloat(11));
        ASSERT_TRUE(req->AppendDouble(11));
        ASSERT_TRUE(req->AppendTimestamp(11));
        ASSERT_TRUE(req->AppendString("11"));
        ASSERT_TRUE(req->AppendDate(11));
        // filter = null
        req->AppendNULL();
        ASSERT_TRUE(req->Build());
        *rs = std::move(req);
    }

 protected:
    sdk::SQLClusterRouter* sr_;
    absl::BitGen gen_;
    std::string db_;
    std::string table_;
    std::string dp_;
};

TEST_P(DBSDKTest, DeployLongWindowsWithDataFail) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    PrepareDataForLongWindow(base_db, base_table);
    sleep(2);

    std::string deploy_sql =
        "deploy test_aggr options(LONG_WINDOWS='w1:2') select col1, col2,"
        " sum(i64_col) over w1 as w1_sum_i64_col,"
        " from " +
        base_table + " WINDOW w1 AS (PARTITION BY " + base_table + ".col1," + base_table +
        ".col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(!status.IsOK());

    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok) << status.msg;
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsEmpty) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        "deploy test_aggr options(LONG_WINDOWS='w1:2') select col1, col2,"
        " sum(i64_col) over w1 as w1_sum_i64_col,"
        " sum(i16_col) over w1 as w1_sum_i16_col,"
        " sum(i32_col) over w1 as w1_sum_i32_col,"
        " sum(f_col) over w1 as w1_sum_f_col,"
        " sum(d_col) over w1 as w1_sum_d_col,"
        " sum(t_col) over w1 as w1_sum_t_col,"
        " sum(col3) over w2 as w2_sum_col3"
        " from " +
        base_table + " WINDOW w1 AS (PARTITION BY " + base_table + ".col1," + base_table +
        ".col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW), "
        " w2 AS (PARTITION BY col1,col2 ORDER BY i64_col"
        " ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i64_col";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(0, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i16_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(0, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i32_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(0, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_f_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(0, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_d_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(0, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_t_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(0, rs->Size());

    int req_num = 2;
    for (int i = 0; i < req_num; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(1, res->Size());
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("str1", res->GetStringUnsafe(0));
        ASSERT_EQ("str2", res->GetStringUnsafe(1));
        int64_t exp = 11;
        ASSERT_EQ(exp, res->GetInt64Unsafe(2));
        ASSERT_EQ(exp, res->GetInt16Unsafe(3));
        ASSERT_EQ(exp, res->GetInt32Unsafe(4));
        ASSERT_EQ(exp, res->GetFloatUnsafe(5));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(6));
        ASSERT_EQ(exp, res->GetTimeUnsafe(7));
        ASSERT_EQ(exp, res->GetInt64Unsafe(8));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i64_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i16_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i32_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_f_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_d_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_t_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsWithExcludeCurrentRow) {
    auto& cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        "deploy test_aggr options(LONG_WINDOWS='w1:2') select col1, col2,"
        " sum(i64_col) over w1 as w1_sum_i64_col,"
        " from " +
        base_table + " WINDOW w1 AS (PARTITION BY " + base_table + ".col1," + base_table +
        ".col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND 0 PRECEDING EXCLUDE CURRENT_ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    PrepareDataForLongWindow(base_db, base_table);
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i64_col";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    int req_num = 2;
    for (int i = 0; i < req_num; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(1, res->Size());
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("str1", res->GetStringUnsafe(0));
        ASSERT_EQ("str2", res->GetStringUnsafe(1));
        int64_t exp = 11 + 10 + 9 + 8 + 7 + 6;
        ASSERT_EQ(exp, res->GetInt64Unsafe(2));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i64_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsExecuteSum) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        "deploy test_aggr options(LONG_WINDOWS='w1:2') select col1, col2,"
        " sum(i64_col) over w1 as w1_sum_i64_col,"
        " sum(i16_col) over w1 as w1_sum_i16_col,"
        " sum(i32_col) over w1 as w1_sum_i32_col,"
        " sum(f_col) over w1 as w1_sum_f_col,"
        " sum(d_col) over w1 as w1_sum_d_col,"
        " sum(t_col) over w1 as w1_sum_t_col,"
        " sum(col3) over w2 as w2_sum_col3"
        " from " +
        base_table + " WINDOW w1 AS (PARTITION BY " + base_table + ".col1," + base_table +
        ".col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW), "
        " w2 AS (PARTITION BY col1,col2 ORDER BY i64_col"
        " ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    PrepareDataForLongWindow(base_db, base_table);
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i64_col";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    for (int i = 5; i >= 1; i--) {
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ("str1|str2", rs->GetStringUnsafe(0));
        ASSERT_EQ(i * 2 - 1, rs->GetInt64Unsafe(1));
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(2));
        ASSERT_EQ(2, rs->GetInt32Unsafe(3));
        std::string aggr_val_str = rs->GetStringUnsafe(4);
        int64_t aggr_val = *reinterpret_cast<int64_t*>(&aggr_val_str[0]);
        ASSERT_EQ(i * 4 - 1, aggr_val);
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(5));
    }

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i16_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i32_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_f_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_d_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_t_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    int req_num = 2;
    for (int i = 0; i < req_num; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(1, res->Size());
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("str1", res->GetStringUnsafe(0));
        ASSERT_EQ("str2", res->GetStringUnsafe(1));
        int64_t exp = 11 + 11 + 19 + 15 + 6;
        ASSERT_EQ(exp, res->GetInt64Unsafe(2));
        ASSERT_EQ(exp, res->GetInt16Unsafe(3));
        ASSERT_EQ(exp, res->GetInt32Unsafe(4));
        ASSERT_EQ(exp, res->GetFloatUnsafe(5));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(6));
        ASSERT_EQ(exp, res->GetTimeUnsafe(7));
        ASSERT_EQ(exp, res->GetInt64Unsafe(8));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i64_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i16_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_i32_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_f_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_d_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_t_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsExecuteAvg) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        "deploy test_aggr options(long_windows='w1:2') select col1, col2,"
        " avg(i64_col) over w1 as w1_avg_i64_col,"
        " avg(i16_col) over w1 as w1_avg_i16_col,"
        " avg(i32_col) over w1 as w1_avg_i32_col,"
        " avg(f_col) over w1 as w1_avg_f_col,"
        " avg(d_col) over w1 as w1_avg_d_col,"
        " avg(i64_col) over w2 as w2_avg_col3"
        " from " +
        base_table +
        " WINDOW w1 AS (PARTITION BY col1,col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW), "
        " w2 AS (PARTITION BY col1,col2 ORDER BY i64_col"
        " ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    PrepareDataForLongWindow(base_db, base_table);
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_i64_col";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    for (int i = 5; i >= 1; i--) {
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ("str1|str2", rs->GetStringUnsafe(0));
        ASSERT_EQ(i * 2 - 1, rs->GetInt64Unsafe(1));
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(2));
        ASSERT_EQ(2, rs->GetInt32Unsafe(3));
        std::string aggr_val_str = rs->GetStringUnsafe(4);
        ASSERT_EQ(16, aggr_val_str.size());
        double aggr_sum = *reinterpret_cast<double*>(&aggr_val_str[0]);
        ASSERT_EQ(i * 4 - 1, aggr_sum);
        int64_t aggr_count = *reinterpret_cast<int64_t*>(&aggr_val_str[sizeof(int64_t)]);
        ASSERT_EQ(2, aggr_count);
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(5));
    }

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_i16_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_i32_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_f_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_d_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    int req_num = 2;
    for (int i = 0; i < req_num; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(1, res->Size());
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("str1", res->GetStringUnsafe(0));
        ASSERT_EQ("str2", res->GetStringUnsafe(1));
        double exp = static_cast<double>(11 + 11 + 19 + 15 + 6) / 7;
        ASSERT_EQ(exp, res->GetDoubleUnsafe(2));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(3));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(4));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(5));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(6));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(7));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_i64_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_i16_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_i32_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_f_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_avg_d_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsExecuteMin) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        "deploy test_aggr options(long_windows='w1:2') select col1, col2,"
        " min(i64_col) over w1 as w1_min_i64_col,"
        " min(i16_col) over w1 as w1_min_i16_col,"
        " min(i32_col) over w1 as w1_min_i32_col,"
        " min(f_col) over w1 as w1_min_f_col,"
        " min(d_col) over w1 as w1_min_d_col,"
        " min(t_col) over w1 as w1_min_t_col,"
        " min(s_col) over w1 as w1_min_s_col,"
        " min(date_col) over w1 as w1_min_date_col,"
        " min(col3) over w2 as w2_min_col3"
        " from " +
        base_table +
        " WINDOW w1 AS (PARTITION BY col1,col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW), "
        " w2 AS (PARTITION BY col1,col2 ORDER BY i64_col"
        " ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    PrepareDataForLongWindow(base_db, base_table);
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_i64_col";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    for (int i = 5; i >= 1; i--) {
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ("str1|str2", rs->GetStringUnsafe(0));
        ASSERT_EQ(i * 2 - 1, rs->GetInt64Unsafe(1));
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(2));
        ASSERT_EQ(2, rs->GetInt32Unsafe(3));
        std::string aggr_val_str = rs->GetStringUnsafe(4);
        int64_t aggr_val = *reinterpret_cast<int64_t*>(&aggr_val_str[0]);
        ASSERT_EQ(i * 2 - 1, aggr_val);
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(5));
    }

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_i16_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_i32_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_f_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_d_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_t_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_s_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_date_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    int req_num = 2;
    for (int i = 0; i < req_num; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(1, res->Size());
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("str1", res->GetStringUnsafe(0));
        ASSERT_EQ("str2", res->GetStringUnsafe(1));
        int64_t exp = 6;
        ASSERT_EQ(exp, res->GetInt64Unsafe(2));
        ASSERT_EQ(exp, res->GetInt16Unsafe(3));
        ASSERT_EQ(exp, res->GetInt32Unsafe(4));
        ASSERT_EQ(exp, res->GetFloatUnsafe(5));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(6));
        ASSERT_EQ(exp, res->GetTimeUnsafe(7));
        ASSERT_EQ("10", res->GetStringUnsafe(8));
        ASSERT_EQ(exp, res->GetDateUnsafe(9));
        ASSERT_EQ(exp, res->GetInt64Unsafe(10));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_i64_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_i16_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_i32_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_f_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_d_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_t_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_s_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_min_date_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsExecuteMax) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        "deploy test_aggr options(long_windows='w1:2') select col1, col2,"
        " max(i64_col) over w1 as w1_max_i64_col,"
        " max(i16_col) over w1 as w1_max_i16_col,"
        " max(i32_col) over w1 as w1_max_i32_col,"
        " max(f_col) over w1 as w1_max_f_col,"
        " max(d_col) over w1 as w1_max_d_col,"
        " max(t_col) over w1 as w1_max_t_col,"
        " max(s_col) over w1 as w1_max_s_col,"
        " max(date_col) over w1 as w1_max_date_col,"
        " max(col3) over w2 as w2_max_col3"
        " from " +
        base_table +
        " WINDOW w1 AS (PARTITION BY col1,col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW), "
        " w2 AS (PARTITION BY col1,col2 ORDER BY i64_col"
        " ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    PrepareDataForLongWindow(base_db, base_table);
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_i64_col";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    for (int i = 5; i >= 1; i--) {
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ("str1|str2", rs->GetStringUnsafe(0));
        ASSERT_EQ(i * 2 - 1, rs->GetInt64Unsafe(1));
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(2));
        ASSERT_EQ(2, rs->GetInt32Unsafe(3));
        std::string aggr_val_str = rs->GetStringUnsafe(4);
        int64_t aggr_val = *reinterpret_cast<int64_t*>(&aggr_val_str[0]);
        ASSERT_EQ(i * 2, aggr_val);
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(5));
    }

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_i16_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_i32_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_f_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_d_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_t_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_s_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_date_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    int req_num = 2;
    for (int i = 0; i < req_num; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(1, res->Size());
        ASSERT_TRUE(res->Next());
        ASSERT_EQ("str1", res->GetStringUnsafe(0));
        ASSERT_EQ("str2", res->GetStringUnsafe(1));
        int64_t exp = 11;
        ASSERT_EQ(exp, res->GetInt64Unsafe(2));
        ASSERT_EQ(exp, res->GetInt16Unsafe(3));
        ASSERT_EQ(exp, res->GetInt32Unsafe(4));
        ASSERT_EQ(exp, res->GetFloatUnsafe(5));
        ASSERT_EQ(exp, res->GetDoubleUnsafe(6));
        ASSERT_EQ(exp, res->GetTimeUnsafe(7));
        ASSERT_EQ("9", res->GetStringUnsafe(8));
        ASSERT_EQ(exp, res->GetDateUnsafe(9));
        ASSERT_EQ(exp, res->GetInt64Unsafe(10));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_i64_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_i16_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_i32_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_f_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_d_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_t_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_s_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_max_date_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsExecuteCount) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        "deploy test_aggr options(long_windows='w1:2') select col1, col2,"
        " count(*) over w1 as w1_count_all,"
        " count(i64_col) over w1 as w1_count_i64_col,"
        " count(i16_col) over w1 as w1_count_i16_col,"
        " count(i32_col) over w1 as w1_count_i32_col,"
        " count(f_col) over w1 as w1_count_f_col,"
        " count(d_col) over w1 as w1_count_d_col,"
        " count(t_col) over w1 as w1_count_t_col,"
        " count(s_col) over w1 as w1_count_s_col,"
        " count(date_col) over w1 as w1_count_date_col,"
        " count(col3) over w2 as w2_count_col3"
        " from " +
        base_table +
        " WINDOW w1 AS (PARTITION BY col1,col2 ORDER BY col3"
        " ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW), "
        " w2 AS (PARTITION BY col1,col2 ORDER BY i64_col"
        " ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);";
    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    PrepareDataForLongWindow(base_db, base_table);
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_i64_col";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    for (int i = 5; i >= 1; i--) {
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ("str1|str2", rs->GetStringUnsafe(0));
        ASSERT_EQ(i * 2 - 1, rs->GetInt64Unsafe(1));
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(2));
        ASSERT_EQ(2, rs->GetInt32Unsafe(3));
        std::string aggr_val_str = rs->GetStringUnsafe(4);
        int64_t aggr_val = *reinterpret_cast<int64_t*>(&aggr_val_str[0]);
        ASSERT_EQ(2, aggr_val);
        ASSERT_EQ(i * 2, rs->GetInt64Unsafe(5));
    }

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_i16_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_i32_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_f_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_d_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_t_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_s_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_date_col";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    int req_num = 2;
    for (int i = 0; i < req_num; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        LOG(WARNING) << "Before CallProcedure";
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        LOG(WARNING) << "After CallProcedure";
        EXPECT_TRUE(status.IsOK());
        EXPECT_EQ(1, res->Size());
        EXPECT_TRUE(res->Next());
        EXPECT_EQ("str1", res->GetStringUnsafe(0));
        EXPECT_EQ("str2", res->GetStringUnsafe(1));
        int64_t exp = 7;
        EXPECT_EQ(exp, res->GetInt64Unsafe(2));
        EXPECT_EQ(exp, res->GetInt64Unsafe(3));
        EXPECT_EQ(exp, res->GetInt64Unsafe(4));
        EXPECT_EQ(exp, res->GetInt64Unsafe(5));
        EXPECT_EQ(exp, res->GetInt64Unsafe(6));
        EXPECT_EQ(exp, res->GetInt64Unsafe(7));
        EXPECT_EQ(exp, res->GetInt64Unsafe(8));
        EXPECT_EQ(exp, res->GetInt64Unsafe(9));
        EXPECT_EQ(exp, res->GetInt64Unsafe(10));
        EXPECT_EQ(exp, res->GetInt64Unsafe(11));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_i64_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_i16_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_i32_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_f_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_d_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_t_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_s_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_date_col";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_P(DBSDKTest, DeployLongWindowsExecuteCountWhere) {
    GTEST_SKIP() << "count_where for rows window un-supported due to pre-agg rows not aligned";

    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ::hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string base_table = "t_lw" + GenRand();
    std::string base_db = "d_lw" + GenRand();
    bool ok;
    std::string msg;
    CreateDBTableForLongWindow(base_db, base_table);

    std::string deploy_sql =
        R"(DEPLOY test_aggr options(long_windows='w1:2')
    SELECT
        col1, col2,
        count_where(i64_col, filter<1) over w1 as w1_count_where_i64_col_filter,
        count_where(i64_col, col1='str1') over w1 as w1_count_where_i64_col_col1,
        count_where(i16_col, filter>1) over w1 as w1_count_where_i16_col,
        count_where(i32_col, 1<filter) over w1 as w1_count_where_i32_col,
        count_where(f_col, 0=filter) over w1 as w1_count_where_f_col,
        count_where(d_col, 1=filter) over w1 as w1_count_where_d_col,
        count_where(t_col, 1>=filter) over w1 as w1_count_where_t_col,
        count_where(s_col, 2<filter) over w1 as w1_count_where_s_col,
        count_where(date_col, 2>filter) over w1 as w1_count_where_date_col,
        count_where(col3, 0>=filter) over w2 as w2_count_where_col3 from )" +
        base_table +
        R"(
    WINDOW
        w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 5 PRECEDING AND CURRENT ROW),
        w2 AS (PARTITION BY col1,col2 ORDER BY i64_col ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);)";

    sr->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    sr->ExecuteSQL(base_db, deploy_sql, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;

    PrepareDataForLongWindow(base_db, base_table);
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    std::string pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i64_col_filter";
    std::string result_sql = "select * from " + pre_aggr_table + ";";
    auto rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    for (int i = 4; i >= 1; i--) {
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ("str1|str2", rs->GetStringUnsafe(0));
        ASSERT_EQ(2, rs->GetInt32Unsafe(3));
        std::string aggr_val_str = rs->GetStringUnsafe(4);
        int64_t aggr_val = *reinterpret_cast<int64_t*>(&aggr_val_str[0]);
        ASSERT_EQ(2, aggr_val);
        std::string filter_key = rs->GetStringUnsafe(6);
        ASSERT_EQ(std::to_string(i % 2), filter_key);
    }

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i64_col_col1";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(5, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i16_col_filter";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i32_col_filter";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_f_col_filter";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_d_col_filter";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_t_col_filter";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_s_col_filter";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_date_col_filter";
    result_sql = "select * from " + pre_aggr_table + ";";
    rs = sr->ExecuteSQL(pre_aggr_db, result_sql, &status);
    ASSERT_EQ(4, rs->Size());

    // 11, 11, 10, 9, 8, 7, 6
    for (int i = 0; i < 2; i++) {
        std::shared_ptr<sdk::SQLRequestRow> req;
        PrepareRequestRowForLongWindow(base_db, "test_aggr", req);
        DLOG(INFO) << "Before CallProcedure";
        auto res = sr->CallProcedure(base_db, "test_aggr", req, &status);
        DLOG(INFO) << "After CallProcedure";
        EXPECT_TRUE(status.IsOK());
        EXPECT_EQ(1, res->Size());
        EXPECT_TRUE(res->Next());
        EXPECT_EQ("str1", res->GetStringUnsafe(0));
        EXPECT_EQ("str2", res->GetStringUnsafe(1));
        EXPECT_EQ(3, res->GetInt64Unsafe(2));
        EXPECT_EQ(7, res->GetInt64Unsafe(3));
        EXPECT_EQ(0, res->GetInt64Unsafe(4));
        EXPECT_EQ(0, res->GetInt64Unsafe(5));
        EXPECT_EQ(3, res->GetInt64Unsafe(6));
        EXPECT_EQ(3, res->GetInt64Unsafe(7));
        EXPECT_EQ(6, res->GetInt64Unsafe(8));
        EXPECT_EQ(0, res->GetInt64Unsafe(9));
        EXPECT_EQ(6, res->GetInt64Unsafe(10));
        EXPECT_EQ(3, res->GetInt64Unsafe(11));
    }

    ASSERT_TRUE(cs->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i64_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i64_col_col1";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i16_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_i32_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_f_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_d_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_t_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_s_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    pre_aggr_table = "pre_" + base_db + "_test_aggr_w1_count_where_date_col_filter";
    ok = sr->ExecuteDDL(pre_aggr_db, "drop table " + pre_aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = sr->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

// pre agg rows is range buckets
TEST_P(DBSDKTest, DeployLongWindowsExecuteCountWhere2) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    class DeployLongWindowCountWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowCountWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowCountWhereEnv() override {}

        void Deploy() override {
            ProcessSQLs(sr_, {absl::Substitute(R"(DEPLOY $0 options(long_windows='w1:2s')
    SELECT
        col1, col2,
        count_where(i64_col, i64_col<8) over w1 as cw_w1_2,
        count_where(i64_col, i16_col > 8) over w1 as cw_w1_3,
        count_where(i16_col, i32_col = 10) over w1 as cw_w1_4,
        count_where(i32_col, f_col != 10) over w1 as cw_w1_5,
        count_where(f_col, d_col <= 10) over w1 as cw_w1_6,
        count_where(d_col, d_col >= 10) over w1 as cw_w1_7,
        count_where(s_col, null = col1) over w1 as cw_w1_8,
        count_where(s_col, 'str0' != col1) over w1 as cw_w1_9,
        count_where(date_col, null != s_col) over w1 as cw_w1_10,
        count_where(*, i64_col > 0) over w1 as cw_w1_11,
        count_where(filter, i64_col > 0) over w1 as cw_w1_12,
    FROM $1 WINDOW
        w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 6s PRECEDING AND CURRENT ROW);)",
                                               dp_, table_)});
        }

        void TearDownPreAggTables() override {
            absl::string_view pre_agg_db = openmldb::nameserver::PRE_AGG_DB;
            ProcessSQLs(sr_, {
                                 absl::StrCat("use ", pre_agg_db),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i64_col_i64_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i64_col_i16_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i16_col_i32_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i32_col_f_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_f_col_d_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_d_col_d_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_s_col_col1"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_date_col_s_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where__i64_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_filter_i64_col"),
                                 absl::StrCat("use ", db_),
                                 absl::StrCat("drop deployment ", dp_),
                             });
        }
    };

    // request window [5s, 11s]
    DeployLongWindowCountWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };

    std::shared_ptr<hybridse::sdk::ResultSet> res;
    env.CallDeploy(&res);
    ASSERT_TRUE(res != nullptr) << "call deploy failed";

    EXPECT_EQ(1, res->Size());
    EXPECT_TRUE(res->Next());
    EXPECT_EQ("str1", res->GetStringUnsafe(0));
    EXPECT_EQ("str2", res->GetStringUnsafe(1));
    EXPECT_EQ(3, res->GetInt64Unsafe(2));
    EXPECT_EQ(4, res->GetInt64Unsafe(3));
    EXPECT_EQ(1, res->GetInt64Unsafe(4));
    EXPECT_EQ(7, res->GetInt64Unsafe(5));
    EXPECT_EQ(6, res->GetInt64Unsafe(6));
    EXPECT_EQ(3, res->GetInt64Unsafe(7));
    EXPECT_EQ(0, res->GetInt64Unsafe(8));
    EXPECT_EQ(8, res->GetInt64Unsafe(9));
    EXPECT_EQ(0, res->GetInt64Unsafe(10));
    EXPECT_EQ(8, res->GetInt64Unsafe(11));
    EXPECT_EQ(7, res->GetInt64Unsafe(12));
}

// pre agg rows is range buckets
TEST_P(DBSDKTest, DeployLongWindowsExecuteCountWhere3) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    class DeployLongWindowCountWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowCountWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowCountWhereEnv() override {}

        void Deploy() override {
            ProcessSQLs(sr_, {absl::Substitute(R"(DEPLOY $0 options(long_windows='w1:3s')
    SELECT
        col1, col2,
        count_where(i64_col, filter<1) over w1 as w1_count_where_i64_col_filter,
        count_where(i64_col, col1='str1') over w1 as w1_count_where_i64_col_col1,
        count_where(i16_col, filter>1) over w1 as w1_count_where_i16_col,
        count_where(i32_col, 1<filter) over w1 as w1_count_where_i32_col,
        count_where(f_col, 0=filter) over w1 as w1_count_where_f_col,
        count_where(d_col, 1=filter) over w1 as w1_count_where_d_col,
        count_where(t_col, 1>=filter) over w1 as w1_count_where_t_col,
        count_where(s_col, 2<filter) over w1 as w1_count_where_s_col,
        count_where(date_col, 2>filter) over w1 as w1_count_where_date_col,
        count_where(col3, 0>=filter) over w2 as w2_count_where_col3
    FROM $1 WINDOW
        w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW),
        w2 AS (PARTITION BY col1,col2 ORDER BY i64_col ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);)",
                                               dp_, table_)});
        }

        void TearDownPreAggTables() override {
            absl::string_view pre_agg_db = openmldb::nameserver::PRE_AGG_DB;
            ProcessSQLs(sr_, {
                                 absl::StrCat("use ", pre_agg_db),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i64_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i64_col_col1"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i16_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_i32_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_f_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_d_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_t_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_s_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_count_where_date_col_filter"),
                                 absl::StrCat("use ", db_),
                                 absl::StrCat("drop deployment ", dp_),
                             });
        }
    };

    // request window [4s, 11s]
    DeployLongWindowCountWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };

    std::shared_ptr<hybridse::sdk::ResultSet> res;
    // ts 11, 11, 10, 9, 8, 7, 6, 5, 4
    env.CallDeploy(&res);
    ASSERT_TRUE(res != nullptr) << "call deploy failed";

    EXPECT_EQ(1, res->Size());
    EXPECT_TRUE(res->Next());
    EXPECT_EQ("str1", res->GetStringUnsafe(0));
    EXPECT_EQ("str2", res->GetStringUnsafe(1));
    EXPECT_EQ(4, res->GetInt64Unsafe(2));
    EXPECT_EQ(9, res->GetInt64Unsafe(3));
    EXPECT_EQ(0, res->GetInt64Unsafe(4));
    EXPECT_EQ(0, res->GetInt64Unsafe(5));
    EXPECT_EQ(4, res->GetInt64Unsafe(6));
    EXPECT_EQ(4, res->GetInt64Unsafe(7));
    EXPECT_EQ(8, res->GetInt64Unsafe(8));
    EXPECT_EQ(0, res->GetInt64Unsafe(9));
    EXPECT_EQ(8, res->GetInt64Unsafe(10));
    EXPECT_EQ(3, res->GetInt64Unsafe(11));
}

TEST_P(DBSDKTest, LongWindowMinMaxWhere) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    class DeployLongWindowMinMaxWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowMinMaxWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowMinMaxWhereEnv() override {}

        void Deploy() override {
            ProcessSQLs(sr_, {absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:3s')
  SELECT
    col1, col2,
    max_where(i64_col, filter<1) over w1 as m1,
    max_where(i64_col, col1='str1') over w1 as m2,
    max_where(i16_col, filter>1) over w1 as m3,
    max_where(i32_col, 1<filter) over w1 as m4,
    max_where(f_col, 0=filter) over w1 as m5,
    max_where(d_col, 1=filter) over w1 as m6,
    min_where(i64_col, i16_col > 8) over w1 as m7,
    min_where(i16_col, i32_col = 10) over w1 as m8,
    min_where(i32_col, f_col != 10) over w1 as m9,
    min_where(f_col, d_col <= 10) over w1 as m10,
    min_where(d_col, d_col >= 10) over w1 as m11,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                               dp_, table_)});
        }

        void TearDownPreAggTables() override {
            absl::string_view pre_agg_db = openmldb::nameserver::PRE_AGG_DB;
            ProcessSQLs(sr_, {
                                 absl::StrCat("use ", pre_agg_db),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_max_where_i64_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_max_where_i64_col_col1"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_max_where_i16_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_max_where_i32_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_max_where_f_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_max_where_d_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_min_where_i64_col_i16_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_min_where_i16_col_i32_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_min_where_i32_col_f_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_min_where_f_col_d_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_min_where_d_col_d_col"),
                                 absl::StrCat("use ", db_),
                                 absl::StrCat("drop deployment ", dp_),
                             });
        }
    };

    // request window [4s, 11s]
    DeployLongWindowMinMaxWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };

    std::shared_ptr<hybridse::sdk::ResultSet> res;
    // ts 11, 11, 10, 9, 8, 7, 6, 5, 4
    env.CallDeploy(&res);
    ASSERT_TRUE(res != nullptr) << "call deploy failed";

    EXPECT_EQ(1, res->Size());
    EXPECT_TRUE(res->Next());
    EXPECT_EQ("str1", res->GetStringUnsafe(0));
    EXPECT_EQ("str2", res->GetStringUnsafe(1));
    EXPECT_EQ(10, res->GetInt64Unsafe(2));
    EXPECT_EQ(11, res->GetInt64Unsafe(3));
    EXPECT_TRUE(res->IsNULL(4));
    EXPECT_TRUE(res->IsNULL(5));
    EXPECT_EQ(10.0, res->GetFloatUnsafe(6));
    EXPECT_EQ(11.0, res->GetDoubleUnsafe(7));
    EXPECT_EQ(9, res->GetInt64Unsafe(8));
    EXPECT_EQ(10, res->GetInt16Unsafe(9));
    EXPECT_EQ(4, res->GetInt32Unsafe(10));
    EXPECT_EQ(4.0, res->GetFloatUnsafe(11));
    EXPECT_EQ(10.0, res->GetDoubleUnsafe(12));
}

TEST_P(DBSDKTest, LongWindowSumWhere) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    class DeployLongWindowSumWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowSumWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowSumWhereEnv() override {}

        void Deploy() override {
            ProcessSQLs(sr_, {absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:4s')
  SELECT
    col1, col2,
    sum_where(i64_col, col1='str1') over w1 as m1,
    sum_where(i16_col, filter>1) over w1 as m2,
    sum_where(i32_col, filter = null) over w1 as m3,
    sum_where(f_col, 0=filter) over w1 as m4,
    sum_where(d_col, 1=filter) over w1 as m5,
    sum_where(i64_col, i16_col > 8) over w1 as m6,
    sum_where(i16_col, i32_col = 10) over w1 as m7,
    sum_where(i32_col, f_col != 10) over w1 as m8,
    sum_where(f_col, d_col <= 10) over w1 as m9,
    sum_where(d_col, d_col >= 10) over w1 as m10,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                               dp_, table_)});
        }

        void TearDownPreAggTables() override {
            absl::string_view pre_agg_db = openmldb::nameserver::PRE_AGG_DB;
            ProcessSQLs(sr_, {
                                 absl::StrCat("use ", pre_agg_db),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_i64_col_col1"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_i16_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_i32_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_f_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_d_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_i64_col_i16_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_i16_col_i32_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_i32_col_f_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_f_col_d_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_sum_where_d_col_d_col"),
                                 absl::StrCat("use ", db_),
                                 absl::StrCat("drop deployment ", dp_),
                             });
        }
    };

    // request window [4s, 11s]
    DeployLongWindowSumWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };

    std::shared_ptr<hybridse::sdk::ResultSet> res;
    // ts 11, 11, 10, 9, 8, 7, 6, 5, 4
    env.CallDeploy(&res);
    ASSERT_TRUE(res != nullptr) << "call deploy failed";

    EXPECT_EQ(1, res->Size());
    EXPECT_TRUE(res->Next());
    EXPECT_EQ("str1", res->GetStringUnsafe(0));
    EXPECT_EQ("str2", res->GetStringUnsafe(1));
    EXPECT_EQ(71, res->GetInt64Unsafe(2));
    EXPECT_TRUE(res->IsNULL(3));
    EXPECT_TRUE(res->IsNULL(4));
    EXPECT_EQ(28.0, res->GetFloatUnsafe(5));
    EXPECT_EQ(32.0, res->GetDoubleUnsafe(6));
    EXPECT_EQ(41, res->GetInt64Unsafe(7));
    EXPECT_EQ(10, res->GetInt16Unsafe(8));
    EXPECT_EQ(61, res->GetInt32Unsafe(9));
    EXPECT_EQ(49.0, res->GetFloatUnsafe(10));
    EXPECT_EQ(32.0, res->GetDoubleUnsafe(11));
}

TEST_P(DBSDKTest, LongWindowAvgWhere) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    class DeployLongWindowAvgWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowAvgWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowAvgWhereEnv() override {}

        void Deploy() override {
            ProcessSQLs(sr_, {absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:3s')
  SELECT
    col1, col2,
    avg_where(i64_col, col1!='str1') over w1 as m1,
    avg_where(i16_col, filter<1) over w1 as m2,
    avg_where(i32_col, filter = null) over w1 as m3,
    avg_where(f_col, 0=filter) over w1 as m4,
    avg_where(d_col, f_col = 11) over w1 as m5,
    avg_where(i64_col, i16_col > 10) over w1 as m6,
    avg_where(i16_col, i32_col = 10) over w1 as m7,
    avg_where(i32_col, f_col != 7) over w1 as m8,
    avg_where(f_col, d_col <= 10) over w1 as m9,
    avg_where(d_col, d_col < 4.5) over w1 as m10,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                               dp_, table_)});
        }

        void TearDownPreAggTables() override {
            absl::string_view pre_agg_db = openmldb::nameserver::PRE_AGG_DB;
            ProcessSQLs(sr_, {
                                 absl::StrCat("use ", pre_agg_db),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i64_col_col1"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i16_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i32_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_f_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_d_col_f_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i64_col_i16_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i16_col_i32_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i32_col_f_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_f_col_d_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_d_col_d_col"),
                                 absl::StrCat("use ", db_),
                                 absl::StrCat("drop deployment ", dp_),
                             });
        }
    };

    // request window [4s, 11s]
    DeployLongWindowAvgWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };

    std::shared_ptr<hybridse::sdk::ResultSet> res;
    // ts 11, 11, 10, 9, 8, 7, 6, 5, 4
    env.CallDeploy(&res);
    ASSERT_TRUE(res != nullptr) << "call deploy failed";

    EXPECT_EQ(1, res->Size());
    EXPECT_TRUE(res->Next());
    EXPECT_EQ("str1", res->GetStringUnsafe(0));
    EXPECT_EQ("str2", res->GetStringUnsafe(1));
    EXPECT_TRUE(res->IsNULL(2));
    EXPECT_EQ(7.0, res->GetDoubleUnsafe(3));
    EXPECT_TRUE(res->IsNULL(4));
    EXPECT_EQ(7.0, res->GetDoubleUnsafe(5));
    EXPECT_EQ(11.0, res->GetDoubleUnsafe(6));
    EXPECT_EQ(11.0, res->GetDoubleUnsafe(7));
    EXPECT_EQ(10.0, res->GetDoubleUnsafe(8));
    EXPECT_EQ(8.0, res->GetDoubleUnsafe(9));
    EXPECT_EQ(7.0, res->GetDoubleUnsafe(10));
    EXPECT_EQ(4.0, res->GetDoubleUnsafe(11));
}

TEST_P(DBSDKTest, LongWindowAnyWhereWithDataOutOfOrder) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    class DeployLongWindowAnyWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowAnyWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowAnyWhereEnv() override {}

        void Deploy() override {
            ProcessSQLs(sr_, {absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:3s')
  SELECT
    col1, col2,
    avg_where(i64_col, col1!='str1') over w1 as m1,
    avg_where(i16_col, filter<1) over w1 as m2,
    avg_where(i32_col, filter = null) over w1 as m3,
    avg_where(f_col, 0=filter) over w1 as m4,
    avg_where(d_col, f_col = 11) over w1 as m5,
    avg_where(i64_col, i16_col > 10) over w1 as m6,
    avg_where(i16_col, i32_col = 10) over w1 as m7,
    avg_where(i32_col, f_col != 7) over w1 as m8,
    avg_where(f_col, d_col <= 10) over w1 as m9,
    avg_where(d_col, d_col < 4.5) over w1 as m10,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                               dp_, table_)});
        }

        void PrepareData() override {
            std::vector<int> order = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
            absl::BitGen gen;
            absl::c_shuffle(order, gen);

            for (auto i : order) {
                std::string val = std::to_string(i);
                std::string filter_val = std::to_string(i % 2);
                std::string date;
                if (i < 10) {
                    date = absl::StrCat("1900-01-0", std::to_string(i));
                } else {
                    date = absl::StrCat("1900-01-", std::to_string(i));
                }
                std::string insert = absl::StrCat("insert into ", table_, " values('str1', 'str2', ", i * 1000, ", ",
                                                  val, ", ", val, ", ", val, ", ", val, ", ", val, ", ", val, ", '",
                                                  val, "', '", date, "', ", filter_val, ");");
                ::hybridse::sdk::Status s;
                bool ok = sr_->ExecuteInsert(db_, insert, &s);
                ASSERT_TRUE(ok && s.IsOK()) << s.msg << "\n" << s.trace;
            }
        }

        void TearDownPreAggTables() override {
            absl::string_view pre_agg_db = openmldb::nameserver::PRE_AGG_DB;
            ProcessSQLs(sr_, {
                                 absl::StrCat("use ", pre_agg_db),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i64_col_col1"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i16_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i32_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_f_col_filter"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_d_col_f_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i64_col_i16_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i16_col_i32_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_i32_col_f_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_f_col_d_col"),
                                 absl::StrCat("drop table pre_", db_, "_", dp_, "_w1_avg_where_d_col_d_col"),
                                 absl::StrCat("use ", db_),
                                 absl::StrCat("drop deployment ", dp_),
                             });
        }
    };

    // request window [4s, 11s]
    DeployLongWindowAnyWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };

    std::shared_ptr<hybridse::sdk::ResultSet> res;
    // ts 11, 11, 10, 9, 8, 7, 6, 5, 4
    env.CallDeploy(&res);
    ASSERT_TRUE(res != nullptr) << "call deploy failed";

    EXPECT_EQ(1, res->Size());
    EXPECT_TRUE(res->Next());
    EXPECT_EQ("str1", res->GetStringUnsafe(0));
    EXPECT_EQ("str2", res->GetStringUnsafe(1));
    EXPECT_TRUE(res->IsNULL(2));
    EXPECT_EQ(7.0, res->GetDoubleUnsafe(3));
    EXPECT_TRUE(res->IsNULL(4));
    EXPECT_EQ(7.0, res->GetDoubleUnsafe(5));
    EXPECT_EQ(11.0, res->GetDoubleUnsafe(6));
    EXPECT_EQ(11.0, res->GetDoubleUnsafe(7));
    EXPECT_EQ(10.0, res->GetDoubleUnsafe(8));
    EXPECT_EQ(8.0, res->GetDoubleUnsafe(9));
    EXPECT_EQ(7.0, res->GetDoubleUnsafe(10));
    EXPECT_EQ(4.0, res->GetDoubleUnsafe(11));
}

TEST_P(DBSDKTest, LongWindowAnyWhereUnsupportRowsBucket) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    class DeployLongWindowAnyWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowAnyWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowAnyWhereEnv() override {}

        void Deploy() override {
            hybridse::sdk::Status status;
            sr_->ExecuteSQL(absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:3')
  SELECT
    col1, col2,
    avg_where(i64_col, col1!='str1') over w1 as m1,
    avg_where(i16_col, filter<1) over w1 as m2,
    avg_where(i32_col, filter = null) over w1 as m3,
    avg_where(f_col, 0=filter) over w1 as m4,
    avg_where(d_col, f_col = 11) over w1 as m5,
    avg_where(i64_col, i16_col > 10) over w1 as m6,
    avg_where(i16_col, i32_col = 10) over w1 as m7,
    avg_where(i32_col, f_col != 7) over w1 as m8,
    avg_where(f_col, d_col <= 10) over w1 as m9,
    avg_where(d_col, d_col < 4.5) over w1 as m10,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                             dp_, table_),
                            &status);
            ASSERT_FALSE(status.IsOK());
            EXPECT_EQ(status.msg, "unsupport *_where op (avg_where) for rows bucket type long window")
                << "code=" << status.code << ", msg=" << status.msg << "\n"
                << status.trace;
        }

        void TearDownPreAggTables() override {}
    };

    // unsupport: deploy any_where with rows bucket
    DeployLongWindowAnyWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };
}

TEST_P(DBSDKTest, LongWindowAnyWhereUnsupportTimeFilter) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    {
        class DeployLongWindowAnyWhereEnv : public DeployLongWindowEnv {
         public:
            explicit DeployLongWindowAnyWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
            ~DeployLongWindowAnyWhereEnv() override {}

            void Deploy() override {
                hybridse::sdk::Status status;
                sr_->ExecuteSQL(absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:3s')
  SELECT
    col1, col2,
    min_where(i64_col, date_col!="2012-12-12") over w1 as m1,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                                 dp_, table_),
                                &status);
                ASSERT_FALSE(status.IsOK());
                EXPECT_EQ(status.msg, "unsupport date or timestamp as filter column (date_col)")
                    << "code=" << status.code << ", msg=" << status.msg << "\n"
                    << status.trace;
            }

            void TearDownPreAggTables() override {}
        };

        DeployLongWindowAnyWhereEnv env(sr);
        env.SetUp();
        absl::Cleanup clean = [&env]() { env.TearDown(); };
    }

    {
        class DeployLongWindowAnyWhereEnv : public DeployLongWindowEnv {
         public:
            explicit DeployLongWindowAnyWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
            ~DeployLongWindowAnyWhereEnv() override {}

            void Deploy() override {
                hybridse::sdk::Status status;
                sr_->ExecuteSQL(absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:3s')
  SELECT
    col1, col2,
    count_where(i64_col, t_col!="2012-12-12") over w1 as m1,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                                 dp_, table_),
                                &status);
                ASSERT_FALSE(status.IsOK());
                EXPECT_EQ(status.msg, "unsupport date or timestamp as filter column (t_col)")
                    << "code=" << status.code << ", msg=" << status.msg << "\n"
                    << status.trace;
            }

            void TearDownPreAggTables() override {}
        };

        DeployLongWindowAnyWhereEnv env(sr);
        env.SetUp();
        absl::Cleanup clean = [&env]() { env.TearDown(); };
    }
}

TEST_P(DBSDKTest, LongWindowAnyWhereUnsupportHDDTable) {
    // *_where over HDD/SSD table main table not support
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    if (cs->IsClusterMode()) {
        GTEST_SKIP() << "cluster mode skiped because it use same hdd path with standalone mode";
    }

    class DeployLongWindowAnyWhereEnv : public DeployLongWindowEnv {
     public:
        explicit DeployLongWindowAnyWhereEnv(sdk::SQLClusterRouter* sr) : DeployLongWindowEnv(sr) {}
        ~DeployLongWindowAnyWhereEnv() override {}

        void PrepareSchema() override {
            ProcessSQLs(
                sr_, {"SET @@execute_mode='online';", absl::StrCat("create database ", db_), absl::StrCat("use ", db_),
                      absl::StrCat("create table ", table_,
                                   R"((col1 string, col2 string, col3 timestamp, i64_col bigint,
        i16_col smallint, i32_col int, f_col float,
        d_col double, t_col timestamp, s_col string,
        date_col date, filter int,
        index(key=(col1,col2), ts=col3, abs_ttl=0, ttl_type=absolute)
    ) options(storage_mode = 'HDD'))")});
        }

        void Deploy() override {
            hybridse::sdk::Status status;
            sr_->ExecuteSQL(absl::Substitute(R"s(DEPLOY $0 options(long_windows='w1:3s')
  SELECT
    col1, col2,
    avg_where(i64_col, col1!='str1') over w1 as m1,
    avg_where(i16_col, filter<1) over w1 as m2,
    avg_where(i32_col, filter = null) over w1 as m3,
    avg_where(f_col, 0=filter) over w1 as m4,
    avg_where(d_col, f_col = 11) over w1 as m5,
    avg_where(i64_col, i16_col > 10) over w1 as m6,
    avg_where(i16_col, i32_col = 10) over w1 as m7,
    avg_where(i32_col, f_col != 7) over w1 as m8,
    avg_where(f_col, d_col <= 10) over w1 as m9,
    avg_where(d_col, d_col < 4.5) over w1 as m10,
  FROM $1 WINDOW
    w1 AS (PARTITION BY col1,col2 ORDER BY col3 ROWS_RANGE BETWEEN 7s PRECEDING AND CURRENT ROW))s",
                                             dp_, table_),
                            &status);
            ASSERT_FALSE(status.IsOK());
            EXPECT_EQ(status.msg, "avg_where only support over memory base table")
                << "code=" << status.code << ", msg=" << status.msg << "\n"
                << status.trace;
        }

        void TearDownPreAggTables() override {}
    };

    DeployLongWindowAnyWhereEnv env(sr);
    env.SetUp();
    absl::Cleanup clean = [&env]() { env.TearDown(); };
}

TEST_P(DBSDKTest, LongWindowsCleanup) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    hybridse::sdk::Status status;
    sr->ExecuteSQL("SET @@execute_mode='online';", &status);
    std::string create_sql =
        "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
        "c8 date, index(key=c1, ts=c4, ttl=0, ttl_type=latest));";
    std::string deploy_sql =
        "deploy demo1 OPTIONS(long_windows='w1:100,w2') SELECT c1, sum(c4) OVER w1 as w1_c4_sum,"
        " max(c5) over w2 as w2_max_c5 FROM trans"
        " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),"
        " w2 AS (PARTITION BY trans.c1 ORDER BY trans.c4 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
    for (int i = 0; i < 10; i++) {
        HandleSQL("create database test2;");
        HandleSQL("use test2;");
        HandleSQL(create_sql);
        sr->ExecuteSQL(deploy_sql, &status);
        ASSERT_TRUE(status.IsOK());
        std::string msg;
        std::string result_sql = "select * from __INTERNAL_DB.PRE_AGG_META_INFO;";
        auto rs = sr->ExecuteSQL("", result_sql, &status);
        ASSERT_EQ(2, rs->Size());
        auto ok = sr->ExecuteDDL(openmldb::nameserver::PRE_AGG_DB, "drop table pre_test2_demo1_w1_sum_c4;", &status);
        ASSERT_TRUE(ok);
        ok = sr->ExecuteDDL(openmldb::nameserver::PRE_AGG_DB, "drop table pre_test2_demo1_w2_max_c5;", &status);
        ASSERT_TRUE(ok);
        result_sql = "select * from __INTERNAL_DB.PRE_AGG_META_INFO;";
        rs = sr->ExecuteSQL("", result_sql, &status);
        ASSERT_EQ(0, rs->Size());
        ASSERT_FALSE(cs->GetNsClient()->DropTable("test2", "trans", msg));
        ASSERT_TRUE(cs->GetNsClient()->DropProcedure("test2", "demo1", msg)) << msg;
        ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "trans", msg)) << msg;
        // helpful for debug
        HandleSQL("show tables;");
        HandleSQL("show deployments;");
        ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test2", msg)) << msg;
    }
}

TEST_P(DBSDKTest, CreateWithoutIndexCol) {
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
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test2", msg));
}

TEST_P(DBSDKTest, CreateIfNotExists) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    HandleSQL("create database test2;");
    HandleSQL("use test2;");
    std::string create_sql = "create table if not exists trans (col1 string);";
    hybridse::sdk::Status status;
    sr->ExecuteSQL(create_sql, &status);
    ASSERT_TRUE(status.IsOK());
    sr->ExecuteSQL("create table t4 (id string) options (partitionnum = 1, replicanum = 0);", &status);
    ASSERT_FALSE(status.IsOK());
    sr->ExecuteSQL("create table t4 (id string) options (partitionnum = 0, replicanum = 1);", &status);
    ASSERT_FALSE(status.IsOK());

    // Run create again and do not get error
    sr->ExecuteSQL(create_sql, &status);
    ASSERT_TRUE(status.IsOK());

    std::string msg;
    ASSERT_TRUE(cs->GetNsClient()->DropTable("test2", "trans", msg)) << msg;
    ASSERT_TRUE(cs->GetNsClient()->DropDatabase("test2", msg)) << msg;
}

TEST_P(DBSDKTest, ShowComponents) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("show components", &status);
    ASSERT_EQ(status.code, 0);

    if (cs->IsClusterMode()) {
        ASSERT_EQ(3, rs->Size());
        ASSERT_EQ(5, rs->GetSchema()->GetColumnCnt());
        const auto& tablet_eps = mc_->GetTbEndpoint();
        const auto& ns_ep = mc_->GetNsEndpoint();
        ASSERT_EQ(2, tablet_eps.size());
        ExpectResultSetStrEq({{"Endpoint", "Role", "Connect_time", "Status", "Ns_role"},
                              {tablet_eps.at(0), "tablet", {}, "online", "NULL"},
                              {tablet_eps.at(1), "tablet", {}, "online", "NULL"},
                              {ns_ep, "nameserver", {}, "online", "master"}},
                             rs.get(), false);
    } else {
        ASSERT_EQ(2, rs->Size());
        ASSERT_EQ(5, rs->GetSchema()->GetColumnCnt());
        const auto& tablet_ep = env.GetTbEndpoint();
        const auto& ns_ep = env.GetNsEndpoint();
        ExpectResultSetStrEq({{"Endpoint", "Role", "Connect_time", "Status", "Ns_role"},
                              {tablet_ep, "tablet", {}, "online", "NULL"},
                              {ns_ep, "nameserver", {}, "online", "master"}},
                             rs.get());
    }

    HandleSQL("show components");
}

void ExpectShowTableStatusResult(const std::vector<std::vector<test::CellExpectInfo>>& expect,
                                 hybridse::sdk::ResultSet* rs, bool all_db = false, bool is_cluster = false) {
    static const std::vector<std::vector<test::CellExpectInfo>> SystemClusterTableStatus = {
        {{}, "PRE_AGG_META_INFO", "__INTERNAL_DB", "memory", {}, {}, {}, "1", "0", "1", "NULL", "NULL", "NULL", ""},
        {{}, "JOB_INFO", "__INTERNAL_DB", "memory", "0", {}, {}, "1", "0", "1", "NULL", "NULL", "NULL", ""},
        {{},
         "GLOBAL_VARIABLES",
         "INFORMATION_SCHEMA",
         "memory",
         "4",
         {},
         {},
         "1",
         "0",
         "1",
         "NULL",
         "NULL",
         "NULL",
         ""},
        {{},
         "DEPLOY_RESPONSE_TIME",
         "INFORMATION_SCHEMA",
         "memory",
         "0",
         "0",
         {},
         "1",
         "0",
         "1",
         "NULL",
         "NULL",
         "NULL",
         ""}};

    static const std::vector<std::vector<test::CellExpectInfo>> SystemStandaloneTableStatus = {
        {{}, "PRE_AGG_META_INFO", "__INTERNAL_DB", "memory", {}, {}, {}, "1", "0", "1", "NULL", "NULL", "NULL", ""},
        {{},
         "GLOBAL_VARIABLES",
         "INFORMATION_SCHEMA",
         "memory",
         "4",
         {},
         {},
         "1",
         "0",
         "1",
         "NULL",
         "NULL",
         "NULL",
         ""},
        {{},
         "DEPLOY_RESPONSE_TIME",
         "INFORMATION_SCHEMA",
         "memory",
         "0",
         "0",
         {},
         "1",
         "0",
         "1",
         "NULL",
         "NULL",
         "NULL",
         ""}};

    std::vector<std::vector<test::CellExpectInfo>> merged_expect = {
        {"Table_id", "Table_name", "Database_name", "Storage_type", "Rows", "Memory_data_size", "Disk_data_size",
         "Partition", "Partition_unalive", "Replica", "Offline_path", "Offline_format", "Offline_symbolic_paths",
         "Warnings"}};
    merged_expect.insert(merged_expect.end(), expect.begin(), expect.end());
    if (all_db) {
        if (is_cluster) {
            merged_expect.insert(merged_expect.end(), SystemClusterTableStatus.begin(), SystemClusterTableStatus.end());
        } else {
            merged_expect.insert(merged_expect.end(), SystemStandaloneTableStatus.begin(),
                                 SystemStandaloneTableStatus.end());
        }
    }
    ExpectResultSetStrEq(merged_expect, rs);
}

TEST_P(DBSDKTest, ShowTableStatusEmptySet) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    sr->SetDatabase("");

    hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("show table status", &status);
    ASSERT_EQ(status.code, 0);
    ExpectShowTableStatusResult({}, rs.get(), false, cs->IsClusterMode());
    HandleSQL("show table status");
}

// show table status when no database is selected
TEST_P(DBSDKTest, ShowTableStatusUnderRoot) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    std::string db_name = absl::StrCat("db_", GenRand());
    std::string tb_name = absl::StrCat("tb_", GenRand());

    // prepare data
    ProcessSQLs(sr,
                {
                    "set @@execute_mode = 'online'",
                    absl::StrCat("create database ", db_name, ";"),
                    absl::StrCat("use ", db_name, ";"),
                    absl::StrCat("create table ", tb_name, " (id int, c1 string, c7 timestamp, index(key=id, ts=c7));"),
                    absl::StrCat("insert into ", tb_name, " values (1, 'aaa', 1635247427000);"),
                });
    // reset to empty db
    sr->SetDatabase("");

    // sleep for 4s, name server should updated TableInfo in schedule
    absl::SleepFor(absl::Seconds(4));

    // test
    hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("show table status", &status);
    ASSERT_EQ(status.code, 0);
    if (cs->IsClusterMode()) {
        // default partition_num = 8 and replica_num = min(tablet,3) in cluster_mode
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "8", "0", "2", "NULL", "NULL", "NULL", ""}},
            rs.get());
    } else {
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}},
            rs.get());
    }
    // runs HandleSQL only for the purpose of pretty print result in console
    HandleSQL("show table status");

    // teardown
    ProcessSQLs(sr, {absl::StrCat("use ", db_name), absl::StrCat("drop table ", tb_name),
                     absl::StrCat("drop database ", db_name)});
    sr->SetDatabase("");
}

// show table status with patterns when no database is selected
TEST_P(DBSDKTest, ShowTableStatusLike) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    std::string db_name = absl::StrCat("db_", GenRand());
    std::string tb_name = absl::StrCat("tb_", GenRand());

    // prepare data
    ProcessSQLs(sr,
                {
                    "set @@execute_mode = 'online'",
                    absl::StrCat("create database ", db_name, ";"),
                    absl::StrCat("use ", db_name, ";"),
                    absl::StrCat("create table ", tb_name, " (id int, c1 string, c7 timestamp, index(key=id, ts=c7));"),
                    absl::StrCat("insert into ", tb_name, " values (1, 'aaa', 1635247427000);"),
                });
    // reset to empty db
    sr->SetDatabase("");

    // sleep for 4s, name server should updated TableInfo in schedule
    absl::SleepFor(absl::Seconds(4));

    // test
    hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("show table status like '%'", &status);
    ASSERT_EQ(status.code, 0) << status.msg;
    if (cs->IsClusterMode()) {
        // default partition_num = 8 and replica_num = min(tablet,3) in cluster_mode
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "8", "0", "2", "NULL", "NULL", "NULL", ""}},
            rs.get(), true, true);
    } else {
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}},
            rs.get(), true, false);
    }
    // runs HandleSQL only for the purpose of pretty print result in console
    HandleSQL("show table status like '%'");

    rs = sr->ExecuteSQL("show table status like '*'", &status);
    ASSERT_EQ(status.code, 0) << status.msg;
    ExpectShowTableStatusResult({}, rs.get(), false, false);

    rs = sr->ExecuteSQL("show table status like 'not_exists'", &status);
    ASSERT_EQ(status.code, 0) << status.msg;
    ExpectShowTableStatusResult({}, rs.get(), false, false);

    rs = sr->ExecuteSQL(absl::StrCat("show table status like '", db_name, "'"), &status);
    ASSERT_EQ(status.code, 0) << status.msg;
    if (cs->IsClusterMode()) {
        // default partition_num = 8 and replica_num = min(tablet,3) in cluster_mode
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "8", "0", "2", "NULL", "NULL", "NULL", ""}},
            rs.get());
    } else {
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}},
            rs.get());
    }

    rs = sr->ExecuteSQL(absl::StrCat("show table status like '", db_name.substr(0, db_name.size() - 1), "_'"), &status);
    ASSERT_EQ(status.code, 0) << status.msg;
    if (cs->IsClusterMode()) {
        // default partition_num = 8 and replica_num = min(tablet,3) in cluster_mode
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "8", "0", "2", "NULL", "NULL", "NULL", ""}},
            rs.get());
    } else {
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}},
            rs.get());
    }

    // reset to db_name
    // like pattern has high priority over the db
    sr->SetDatabase(db_name);
    rs = sr->ExecuteSQL("show table status like '%'", &status);
    ASSERT_EQ(status.code, 0) << status.msg;
    if (cs->IsClusterMode()) {
        // default partition_num = 8 and replica_num = min(tablet,3) in cluster_mode
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "8", "0", "2", "NULL", "NULL", "NULL", ""}},
            rs.get(), true, true);
    } else {
        ExpectShowTableStatusResult(
            {{{}, tb_name, db_name, "memory", "1", {{}, "0"}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}},
            rs.get(), true, false);
    }

    // teardown
    ProcessSQLs(sr, {absl::StrCat("use ", db_name), absl::StrCat("drop table ", tb_name),
                     absl::StrCat("drop database ", db_name)});
    sr->SetDatabase("");
}

TEST_P(DBSDKTest, ShowTableStatusForHddTable) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    if (cs->IsClusterMode()) {
        // cluster mode not asserted because of #1695
        // since tablets use of the same gflag to store table data, in mini cluster environment,
        // it lead to dead lock cause tablets runs on same machine
        return;
    }

    std::string db_name = absl::StrCat("db_", GenRand());
    std::string tb_name = absl::StrCat("tb_", GenRand());

    // prepare data
    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online'",
                        absl::StrCat("create database ", db_name, ";"),
                        absl::StrCat("use ", db_name, ";"),
                        absl::StrCat(
                            "create table ", tb_name,
                            " (id int, c1 string, c7 timestamp, index(key=id, ts=c7)) options (storage_mode = 'HDD');"),
                        absl::StrCat("insert into ", tb_name, " values (1, 'aaa', 1635247427000);"),
                    });
    // reset to empty db
    sr->SetDatabase("");

    // sleep for 4s, name server should updated TableInfo in schedule
    absl::SleepFor(absl::Seconds(4));

    // test
    hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("show table status", &status);
    ASSERT_EQ(status.code, 0);

    // TODO(ace): Memory_data_size not asserted because not implemented
    ExpectShowTableStatusResult(
        {{{}, tb_name, db_name, "hdd", "1", {}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}}, rs.get());

    // runs HandleSQL only for the purpose of pretty print result in console
    HandleSQL("show table status");

    // teardown
    ProcessSQLs(sr, {absl::StrCat("use ", db_name), absl::StrCat("drop table ", tb_name),
                     absl::StrCat("drop database ", db_name)});
    sr->SetDatabase("");
}

// show table status after use db
TEST_P(DBSDKTest, ShowTableStatusUnderDB) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;

    std::string db1_name = absl::StrCat("db1_", GenRand());
    std::string tb1_name = absl::StrCat("tb1_", GenRand());
    std::string db2_name = absl::StrCat("db2_", GenRand());
    std::string tb2_name = absl::StrCat("tb2_", GenRand());

    // prepare data
    ProcessSQLs(
        sr, {
                "set @@execute_mode = 'online'",
                absl::StrCat("create database ", db1_name, ";"),
                absl::StrCat("use ", db1_name, ";"),
                absl::StrCat("create table ", tb1_name, " (id int, c1 string, c7 timestamp, index(key=id, ts=c7));"),
                absl::StrCat("insert into ", tb1_name, " values (1, 'aaa', 1635247427000);"),

                absl::StrCat("create database ", db2_name, ";"),
                absl::StrCat("use ", db2_name),
                absl::StrCat("create table ", tb2_name, " (id int, c1 string, c7 timestamp, index(key=id, ts=c7));"),
                absl::StrCat("insert into ", tb2_name, " values (2, 'aaa', 1635247427000);"),
            });

    // sleep for 10s, name server should updated TableInfo in schedule
    absl::SleepFor(absl::Seconds(4));

    // test
    hybridse::sdk::Status status;
    sr->ExecuteSQL(absl::StrCat("use ", db1_name, ";"), &status);
    ASSERT_TRUE(status.IsOK());
    auto rs = sr->ExecuteSQL("show table status", &status);
    ASSERT_EQ(status.code, 0);
    if (cs->IsClusterMode()) {
        // default partition_num = 8 and replica_num = min(tablet,3) in cluster_mode
        ExpectShowTableStatusResult(
            {{{}, tb1_name, db1_name, "memory", "1", {{}, "0"}, {{}, "0"}, "8", "0", "2", "NULL", "NULL", "NULL", ""}},
            rs.get());

        sr->ExecuteSQL(absl::StrCat("use ", db2_name, ";"), &status);
        ASSERT_TRUE(status.IsOK());
        rs = sr->ExecuteSQL("show table status", &status);
        ASSERT_EQ(status.code, 0);
        ExpectShowTableStatusResult(
            {{{}, tb2_name, db2_name, "memory", "1", {{}, "0"}, {{}, "0"}, "8", "0", "2", "NULL", "NULL", "NULL"}},
            rs.get());
    } else {
        ExpectShowTableStatusResult(
            {{{}, tb1_name, db1_name, "memory", "1", {{}, "0"}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}},
            rs.get());

        sr->ExecuteSQL(absl::StrCat("use ", db2_name, ";"), &status);
        ASSERT_TRUE(status.IsOK());
        rs = sr->ExecuteSQL("show table status", &status);
        ASSERT_EQ(status.code, 0);
        ExpectShowTableStatusResult(
            {{{}, tb2_name, db2_name, "memory", "1", {{}, "0"}, {{}, "0"}, "1", "0", "1", "NULL", "NULL", "NULL", ""}},
            rs.get());
    }

    // show only tables inside hidden db
    HandleSQL("use INFORMATION_SCHEMA");
    rs = sr->ExecuteSQL("show table status", &status);
    ASSERT_EQ(status.code, 0);
    ExpectShowTableStatusResult({{{},
                                  nameserver::GLOBAL_VARIABLES,
                                  nameserver::INFORMATION_SCHEMA_DB,
                                  "memory",
                                  "4",
                                  {},
                                  {},
                                  "1",
                                  "0",
                                  "1",
                                  "NULL",
                                  "NULL",
                                  "NULL",
                                  ""},
                                 {{},
                                  nameserver::DEPLOY_RESPONSE_TIME,
                                  nameserver::INFORMATION_SCHEMA_DB,
                                  "memory",
                                  {},
                                  {},
                                  {},
                                  "1",
                                  "0",
                                  "1",
                                  "NULL",
                                  "NULL",
                                  "NULL",
                                  ""}},
                                rs.get());

    // teardown
    ProcessSQLs(sr, {
                        absl::StrCat("use ", db1_name, ";"),
                        absl::StrCat("drop table ", tb1_name),
                        absl::StrCat("drop database ", db1_name),
                        absl::StrCat("use ", db2_name),
                        absl::StrCat("drop table ", tb2_name),
                        absl::StrCat("drop database ", db2_name),
                    });

    sr->SetDatabase("");
}

TEST_P(DBSDKTest, GlobalVariable) {
    auto cli = GetParam();
    cs = cli->cs;
    sr = cli->sr;
    ProcessSQLs(sr, {
                        "set @@execute_mode='offline';",
                    });

    ::hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("show global variables", &status);
    // init global variable
    ExpectResultSetStrEq({{"Variable_name", "Variable_value"},
                          {"enable_trace", "false"},
                          {"sync_job", "false"},
                          {"job_timeout", "20000"},
                          {"execute_mode", "offline"}},
                         rs.get());
    // init session variables from systemtable
    rs = sr->ExecuteSQL("show session variables", &status);
    ExpectResultSetStrEq({{"Variable_name", "Value"},
                          {"enable_trace", "false"},
                          {"execute_mode", "offline"},
                          {"job_timeout", "20000"},
                          {"sync_job", "false"}},
                         rs.get());
    // set global variables
    ProcessSQLs(sr, {
                        "set @@global.enable_trace='true';",
                        "set @@global.sync_job='true';",
                        "set @@global.execute_mode='online';",
                    });
    rs = sr->ExecuteSQL("show global variables", &status);
    ExpectResultSetStrEq({{"Variable_name", "Variable_value"},
                          {"enable_trace", "true"},
                          {"sync_job", "true"},
                          {"job_timeout", "20000"},
                          {"execute_mode", "online"}},
                         rs.get());
    // update session variables if set global variables
    rs = sr->ExecuteSQL("show session variables", &status);
    ExpectResultSetStrEq({{"Variable_name", "Value"},
                          {"enable_trace", "true"},
                          {"execute_mode", "online"},
                          {"job_timeout", "20000"},
                          {"sync_job", "true"}},
                         rs.get());

    ProcessSQLs(sr, {
                        "set @@global.enable_trace='false';",
                        "set @@global.sync_job='false';",
                        "set @@global.execute_mode='offline';",
                    });
    rs = sr->ExecuteSQL("show global variables", &status);
    ExpectResultSetStrEq({{"Variable_name", "Variable_value"},
                          {"enable_trace", "false"},
                          {"sync_job", "false"},
                          {"job_timeout", "20000"},
                          {"execute_mode", "offline"}},
                         rs.get());
}

TEST_F(SqlCmdTest, SelectWithAddNewIndex) {
    auto cli = cluster_cli;
    auto sr = cli.sr;

    std::string db1_name = absl::StrCat("db1_", GenRand());
    std::string db2_name = absl::StrCat("db2_", GenRand());
    std::string tb1_name = absl::StrCat("tb1_", GenRand());

    ProcessSQLs(sr, {
                        "set @@execute_mode = 'online'",
                        absl::StrCat("create database ", db1_name, ";"),
                        absl::StrCat("create database ", db2_name, ";"),
                        absl::StrCat("use ", db1_name, ";"),

                        absl::StrCat("create table ", tb1_name,
                                     " (id int, c1 string, c2 int, c3 timestamp, c4 timestamp, "
                                     "index(key=(c1),ts=c4))options(partitionnum=1, replicanum=1);"),
                        absl::StrCat("insert into ", tb1_name, " values(1,'aa',1,1590738990000,1637056523316);"),
                        absl::StrCat("insert into ", tb1_name, " values(2,'bb',1,1590738990000,1637056523316);"),
                        absl::StrCat("insert into ", tb1_name, " values(3,'aa',3,1590738990000,1637057123257);"),
                        absl::StrCat("insert into ", tb1_name, " values(4,'aa',1,1590738990000,1637057123317);"),
                        absl::StrCat("use ", db2_name, ";"),
                        absl::StrCat("CREATE INDEX index1 ON ", db1_name, ".", tb1_name,
                                     " (c2) OPTIONS (ttl=10m, ttl_type=absolute);"),
                    });
    absl::SleepFor(absl::Seconds(10));
    hybridse::sdk::Status status;
    auto res = sr->ExecuteSQL(absl::StrCat("use ", db1_name, ";"), &status);
    res = sr->ExecuteSQL(absl::StrCat("select id,c1,c2,c3 from ", tb1_name), &status);
    ASSERT_EQ(res->Size(), 4);
    res = sr->ExecuteSQL(absl::StrCat("select id,c1,c2,c3 from ", tb1_name, " where c1='aa';"), &status);
    ASSERT_EQ(res->Size(), 3);
    res = sr->ExecuteSQL(absl::StrCat("select id,c1,c2,c3 from ", tb1_name, " where c2=1;"), &status);
    ASSERT_EQ(res->Size(), 3);
    ProcessSQLs(sr, {absl::StrCat("drop index ", db1_name, ".", tb1_name, ".index1")});
    absl::SleepFor(absl::Seconds(2));
    res = sr->ExecuteSQL(absl::StrCat("select id,c1,c2,c3 from ", tb1_name, " where c2=1;"), &status);
    ASSERT_EQ(res->Size(), 3);

    ProcessSQLs(sr, {
                        absl::StrCat("use ", db1_name, ";"),
                        absl::StrCat("drop table ", tb1_name),
                        absl::StrCat("drop database ", db1_name),
                    });

    sr->SetDatabase("");
}

// --------------------------------------------------------------------------------------
// basic functional UTs to test if it is correct for deploy query response time collection
// see TabletImpl::CollectDeployStats
// --------------------------------------------------------------------------------------

// a proxy class to create and cleanup deployment stats more gracefully
struct DeploymentEnv {
    explicit DeploymentEnv(sdk::SQLClusterRouter* sr) : sr_(sr) {
        db_ = absl::StrCat("db_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));
        table_ = absl::StrCat("tb_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));
        dp_name_ = absl::StrCat("dp_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));
        procedure_name_ = absl::StrCat("procedure_", absl::Uniform(gen_, 0, std::numeric_limits<int32_t>::max()));
    }

    virtual ~DeploymentEnv() { TearDown(); }

    void SetUp() {
        ProcessSQLs(
            sr_,
            {"set session execute_mode = 'online'", absl::StrCat("create database ", db_), absl::StrCat("use ", db_),
             absl::StrCat("create table ", table_,
                          " (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, "
                          "c8 date, index(key=c1, ts=c4, abs_ttl=0, ttl_type=absolute)) "
                          "OPTIONS(partitionnum=1,replicanum=1);"),
             // deploy will create index c1,c7,lat 2, may fail in workflow cpp
             absl::StrCat("deploy ", dp_name_, " SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM ", table_,
                          " WINDOW w1 AS (PARTITION BY c1 ORDER BY c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);"),
             absl::StrCat(
                 "create procedure ", procedure_name_,
                 " (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date) BEGIN SELECT c1, "
                 "c3, "
                 "sum(c4) OVER w1 as w1_c4_sum FROM ",
                 table_, " WINDOW w1 AS (PARTITION BY c1 ORDER BY c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW); END")});
    }

    void TearDown() {
        ProcessSQLs(sr_, {
                             absl::StrCat("drop deployment ", dp_name_),
                             absl::StrCat("drop procedure ", procedure_name_),
                             absl::StrCat("drop table ", table_),
                             absl::StrCat("drop database ", db_),
                             "set global deploy_stats = 'off'",
                         });
    }

    // A bacth request increase deployment cnt by 1
    // yet may greatly impact deploy response time, if the batch size is huge
    // maybe it requires a revision
    void CallDeployProcedureBatch() {
        hybridse::sdk::Status status;
        std::shared_ptr<sdk::SQLRequestRow> rr = std::make_shared<sdk::SQLRequestRow>();
        GetRequestRow(&rr, dp_name_);
        auto common_column_indices = std::make_shared<sdk::ColumnIndicesSet>();
        auto row_batch = std::make_shared<sdk::SQLRequestRowBatch>(rr->GetSchema(), common_column_indices);
        ASSERT_TRUE(row_batch->AddRow(rr));
        sr->CallSQLBatchRequestProcedure(db_, dp_name_, row_batch, &status);
        ASSERT_TRUE(status.IsOK()) << status.msg << "\n" << status.trace;
    }

    void CallDeployProcedure() {
        hybridse::sdk::Status status;
        std::shared_ptr<sdk::SQLRequestRow> rr = std::make_shared<sdk::SQLRequestRow>();
        GetRequestRow(&rr, dp_name_);
        sr->CallProcedure(db_, dp_name_, rr, &status);
        ASSERT_TRUE(status.IsOK()) << status.msg << "\n" << status.trace;
    }

    void CallProcedure() {
        hybridse::sdk::Status status;
        std::shared_ptr<sdk::SQLRequestRow> rr = std::make_shared<sdk::SQLRequestRow>();
        GetRequestRow(&rr, procedure_name_);
        sr->CallProcedure(db_, procedure_name_, rr, &status);
        ASSERT_TRUE(status.IsOK()) << status.msg << "\n" << status.trace;
    }

    sdk::SQLClusterRouter* sr_;
    absl::BitGen gen_;
    // variables generate randomly in SetUp
    std::string db_;
    std::string table_;
    std::string dp_name_;
    std::string procedure_name_;

 private:
    void GetRequestRow(std::shared_ptr<sdk::SQLRequestRow>* rs, const std::string& name) {  // NOLINT
        hybridse::sdk::Status s;
        auto res = sr_->GetRequestRowByProcedure(db_, name, &s);
        ASSERT_TRUE(s.IsOK());
        ASSERT_TRUE(res->Init(5));
        ASSERT_TRUE(res->AppendString("hello"));
        ASSERT_TRUE(res->AppendInt32(5));
        ASSERT_TRUE(res->AppendInt64(5));
        ASSERT_TRUE(res->AppendFloat(0.1));
        ASSERT_TRUE(res->AppendDouble(0.1));
        ASSERT_TRUE(res->AppendTimestamp(100342));
        ASSERT_TRUE(res->AppendDate(2012, 10, 10));
        ASSERT_TRUE(res->Build());
        *rs = res;
    }
};

class StripSpaceTest : public ::testing::TestWithParam<std::pair<std::string_view, std::string_view>> {};

std::vector<std::pair<std::string_view, std::string_view>> strip_cases = {
    {"show components;", "show components;"},
    {"show components;  ", "show components;"},
    {"show components;\t", "show components;"},
    {"show components; \t", "show components;"},
    {"show components; \v\t\r\n\f", "show components;"},
    {"show components; show", "show components;show"},
};

INSTANTIATE_TEST_SUITE_P(Strip, StripSpaceTest, ::testing::ValuesIn(strip_cases));

TEST_P(StripSpaceTest, Correctness) {
    auto& cs = GetParam();

    std::string output;
    StripStartingSpaceOfLastStmt(cs.first, &output);
    EXPECT_EQ(cs.second, output);
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
    ::openmldb::base::SetupGlog(true);

    FLAGS_traverse_cnt_limit = 500;
    FLAGS_zk_session_timeout = 100000;
    FLAGS_get_table_status_interval = 1000;
    // enable disk table flags
    std::filesystem::path tmp_path = std::filesystem::temp_directory_path() / "openmldb";
    absl::Cleanup clean = [&tmp_path]() { std::filesystem::remove_all(tmp_path); };

    const std::string& tmp_path_str = tmp_path.string();
    FLAGS_ssd_root_path = absl::StrCat(tmp_path_str, "/ssd_root_random_", ::openmldb::test::GenRand());
    FLAGS_hdd_root_path = absl::StrCat(tmp_path_str, "/hdd_root_random_", ::openmldb::test::GenRand());
    FLAGS_recycle_bin_hdd_root_path =
        absl::StrCat(tmp_path_str, "/recycle_hdd_root_random_", ::openmldb::test::GenRand());
    FLAGS_recycle_bin_ssd_root_path =
        absl::StrCat(tmp_path_str, "/recycle_ssd_root_random_", ::openmldb::test::GenRand());

    ::openmldb::sdk::MiniCluster mc(6181);
    ::openmldb::cmd::mc_ = &mc;
    FLAGS_enable_distsql = true;
    int ok = ::openmldb::cmd::mc_->SetUp(2);
    sleep(5);
    srand(time(NULL));
    ::openmldb::sdk::ClusterOptions copt;
    copt.zk_cluster = mc.GetZkCluster();
    copt.zk_path = mc.GetZkPath();
    copt.zk_session_timeout = FLAGS_zk_session_timeout;
    ::openmldb::cmd::cluster_cli.cs = new ::openmldb::sdk::ClusterSDK(copt);
    ::openmldb::cmd::cluster_cli.cs->Init();
    ::openmldb::cmd::cluster_cli.sr = new ::openmldb::sdk::SQLClusterRouter(::openmldb::cmd::cluster_cli.cs);
    ::openmldb::cmd::cluster_cli.sr->Init();

    env.SetUp();
    FLAGS_host = "127.0.0.1";
    FLAGS_port = env.GetNsPort();
    ::openmldb::cmd::standalone_cli.cs = new ::openmldb::sdk::StandAloneSDK(FLAGS_host, FLAGS_port);
    ::openmldb::cmd::standalone_cli.cs->Init();
    ::openmldb::cmd::standalone_cli.sr = new ::openmldb::sdk::SQLClusterRouter(::openmldb::cmd::standalone_cli.cs);
    ::openmldb::cmd::standalone_cli.sr->Init();
    sleep(5);

    ok = RUN_ALL_TESTS();

    // sr owns relative cs
    delete openmldb::cmd::cluster_cli.sr;
    delete openmldb::cmd::standalone_cli.sr;

    mc.Close();
    env.Close();

    return ok;
}
