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
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "base/glog_wrapper.h"
#include "codec/fe_row_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "sdk/sql_cluster_router.h"
#include "sdk/sql_router.h"
#include "sdk/sql_sdk_test.h"
#include "vm/catalog.h"


DECLARE_uint32(max_traverse_cnt);
DECLARE_uint32(traverse_cnt_limit);

namespace openmldb::sdk {

static void SetOnlineMode(const std::shared_ptr<SQLRouter>& router) {
    ::hybridse::sdk::Status status;
    router->ExecuteSQL("SET @@execute_mode='online';", &status);
}

::openmldb::sdk::MiniCluster* mc_;
std::shared_ptr<SQLRouter> router_;

class SQLClusterTest : public ::testing::Test {
 public:
    SQLClusterTest() = default;
    ~SQLClusterTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() = default;
    ~MockClosure() override = default;
    void Run() override {}
};

class SQLClusterDDLTest : public SQLClusterTest {
 public:
    void SetUp() override {
        SQLRouterOptions sql_opt;
        sql_opt.zk_cluster = mc_->GetZkCluster();
        sql_opt.zk_path = mc_->GetZkPath();
        router = NewClusterSQLRouter(sql_opt);
        ASSERT_TRUE(router != nullptr);
        SetOnlineMode(router);
        db = "db" + GenRand();
        ::hybridse::sdk::Status status;
        ASSERT_TRUE(router->CreateDB(db, &status));
    }

    void TearDown() override {
        ::hybridse::sdk::Status status;
        ASSERT_TRUE(router->DropDB(db, &status));
        router.reset();
    }

    void RightDDL(const std::string& db, const std::string& name, const std::string& ddl) {
        ::hybridse::sdk::Status status;
        ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status)) << "ddl: " << ddl;
        ASSERT_TRUE(router->ExecuteDDL(db, "drop table " + name + ";", &status));
    }
    void RightDDL(const std::string& name, const std::string& ddl) { RightDDL(db, name, ddl); }

    void WrongDDL(const std::string& name, const std::string& ddl) { WrongDDL(db, name, ddl); }
    void WrongDDL(const std::string& db, const std::string& name, const std::string& ddl) {
        ::hybridse::sdk::Status status;
        ASSERT_FALSE(router->ExecuteDDL(db, ddl, &status)) << "ddl: " << ddl;
        ASSERT_FALSE(router->ExecuteDDL(db, "drop table " + name + ";", &status));
    }

    std::shared_ptr<SQLRouter> router;
    std::string db;
};
TEST_F(SQLClusterDDLTest, CreateTableWithDatabase) {
    std::string name = "test" + GenRand();
    ::hybridse::sdk::Status status;
    std::string ddl;

    std::string db2 = "db" + GenRand();
    ASSERT_TRUE(router->CreateDB(db2, &status));
    // create table db2.name
    ddl = "create table " + db2 + "." + name +
          "("
          "col1 int, col2 bigint, col3 string,"
          "index(key=col3, ts=col2));";
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status)) << "ddl: " << ddl;
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table " + db2 + "." + name + ";", &status));

    // create table db2.name when default database is empty
    ddl = "create table " + db2 + "." + name +
          "("
          "col1 int, col2 bigint, col3 string,"
          "index(key=col3, ts=col2));";
    ASSERT_TRUE(router->ExecuteDDL("", ddl, &status)) << "ddl: " << ddl;
    ASSERT_TRUE(router->ExecuteDDL("", "drop table " + db2 + "." + name + ";", &status));

    ASSERT_TRUE(router->DropDB(db2, &status));
}
TEST_F(SQLClusterDDLTest, CreateTableWithDatabaseWrongDDL) {
    std::string name = "test" + GenRand();
    ::hybridse::sdk::Status status;
    std::string ddl;

    // create table db2.name when db2 not exist
    ddl = "create table db2." + name +
          "("
          "col1 int, col2 bigint, col3 string,"
          "index(key=col3, ts=col2));";
    ASSERT_FALSE(router->ExecuteDDL(db, ddl, &status)) << "ddl: " << ddl;
    ASSERT_FALSE(router->ExecuteDDL(db, "drop table " + name + ";", &status));

    // create table db2.name when db2 not exit
    ddl = "create table db2." + name +
          "("
          "col1 int, col2 bigint, col3 string,"
          "index(key=col3, ts=col2));";
    ASSERT_FALSE(router->ExecuteDDL("", ddl, &status)) << "ddl: " << ddl;
    ASSERT_FALSE(router->ExecuteDDL("", "drop table " + name + ";", &status));
}

TEST_F(SQLClusterDDLTest, ColumnDefaultValue) {
    std::string name = "test" + GenRand();
    ::hybridse::sdk::Status status;
    std::string ddl;
    ddl = "create table " + name +
          "("
          "col1 bool default true, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 bool default 12, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 int default 12, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 bigint default 50000000000l, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 float default 123, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 float default 1.23, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 double default 0.1, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 date default '2021-10-01', col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 timestamp default 1634890378, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    RightDDL(name, ddl);
}

TEST_F(SQLClusterDDLTest, ColumnDefaultValueWrongType) {
    std::string name = "test" + GenRand();
    ::hybridse::sdk::Status status;
    std::string ddl;
    ddl = "create table " + name +
          "("
          "col1 bool default test, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 int default 'test', col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 int default 1.0, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 int default 1+1, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 string default 12, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 double default 'test', col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 date default 'test', col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 date default 1234, col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
    ddl = "create table " + name +
          "("
          "col1 timestamp default 'test', col2 bigint not null, col3 string,"
          "index(key=col3, ts=col2));";
    WrongDDL(name, ddl);
}

TEST_F(SQLClusterTest, ClusterInsert) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2)) options(partitionnum=8);";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::map<uint32_t, std::vector<std::string>> key_map;
    for (int i = 0; i < 100; i++) {
        std::string key = "hello" + std::to_string(i);
        std::string insert = "insert into " + name + " values('" + key + "', 1590);";
        ok = router->ExecuteInsert(db, insert, &status);
        ASSERT_TRUE(ok);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key) % 8);
        key_map[pid].push_back(key);
    }
    std::vector<::openmldb::nameserver::TableInfo> tables;
    auto ns = mc_->GetNsClient();
    auto ret = ns->ShowDBTable(db, &tables);
    ASSERT_TRUE(ret.OK());
    ASSERT_EQ(tables.size(), 1);
    auto tid = tables[0].tid();
    auto endpoints = mc_->GetTbEndpoint();
    uint32_t count = 0;
    for (const auto& endpoint : endpoints) {
        ::openmldb::tablet::TabletImpl* tb1 = mc_->GetTablet(endpoint);
        ::openmldb::api::GetTableStatusRequest request;
        ::openmldb::api::GetTableStatusResponse response;
        request.set_tid(tid);
        MockClosure closure;
        tb1->GetTableStatus(NULL, &request, &response, &closure);
        for (const auto& table_status : response.all_table_status()) {
            count += table_status.record_cnt();
            auto iter = key_map.find(table_status.pid());
            if (table_status.record_cnt() != 0) {
                ASSERT_EQ(iter->second.size(), table_status.record_cnt());
            }
        }
    }
    ASSERT_EQ(100u, count);
    ok = router->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLClusterTest, ClusterInsertWithColumnDefaultValue) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 int not null,"
                      "col2 bigint default 112 not null,"
                      "col4 string default 'test4' not null,"
                      "col5 date default '2000-01-01' not null,"
                      "col6 timestamp default 10000 not null,"
                      "index(key=col1, ts=col2)) options(partitionnum=1);";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::string sql;
    sql = "insert into " + name + " values(1, 1, '1', '2021-01-01', 1);";
    ASSERT_TRUE(router->ExecuteInsert(db, sql, &status));
    auto res = router->ExecuteSQL(db, "select * from " + name + " where col1=1", &status);
    ASSERT_TRUE(res);
    ASSERT_TRUE(res->Next());
    ASSERT_EQ("1, 1, 1, 2021-01-01, 1", res->GetRowString());
    ASSERT_FALSE(res->Next());

    sql = "insert into " + name + "(col1, col2) values(2, 2);";
    ASSERT_TRUE(router->ExecuteInsert(db, sql, &status));
    res = router->ExecuteSQL(db, "select * from " + name + " where col1=2", &status);
    ASSERT_TRUE(res);
    ASSERT_TRUE(res->Next());
    ASSERT_EQ("2, 2, test4, 2000-01-01, 10000", res->GetRowString());
    ASSERT_FALSE(res->Next());
    sql = "insert into " + name + "(col1) values(3);";
    ASSERT_TRUE(router->ExecuteInsert(db, sql, &status));
    res = router->ExecuteSQL(db, "select * from " + name + " where col1=3", &status);
    ASSERT_TRUE(res);
    ASSERT_TRUE(res->Next());
    ASSERT_EQ("3, 112, test4, 2000-01-01, 10000", res->GetRowString());
    ASSERT_FALSE(res->Next());
    sql = "insert into " + name + "(col2) values(4);";
    ASSERT_FALSE(router->ExecuteInsert(db, sql, &status));

    std::vector<::openmldb::nameserver::TableInfo> tables;
    auto ns = mc_->GetNsClient();
    auto ret = ns->ShowDBTable(db, &tables);
    ASSERT_TRUE(ret.OK());
    ASSERT_EQ(tables.size(), 1);
    {
        ::hybridse::sdk::Status status;
        res = router->ExecuteSQL(db, absl::StrCat("select * from ", name), &status);
        ASSERT_TRUE(status.IsOK()) << status.msg;
        ASSERT_EQ(3u, res->Size());
    }
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
        "                index(key=col2, ts=col3)) "
        "options(partitionnum=2);";
    SQLRouterOptions sql_opt;
    sql_opt.zk_session_timeout = 30000;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    SetOnlineMode(router);
    std::string db = "gettabletclient;";
    hybridse::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));
    ASSERT_TRUE(router->ExecuteDDL(db, ddl, &status));
    ASSERT_TRUE(router->RefreshCatalog());
    std::string sql =
        "select col2, sum(col1) over w1 from t1 \n"
        "window w1 as (partition by col2 \n"
        "order by col3 rows between 3 preceding and current row);";
    auto ns_client = mc_->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
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
        hybridse::sdk::Status sdk_status;
        auto client =
            sql_cluster_router->GetTabletClient(db, sql, hybridse::vm::kRequestMode, request_row, &sdk_status);
        int pid = ::openmldb::base::hash64(pk) % 2;
        // only assert leader paritition
        for (int i = 0; i < 3; i++) {
            if (tables[0].table_partition(pid).partition_meta(i).is_leader()) {
                ASSERT_EQ(client->GetEndpoint(), tables[0].table_partition(pid).partition_meta(i).endpoint());
            }
        }
    }
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table t1;", &status));
    ASSERT_TRUE(router->DropDB(db, &status));
}

TEST_F(SQLClusterTest, CreatePreAggrTable) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    SetOnlineMode(router);
    ASSERT_TRUE(router != nullptr);
    std::string base_table = "test" + GenRand();
    std::string base_db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(base_db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + base_table +
                      "("
                      "col1 string, col2 bigint, col3 int,"
                      " index(key=col1, ts=col2,"
                      " TTL_TYPE=absolute, TTL=1m)) options(partitionnum=8, replicanum=2);";
    ok = router->ExecuteDDL(base_db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    auto ns_client = mc_->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> base_tables;
    std::string msg;
    ASSERT_TRUE(ns_client->ShowTable(base_table, base_db, false, base_tables, msg));
    ASSERT_EQ(base_tables.size(), 1);

    // normal case
    {
        std::string deploy_sql =
            "deploy test1 options(long_windows='w1:1000') select col1,"
            " sum(col3) over w1 as w1_sum_col3 from " +
            base_table +
            " WINDOW w1 AS (PARTITION BY col1 ORDER BY col2"
            " ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW);";
        router->ExecuteSQL(base_db, "use " + base_db + ";", &status);
        router->ExecuteSQL(base_db, deploy_sql, &status);
        ASSERT_TRUE(router->RefreshCatalog());

        std::vector<::openmldb::nameserver::TableInfo> agg_tables;
        std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
        std::string aggr_table = "pre_" + base_db + "_test1_w1_sum_col3";
        ASSERT_TRUE(ns_client->ShowTable(aggr_table, pre_aggr_db, false, agg_tables, msg));
        ASSERT_EQ(1, agg_tables.size());
        ASSERT_EQ(1, agg_tables[0].column_key_size());
        ASSERT_EQ("key", agg_tables[0].column_key(0).col_name(0));
        ASSERT_EQ("ts_start", agg_tables[0].column_key(0).ts_name());
        ASSERT_EQ(base_tables[0].column_key(0).ttl().ttl_type(), agg_tables[0].column_key(0).ttl().ttl_type());
        ASSERT_EQ(base_tables[0].column_key(0).ttl().lat_ttl(), agg_tables[0].column_key(0).ttl().lat_ttl());
        ASSERT_EQ(base_tables[0].replica_num(), agg_tables[0].replica_num());
        ASSERT_EQ(base_tables[0].partition_num(), agg_tables[0].partition_num());
        ASSERT_EQ(base_tables[0].table_partition().size(), agg_tables[0].table_partition().size());
        ASSERT_EQ(base_tables[0].storage_mode(), agg_tables[0].storage_mode());

        std::string meta_db = openmldb::nameserver::INTERNAL_DB;
        std::string meta_table = openmldb::nameserver::PRE_AGG_META_NAME;
        std::string meta_sql = "select * from " + meta_table + ";";

        auto rs = router->ExecuteSQL(meta_db, meta_sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(1, rs->Size());
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ(aggr_table, rs->GetStringUnsafe(0));
        ASSERT_EQ(pre_aggr_db, rs->GetStringUnsafe(1));
        ASSERT_EQ(base_db, rs->GetStringUnsafe(2));
        ASSERT_EQ(base_table, rs->GetStringUnsafe(3));
        ASSERT_EQ("sum", rs->GetStringUnsafe(4));
        ASSERT_EQ("col3", rs->GetStringUnsafe(5));
        ASSERT_EQ("col1", rs->GetStringUnsafe(6));
        ASSERT_EQ("col2", rs->GetStringUnsafe(7));
        ASSERT_EQ("1000", rs->GetStringUnsafe(8));

        ASSERT_TRUE(mc_->GetNsClient()->DropProcedure(base_db, "test1", msg));
        ok = router->ExecuteDDL(pre_aggr_db, "drop table " + aggr_table + ";", &status);
        ASSERT_TRUE(ok);
    }

    // window doesn't match window in sql
    {
        std::string deploy_sql =
            "deploy test1 options(long_windows='w2:1000,w1:1d') select col1,"
            " sum(col3) over w1 as w1_sum_col3 from " +
            base_table +
            " WINDOW w1 AS (PARTITION BY col1 ORDER BY col2"
            " ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW);";
        router->ExecuteSQL(base_db, "use " + base_db + ";", &status);
        router->ExecuteSQL(base_db, deploy_sql, &status);
        ASSERT_EQ(status.code, ::hybridse::common::StatusCode::kSyntaxError);
        ASSERT_EQ(status.msg, "long_windows option doesn't match window in sql");
    }
    {
        std::string deploy_sql =
            "deploy test1 options(long_windows='w_error:1d') select col1,"
            " sum(col3) over w1 as w1_sum_col3 from " +
            base_table +
            " WINDOW w1 AS (PARTITION BY col1 ORDER BY col2"
            " ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW);";
        router->ExecuteSQL(base_db, "use " + base_db + ";", &status);
        router->ExecuteSQL(base_db, deploy_sql, &status);
        ASSERT_EQ(status.code, ::hybridse::common::StatusCode::kSyntaxError);
        ASSERT_EQ(status.msg, "long_windows option doesn't match window in sql");
    }
    ok = router->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLClusterTest, Aggregator) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    SetOnlineMode(router);
    ASSERT_TRUE(router != nullptr);
    std::string base_table = "t" + GenRand();
    std::string base_db = "d" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(base_db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + base_table +
                      "(col1 string, col2 string, col3 timestamp, col4 bigint) "
                      "options(partitionnum=8);";
    ok = router->ExecuteDDL(base_db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    auto ns_client = mc_->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    ASSERT_TRUE(ns_client->ShowTable(base_table, base_db, false, tables, msg));
    ASSERT_EQ(tables.size(), 1);

    std::string deploy_sql =
        "deploy test_aggr options(long_windows='w1:2') select col1, col2,"
        " sum(col4) over w1 as w1_sum_col4 from " +
        base_table +
        " WINDOW w1 AS (PARTITION BY col1,col2 ORDER BY col3"
        " ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);";
    router->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    router->ExecuteSQL(base_db, deploy_sql, &status);

    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;

    for (int i = 1; i <= 11; i++) {
        std::string insert = "insert into " + base_table + " values('str1', 'str2', " + std::to_string(i) + ", " +
                             std::to_string(i) + ");";
        ok = router->ExecuteInsert(base_db, insert, &status);
        ASSERT_TRUE(ok);
    }

    std::string aggr_table = "pre_" + base_db + "_test_aggr_w1_sum_col4";
    std::string result_sql = "select * from " + aggr_table + ";";

    auto rs = router->ExecuteSQL(pre_aggr_db, result_sql, &status);
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

    ASSERT_TRUE(mc_->GetNsClient()->DropProcedure(base_db, "test_aggr", msg));
    ok = router->ExecuteDDL(pre_aggr_db, "drop table " + aggr_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLClusterTest, PreAggrTableExist) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string base_table = "test" + GenRand();
    std::string base_db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(base_db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + base_table +
                      "("
                      " col1 string, col2 bigint, col3 int,"
                      " index(key=col1, ts=col2,"
                      " TTL_TYPE=absolute, TTL=1m)) options(partitionnum=8);";
    ok = router->ExecuteDDL(base_db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    auto ns_client = mc_->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    ASSERT_TRUE(ns_client->ShowTable(base_table, base_db, false, tables, msg));
    ASSERT_EQ(tables.size(), 1);

    std::string deploy_sql =
        "deploy test1 options(long_windows='w1:1000') select col1,"
        " sum(col3) over w1 as w1_sum_col3 from " +
        base_table +
        " WINDOW w1 AS (PARTITION BY col1 ORDER BY col2"
        " ROWS_RANGE BETWEEN 20s PRECEDING AND CURRENT ROW);";
    router->ExecuteSQL(base_db, "use " + base_db + ";", &status);
    router->ExecuteSQL(base_db, deploy_sql, &status);

    std::string deploy_sql2 =
        "deploy test2 options(long_windows='w1:1000') select col1,"
        " sum(col3) over w1 as w1_sum_col3 from " +
        base_table +
        " WINDOW w1 AS (PARTITION BY col1 ORDER BY col2"
        " ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW);";
    router->ExecuteSQL(base_db, deploy_sql2, &status);

    tables.clear();
    std::string pre_aggr_db = openmldb::nameserver::PRE_AGG_DB;
    ASSERT_TRUE(ns_client->ShowTable("", pre_aggr_db, false, tables, msg));

    // pre-aggr table with same meta info only create once.
    ASSERT_EQ(tables.size(), 1);

    std::string meta_db = openmldb::nameserver::INTERNAL_DB;
    std::string meta_table = openmldb::nameserver::PRE_AGG_META_NAME;
    std::string meta_sql = "select * from " + meta_table + " where base_db='" + base_db + "';";

    auto rs = router->ExecuteSQL(meta_db, meta_sql, &status);
    std::string aggr_table = "pre_" + base_db + "_test1_w1_sum_col3";
    ASSERT_EQ(0, static_cast<int>(status.code));
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(aggr_table, rs->GetStringUnsafe(0));
    ASSERT_EQ(pre_aggr_db, rs->GetStringUnsafe(1));
    ASSERT_EQ(base_db, rs->GetStringUnsafe(2));
    ASSERT_EQ(base_table, rs->GetStringUnsafe(3));
    ASSERT_EQ("sum", rs->GetStringUnsafe(4));
    ASSERT_EQ("col3", rs->GetStringUnsafe(5));
    ASSERT_EQ("col1", rs->GetStringUnsafe(6));
    ASSERT_EQ("col2", rs->GetStringUnsafe(7));
    ASSERT_EQ("1000", rs->GetStringUnsafe(8));
    ASSERT_TRUE(mc_->GetNsClient()->DropProcedure(base_db, "test2", msg));
    ASSERT_TRUE(mc_->GetNsClient()->DropProcedure(base_db, "test1", msg));
    ok = router->ExecuteDDL(base_db, "drop table " + base_table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(base_db, &status);
    ASSERT_TRUE(ok);
}

static std::shared_ptr<SQLRouter> GetNewSQLRouter() {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.zk_session_timeout = 60000;
    sql_opt.enable_debug = hybridse::sqlcase::SqlCase::IsDebug();
    auto router = NewClusterSQLRouter(sql_opt);
    SetOnlineMode(router);
    return router;
}
static bool IsRequestSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos || mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("request-unsupport") != std::string::npos || mode.find("cluster-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
static bool IsBatchRequestSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos || mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("batch-request-unsupport") != std::string::npos ||
        mode.find("request-unsupport") != std::string::npos || mode.find("cluster-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}
static bool IsBatchSupportMode(const std::string& mode) {
    if (mode.find("hybridse-only") != std::string::npos || mode.find("rtidb-unsupport") != std::string::npos ||
        mode.find("performance-sensitive-unsupport") != std::string::npos ||
        mode.find("batch-unsupport") != std::string::npos || mode.find("cluster-unsupport") != std::string::npos) {
        return false;
    }
    return true;
}

TEST_P(SQLSDKQueryTest, SqlSdkDistributeBatchRequestTest) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DistributeRunBatchRequestModeSDK(sql_case, router_);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, SqlSdkDistributeBatchRequestTest) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DistributeRunBatchRequestModeSDK(sql_case, router_);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, SqlSdkDistributeRequestTest) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestModeSDK(sql_case, router_);
    LOG(INFO) << "Finish sql_sdk_distribute_request_test: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKQueryTest, SqlSdkDistributeBatchRequestSinglePartitionTest) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DistributeRunBatchRequestModeSDK(sql_case, router_, 1);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_single_partition_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, SqlSdkDistributeBatchRequestSinglePartitionTest) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DistributeRunBatchRequestModeSDK(sql_case, router_, 1);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_single_partition_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

/* TEST_P(SQLSDKQueryTest, sql_sdk_distribute_request_single_partition_test) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (boost::contains(sql_case.mode(), "rtidb-unsupport") ||
        boost::contains(sql_case.mode(), "rtidb-request-unsupport") ||
        boost::contains(sql_case.mode(), "request-unsupport") ||
        boost::contains(sql_case.mode(), "cluster-unsupport")) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestModeSDK(sql_case, router_, 1);
    LOG(INFO) << "Finish sql_sdk_distribute_request_single_partition_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
} */

TEST_P(SQLSDKBatchRequestQueryTest, SqlSdkDistributeBatchRequestProcedureTest) {
    auto sql_case = GetParam();
    if (!IsBatchRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DistributeRunBatchRequestProcedureModeSDK(sql_case, router_, 8, false);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_procedure_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, SqlSdkDistributeRequestProcedureTest) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestProcedureModeSDK(sql_case, router_, 8, false);
    LOG(INFO) << "Finish sql_sdk_distribute_request_procedure_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}
TEST_P(SQLSDKBatchRequestQueryTest, SqlSdkDistributeBatchRequestProcedureAsyncTest) {
    auto sql_case = GetParam();
    if (!IsRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    if (sql_case.batch_request().columns_.empty()) {
        LOG(WARNING) << "No batch request specified";
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DistributeRunBatchRequestProcedureModeSDK(sql_case, router_, 8, true);
    LOG(INFO) << "Finish sql_sdk_distribute_batch_request_procedure_async_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_P(SQLSDKQueryTest, SqlSdkDistributeRequestProcedureAsyncTest) {
    auto sql_case = GetParam();
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    if (!IsRequestSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router with multi partitions";
    DistributeRunRequestProcedureModeSDK(sql_case, router_, 8, true);
    LOG(INFO) << "Finish sql_sdk_distribute_request_procedure_async_test: ID: " << sql_case.id()
              << ", DESC: " << sql_case.desc();
}

TEST_F(SQLClusterTest, CreateTable) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    for (int i = 0; i < 2; i++) {
        std::string name = "test" + std::to_string(i);
        std::string ddl = "create table " + name +
                          "("
                          "col1 string, col2 bigint,"
                          "index(key=col1, ts=col2)) "
                          "options(partitionnum=3);";
        ok = router->ExecuteDDL(db, ddl, &status);
        ASSERT_TRUE(ok);
    }
    ASSERT_TRUE(router->RefreshCatalog());
    auto ns_client = mc_->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
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
    ASSERT_EQ(pid_map.size(), 3u);
    ASSERT_EQ(pid_map.begin()->second, pid_map.rbegin()->second);
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test0;", &status));
    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test1;", &status));
    ASSERT_TRUE(router->DropDB(db, &status));
}

TEST_F(SQLClusterTest, GetTableSchema) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string db = "db" + GenRand();
    std::string table = "test0";
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);

    std::string ddl = "create table " + table +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2)) "
                      "options(partitionnum=3);";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    auto schema = router->GetTableSchema(db, table);
    ASSERT_EQ(schema->GetColumnCnt(), 2);
    ASSERT_EQ(schema->GetColumnName(0), "col1");
    ASSERT_EQ(schema->GetColumnType(0), hybridse::sdk::DataType::kTypeString);
    ASSERT_EQ(schema->GetColumnName(1), "col2");
    ASSERT_EQ(schema->GetColumnType(1), hybridse::sdk::DataType::kTypeInt64);

    ASSERT_TRUE(router->ExecuteDDL(db, "drop table test0;", &status));
    ASSERT_TRUE(router->DropDB(db, &status));
}

TEST_P(SQLSDKClusterOnlineBatchQueryTest, SqlSdkDistributeBatchTest) {
    auto sql_case = GetParam();
    if (!IsBatchSupportMode(sql_case.mode())) {
        LOG(WARNING) << "Unsupport mode: " << sql_case.mode();
        return;
    }
    LOG(INFO) << "ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
    ASSERT_TRUE(router_ != nullptr) << "Fail new cluster sql router";
    DistributeRunBatchModeSDK(sql_case, router_, mc_->GetTbEndpoint());
    LOG(INFO) << "Finish SqlSdkDistributeBatchTest: ID: " << sql_case.id() << ", DESC: " << sql_case.desc();
}

TEST_F(SQLClusterTest, ClusterSelect) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string table = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + table +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2)) options(partitionnum=8, replicanum=3);";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    std::string insert = "insert into " + table + " values('helloworld', 1024);";
    ok = router->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);

    auto res = router->ExecuteSQL(db, "select * from " + table, &status);
    ASSERT_TRUE(res);
    ASSERT_EQ(res->Size(), 1);
    ASSERT_TRUE(res->Next());
    ASSERT_EQ("helloworld, 1024", res->GetRowString());
    ASSERT_FALSE(res->Next());

    ok = router->ExecuteDDL(db, "drop table " + table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLClusterTest, ClusterOnlineAgg) {
    SQLRouterOptions sql_opt;
    sql_opt.enable_debug = false;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    sql_opt.zk_session_timeout = 1000000;
    sql_opt.request_timeout = 1000000;
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string table = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + table +
                      "(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date, index(key=c1, "
                      "ts=c6)) options(partitionnum=8, replicanum=1);";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok) << status.msg;
    ASSERT_TRUE(router->RefreshCatalog());

    int row_num = 500;
    for (int i = 0; i < row_num; i++) {
        std::string insert = "insert into " + table + " values('key2" + "',20,22,9.2,19.3," + std::to_string(1000 + i) +
                             ",'2021-01-10');";
        ok = router->ExecuteInsert(db, insert, &status);
        ASSERT_TRUE(ok) << status.msg;
    }

    {
        auto res = router->ExecuteSQL(db, "select sum(c2), count(c2), sum(c3), sum(c4), sum(c5) from " + table + ";",
                                      true, true, 0, &status);
        ASSERT_TRUE(res) << "failed: " << status.msg << ", " << status.code;
        ASSERT_EQ(res->Size(), 1);
        while (res->Next()) {
            int32_t sum_c2 = res->GetInt32Unsafe(0);
            int64_t count = res->GetInt64Unsafe(1);
            int64_t sum_c3 = res->GetInt64Unsafe(2);
            float sum_c4 = res->GetFloatUnsafe(3);
            double sum_c5 = res->GetDoubleUnsafe(4);
            LOG(INFO) << "res = " << res->GetRowString();
            ASSERT_EQ(sum_c2, row_num * 20);
            ASSERT_EQ(sum_c3, row_num * 22);
            ASSERT_EQ(count, row_num);
            ASSERT_TRUE(std::abs(sum_c4 / row_num - 9.2) < 0.1);
            ASSERT_TRUE(std::abs(sum_c5 / row_num - 19.3) < 0.1);
        }
    }

    ok = router->ExecuteDDL(db, "drop table " + table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

bool contains(const google::protobuf::RepeatedPtrField<std::string>& field, const std::string& value) {
    return std::find(field.begin(), field.end(), value) != field.end();
}

TEST_F(SQLClusterTest, AlterTableAddDropOfflinePath) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string table = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + table + " (col1 int)";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    // Add path
    ddl = "ALTER TABLE " + table + " ADD offline_path 'hdfs://foo/bar'";
    router->ExecuteSQL(db, ddl, &status);
    ASSERT_TRUE(router->RefreshCatalog());

    auto paths = router->GetTableInfo(db, table).offline_table_info().symbolic_paths();
    ASSERT_TRUE(contains(paths, "hdfs://foo/bar"));

    // Drop path
    ddl = "ALTER TABLE " + table + " DROP offline_path 'hdfs://foo/bar'";
    router->ExecuteSQL(db, ddl, &status);
    ASSERT_TRUE(router->RefreshCatalog());

    paths = router->GetTableInfo(db, table).offline_table_info().symbolic_paths();
    ASSERT_TRUE(!contains(paths, "hdfs://foo/bar"));

    // Add path
    ddl = "ALTER TABLE " + table + " ADD offline_path 'hdfs://foo/bar'";
    router->ExecuteSQL(db, ddl, &status);
    ASSERT_TRUE(router->RefreshCatalog());

    paths = router->GetTableInfo(db, table).offline_table_info().symbolic_paths();
    ASSERT_TRUE(contains(paths, "hdfs://foo/bar"));

    // Add path and drop path
    ddl = "ALTER TABLE " + table + " ADD offline_path 'hdfs://foo/bar2', DROP offline_path 'hdfs://foo/bar'";
    router->ExecuteSQL(db, ddl, &status);
    ASSERT_TRUE(router->RefreshCatalog());

    paths = router->GetTableInfo(db, table).offline_table_info().symbolic_paths();
    ASSERT_TRUE(!contains(paths, "hdfs://foo/bar"));
    ASSERT_TRUE(contains(paths, "hdfs://foo/bar2"));

    // Clear offline table to drop, otherwise it requires taskmanager to drop table
    auto table_info = router->GetTableInfo(db, table);
    table_info.clear_offline_table_info();
    router->UpdateOfflineTableInfo(table_info);

    ok = router->ExecuteDDL(db, "drop table " + table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLClusterTest, MultiThreadAlterTableOfflinePath) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_->GetZkCluster();
    sql_opt.zk_path = mc_->GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router != nullptr);
    SetOnlineMode(router);
    std::string table = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + table + " (col1 int)";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());

    auto run_and_check = [&]() {
        // Add path
        ddl = "ALTER TABLE " + table + " ADD offline_path 'hdfs://foo/bar'";
        router->ExecuteSQL(db, ddl, &status);
        ASSERT_TRUE(router->RefreshCatalog());

        // Drop path
        ddl = "ALTER TABLE " + table + " DROP offline_path 'hdfs://foo/bar'";
        router->ExecuteSQL(db, ddl, &status);
        ASSERT_TRUE(router->RefreshCatalog());
    };

    int iter = 1;
    for (int i = 0; i < iter; i++) {
        std::thread t1(run_and_check);
        std::thread t2(run_and_check);
        std::thread t3(run_and_check);
        t1.join();
        t2.join();
        t3.join();
    }

    auto paths = router->GetTableInfo(db, table).offline_table_info().symbolic_paths();
    ASSERT_TRUE(!contains(paths, "hdfs://foo/bar"));

    // Clear offline table to drop, otherwise it requires taskmanager to drop table
    auto table_info = router->GetTableInfo(db, table);
    table_info.clear_offline_table_info();
    router->UpdateOfflineTableInfo(table_info);

    ok = router->ExecuteDDL(db, "drop table " + table + ";", &status);
    ASSERT_TRUE(ok);
    ok = router->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

}  // namespace openmldb::sdk

int main(int argc, char** argv) {
    FLAGS_traverse_cnt_limit = 10;
    FLAGS_max_traverse_cnt = 5000;
    // init google test first for gtest_xxx flags
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    FLAGS_zk_session_timeout = 100000;
    ::openmldb::base::SetupGlog(true);

    ::openmldb::sdk::MiniCluster mc(6181);
    ::openmldb::sdk::mc_ = &mc;
    FLAGS_enable_distsql = true;
    int ok = ::openmldb::sdk::mc_->SetUp(3);
    sleep(5);

    ::openmldb::sdk::router_ = ::openmldb::sdk::GetNewSQLRouter();
    if (nullptr == ::openmldb::sdk::router_) {
        LOG(ERROR) << "Test failed with NULL SQL router";
        return -1;
    }

    srand(time(nullptr));
    ok = RUN_ALL_TESTS();
    ::openmldb::sdk::mc_->Close();
    return ok;
}
