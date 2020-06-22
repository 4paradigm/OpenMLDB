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

#include "sdk/sql_router.h"

#include <sched.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wapper.h"  // NOLINT
#include "catalog/schema_adapter.h"
#include "codec/fe_row_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "timer.h"  // NOLINT
#include "vm/catalog.h"

namespace rtidb {
namespace sdk {

typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>
    RtiDBSchema;
typedef ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>
    RtiDBIndex;
inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class SQLRouterTest : public ::testing::Test {
 public:
    SQLRouterTest() : mc_(6181) {}
    ~SQLRouterTest() {}
    void SetUp() {
        bool ok = mc_.SetUp();
        ASSERT_TRUE(ok);
    }
    void TearDown() { mc_.Close(); }

 public:
    MiniCluster mc_;
};

TEST_F(SQLRouterTest, smoketest) {
    ::rtidb::nameserver::TableInfo table_info;
    table_info.set_format_version(1);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    auto ns_client = mc_.GetNsClient();
    std::string error;
    bool ok = ns_client->CreateDatabase(db, error);
    ASSERT_TRUE(ok);
    table_info.set_name(name);
    table_info.set_db(db);
    table_info.set_partition_num(1);
    RtiDBSchema* schema = table_info.mutable_column_desc_v1();
    auto col1 = schema->Add();
    col1->set_name("col1");
    col1->set_data_type(::rtidb::type::kVarchar);
    col1->set_type("string");
    auto col2 = schema->Add();
    col2->set_name("col2");
    col2->set_data_type(::rtidb::type::kBigInt);
    col2->set_type("int64");
    col2->set_is_ts_col(true);
    RtiDBIndex* index = table_info.mutable_column_key();
    auto key1 = index->Add();
    key1->set_index_name("index0");
    key1->add_col_name("col1");
    key1->add_ts_name("col2");
    ok = ns_client->CreateTable(table_info, error);

    ::fesql::vm::Schema fe_schema;
    ::rtidb::catalog::SchemaAdapter::ConvertSchema(table_info.column_desc_v1(),
                                                   &fe_schema);
    ::fesql::codec::RowBuilder rb(fe_schema);
    std::string pk = "pk1";
    uint64_t ts = 1589780888000l;
    uint32_t size = rb.CalTotalLength(pk.size());
    std::string value;
    value.resize(size);
    rb.SetBuffer(reinterpret_cast<int8_t*>(&(value[0])), size);
    rb.AppendString(pk.c_str(), pk.size());
    rb.AppendInt64(ts);

    ASSERT_TRUE(ok);
    ClusterOptions option;
    option.zk_cluster = mc_.GetZkCluster();
    option.zk_path = mc_.GetZkPath();

    ClusterSDK sdk(option);
    ASSERT_TRUE(sdk.Init());
    std::vector<std::shared_ptr<::rtidb::client::TabletClient>> tablet;
    ok = sdk.GetTabletByTable(db, name, &tablet);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1, tablet.size());
    uint32_t tid = sdk.GetTableId(db, name);
    ASSERT_NE(tid, 0);
    ok = tablet[0]->Put(tid, 0, pk, ts, value, 1);
    ASSERT_TRUE(ok);

    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_.GetZkCluster();
    sql_opt.zk_path = mc_.GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) ASSERT_TRUE(false);
    std::string sql = "select col1, col2 + 1 from " + name + " ;";
    ::fesql::sdk::Status status;
    auto rs = router->ExecuteSQL(db, sql, &status);
    if (!rs) ASSERT_TRUE(false);
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(1, rs->Size());
    ASSERT_EQ(2, rs->GetSchema()->GetColumnCnt());
    ASSERT_FALSE(rs->IsNULL(0));
    ASSERT_FALSE(rs->IsNULL(1));
    ASSERT_EQ(ts + 1, rs->GetInt64Unsafe(1));
    ASSERT_EQ(pk, rs->GetStringUnsafe(0));
}

TEST_F(SQLRouterTest, smoketest_on_sql) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_.GetZkCluster();
    sql_opt.zk_path = mc_.GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) ASSERT_TRUE(false);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2));";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::string insert = "insert into " + name + " values('hello', 1590);";
    ok = router->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::string sql_select = "select col1 from " + name + " ;";
    auto rs = router->ExecuteSQL(db, sql_select, &status);
    if (!rs) ASSERT_TRUE(false);
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    std::string sql_window_batch =
        "select sum(col2) over w from " + name + " window w as (partition by " +
        name + ".col1 order by " + name +
        ".col2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
    rs = router->ExecuteSQL(db, sql_window_batch, &status);
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(1590, rs->GetInt64Unsafe(0));
    std::shared_ptr<SQLRequestRow> row =
        router->GetRequestRow(db, sql_window_batch, &status);
    if (!row) ASSERT_FALSE(true);
    ASSERT_EQ(2, row->GetSchema()->GetColumnCnt());
    ASSERT_TRUE(row->Init(5));
    ASSERT_TRUE(row->AppendString("hello"));
    ASSERT_TRUE(row->AppendInt64(100));
    ASSERT_TRUE(row->Build());

    std::string sql_window_request =
        "select sum(col2)  over w as sum_col2 from " + name +
        " window w as (partition by " + name + ".col1 order by " + name +
        ".col2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

    rs = router->ExecuteSQL(db, sql_window_request, row, &status);
    if (!rs) ASSERT_FALSE(true);
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(100, rs->GetInt64Unsafe(0));
}

TEST_F(SQLRouterTest, smoke_explain_on_sql) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_.GetZkCluster();
    sql_opt.zk_path = mc_.GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) ASSERT_TRUE(false);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 timestamp, col3 date,"
                      "index(key=col1, ts=col2));";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::string insert = "insert into " + name +
                         " values('hello', 1591174600000l, '2020-06-03');";
    ok = router->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::string sql_select = "select * from " + name + " ;";
    auto explain = router->Explain(db, sql_select, &status);
    if (explain) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_TRUE(false);
    }
    std::cout << explain->GetPhysicalPlan() << std::endl;
}

TEST_F(SQLRouterTest, smoketimestamptest_on_sql) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = mc_.GetZkCluster();
    sql_opt.zk_path = mc_.GetZkPath();
    auto router = NewClusterSQLRouter(sql_opt);
    if (!router) ASSERT_TRUE(false);
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::fesql::sdk::Status status;
    bool ok = router->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 timestamp, col3 date,"
                      "index(key=col1, ts=col2));";
    ok = router->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);

    ASSERT_TRUE(router->RefreshCatalog());
    std::string insert = "insert into " + name +
                         " values('hello', 1591174600000l, '2020-06-03');";
    ok = router->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router->RefreshCatalog());
    std::string sql_select = "select * from " + name + " ;";
    auto rs = router->ExecuteSQL(db, sql_select, &status);
    if (!rs) ASSERT_TRUE(false);
    ASSERT_EQ(1, rs->Size());
    ASSERT_EQ(3, rs->GetSchema()->GetColumnCnt());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(1591174600000l, rs->GetTimeUnsafe(1));
    int32_t year = 0;
    int32_t month = 0;
    int32_t day = 0;
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(2020, year);
    ASSERT_EQ(6, month);
    ASSERT_EQ(3, day);
    ASSERT_FALSE(rs->Next());
}

}  // namespace sdk
}  // namespace rtidb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
