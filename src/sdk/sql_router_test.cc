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

#include "sdk/sql_router.h"

#include <gmock/gmock-matchers.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "sdk/mini_cluster.h"
#include "vm/catalog.h"

namespace openmldb::sdk {

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> PBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> RtiDBIndex;

::openmldb::sdk::MiniCluster* mc_;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class SQLRouterTest : public ::testing::Test {
 public:
    SQLRouterTest() = default;
    ~SQLRouterTest() override = default;
    void SetUp() override {
        SQLRouterOptions sql_opt;
        sql_opt.zk_cluster = mc_->GetZkCluster();
        sql_opt.zk_path = mc_->GetZkPath();
        router_ = NewClusterSQLRouter(sql_opt);
        ASSERT_TRUE(router_);
        ::hybridse::sdk::Status status;
        router_->ExecuteSQL("SET @@execute_mode='online';", &status);
        ASSERT_TRUE(status.IsOK());
    }
    void TearDown() override {}

    template <typename A, typename B, typename C, typename D>
    void CheckNextRow(std::shared_ptr<hybridse::sdk::ResultSet> rs, A get_col1, B get_col2, C v1, D v2) {
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ(get_col1(rs), v1) << "expect " << v1 << ", " << v2;
        ASSERT_EQ(get_col2(rs), v2);
    }

    template <typename A, typename B, typename C, typename D>
    void CheckCurRow(std::shared_ptr<hybridse::sdk::ResultSet> rs, A get_col1, B get_col2, C v1, D v2) {
        ASSERT_EQ(get_col1(rs), v1) << "expect " << v1 << ", " << v2;
        ASSERT_EQ(get_col2(rs), v2);
    }

    static void Step(const std::shared_ptr<hybridse::sdk::ResultSet>& rs, int step) {
        for (int i = 0; i < step; ++i) {
            ASSERT_TRUE(rs->Next());
        }
    }
    template <typename A, typename B>
    static void PrintRsAndReset(const std::shared_ptr<hybridse::sdk::ResultSet>& rs, A get_col1, B get_col2) {
        std::stringstream ss("result set:\n");
        while (rs->Next()) {
            ss << get_col1(rs) << " " << get_col2(rs) << "\n";
        }
        ASSERT_TRUE(rs->Reset());
        LOG(INFO) << ss.str();
    }

 protected:
    std::string db_ = "sql_router_test";
    std::shared_ptr<SQLRouter> router_;
};

TEST_F(SQLRouterTest, badZk) {
    SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = "127.0.0.1:1111";
    sql_opt.zk_path = "/path";
    sql_opt.zk_session_timeout = 10;
    auto router = NewClusterSQLRouter(sql_opt);
    ASSERT_TRUE(router == nullptr);
}

TEST_F(SQLRouterTest, db_name_test) {
    ASSERT_TRUE(router_ != nullptr);
    ::hybridse::sdk::Status status;
    ASSERT_FALSE(router_->CreateDB("", &status));

    std::string db = "123456";
    ASSERT_TRUE(router_->CreateDB(db, &status)) << db << ": " << status.msg;
    ASSERT_TRUE(router_->DropDB(db, &status)) << db << ": " << status.msg;
    // SQL_IDENTIFIER: [^`/\\.\n]+, so use '/' in name is not allowed
    db = "1/2";
    ASSERT_FALSE(router_->CreateDB(db, &status) && status.code == -2) << db << ": " << status.msg;
    ASSERT_FALSE(router_->DropDB(db, &status) && status.code == -2) << db << ": " << status.msg;
}

TEST_F(SQLRouterTest, db_api_test) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    std::vector<std::string> dbs;
    ASSERT_TRUE(router_->ShowDB(&dbs, &status));
    uint32_t origin = dbs.size();
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    dbs.clear();
    ASSERT_TRUE(router_->ShowDB(&dbs, &status));
    ASSERT_EQ(1u, dbs.size() - origin);
    ASSERT_EQ(db, dbs[0]);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, create_and_drop_table_test) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2));";
    std::string insert = "insert into " + name + " values('hello', 1590);";
    std::string select = "select * from " + name + ";";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    ok = router_->ExecuteInsert(db, insert, &status);

    auto rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_TRUE(rs != nullptr);
    ASSERT_EQ(1, rs->Size());

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);

    // test stmt with db name prefix
    ddl = "create table " + db + "." + name + "(col1 string, col2 bigint, index(key=col1, ts=col2));";
    insert = "insert into " + db + "." + name + " values('hello', 1590);";
    select = "select * from " + db + "." + name + ";";
    ok = router_->ExecuteDDL("", ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    ok = router_->ExecuteInsert("", insert, &status);

    rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_TRUE(rs != nullptr);
    ASSERT_EQ(1, rs->Size());
    ok = router_->ExecuteDDL("", "drop table " + db + "." + name + ";", &status);
    ASSERT_TRUE(ok);

    std::string ddl_fake = "create table " + name +
                           "("
                           "col1 int, col2 bigint,"
                           "index(key=col1, ts=col2));";

    ok = router_->ExecuteDDL(db, ddl_fake, &status);
    ASSERT_TRUE(ok);

    ASSERT_TRUE(router_->RefreshCatalog());

    rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_TRUE(rs != nullptr);
    ASSERT_EQ(0, rs->Size());
    // db still has table, drop fail
    ok = router_->DropDB(db, &status);
    ASSERT_FALSE(ok);

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, testSqlInsertPlaceholder) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    std::string insert = "insert into " + name + " values('hello', 1590);";
    std::string insert_placeholder1 = "insert into " + name + " values(?, ?);";
    std::string insert_placeholder2 = "insert into " + name + " values(?, 1592);";
    std::string insert_placeholder3 = "insert into " + name + " values('hi', ?);";

    ok = router_->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);

    std::string sql_select = "select col1, col2 from " + name + ";";
    auto rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_EQ(rs->Size(), 1);

    auto get_col1 = [](const std::shared_ptr<hybridse::sdk::ResultSet>& rs) { return rs->GetStringUnsafe(0); };
    auto get_col2 = [](const std::shared_ptr<hybridse::sdk::ResultSet>& rs) { return rs->GetInt64Unsafe(1); };

    CheckNextRow(rs, get_col1, get_col2, "hello", 1590);

    std::shared_ptr<SQLInsertRow> insert_row1 = router_->GetInsertRow(db, insert_placeholder1, &status);
    ASSERT_EQ(status.code, 0);
    ASSERT_TRUE(insert_row1->Init(5));
    ASSERT_TRUE(insert_row1->AppendString("world"));
    ASSERT_TRUE(insert_row1->AppendInt64(1591));
    ASSERT_TRUE(insert_row1->Build());
    ok = router_->ExecuteInsert(db, insert_placeholder1, insert_row1, &status);
    ASSERT_TRUE(ok);
    rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_EQ(rs->Size(), 2);
    // world, 1591 - 1st row
    // hello, 1590
    CheckNextRow(rs, get_col1, get_col2, "world", 1591);
    CheckNextRow(rs, get_col1, get_col2, "hello", 1590);

    {
        std::shared_ptr<SQLInsertRow> insert_row2 = router_->GetInsertRow(db, insert_placeholder2, &status);
        ASSERT_EQ(status.code, 0);
        ASSERT_TRUE(insert_row2->Init(4));
        ASSERT_TRUE(insert_row2->AppendString("wrd"));
        ASSERT_FALSE(insert_row2->Build());
    }
    {
        std::shared_ptr<SQLInsertRow> insert_row2 = router_->GetInsertRow(db, insert_placeholder2, &status);
        ASSERT_EQ(status.code, 0);
        ASSERT_TRUE(insert_row2->Init(4));
        ASSERT_FALSE(insert_row2->AppendString("wordd"));
        ASSERT_FALSE(insert_row2->Build());
    }

    // ?, 1592
    std::shared_ptr<SQLInsertRow> insert_row2 = router_->GetInsertRow(db, insert_placeholder2, &status);
    ASSERT_EQ(status.code, 0);
    ASSERT_TRUE(insert_row2->Init(4));
    ASSERT_TRUE(insert_row2->AppendString("word"));
    ASSERT_TRUE(insert_row2->Build());
    ok = router_->ExecuteInsert(db, insert_placeholder2, insert_row2, &status);
    ASSERT_TRUE(ok);
    rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_EQ(rs->Size(), 3);
    // world, 1591 - 1st row
    // word, 1592 - 2nd row
    // hello, 1590
    // check 2nd row
    Step(rs, 2);
    CheckCurRow(rs, get_col1, get_col2, "word", 1592);

    // hi, ?
    std::shared_ptr<SQLInsertRow> insert_row3 = router_->GetInsertRow(db, insert_placeholder3, &status);
    ASSERT_EQ(status.code, 0);
    ASSERT_TRUE(insert_row3->Init(0));
    ASSERT_TRUE(insert_row3->AppendInt64(1593));
    ASSERT_TRUE(insert_row3->Build());
    ok = router_->ExecuteInsert(db, insert_placeholder3, insert_row3, &status);
    ASSERT_TRUE(ok);
    rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_EQ(rs->Size(), 4);
    // world, 1591 - 1st row
    // word, 1592 - 2nd row
    // hello, 1590
    // hi, 1593 - 4th row
    Step(rs, 4);
    CheckCurRow(rs, get_col1, get_col2, "hi", 1593);

    // ?,?, insert 2 rows
    std::shared_ptr<SQLInsertRows> insert_rows1 = router_->GetInsertRows(db, insert_placeholder1, &status);
    ASSERT_EQ(status.code, 0);
    std::shared_ptr<SQLInsertRow> insert_rows1_1 = insert_rows1->NewRow();
    ASSERT_TRUE(insert_rows1_1->Init(2));
    ASSERT_TRUE(insert_rows1_1->AppendString("11"));
    ASSERT_TRUE(insert_rows1_1->AppendInt64(1594));
    ASSERT_TRUE(insert_rows1_1->Build());
    std::shared_ptr<SQLInsertRow> insert_rows1_2 = insert_rows1->NewRow();
    ASSERT_TRUE(insert_rows1_2->Init(2));
    ASSERT_TRUE(insert_rows1_2->AppendString("12"));
    ASSERT_TRUE(insert_rows1_2->AppendInt64(1595));
    ASSERT_TRUE(insert_rows1_2->Build());
    ok = router_->ExecuteInsert(db, insert_placeholder1, insert_rows1, &status);
    ASSERT_TRUE(ok);
    rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_EQ(rs->Size(), 6);
    PrintRsAndReset(rs, get_col1, get_col2);
    // world, 1591 - 1st row
    // word 1592
    // hello 1590
    // 11 1594
    // hi 1593
    // 12 1595
    Step(rs, 4);
    CheckCurRow(rs, get_col1, get_col2, "11", 1594);
    Step(rs, 2);
    CheckCurRow(rs, get_col1, get_col2, "12", 1595);

    // ?, 1592 - insert 2 rows
    std::shared_ptr<SQLInsertRows> insert_rows2 = router_->GetInsertRows(db, insert_placeholder2, &status);
    ASSERT_EQ(status.code, 0);
    std::shared_ptr<SQLInsertRow> insert_rows2_1 = insert_rows2->NewRow();
    ASSERT_TRUE(insert_rows2_1->Init(2));
    ASSERT_TRUE(insert_rows2_1->AppendString("21"));
    ASSERT_TRUE(insert_rows2_1->Build());
    std::shared_ptr<SQLInsertRow> insert_rows2_2 = insert_rows2->NewRow();
    ASSERT_TRUE(insert_rows2_2->Init(2));
    ASSERT_TRUE(insert_rows2_2->AppendString("22"));
    ASSERT_TRUE(insert_rows2_2->Build());
    ok = router_->ExecuteInsert(db, insert_placeholder2, insert_rows2, &status);
    ASSERT_TRUE(ok);
    rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_EQ(rs->Size(), 8);
    PrintRsAndReset(rs, get_col1, get_col2);
    Step(rs, 5);
    CheckCurRow(rs, get_col1, get_col2, "22", 1592);
    Step(rs, 2);
    CheckCurRow(rs, get_col1, get_col2, "21", 1592);

    // hi, ?
    std::shared_ptr<SQLInsertRows> insert_rows3 = router_->GetInsertRows(db, insert_placeholder3, &status);
    ASSERT_EQ(status.code, 0);
    std::shared_ptr<SQLInsertRow> insert_rows3_1 = insert_rows3->NewRow();
    ASSERT_TRUE(insert_rows3_1->Init(0));
    ASSERT_TRUE(insert_rows3_1->AppendInt64(1596));
    ASSERT_TRUE(insert_rows3_1->Build());
    std::shared_ptr<SQLInsertRow> insert_rows3_2 = insert_rows3->NewRow();
    ASSERT_TRUE(insert_rows3_2->Init(0));
    ASSERT_TRUE(insert_rows3_2->AppendInt64(1597));
    ASSERT_TRUE(insert_rows3_2->Build());
    ok = router_->ExecuteInsert(db, insert_placeholder3, insert_rows3, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_TRUE(rs);
    PrintRsAndReset(rs, get_col1, get_col2);
    ASSERT_EQ(10, rs->Size());
    Step(rs, 6);
    CheckCurRow(rs, get_col1, get_col2, "hi", 1597);
    CheckNextRow(rs, get_col1, get_col2, "hi", 1596);

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, test_sql_insert_with_column_list) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 int, col2 int, col3 string NOT NULL, col4 "
                      "bigint NOT NULL, index(key=col3, ts=col4));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    // normal insert
    std::string insert1 = "insert into " + name + "(col3, col4) values('hello', 1000);";
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert1, &status);
    ASSERT_TRUE(ok);

    // col3 shouldn't be null
    std::string insert2 = "insert into " + name + "(col4) values(1000);";
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert2, &status);
    ASSERT_FALSE(ok);

    // col5 not exist
    std::string insert3 = "insert into " + name + "(col5) values(1000);";
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert3, &status);
    ASSERT_FALSE(ok);

    // duplicate col4
    std::string insert4 = "insert into " + name + "(col4, col4) values(1000, 1000);";
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert4, &status);
    ASSERT_FALSE(ok);

    // normal placeholder insert
    std::string insert5 = "insert into " + name + "(col2, col3, col4) values(?, 'hello', ?);";
    std::shared_ptr<SQLInsertRow> r5 = router_->GetInsertRow(db, insert5, &status);
    ASSERT_TRUE(r5->Init(0));
    ASSERT_TRUE(r5->AppendInt32(123));
    ASSERT_TRUE(r5->AppendInt64(1001));
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert5, r5, &status);
    ASSERT_TRUE(ok);

    // todo: if placeholders are out of order. eg: insert into [table] (col4,
    // col3, col2) value (?, 'hello', ?);

    std::string select = "select * from " + name + ";";
    status = ::hybridse::sdk::Status();
    auto rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_FALSE(rs == nullptr) << status.msg << "\n" << status.trace;

    ASSERT_EQ(2, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_TRUE(rs->IsNULL(0));
    ASSERT_EQ(123, rs->GetInt32Unsafe(1));
    ASSERT_EQ("hello", rs->GetStringUnsafe(2));
    ASSERT_EQ(1001, rs->GetInt64Unsafe(3));

    ASSERT_TRUE(rs->Next());
    ASSERT_TRUE(rs->IsNULL(0));
    ASSERT_TRUE(rs->IsNULL(1));
    ASSERT_EQ("hello", rs->GetStringUnsafe(2));
    ASSERT_EQ(1000, rs->GetInt64Unsafe(3));

    ASSERT_FALSE(rs->Next());

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, testSqlInsertPlaceholderWithDateColumnKey) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 int, col2 date NOT NULL, col3 "
                      "bigint NOT NULL, index(key=col2, ts=col3));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    std::string insert1 = "insert into " + name + " values(?, ?, ?);";
    std::shared_ptr<SQLInsertRow> r1 = router_->GetInsertRow(db, insert1, &status);
    ASSERT_FALSE(r1 == nullptr);
    ASSERT_TRUE(r1->Init(0));
    ASSERT_TRUE(r1->AppendInt32(123));
    ASSERT_TRUE(r1->AppendDate(2020, 7, 22));
    ASSERT_TRUE(r1->AppendInt64(1000));
    ok = router_->ExecuteInsert(db, insert1, r1, &status);
    ASSERT_TRUE(ok);
    std::string select = "select * from " + name + ";";
    auto rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_FALSE(rs == nullptr);
    ASSERT_EQ(1, rs->Size());
    int32_t year;
    int32_t month;
    int32_t day;

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(123, rs->GetInt32Unsafe(0));
    ASSERT_TRUE(rs->GetDate(1, &year, &month, &day));
    ASSERT_EQ(2020, year);
    ASSERT_EQ(7, month);
    ASSERT_EQ(22, day);
    ASSERT_EQ(1000, rs->GetInt64Unsafe(2));

    ASSERT_FALSE(rs->Next());

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, testSqlInsertPlaceholderWithColumnKey1) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 int, col2 int NOT NULL, col3 string NOT NULL, col4 "
                      "bigint NOT NULL, index(key=(col2, col3), ts=col4));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    std::string insert1 = "insert into " + name + " values(?, ?, ?, ?);";
    std::shared_ptr<SQLInsertRow> r1 = router_->GetInsertRow(db, insert1, &status);
    ASSERT_FALSE(r1 == nullptr);
    ASSERT_TRUE(r1->Init(5));
    ASSERT_TRUE(r1->AppendInt32(123));
    ASSERT_TRUE(r1->AppendInt32(321));
    ASSERT_TRUE(r1->AppendString("hello"));
    ASSERT_TRUE(r1->AppendInt64(1000));
    ok = router_->ExecuteInsert(db, insert1, r1, &status);
    ASSERT_TRUE(ok);

    std::string insert2 = "insert into " + name + " values(?, ?, 'hello', ?);";
    std::shared_ptr<SQLInsertRow> r2 = router_->GetInsertRow(db, insert2, &status);
    ASSERT_FALSE(r2 == nullptr);
    ASSERT_TRUE(r2->Init(0));
    ASSERT_TRUE(r2->AppendInt32(456));
    ASSERT_TRUE(r2->AppendInt32(654));
    ASSERT_TRUE(r2->AppendInt64(1001));
    ok = router_->ExecuteInsert(db, insert2, r2, &status);
    ASSERT_TRUE(ok);
    std::string insert3 = "insert into " + name + " values(?, 987, ?, ?);";
    std::shared_ptr<SQLInsertRow> r3 = router_->GetInsertRow(db, insert3, &status);
    ASSERT_FALSE(r3 == nullptr);
    ASSERT_TRUE(r3->Init(5));
    ASSERT_TRUE(r3->AppendInt32(789));
    ASSERT_TRUE(r3->AppendString("hello"));
    ASSERT_TRUE(r3->AppendInt64(1002));
    ok = router_->ExecuteInsert(db, insert3, r3, &status);
    ASSERT_TRUE(ok);

    std::string insert4 = "insert into " + name + " values(?, 0,'hello', ?);";
    std::shared_ptr<SQLInsertRow> r4 = router_->GetInsertRow(db, insert4, &status);
    ASSERT_FALSE(r4 == nullptr);
    ASSERT_TRUE(r4->Init(0));
    ASSERT_TRUE(r4->AppendInt32(1));
    ASSERT_TRUE(r4->AppendInt64(1003));
    ok = router_->ExecuteInsert(db, insert4, r4, &status);
    ASSERT_TRUE(ok);

    std::string select = "select * from " + name + ";";
    auto rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_FALSE(rs == nullptr);
    ASSERT_EQ(4, rs->Size());

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(1, rs->GetInt32Unsafe(0));
    ASSERT_EQ(0, rs->GetInt32Unsafe(1));
    ASSERT_EQ("hello", rs->GetStringUnsafe(2));
    ASSERT_EQ(rs->GetInt64Unsafe(3), 1003);

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(456, rs->GetInt32Unsafe(0));
    ASSERT_EQ(654, rs->GetInt32Unsafe(1));
    ASSERT_EQ("hello", rs->GetStringUnsafe(2));
    ASSERT_EQ(rs->GetInt64Unsafe(3), 1001);

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(123, rs->GetInt32Unsafe(0));
    ASSERT_EQ(321, rs->GetInt32Unsafe(1));
    ASSERT_EQ("hello", rs->GetStringUnsafe(2));
    ASSERT_EQ(rs->GetInt64Unsafe(3), 1000);

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(789, rs->GetInt32Unsafe(0));
    ASSERT_EQ(987, rs->GetInt32Unsafe(1));
    ASSERT_EQ("hello", rs->GetStringUnsafe(2));
    ASSERT_EQ(rs->GetInt64Unsafe(3), 1002);

    ASSERT_FALSE(rs->Next());

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, testSqlInsertPlaceholderWithColumnKey2) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string NOT NULL, col2 bigint NOT NULL, col3 date NOT NULL, col4 "
                      "int NOT NULL, index(key=(col1, col4), ts=col2));";
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    std::string insert1 = "insert into " + name + " values(?, ?, ?, ?);";
    status = ::hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r1 = router_->GetInsertRow(db, insert1, &status);
    ASSERT_TRUE(r1->Init(5));
    ASSERT_TRUE(r1->AppendString("hello"));
    ASSERT_TRUE(r1->AppendInt64(1000));
    ASSERT_TRUE(r1->AppendDate(2020, 7, 13));
    ASSERT_TRUE(r1->AppendInt32(123));
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert1, r1, &status);
    ASSERT_TRUE(ok);

    std::string insert2 = "insert into " + name + " values('hello', ?, ?, ?);";
    status = ::hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r2 = router_->GetInsertRow(db, insert2, &status);
    ASSERT_TRUE(r2->Init(0));
    ASSERT_TRUE(r2->AppendInt64(1001));
    ASSERT_TRUE(r2->AppendDate(2020, 7, 20));
    ASSERT_TRUE(r2->AppendInt32(456));
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert2, r2, &status);
    ASSERT_TRUE(ok);

    std::string insert3 = "insert into " + name + " values(?, ?, ?, 789);";
    status = ::hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r3 = router_->GetInsertRow(db, insert3, &status);
    ASSERT_TRUE(r3->Init(5));
    ASSERT_TRUE(r3->AppendString("hello"));
    ASSERT_TRUE(r3->AppendInt64(1002));
    ASSERT_TRUE(r3->AppendDate(2020, 7, 22));
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert3, r3, &status);
    ASSERT_TRUE(ok);

    std::string insert4 = "insert into " + name + " values('hello', ?, ?, 000);";
    status = ::hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r4 = router_->GetInsertRow(db, insert4, &status);
    ASSERT_TRUE(r4->Init(0));
    ASSERT_TRUE(r4->AppendInt64(1003));
    ASSERT_TRUE(r4->AppendDate(2020, 7, 22));
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert4, r4, &status);
    ASSERT_TRUE(ok);

    std::string insert5 = "insert into " + name + " values('hello', 1004, '2020-07-31', 001);";
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert5, &status);
    ASSERT_TRUE(ok);

    std::string insert6 = "insert into " + name + " values('hello', 1004, '2020-07-31', ?);";
    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert6, &status);
    ASSERT_FALSE(ok);

    int32_t year;
    int32_t month;
    int32_t day;
    std::string select = "select * from " + name + ";";
    status = ::hybridse::sdk::Status();
    auto rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_TRUE(nullptr != rs) << status.msg;
    ASSERT_EQ(5, rs->Size());

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1001);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 7);
    ASSERT_EQ(day, 20);
    ASSERT_EQ(456, rs->GetInt32Unsafe(3));

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1000);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 7);
    ASSERT_EQ(day, 13);
    ASSERT_EQ(123, rs->GetInt32Unsafe(3));

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1004);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 7);
    ASSERT_EQ(day, 31);
    ASSERT_EQ(1, rs->GetInt32Unsafe(3));

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1003);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 7);
    ASSERT_EQ(day, 22);
    ASSERT_EQ(0, rs->GetInt32Unsafe(3));

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1002);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 7);
    ASSERT_EQ(day, 22);
    ASSERT_EQ(789, rs->GetInt32Unsafe(3));

    ASSERT_FALSE(rs->Next());

    status = ::hybridse::sdk::Status();
    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, test_sql_insert_placeholder_with_type_check) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string NOT NULL, col2 bigint NOT NULL, col3 date NOT NULL, col4 "
                      "int, col5 smallint, col6 float, col7 double,"
                      "index(key=col1, ts=col2));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());

    // test null
    std::string insert1 = "insert into " + name + " values(?, ?, ?, ?, ?, ?, ?);";

    status = hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r1 = router_->GetInsertRow(db, insert1, &status);

    // test schema
    std::shared_ptr<hybridse::sdk::Schema> schema = r1->GetSchema();
    ASSERT_EQ(schema->GetColumnCnt(), 7);
    ASSERT_EQ(schema->GetColumnName(0), "col1");
    ASSERT_EQ(schema->GetColumnType(0), hybridse::sdk::kTypeString);
    ASSERT_EQ(schema->GetColumnName(1), "col2");
    ASSERT_EQ(schema->GetColumnType(1), hybridse::sdk::kTypeInt64);
    ASSERT_EQ(schema->GetColumnName(2), "col3");
    ASSERT_EQ(schema->GetColumnType(2), hybridse::sdk::kTypeDate);
    ASSERT_EQ(schema->GetColumnName(3), "col4");
    ASSERT_EQ(schema->GetColumnType(3), hybridse::sdk::kTypeInt32);
    ASSERT_EQ(schema->GetColumnName(4), "col5");
    ASSERT_EQ(schema->GetColumnType(4), hybridse::sdk::kTypeInt16);
    ASSERT_EQ(schema->GetColumnName(5), "col6");
    ASSERT_EQ(schema->GetColumnType(5), hybridse::sdk::kTypeFloat);
    ASSERT_EQ(schema->GetColumnName(6), "col7");
    ASSERT_EQ(schema->GetColumnType(6), hybridse::sdk::kTypeDouble);

    ASSERT_TRUE(r1->Init(5));
    ASSERT_TRUE(r1->AppendString("hello"));
    ASSERT_TRUE(r1->AppendInt64(1000));
    ASSERT_FALSE(r1->AppendNULL());
    ASSERT_TRUE(r1->AppendDate(2020, 7, 13));
    ASSERT_TRUE(r1->AppendNULL());
    // appendnull automatically
    status = hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert1, r1, &status);
    ASSERT_TRUE(ok);

    // test int convert and float convert
    std::string insert2 = "insert into " + name + " values('hello', ?, '2020-02-29', NULL, 123, 2.33, NULL);";
    status = hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r2 = router_->GetInsertRow(db, insert2, &status);
    ASSERT_EQ(status.code, 0);
    ASSERT_TRUE(r2->Init(0));
    ASSERT_TRUE(r2->AppendInt64(1001));
    status = hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert2, r2, &status);
    ASSERT_TRUE(ok);

    // test int to float
    std::string insert3 = "insert into " + name + " values('hello', ?, '2020-12-31', NULL, NULL, 123, 123);";
    status = hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r3 = router_->GetInsertRow(db, insert3, &status);
    ASSERT_EQ(status.code, 0);
    ASSERT_TRUE(r3->Init(0));
    ASSERT_TRUE(r3->AppendInt64(1002));
    status = hybridse::sdk::Status();
    ok = router_->ExecuteInsert(db, insert3, r3, &status);
    ASSERT_TRUE(ok);

    // test float to int
    std::string insert4 = "insert into " + name + " values('hello', ?, '2020-02-29', 2.33, 2.33, 123, 123);";
    status = hybridse::sdk::Status();
    std::shared_ptr<SQLInsertRow> r4 = router_->GetInsertRow(db, insert4, &status);
    ASSERT_EQ(status.code, hybridse::common::StatusCode::kCmdError) << status.ToString();

    int32_t year;
    int32_t month;
    int32_t day;
    std::string select = "select * from " + name + ";";
    status = hybridse::sdk::Status();
    auto rs = router_->ExecuteSQL(db, select, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_EQ(3, rs->Size());

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1002);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 12);
    ASSERT_EQ(day, 31);
    ASSERT_TRUE(rs->IsNULL(3));
    ASSERT_TRUE(rs->IsNULL(4));
    ASSERT_FLOAT_EQ(rs->GetFloatUnsafe(5), 123.0);
    ASSERT_DOUBLE_EQ(rs->GetDoubleUnsafe(6), 123.0);

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1001);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 2);
    ASSERT_EQ(day, 29);
    ASSERT_TRUE(rs->IsNULL(3));
    ASSERT_EQ(rs->GetInt16Unsafe(4), 123);
    ASSERT_FLOAT_EQ(rs->GetFloatUnsafe(5), 2.33);
    ASSERT_TRUE(rs->IsNULL(6));

    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    ASSERT_EQ(rs->GetInt64Unsafe(1), 1000);
    ASSERT_TRUE(rs->GetDate(2, &year, &month, &day));
    ASSERT_EQ(year, 2020);
    ASSERT_EQ(month, 7);
    ASSERT_EQ(day, 13);
    ASSERT_TRUE(rs->IsNULL(3));
    ASSERT_TRUE(rs->IsNULL(4));
    ASSERT_TRUE(rs->IsNULL(5));
    ASSERT_TRUE(rs->IsNULL(6));

    ASSERT_FALSE(rs->Next());

    status = hybridse::sdk::Status();
    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    status = hybridse::sdk::Status();
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, smoketest_on_sql) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    std::string insert = "insert into " + name + " values('hello', 1590);";
    ok = router_->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    std::string sql_select = "select col1 from " + name + " ;";
    auto rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_TRUE(rs != nullptr);
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ("hello", rs->GetStringUnsafe(0));
    std::string sql_window_batch = "select sum(col2) over w from " + name + " window w as (partition by " + name +
                                   ".col1 order by " + name + ".col2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";
    rs = router_->ExecuteSQL(db, sql_window_batch, &status);
    ASSERT_EQ(1, rs->Size());
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(1590, rs->GetInt64Unsafe(0));
    {
        std::shared_ptr<SQLRequestRow> row = router_->GetRequestRow(db, sql_window_batch, &status);
        ASSERT_TRUE(row != nullptr);
        ASSERT_EQ(2, row->GetSchema()->GetColumnCnt());
        ASSERT_TRUE(row->Init(5));
        ASSERT_TRUE(row->AppendString("hello"));
        ASSERT_TRUE(row->AppendInt64(100));
        ASSERT_TRUE(row->Build());

        std::string sql_window_request = "select sum(col2)  over w as sum_col2 from " + name +
                                         " window w as (partition by " + name + ".col1 order by " + name +
                                         ".col2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

        rs = router_->ExecuteSQLRequest(db, sql_window_request, row, &status);
        ASSERT_TRUE(rs != nullptr);
        ASSERT_EQ(1, rs->Size());
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ(100, rs->GetInt64Unsafe(0));
    }
    {
        std::shared_ptr<SQLRequestRow> row = router_->GetRequestRow(db, sql_window_batch, &status);
        ASSERT_TRUE(row != nullptr);
        ASSERT_EQ(2, row->GetSchema()->GetColumnCnt());
        ASSERT_TRUE(row->Init(5));
        ASSERT_TRUE(row->AppendString("hello"));
        ASSERT_TRUE(row->AppendInt64(100));
        ASSERT_TRUE(row->Build());

        std::string sql_window_request = "select sum(col2)  over w as sum_col2 from " + name +
                                         " window w as (partition by " + name + ".col1 order by " + name +
                                         ".col2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);";

        rs = router_->ExecuteSQLRequest(db, sql_window_request, row, &status);
        ASSERT_TRUE(rs != nullptr);
        ASSERT_EQ(1, rs->Size());
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ(100, rs->GetInt64Unsafe(0));
    }

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, smoke_explain_on_sql) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 timestamp, col3 date,"
                      "index(key=col1, ts=col2));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    std::string insert = "insert into " + name + " values('hello', 1591174600000l, '2020-06-03');";
    ok = router_->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    std::string sql_select = "select * from " + name + " ;";
    auto explain = router_->Explain(db, sql_select, &status);
    ASSERT_TRUE(explain != nullptr);

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, smoke_not_null) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 timestamp, col3 date not null,"
                      "index(key=col1, ts=col2));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    std::string insert = "insert into " + name + " values('hello', 1591174600000l, null);";
    ok = router_->ExecuteInsert(db, insert, &status);
    ASSERT_FALSE(ok);

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, smoketimestamptest_on_sql) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 timestamp, col3 date,"
                      "index(key=col1, ts=col2));";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);

    ASSERT_TRUE(router_->RefreshCatalog());
    std::string insert = "insert into " + name + " values('hello', 1591174600000l, '2020-06-03');";
    ok = router_->ExecuteInsert(db, insert, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    std::string sql_select = "select * from " + name + " ;";
    auto rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_TRUE(rs != nullptr);
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

    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, smoketest_on_muti_partitions) {
    std::string name = "test" + GenRand();
    std::string db = "db" + GenRand();
    ::hybridse::sdk::Status status;
    bool ok = router_->CreateDB(db, &status);
    ASSERT_TRUE(ok);
    auto endpoints = mc_->GetTbEndpoint();
    std::string ddl = "create table " + name +
                      "("
                      "col1 string, col2 bigint,"
                      "index(key=col1, ts=col2)) options(partitionnum=8);";
    ok = router_->ExecuteDDL(db, ddl, &status);
    ASSERT_TRUE(ok);
    ASSERT_TRUE(router_->RefreshCatalog());
    for (int i = 0; i < 100; i++) {
        std::string key = "'hello" + std::to_string(i) + "'";
        std::string insert = "insert into " + name + " values(" + key + ", 1590);";
        ok = router_->ExecuteInsert(db, insert, &status);
        ASSERT_TRUE(ok);
    }
    ASSERT_TRUE(router_->RefreshCatalog());
    std::string sql_select = "select col1 from " + name + " ;";
    auto rs = router_->ExecuteSQL(db, sql_select, &status);
    ASSERT_TRUE(rs != nullptr);
    ASSERT_EQ(100, rs->Size());
    ASSERT_TRUE(rs->Next());
    ok = router_->ExecuteDDL(db, "drop table " + name + ";", &status);
    ASSERT_TRUE(ok);
    ok = router_->DropDB(db, &status);
    ASSERT_TRUE(ok);
}

TEST_F(SQLRouterTest, testGetHoleIdx) {
    ::hybridse::sdk::Status status;
    router_->ExecuteSQL("create database if not exists " + db_, &status);
    ASSERT_TRUE(status.IsOK());
    std::string name("get_hold_idx_test");
    router_->ExecuteSQL(db_, "create table if not exists " + name + "(c1 int, c2 string, c3 timestamp)", &status);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(router_->RefreshCatalog());

    // test
    std::string insert1 = "insert into " + name + "(c3,c2,c1) values(?, 'abc', ?);";
    auto r1 = router_->GetInsertRow(db_, insert1, &status);
    ASSERT_TRUE(r1);
    auto hole_vec = r1->GetHoleIdx();
    ASSERT_EQ(hole_vec.size(), 2);
    ASSERT_THAT(hole_vec, ::testing::ElementsAre(2, 0));

    std::string insert2 = "insert into " + name + "(c3,c2,c1) values(?, 'abc', 123456);";
    auto r2 = router_->GetInsertRow(db_, insert2, &status);
    ASSERT_TRUE(r2);
    hole_vec = r2->GetHoleIdx();
    ASSERT_EQ(hole_vec.size(), 1);
    ASSERT_THAT(hole_vec, ::testing::ElementsAre(2));
}

class TableSchemaBuilder {
 public:
    explicit TableSchemaBuilder(std::string table_name) : table_name_(std::move(table_name)) {}
    TableSchemaBuilder& AddCol(const std::string& name, hybridse::sdk::DataType type) {
        cols_.emplace_back(name, type);
        return *this;
    }

    std::string table_name_;
    std::vector<std::pair<std::string, hybridse::sdk::DataType>> cols_;
};

TEST_F(SQLRouterTest, DDLParseMethods) {
    std::string sql =
        "SELECT\n behaviourTable.itemId as itemId,\n  behaviourTable.ip as ip,\n  behaviourTable.query as query,\n  "
        "behaviourTable.mcuid as mcuid,\n adinfo.brandName as name,\n  adinfo.brandId as brandId,\n "
        "feedbackTable.actionValue as label\n FROM behaviourTable\n LAST JOIN feedbackTable ON feedbackTable.itemId = "
        "behaviourTable.itemId\n LAST JOIN adinfo ON behaviourTable.itemId = adinfo.id;";

    std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>> table_map;
    // table a
    {
        TableSchemaBuilder builder("behaviourTable");
        builder.AddCol("itemId", hybridse::sdk::DataType::kTypeString)
            .AddCol("reqId", hybridse::sdk::DataType::kTypeString)
            .AddCol("tags", hybridse::sdk::DataType::kTypeString)
            .AddCol("ip", hybridse::sdk::DataType::kTypeString)
            .AddCol("query", hybridse::sdk::DataType::kTypeString)
            .AddCol("instanceKey", hybridse::sdk::DataType::kTypeString)
            .AddCol("eventTime", hybridse::sdk::DataType::kTypeTimestamp)
            .AddCol("browser", hybridse::sdk::DataType::kTypeString)
            .AddCol("mcuid", hybridse::sdk::DataType::kTypeString)
            .AddCol("weight", hybridse::sdk::DataType::kTypeDouble)
            .AddCol("page", hybridse::sdk::DataType::kTypeInt32)
            .AddCol("rank", hybridse::sdk::DataType::kTypeInt32)
            .AddCol("_i_rank", hybridse::sdk::DataType::kTypeString);
        table_map.emplace_back(builder.table_name_, builder.cols_);
    }

    // table b
    {
        TableSchemaBuilder builder("adinfo");
        builder.AddCol("id", hybridse::sdk::DataType::kTypeString)
            .AddCol("brandName", hybridse::sdk::DataType::kTypeString)
            .AddCol("brandId", hybridse::sdk::DataType::kTypeString)
            .AddCol("name", hybridse::sdk::DataType::kTypeString)
            .AddCol("ingestionTime", hybridse::sdk::DataType::kTypeTimestamp);
        table_map.emplace_back(builder.table_name_, builder.cols_);
    }

    // table c
    {
        TableSchemaBuilder builder("feedbackTable");
        builder.AddCol("itemId", hybridse::sdk::DataType::kTypeString)
            .AddCol("reqId", hybridse::sdk::DataType::kTypeString)
            .AddCol("instanceKey", hybridse::sdk::DataType::kTypeString)
            .AddCol("eventTime", hybridse::sdk::DataType::kTypeTimestamp)
            .AddCol("ingestionTime", hybridse::sdk::DataType::kTypeTimestamp)
            .AddCol("actionValue", hybridse::sdk::DataType::kTypeDouble);
        table_map.emplace_back(builder.table_name_, builder.cols_);
    }

    std::vector<std::string> ddl_list = GenDDL(sql, table_map);
    ASSERT_EQ(3, ddl_list.size());
    // sorted by table name, so adinfo is the first table
    EXPECT_EQ(
        "CREATE TABLE IF NOT EXISTS adinfo(\n\tid string,\n\tbrandName string,\n\tbrandId string,\n\tname "
        "string,\n\tingestionTime timestamp,\n\tindex(key=(id), ttl=1, ttl_type=latest)\n);",
        ddl_list.at(0));

    auto output_schema = GenOutputSchema(sql, "foo", {{"foo", table_map}});
    ASSERT_EQ(output_schema->GetColumnCnt(), 7);
    std::vector<std::string> output_col_names;
    output_col_names.reserve(output_schema->GetColumnCnt());
    for (auto i = 0; i < output_schema->GetColumnCnt(); ++i) {
        output_col_names.push_back(output_schema->GetColumnName(i));
    }
    std::vector<std::string> expected{"itemId", "ip", "query", "mcuid", "name", "brandId", "label"};
    ASSERT_EQ(output_col_names, expected);

    // multi db output schema
    std::vector<std::pair<std::string, decltype(table_map)>> db_table_map;
    db_table_map.emplace_back("db1", table_map);
    db_table_map.emplace_back("db2", table_map);
    // feedbackTable is in db1(used_db)
    auto multi_db_output_schema = GenOutputSchema(
        "SELECT\n db1.behaviourTable.itemId as itemId,\n  db1.behaviourTable.ip as ip,\n  db1.behaviourTable.query as "
        "query,\n  "
        "db1.behaviourTable.mcuid as mcuid,\n db2.adinfo.brandName as name,\n  db2.adinfo.brandId as brandId,\n "
        "feedbackTable.actionValue as label\n FROM db1.behaviourTable\n LAST JOIN feedbackTable ON "
        "feedbackTable.itemId = "
        "behaviourTable.itemId\n LAST JOIN db2.adinfo ON behaviourTable.itemId = db2.adinfo.id;",
        "db1", db_table_map);
    ASSERT_TRUE(multi_db_output_schema);
    ASSERT_EQ(multi_db_output_schema->GetColumnCnt(), 7);

    // no use db
    auto db_table_sql =
        "SELECT\n db1.behaviourTable.itemId as itemId,\n  db1.behaviourTable.ip as ip,\n  db1.behaviourTable.query as "
        "query,\n  "
        "db1.behaviourTable.mcuid as mcuid,\n db2.adinfo.brandName as name,\n  db2.adinfo.brandId as brandId,\n "
        "feedbackTable.actionValue as label\n FROM db1.behaviourTable behaviourTable\n LAST JOIN db1.feedbackTable "
        "feedbackTable ON feedbackTable.itemId = behaviourTable.itemId\n LAST JOIN db2.adinfo ON behaviourTable.itemId "
        "= db2.adinfo.id;";
    multi_db_output_schema = GenOutputSchema(db_table_sql, "", db_table_map);
    ASSERT_TRUE(multi_db_output_schema);
    ASSERT_EQ(multi_db_output_schema->GetColumnCnt(), 7);

    auto tables = GetDependentTables(db_table_sql, "", db_table_map);
    ASSERT_EQ(tables.size(), 3);
}

TEST_F(SQLRouterTest, DDLParseMethodsCombineIndex) {
    std::string sql =
        "select reqId as reqId_75, \n"
        "max(`fWatchedTimeLen`) over bo_hislabel_zUserId_uUserId_ingestionTime_1s_172801s_100 "
        "as bo_hislabel_fWatchedTimeLen_multi_max_74, \n"
        "avg(`fWatchedTimeLen`) over bo_hislabel_zUserId_uUserId_ingestionTime_1s_172801s_100 "
        "as bo_hislabel_fWatchedTimeLen_multi_avg_75 \n"
        "from \n"
        "(select `eventTime` as `ingestionTime`, `zUserId` as `zUserId`, `uUserId` as `uUserId`, "
        "timestamp('2019-07-18 09:20:20') as `nRequestTime`, double(0) as `fWatchedTimeLen`, "
        "reqId from `flattenRequest`) \n"
        "window bo_hislabel_zUserId_uUserId_ingestionTime_1s_172801s_100 as ( \n"
        "UNION (select `ingestionTime`, `zUserId`, `uUserId`, `nRequestTime`, `fWatchedTimeLen`, "
        "'' as reqId from `bo_hislabel`) \n"
        "partition by `zUserId`,`uUserId` order by `ingestionTime` "
        "rows_range between 172800999 preceding and 1s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW);";
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, hybridse::sdk::DataType>>>> table_map;
    {
        TableSchemaBuilder builder("flattenRequest");
        builder.AddCol("reqId", hybridse::sdk::DataType::kTypeString)
            .AddCol("eventTime", hybridse::sdk::DataType::kTypeTimestamp)
            .AddCol("index1", hybridse::sdk::DataType::kTypeString)
            .AddCol("uUserId", hybridse::sdk::DataType::kTypeString)
            .AddCol("zUserId", hybridse::sdk::DataType::kTypeString);
        table_map.emplace_back(builder.table_name_, builder.cols_);
    }
    {
        TableSchemaBuilder builder("bo_hislabel");
        builder.AddCol("ingestionTime", hybridse::sdk::DataType::kTypeTimestamp)
            .AddCol("zUserId", hybridse::sdk::DataType::kTypeString)
            .AddCol("uUserId", hybridse::sdk::DataType::kTypeString)
            .AddCol("nRequestTime", hybridse::sdk::DataType::kTypeTimestamp)
            .AddCol("fWatchedTimeLen", hybridse::sdk::DataType::kTypeDouble);
        table_map.emplace_back(builder.table_name_, builder.cols_);
    }

    std::vector<std::string> ddl_list = GenDDL(sql, table_map);
    ASSERT_EQ(2, ddl_list.size());
    EXPECT_EQ(
        "CREATE TABLE IF NOT EXISTS bo_hislabel(\n\tingestionTime timestamp,\n\tzUserId string,\n\tuUserId string,\n\t"
        "nRequestTime timestamp,\n\tfWatchedTimeLen double,\n\t"
        "index(key=(uUserId,zUserId), ttl=2881m, ttl_type=absolute, ts=`ingestionTime`)\n);",
        ddl_list.at(0));
}

TEST_F(SQLRouterTest, SQLToDAG) {
    auto sql = R"(WITH q1 as (
        WITH q3 as (select * from t1 ORDER BY ts),
        q4 as (select * from t2 LIMIT 10)

        select * from q3 left join q4 on q3.key = q4.key
        ),
        q2 as (select * from t3)

        select * from q1 last join q2 on q1.id = q2.id)";


    hybridse::sdk::Status status;
    auto dag = router_->SQLToDAG(sql, &status);
    ASSERT_TRUE(status.IsOK());

    std::string_view q3 = R"(SELECT
  *
FROM
  t1
ORDER BY ts
)";
    std::string_view q4 = R"(SELECT
  *
FROM
  t2
LIMIT 10
)";
    std::string_view q2 = R"(SELECT
  *
FROM
  t3
)";
    std::string_view q1 = R"(SELECT
  *
FROM
  q3
  LEFT JOIN
  q4
  ON q3.key = q4.key
)";
    std::string_view q = R"(SELECT
  *
FROM
  q1
  LAST JOIN
  q2
  ON q1.id = q2.id
)";

    std::shared_ptr<DAGNode> dag_q3 = std::make_shared<DAGNode>("q3", q3);
    std::shared_ptr<DAGNode> dag_q4 = std::make_shared<DAGNode>("q4", q4);

    std::shared_ptr<DAGNode> dag_q1 =
        std::make_shared<DAGNode>("q1", q1, std::vector<std::shared_ptr<DAGNode>>({dag_q3, dag_q4}));
    std::shared_ptr<DAGNode> dag_q2 = std::make_shared<DAGNode>("q2", q2);

    std::shared_ptr<DAGNode> expect =
        std::make_shared<DAGNode>("", q, std::vector<std::shared_ptr<DAGNode>>({dag_q1, dag_q2}));

    EXPECT_EQ(*dag, *expect);
}

}  // namespace openmldb::sdk

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();

    ::openmldb::base::SetupGlog(true);
    FLAGS_zk_session_timeout = 100000;
    ::openmldb::sdk::MiniCluster mc(6181);
    ::openmldb::sdk::mc_ = &mc;
    int ok = ::openmldb::sdk::mc_->SetUp(1);
    sleep(1);
    srand(time(nullptr));
    ok = RUN_ALL_TESTS();
    ::openmldb::sdk::mc_->Close();
    return ok;
}
