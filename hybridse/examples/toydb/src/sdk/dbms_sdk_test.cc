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

#include "sdk/dbms_sdk.h"
#include <unistd.h>
#include <map>
#include <string>
#include <utility>
#include "base/texttable.h"
#include "brpc/server.h"
#include "case/sql_case.h"
#include "dbms/dbms_server_impl.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "tablet/tablet_server_impl.h"

DECLARE_string(dbms_endpoint);
DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_bool(enable_keep_alive);

namespace hybridse {
namespace sdk {
using hybridse::sqlcase::SqlCase;
class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};
class DBMSSdkTest : public ::testing::TestWithParam<SqlCase> {
 public:
    DBMSSdkTest()
        : dbms_server_(), tablet_server_(), tablet_(NULL), dbms_(NULL) {}
    ~DBMSSdkTest() {}
    void SetUp() {
        brpc::ServerOptions options;
        tablet_ = new tablet::TabletServerImpl();
        tablet_->Init();
        tablet_server_.AddService(tablet_, brpc::SERVER_DOESNT_OWN_SERVICE);
        tablet_server_.Start(tablet_port, &options);
        dbms_ = new ::hybridse::dbms::DBMSServerImpl();
        dbms_server_.AddService(dbms_, brpc::SERVER_DOESNT_OWN_SERVICE);
        dbms_server_.Start(dbms_port, &options);
        {
            std::string tablet_endpoint =
                "127.0.0.1:" + std::to_string(tablet_port);
            MockClosure closure;
            dbms::KeepAliveRequest request;
            request.set_endpoint(tablet_endpoint);
            dbms::KeepAliveResponse response;
            dbms_->KeepAlive(NULL, &request, &response, &closure);
        }
    }

    void TearDown() {
        dbms_server_.Stop(10);
        tablet_server_.Stop(10);
        delete tablet_;
        delete dbms_;
    }

 public:
    brpc::Server dbms_server_;
    brpc::Server tablet_server_;
    int tablet_port = 7212;
    int dbms_port = 7211;
    tablet::TabletServerImpl *tablet_;
    dbms::DBMSServerImpl *dbms_;
};

TEST_F(DBMSSdkTest, DatabasesAPITest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);
    {
        Status status;
        std::vector<std::string> names = dbms_sdk->GetDatabases(&status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0u, names.size());
    }

    // create database db1
    {
        Status status;
        std::string name = "db_1";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    // create database db2
    {
        Status status;
        std::string name = "db_2xxx";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // create database db3
    {
        Status status;
        std::string name = "db_3";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // get databases
        Status status;
        std::vector<std::string> names = dbms_sdk->GetDatabases(&status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(3u, names.size());
    }
}

TEST_F(DBMSSdkTest, TableAPITest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);
    // create database db1
    {
        Status status;
        std::string name = "db_1";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    // create database db2
    {
        Status status;
        std::string name = "db_2x";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // create table test1
        std::string sql =
            "create table test1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int,\n"
            "    index(key=(column4, column3), ts=column2, ttl=60d)\n"
            ");";
        std::string name = "db_1";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // create table test2
        std::string sql =
            "create table IF NOT EXISTS test2(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column1), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table test3
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // show db_1 tables
        std::string name = "db_1";
        Status status;
        std::shared_ptr<TableSet> tablet_set =
            dbms_sdk->GetTables(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(3u, tablet_set->Size());
    }
    {
        // show tables empty
        std::string name = "db_2x";
        Status status;
        std::shared_ptr<TableSet> ts = dbms_sdk->GetTables(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(0u, ts->Size());
    }
}

TEST_F(DBMSSdkTest, GetInputSchema_ns_not_exist) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);
    std::string name = "db_x123";
    {
        Status status;
        // select
        std::string sql = "select column1 from test3;";
        dbms_sdk->GetInputSchema(name, sql, &status);
        ASSERT_EQ(common::kTableNotFound, static_cast<int>(status.code));
        LOG(INFO) << status.msg << "\n" << status.trace;
    }
}

TEST_F(DBMSSdkTest, request_mode) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);
    std::string name = "db_x123";
    {
        Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        Status status;
        // create table db1
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 bigint NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=column4, ts=column2)\n"
            ");";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // insert
        std::string sql = "insert into test3 values(1, 4000, 2, \"hello\", 3);";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // select
        std::string sql = "select column1 + 5 from test3;";
        std::shared_ptr<RequestRow> row =
            dbms_sdk->GetRequestRow(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        std::string column4 = "hello";
        ASSERT_EQ(5, row->GetSchema()->GetColumnCnt());
        ASSERT_TRUE(row->Init(column4.size()));
        ASSERT_TRUE(row->AppendInt32(32));
        ASSERT_TRUE(row->AppendInt64(64));
        ASSERT_TRUE(row->AppendInt32(32));
        ASSERT_TRUE(row->AppendString(column4));
        ASSERT_TRUE(row->AppendInt32(32));
        ASSERT_TRUE(row->Build());
        std::shared_ptr<ResultSet> rs =
            dbms_sdk->ExecuteQuery(name, sql, row, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(1, rs->Size());
        ASSERT_TRUE(rs->Next());
        ASSERT_EQ(37, rs->GetInt32Unsafe(0));
    }
}

TEST_F(DBMSSdkTest, GetInputSchema_table_not_exist) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);

    std::string name = "db_x12";
    {
        Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // select
        std::string sql = "select column1 from test3;";
        dbms_sdk->GetInputSchema(name, sql, &status);
        ASSERT_EQ(common::kTableNotFound, static_cast<int>(status.code));
    }
}

TEST_F(DBMSSdkTest, GetInputSchema1) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);

    std::string name = "db_x11";
    {
        Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // create table db1
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 bigint NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=column4, ts=column2)\n"
            ");";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // select
        std::string sql = "select column1 from test3;";
        const Schema &schema = dbms_sdk->GetInputSchema(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        ASSERT_EQ(5, schema.GetColumnCnt());
    }
}

TEST_F(DBMSSdkTest, ExecuteSqlTest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);
    std::string name = "db_2";
    {
        Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // create table db1
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 bigint NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=column4, ts=column2)\n"
            ");";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        // insert
        std::string sql = "insert into test3 values(1, 4000, 2, \"hello\", 3);";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        Status status;
        std::string sql =
            "select column1, column2, column3, column4, column5 from test3 "
            "limit 1;";
        std::shared_ptr<ResultSet> rs =
            dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        if (rs) {
            const Schema *schema = rs->GetSchema();
            ASSERT_EQ(5, schema->GetColumnCnt());
            ASSERT_EQ("column1", schema->GetColumnName(0));
            ASSERT_EQ("column2", schema->GetColumnName(1));
            ASSERT_EQ("column3", schema->GetColumnName(2));
            ASSERT_EQ("column4", schema->GetColumnName(3));
            ASSERT_EQ("column5", schema->GetColumnName(4));

            ASSERT_EQ(kTypeInt32, schema->GetColumnType(0));
            ASSERT_EQ(kTypeInt64, schema->GetColumnType(1));
            ASSERT_EQ(kTypeInt32, schema->GetColumnType(2));
            ASSERT_EQ(kTypeString, schema->GetColumnType(3));
            ASSERT_EQ(kTypeInt32, schema->GetColumnType(4));

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(1, &val));
                ASSERT_EQ(val, 4000);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(2, &val));
                ASSERT_EQ(val, 2);
            }

            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 3);
            }

            {
                std::string val;
                ASSERT_TRUE(rs->GetString(3, &val));
                ASSERT_EQ(val, "hello");
            }
        } else {
            ASSERT_FALSE(true);
        }
    }
}

TEST_F(DBMSSdkTest, ExecuteScriptAPITest) {
    usleep(2000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);

    {
        Status status;
        std::string name = "db_1";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table test3(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table test4(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    index(key=(column1), ts=column2)\n"
            ");";

        std::string name = "db_1";
        Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
}

void PrintRows(const vm::Schema &schema, const std::vector<codec::Row> &rows) {
    std::ostringstream oss;
    codec::RowView row_view(schema);
    ::hybridse::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
        if (t.current_columns_size() >= 20) {
            t.add("...");
            break;
        }
    }
    t.end_of_row();
    if (rows.empty()) {
        t.add("Empty set");
        t.end_of_row();
        return;
    }

    for (auto row : rows) {
        row_view.Reset(row.buf());
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            t.add(str);
            if (t.current_columns_size() >= 20) {
                t.add("...");
                break;
            }
        }
        t.end_of_row();
        if (t.rows().size() > 10) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

void PrintResultSet(std::shared_ptr<ResultSet> rs) {
    std::ostringstream oss;
    ::hybridse::base::TextTable t('-', '|', '+');
    auto schema = rs->GetSchema();
    // Add Header
    for (int i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
        if (t.current_columns_size() >= 20) {
            t.add("...");
            break;
        }
    }
    t.end_of_row();
    if (0 == rs->Size()) {
        t.add("Empty set");
        t.end_of_row();
        return;
    }

    while (rs->Next()) {
        for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
            std::string str = rs->GetAsStringUnsafe(idx);
            t.add(str);
            if (t.current_columns_size() >= 20) {
                t.add("...");
                break;
            }
        }
        t.end_of_row();
        if (t.rows().size() > 10) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}
void CheckRows(const vm::Schema &schema, const std::string &order_col,
               const std::vector<codec::Row> &rows,
               std::shared_ptr<ResultSet> rs) {
    ASSERT_EQ(rows.size(), rs->Size());

    LOG(INFO) << "Expected Rows: \n";
    PrintRows(schema, rows);
    LOG(INFO) << "ResultSet Rows: \n";
    PrintResultSet(rs);
    codec::RowView row_view(schema);
    int order_idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            order_idx = i;
            break;
        }
    }
    std::map<std::string, std::pair<codec::Row, bool>> rows_map;
    if (order_idx >= 0) {
        for (auto row : rows) {
            row_view.Reset(row.buf());
            std::string key = row_view.GetAsString(order_idx);
            rows_map.insert(std::make_pair(key, std::make_pair(row, false)));
        }
    }
    int32_t index = 0;
    rs->Reset();
    while (rs->Next()) {
        if (order_idx >= 0) {
            std::string key = rs->GetAsStringUnsafe(order_idx);
            LOG(INFO) << "key : " << key;
            ASSERT_TRUE(rows_map.find(key) != rows_map.cend())
                << "CheckRows fail: row[" << index << "] order not expected";
            ASSERT_FALSE(rows_map[key].second)
                << "CheckRows fail: row[" << index << "] duplicate key";
            row_view.Reset(rows_map[key].first.buf());
            rows_map[key].second = true;
        } else {
            row_view.Reset(rows[index++].buf());
        }
        for (int i = 0; i < schema.size(); i++) {
            if (row_view.IsNULL(i)) {
                ASSERT_TRUE(rs->IsNULL(i)) << " At " << i;
                continue;
            }
            switch (schema.Get(i).type()) {
                case hybridse::type::kInt32: {
                    ASSERT_EQ(row_view.GetInt32Unsafe(i), rs->GetInt32Unsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i), rs->GetInt64Unsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i), rs->GetInt16Unsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kFloat: {
                    ASSERT_FLOAT_EQ(row_view.GetFloatUnsafe(i),
                                    rs->GetFloatUnsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kDouble: {
                    ASSERT_DOUBLE_EQ(row_view.GetDoubleUnsafe(i),
                                     rs->GetDoubleUnsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i),
                              rs->GetStringUnsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i),
                              rs->GetTimeUnsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i), rs->GetDateUnsafe(i))
                        << " At " << i;
                    break;
                }
                case hybridse::type::kBool: {
                    ASSERT_EQ(row_view.GetBoolUnsafe(i), rs->GetBoolUnsafe(i))
                        << " At " << i;
                    break;
                }
                default: {
                    FAIL() << "Invalid Column Type";
                    break;
                }
            }
        }
    }
}
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(DBMSSdkTest);
INSTANTIATE_TEST_SUITE_P(
    SdkMultiInsert, DBMSSdkTest,
    testing::ValuesIn(sqlcase::InitCases("/cases/function/dml/multi_insert.yaml")));
INSTANTIATE_TEST_SUITE_P(
    SdkSimpleInsert, DBMSSdkTest,
    testing::ValuesIn(sqlcase::InitCases("/cases/function/dml/test_insert.yaml")));

TEST_P(DBMSSdkTest, ExecuteQueryTest) {
    auto sql_case = GetParam();
    usleep(1000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(dbms_port);
    std::shared_ptr<::hybridse::sdk::DBMSSdk> dbms_sdk =
        ::hybridse::sdk::CreateDBMSSdk(endpoint);

    if (sql_case.db_.empty()) {
        sql_case.db_ = sqlcase::SqlCase::GenRand("auto_db");
    }
    std::string db = sql_case.db();
    {
        Status status;
        dbms_sdk->CreateDatabase(db, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }
    {
        // create and insert inputs
        for (size_t i = 0; i < sql_case.inputs().size(); i++) {
            if (sql_case.inputs()[i].name_.empty()) {
                sql_case.set_input_name(SqlCase::GenRand("auto_t"), i);
            }
            Status status;
            std::string create;
            if (sql_case.BuildCreateSqlFromInput(i, &create) && !create.empty()) {
                std::string placeholder = "{" + std::to_string(i) + "}";
                boost::replace_all(create, placeholder, sql_case.inputs()[i].name_);
                LOG(INFO) << create;
                dbms_sdk->ExecuteQuery(db, create, &status);
                ASSERT_EQ(0, static_cast<int>(status.code));

                std::string insert;
                ASSERT_TRUE(sql_case.BuildInsertSqlFromInput(i, &insert));
                boost::replace_all(insert, placeholder, sql_case.inputs()[i].name_);
                LOG(INFO) << insert;
                dbms_sdk->ExecuteQuery(db, insert, &status);
                if (!sql_case.expect_.success_) {
                    ASSERT_NE(0, static_cast<int>(status.code));
                    LOG(INFO) << status.msg;
                    return;
                } else {
                    ASSERT_EQ(0, static_cast<int>(status.code));
                }
            }
        }
    }
    {
        Status status;
        std::string sql = sql_case.sql_str();
        for (size_t i = 0; i < sql_case.inputs().size(); i++) {
            std::string placeholder = "{" + std::to_string(i) + "}";
            boost::replace_all(sql, placeholder, sql_case.inputs()[i].name_);
        }
        LOG(INFO) << sql;
        std::shared_ptr<ResultSet> rs =
            dbms_sdk->ExecuteQuery(db, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
        std::vector<codec::Row> rows;
        sql_case.ExtractOutputData(rows);
        type::TableDef output_table;
        sql_case.ExtractOutputSchema(output_table);
        CheckRows(output_table.columns(), sql_case.expect().order_, rows, rs);
    }
}
}  // namespace sdk
}  // namespace hybridse
int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    hybridse::vm::Engine::InitializeGlobalLLVM();
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_enable_keep_alive = false;
    return RUN_ALL_TESTS();
}
