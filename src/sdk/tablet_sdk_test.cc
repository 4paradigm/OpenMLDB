/*
 * tablet_sdk_test.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include  <unistd.h>
#include "sdk/tablet_sdk.h"
#include "base/strings.h"
#include "brpc/server.h"
#include "dbms/dbms_server_impl.h"
#include "gtest/gtest.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "sdk/dbms_sdk.h"
#include "tablet/tablet_internal_sdk.h"
#include "tablet/tablet_server_impl.h"
#include "gflags/gflags.h"

DECLARE_bool(enable_keep_alive);
using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

namespace fesql {
namespace sdk {
class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

enum EngineRunMode { RUN, RUNBATCH, RUNONE };
class TabletSdkTest : public ::testing::TestWithParam<EngineRunMode> {
 public:
    TabletSdkTest():base_tablet_port_(8300), base_dbms_port_(9500), tablet_server_(),
    tablet_(NULL), dbms_server_(), dbms_(NULL){
    }
    ~TabletSdkTest() {}

    void SetUp() {
        brpc::ServerOptions options;
        tablet_ = new tablet::TabletServerImpl();
        tablet_->Init();
        tablet_server_.AddService(tablet_, brpc::SERVER_DOESNT_OWN_SERVICE);
        tablet_server_.Start(base_tablet_port_, &options);

        dbms_ = new ::fesql::dbms::DBMSServerImpl();
        dbms_server_.AddService(dbms_, brpc::SERVER_DOESNT_OWN_SERVICE);
        dbms_server_.Start(base_dbms_port_, &options);
        {
            std::string tablet_endpoint = "127.0.0.1:" + std::to_string(base_tablet_port_);
            MockClosure closure;
            dbms::KeepAliveRequest request;
            request.set_endpoint(tablet_endpoint);
            dbms::KeepAliveResponse response;
            dbms_->KeepAlive(NULL, &request, &response, &closure);
        }
    }

    void TearDown() {
        tablet_server_.Stop(10);
        dbms_server_.Stop(10);
        delete tablet_;
        delete dbms_;
    }

 protected:
    int base_tablet_port_;
    int base_dbms_port_;
    brpc::Server tablet_server_;
    tablet::TabletServerImpl* tablet_;
    brpc::Server dbms_server_;
    dbms::DBMSServerImpl *dbms_;
};

INSTANTIATE_TEST_CASE_P(TabletRUNAndBatchMode, TabletSdkTest,
                        testing::Values(RUNBATCH));

TEST_P(TabletSdkTest, test_normal) {
    tablet::TabletInternalSDK interal_sdk("127.0.0.1:" +
                                          std::to_string(base_tablet_port_));
    bool ok = interal_sdk.Init();
    ASSERT_TRUE(ok);
    tablet::CreateTableRequest req;
    req.set_tid(1);
    req.add_pids(0);
    req.set_db("db1");
    type::TableDef* table_def = req.mutable_table();
    table_def->set_catalog("db1");
    table_def->set_name("t1");
    {
        ::fesql::type::ColumnDef* column = table_def->add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table_def->add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table_def->add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table_def->add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }
    {
        ::fesql::type::ColumnDef* column = table_def->add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }
    ::fesql::type::IndexDef* index = table_def->add_indexes();
    index->set_name("idx1");
    index->add_first_keys("col1");
    index->set_second_key("col5");
    common::Status status;
    interal_sdk.CreateTable(&req, status);
    ASSERT_EQ(status.code(), common::kOk);
    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(base_tablet_port_));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    std::string sql = "insert into t1 values(1, 2, 3.1, 4.1,5);";
    std::string db = "db1";
    ::fesql::sdk::Status insert_status;
    sdk->Insert(db, sql, &insert_status);
    ASSERT_EQ(0, static_cast<int>(insert_status.code));
    {
        std::string sql = "select col1, col2, col3, col4, col5 from t1 limit 1;";
        sdk::Status query_status;
        std::unique_ptr<ResultSet> rs = sdk->Query(db, sql, &query_status);
        if (rs) {

            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(5, schema.GetColumnCnt());
            ASSERT_EQ("col1", schema.GetColumnName(0));
            ASSERT_EQ("col2", schema.GetColumnName(1));
            ASSERT_EQ("col3", schema.GetColumnName(2));
            ASSERT_EQ("col4", schema.GetColumnName(3));
            ASSERT_EQ("col5", schema.GetColumnName(4));

            ASSERT_EQ(kTypeInt32, schema.GetColumnType(0));
            ASSERT_EQ(kTypeInt16, schema.GetColumnType(1));
            ASSERT_EQ(kTypeFloat, schema.GetColumnType(2));
            ASSERT_EQ(kTypeDouble, schema.GetColumnType(3));
            ASSERT_EQ(kTypeInt64, schema.GetColumnType(4));

            ASSERT_EQ(1, rs->Size());
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int16_t val = 0;
                ASSERT_TRUE(rs->GetInt16(1, &val));
                ASSERT_EQ(val, 2u);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(4, &val));
                ASSERT_EQ(val, 5L);
            }
            {
                double val = 0;
                ASSERT_TRUE(rs->GetDouble(3, &val));
                ASSERT_EQ(val, 4.1);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.1f);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }

    {
        sdk::Status query_status;
        std::string sql = "select col1, col5 from t1 limit 1;";
        std::unique_ptr<ResultSet> rs = sdk->Query(db, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(2u, schema.GetColumnCnt());
            ASSERT_EQ("col1", schema.GetColumnName(0));
            ASSERT_EQ("col5", schema.GetColumnName(1));
            ASSERT_EQ(1, rs->Size());
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(1, &val));
                ASSERT_EQ(val, 5L);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_P(TabletSdkTest, test_create_and_query) {
    usleep(4000 * 1000);
    ParamType mode = GetParam();
    const std::string endpoint = "127.0.0.1:" + std::to_string(base_dbms_port_);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);
    // create database db1
    {
        fesql::sdk::Status status;
        std::string name = "db_1";
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table t1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 double NOT NULL,\n"
            "    column3 float NOT NULL,\n"
            "    column4 bigint NOT NULL,\n"
            "    column5 int NOT NULL\n,"
            "    index(key=column1, ts=column5)\n"
            ");";

        std::string name = "db_1";
        fesql::sdk::Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(base_tablet_port_));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    sdk->Insert("db_1", "insert into t1 values(1, 2.2, 3.3, 4, 5);",
                  &insert_status);
    ASSERT_EQ(0, static_cast<int>(insert_status.code));
    {
        sdk::Status query_status;
        std::string db = "db_1";
        std::string sql = "select column1, column2 from t1 limit 1;";
        std::unique_ptr<ResultSet> rs = sdk->Query(db, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(2u, schema.GetColumnCnt());
            ASSERT_EQ("column1", schema.GetColumnName(0));
            ASSERT_EQ("column2", schema.GetColumnName(1));
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                double val = 0;
                ASSERT_TRUE(rs->GetDouble(1, &val));
                ASSERT_EQ(val, 2.2);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }

    {
        sdk::Status query_status;
        std::string db = "db_1";
        std::string sql =
            "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    "
            "return d\nend\n%%sql\nSELECT column1, column2, "
            "test(column1,column5) as f1, column1 + column5 as f2 FROM t1 "
            "limit 10;";
        std::unique_ptr<ResultSet> rs = sdk->Query(db, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(4u, schema.GetColumnCnt());
            ASSERT_EQ("column1", schema.GetColumnName(0));
            ASSERT_EQ("column2", schema.GetColumnName(1));
            ASSERT_EQ("f1", schema.GetColumnName(2));
            ASSERT_EQ("f2", schema.GetColumnName(3));
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                double val = 0;
                ASSERT_TRUE(rs->GetDouble(1, &val));
                ASSERT_EQ(val, 2.2);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(2, &val));
                ASSERT_EQ(val, 7);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(3, &val));
                ASSERT_EQ(val, 6);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }

    {
        sdk::Status query_status;
        std::string db = "db_1";
        std::string sql =
            "select column1, column2, column3, column4, column5 from t1 limit "
            "1;";
        std::unique_ptr<ResultSet> rs = sdk->Query(db, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(5, schema.GetColumnCnt());
            ASSERT_EQ("column1", schema.GetColumnName(0));
            ASSERT_EQ("column2", schema.GetColumnName(1));
            ASSERT_EQ("column3", schema.GetColumnName(2));
            ASSERT_EQ("column4", schema.GetColumnName(3));
            ASSERT_EQ("column5", schema.GetColumnName(4));
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                double val = 0;
                ASSERT_TRUE(rs->GetDouble(1, &val));
                ASSERT_EQ(val, 2.2);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 4L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_P(TabletSdkTest, test_udf_query) {
    usleep(2000 * 1000);
    ParamType mode = GetParam();
    const std::string endpoint = "127.0.0.1:" + std::to_string(base_dbms_port_);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);
    std::string name = "db_2";
    // create database db1
    {
        fesql::sdk::Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        fesql::sdk::Status status;
        // create table db1
        std::string sql =
            "create table t1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 int NOT NULL,\n"
            "    column3 float NOT NULL,\n"
            "    column4 bigint NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    column6 string,\n"
            "    index(key=column1, ts=column5)\n"
            ");";
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(base_tablet_port_));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    sdk->Insert(name,
                "insert into t1 values(1, 2, 3.3, 4, 5, \"hello\");",
                 &insert_status);
    if (0 != insert_status.code) {
        std::cout << insert_status.msg << std::endl;
    }
    ASSERT_EQ(0, static_cast<int>(insert_status.code));
    {
        sdk::Status query_status;
        std::string sql =
            "select column1, column2, column3, column4, column5, column6 from "
            "t1 limit "
            "1;";
        std::unique_ptr<ResultSet> rs = sdk->Query(name, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(6u, schema.GetColumnCnt());
            ASSERT_EQ("column1", schema.GetColumnName(0));
            ASSERT_EQ("column2", schema.GetColumnName(1));
            ASSERT_EQ("column3", schema.GetColumnName(2));
            ASSERT_EQ("column4", schema.GetColumnName(3));
            ASSERT_EQ("column5", schema.GetColumnName(4));
            ASSERT_EQ("column6", schema.GetColumnName(5));
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 4L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }
            {
                char* val = NULL;
                uint32_t size = 0;
                ASSERT_TRUE(rs->GetString(5, &val, &size));
                ASSERT_EQ(size, 5);
                std::string str(val, 5);
                ASSERT_EQ(str, "hello");
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
    {
        sdk::Status query_status;
        std::string sql =
            "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    "
            "return d\nend\n%%sql\nSELECT column1, column2, "
            "test(column1,column5) as f1 FROM t1 limit 10;";
        std::unique_ptr<ResultSet> rs = sdk->Query(name, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(3u, schema.GetColumnCnt());
            ASSERT_EQ("column1", schema.GetColumnName(0));
            ASSERT_EQ("column2", schema.GetColumnName(1));
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(2, &val));
                ASSERT_EQ(val, 7);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(TabletSdkTest, test_window_udf_query) {
    usleep(4000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(base_dbms_port_);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);
    std::string name = "db_3";
    // create database db1
    {
        fesql::sdk::Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table t1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 int NOT NULL,\n"
            "    column3 float NOT NULL,\n"
            "    column4 bigint NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    column6 string,\n"
            "    index(key=column1, ts=column4)\n"
            ");";
        fesql::sdk::Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(base_tablet_port_));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    {
        sdk->Insert(name,
                        "insert into t1 values(1, 2, 3.3, 1000, 5, \"hello\");",
                        &insert_status);
        ASSERT_EQ(0, insert_status.code);
    }
    {
        sdk->Insert(name,
                        "insert into t1 values(1, 3, 4.4, 2000, 6, \"world\");",
                        &insert_status);
        ASSERT_EQ(0, insert_status.code);
    }
    {
        sdk->Insert(
            name, "insert into t1 values(11, 4, 5.5, 3000, 7, \"string1\");",
            &insert_status);
        ASSERT_EQ(0, insert_status.code);
    }
    {
        sdk->Insert(
            name, "insert into t1 values(11, 5, 6.6, 4000, 8, \"string2\");",
            &insert_status);
        ASSERT_EQ(0, insert_status.code);
    }
    {
        sdk->Insert(
            name, "insert into t1 values(11, 6, 7.7, 5000, 9, \"string3\");",
            &insert_status);
        ASSERT_EQ(0, insert_status.code);
    }

    {
        sdk::Status query_status;
        std::string sql =
            "select "
            "sum(column1) OVER w1 as w1_col1_sum, "
            "sum(column2) OVER w1 as w1_col2_sum, "
            "sum(column3) OVER w1 as w1_col3_sum, "
            "sum(column4) OVER w1 as w1_col4_sum, "
            "sum(column5) OVER w1 as w1_col5_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY column1 ORDER BY column4 ROWS "
            "BETWEEN 3000"
            "PRECEDING AND CURRENT ROW) limit 10;";
        std::unique_ptr<ResultSet> rs = sdk->Query(name, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(5, schema.GetColumnCnt());
            ASSERT_EQ("w1_col1_sum", schema.GetColumnName(0));
            ASSERT_EQ("w1_col2_sum", schema.GetColumnName(1));
            ASSERT_EQ("w1_col3_sum", schema.GetColumnName(2));
            ASSERT_EQ("w1_col4_sum", schema.GetColumnName(3));
            ASSERT_EQ("w1_col5_sum", schema.GetColumnName(4));

            ASSERT_EQ(kTypeInt32, schema.GetColumnType(0));
            ASSERT_EQ(kTypeInt32, schema.GetColumnType(1));
            ASSERT_EQ(kTypeFloat, schema.GetColumnType(2));
            ASSERT_EQ(kTypeInt64, schema.GetColumnType(3));
            ASSERT_EQ(kTypeInt32, schema.GetColumnType(4));

            ASSERT_EQ(5, rs->Size());
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 11);
            }

            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 7);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 3000);
            }
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 11 + 11);
            }

            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4 + 5);
            }
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 11 + 11 + 11);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4 + 5 + 6);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 5.5f + 6.6f + 7.7f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 3000L + 4000L + 5000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 7 + 8 + 9);
            }
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2 + 3);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f + 4.4f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L + 2000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5 + 6);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(TabletSdkTest, test_window_udf_batch_query) {

    usleep(4000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(base_dbms_port_);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);
    std::string name = "db_4";
    // create database db1
    {
        fesql::sdk::Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table t1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 int NOT NULL,\n"
            "    column3 float NOT NULL,\n"
            "    column4 bigint NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    column6 string,\n"
            "    index(key=column1, ts=column4)\n"
            ");";
        fesql::sdk::Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(base_tablet_port_));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    {
        sdk->Insert(name,
                        "insert into t1 values(1, 2, 3.3, 1000, 5, \"hello\");",
                    &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(name,
                        "insert into t1 values(1, 3, 4.4, 2000, 6, \"world\");",
                       &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(11, 4, 5.5, 3000, 7, \"string1\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(11, 5, 6.6, 4000, 8, \"string2\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(11, 6, 7.7, 5000, 9, \"string3\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }


    {
        sdk::Status query_status;
        std::string sql =
            "select "
            "sum(column1) OVER w1 as w1_col1_sum, "
            "sum(column2) OVER w1 as w1_col2_sum, "
            "sum(column3) OVER w1 as w1_col3_sum, "
            "sum(column4) OVER w1 as w1_col4_sum, "
            "sum(column5) OVER w1 as w1_col5_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY column1 ORDER BY column4 ROWS "
            "BETWEEN 3000"
            "PRECEDING AND CURRENT ROW) limit 10;";
        std::unique_ptr<ResultSet> rs = sdk->Query(name, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(5u, schema.GetColumnCnt());
            ASSERT_EQ("w1_col1_sum", schema.GetColumnName(0));
            ASSERT_EQ("w1_col2_sum", schema.GetColumnName(1));
            ASSERT_EQ("w1_col3_sum", schema.GetColumnName(2));
            ASSERT_EQ("w1_col4_sum", schema.GetColumnName(3));
            ASSERT_EQ("w1_col5_sum", schema.GetColumnName(4));

            ASSERT_EQ(5, rs->Size());
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 11);
            }

            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 11 + 11);
            }

            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4 + 5);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 11 + 11 + 11);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4 + 5 + 6);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2 + 3);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f + 4.4f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L + 2000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5 + 6);
            }

        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(TabletSdkTest, test_window_udf_no_partition_query) {

    usleep(4000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(base_dbms_port_);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);
    std::string name = "db_5";

    // create database db1
    {
        fesql::sdk::Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table t1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 int NOT NULL,\n"
            "    column3 float NOT NULL,\n"
            "    column4 bigint NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    column6 string,\n"
            "    index(key=column1, ts=column4)\n"
            ");";
        fesql::sdk::Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(base_tablet_port_));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    {
        sdk->Insert(name,
                    "insert into t1 values(1, 2, 3.3, 1000, 5, \"hello\");",
                     &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(name,
                   "insert into t1 values(1, 3, 4.4, 2000, 6, \"world\");",
                    &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, 
            "insert into t1 values(1, 4, 5.5, 3000, 7, \"string1\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(1, 5, 6.6, 4000, 8, \"string2\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(1, 6, 7.7, 5000, 9, \"string3\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }


    {
        sdk::Status query_status;
        std::string sql =
            "select "
            "sum(column1) OVER w1 as w1_col1_sum, "
            "sum(column2) OVER w1 as w1_col2_sum, "
            "sum(column3) OVER w1 as w1_col3_sum, "
            "sum(column4) OVER w1 as w1_col4_sum, "
            "sum(column5) OVER w1 as w1_col5_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY column1 ORDER BY column4 ROWS "
            "BETWEEN 3s "
            "PRECEDING AND CURRENT ROW) limit 10;";
        std::unique_ptr<ResultSet> rs = sdk->Query(name, sql, &query_status);
        if (rs) {
            const Schema& schema =rs->GetSchema();
            ASSERT_EQ(5u, schema.GetColumnCnt());
            ASSERT_EQ("w1_col1_sum", schema.GetColumnName(0));
            ASSERT_EQ("w1_col2_sum", schema.GetColumnName(1));
            ASSERT_EQ("w1_col3_sum", schema.GetColumnName(2));
            ASSERT_EQ("w1_col4_sum", schema.GetColumnName(3));
            ASSERT_EQ("w1_col5_sum", schema.GetColumnName(4));

            ASSERT_EQ(5, rs->Size());
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2 + 3);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f + 4.4f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L + 2000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5 + 6);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2 + 3 + 4);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f + 4.4f + 5.5f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L + 2000L + 3000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5 + 6 + 7);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 3 + 4 + 5);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 4.4f + 5.5f + 6.6f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 2000L + 3000L + 4000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 6 + 7 + 8);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4 + 5 + 6);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 5.5f + 6.6f + 7.7f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 3000L + 4000L + 5000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 7 + 8 + 9);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(TabletSdkTest, test_window_udf_no_partition_batch_query) {

    usleep(4000 * 1000);
    const std::string endpoint = "127.0.0.1:" + std::to_string(base_dbms_port_);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);
    // create database db1
    std::string name = "db_6";
    {
        fesql::sdk::Status status;
        dbms_sdk->CreateDatabase(name, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        std::string sql =
            "create table t1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 int NOT NULL,\n"
            "    column3 float NOT NULL,\n"
            "    column4 bigint NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    column6 string,\n"
            "    index(key=column1, ts=column4)\n"
            ");";
        fesql::sdk::Status status;
        dbms_sdk->ExecuteQuery(name, sql, &status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(base_tablet_port_));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    {
        sdk->Insert(name,
                        "insert into t1 values(1, 2, 3.3, 1000, 5, \"hello\");",
                        &insert_status);

        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(name,
                        "insert into t1 values(1, 3, 4.4, 2000, 6, \"world\");",
                        &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(1, 4, 5.5, 3000, 7, \"string1\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(1, 5, 6.6, 4000, 8, \"string2\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }
    {
        sdk->Insert(
            name, "insert into t1 values(1, 6, 7.7, 5000, 9, \"string3\");",
            &insert_status);
        ASSERT_EQ(0, static_cast<int>(insert_status.code));
    }


    {
        sdk::Status query_status;
        std::string sql =
            "select "
            "sum(column1) OVER w1 as w1_col1_sum, "
            "sum(column2) OVER w1 as w1_col2_sum, "
            "sum(column3) OVER w1 as w1_col3_sum, "
            "sum(column4) OVER w1 as w1_col4_sum, "
            "sum(column5) OVER w1 as w1_col5_sum "
            "FROM t1 WINDOW w1 AS (PARTITION BY column1 ORDER BY column4 ROWS "
            "BETWEEN 3s "
            "PRECEDING AND CURRENT ROW) limit 10;";
        std::unique_ptr<ResultSet> rs = sdk->Query(name, sql, &query_status);
        if (rs) {
            const Schema& schema = rs->GetSchema();
            ASSERT_EQ(5, schema.GetColumnCnt());
            ASSERT_EQ("w1_col1_sum", schema.GetColumnName(0));
            ASSERT_EQ("w1_col2_sum", schema.GetColumnName(1));
            ASSERT_EQ("w1_col3_sum", schema.GetColumnName(2));
            ASSERT_EQ("w1_col4_sum", schema.GetColumnName(3));
            ASSERT_EQ("w1_col5_sum", schema.GetColumnName(4));

            ASSERT_EQ(5, rs->Size());
            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2 + 3);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f + 4.4f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L + 2000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5 + 6);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 2 + 3 + 4);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f + 4.4f + 5.5f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 1000L + 2000L + 3000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 5 + 6 + 7);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 3 + 4 + 5);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 4.4f + 5.5f + 6.6f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 2000L + 3000L + 4000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 6 + 7 + 8);
            }

            ASSERT_TRUE(rs->Next());
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(0, &val));
                ASSERT_EQ(val, 1 + 1 + 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(rs->GetInt32(1, &val));
                ASSERT_EQ(val, 4 + 5 + 6);
            }
            {
                float val = 0;
                ASSERT_TRUE(rs->GetFloat(2, &val));
                ASSERT_EQ(val, 5.5f + 6.6f + 7.7f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(rs->GetInt64(3, &val));
                ASSERT_EQ(val, 3000L + 4000L + 5000L);
            }
            {
                int val = 0;
                ASSERT_TRUE(rs->GetInt32(4, &val));
                ASSERT_EQ(val, 7 + 8 + 9);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
}

}  // namespace sdk
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitLLVM X(argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_enable_keep_alive = false;
    return RUN_ALL_TESTS();
}
