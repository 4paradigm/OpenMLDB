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

#include "sdk/tablet_sdk.h"
#include "base/strings.h"
#include "brpc/server.h"
#include "dbms/dbms_server_impl.h"
#include "gtest/gtest.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "sdk/dbms_sdk.h"
#include "storage/codec.h"
#include "tablet/tablet_internal_sdk.h"
#include "tablet/tablet_server_impl.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

namespace fesql {
namespace sdk {

class TabletSdkTest : public ::testing::Test {};

TEST_F(TabletSdkTest, test_normal) {
    tablet::TabletServerImpl* tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    brpc::Server server;
    server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE);
    server.Start(8121, &options);

    tablet::TabletInternalSDK interal_sdk("127.0.0.1:8121");
    bool ok = interal_sdk.Init();
    ASSERT_TRUE(ok);

    tablet::CreateTableRequest req;
    req.set_tid(1);
    req.add_pids(0);
    req.set_db("db1");
    type::TableDef* table_def = req.mutable_table();
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
    std::unique_ptr<TabletSdk> sdk = CreateTabletSdk("127.0.0.1:8121");
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    Insert insert;

    insert.values.push_back(fesql::sdk::Value(1));
    insert.values.push_back(fesql::sdk::Value(2));
    insert.values.push_back(fesql::sdk::Value(3.1));
    insert.values.push_back(fesql::sdk::Value(4.1));
    insert.values.push_back(fesql::sdk::Value(5));

    insert.db = "db1";
    insert.table = "t1";
    insert.key = "k";
    insert.ts = 1024;

    ::fesql::sdk::Status insert_status;
    sdk->SyncInsert(insert, insert_status);
    ASSERT_EQ(0, static_cast<int>(insert_status.code));
    {
        Query query;
        query.db = "db1";
        query.sql = "select col1, col2, col3, col4, col5 from t1 limit 1;";
        sdk::Status query_status;
        std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query, query_status);
        if (rs) {
            ASSERT_EQ(5u, rs->GetColumnCnt());
            ASSERT_EQ("col1", rs->GetColumnName(0));
            ASSERT_EQ("col2", rs->GetColumnName(1));
            ASSERT_EQ("col3", rs->GetColumnName(2));
            ASSERT_EQ("col4", rs->GetColumnName(3));
            ASSERT_EQ("col5", rs->GetColumnName(4));
            ASSERT_EQ(1, rs->GetRowCnt());
            std::unique_ptr<ResultSetIterator> it = rs->Iterator();
            ASSERT_TRUE(it->HasNext());
            it->Next();
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int16_t val = 0;
                ASSERT_TRUE(it->GetInt16(1, &val));
                ASSERT_EQ(val, 2u);
            }
            {
                float val = 0;
                ASSERT_TRUE(it->GetFloat(2, &val));
                ASSERT_EQ(val, static_cast<float>(3.1));
            }
            {
                double val = 0;
                ASSERT_TRUE(it->GetDouble(3, &val));
                ASSERT_EQ(val, 4.1);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(it->GetInt64(4, &val));
                ASSERT_EQ(val, 5L);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }

    {
        Query query;
        sdk::Status query_status;
        query.db = "db1";
        query.sql = "select col1, col5 from t1 limit 1;";
        std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query, query_status);
        if (rs) {
            ASSERT_EQ(2u, rs->GetColumnCnt());
            ASSERT_EQ("col1", rs->GetColumnName(0));
            ASSERT_EQ("col5", rs->GetColumnName(1));
            ASSERT_EQ(1, rs->GetRowCnt());
            std::unique_ptr<ResultSetIterator> it = rs->Iterator();
            ASSERT_TRUE(it->HasNext());
            it->Next();
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(it->GetInt64(1, &val));
                ASSERT_EQ(val, 5L);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(TabletSdkTest, test_create_and_query) {
    // prepare servive
    brpc::Server server;
    brpc::Server tablet_server;
    int tablet_port = 8300;
    int port = 9500;

    tablet::TabletServerImpl* tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }
    tablet_server.Start(tablet_port, &options);

    ::fesql::dbms::DBMSServerImpl* dbms = new ::fesql::dbms::DBMSServerImpl();
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }
    server.Start(port, &options);
    dbms->SetTabletEndpoint("127.0.0.1:" + std::to_string(tablet_port));

    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    // create database db1
    {
        fesql::sdk::Status status;
        DatabaseDef db;
        db.name = "db_1";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        DatabaseDef db;
        std::string sql =
            "create table t1(\n"
            "    column1 int NOT NULL,\n"
            "    column2 double NOT NULL,\n"
            "    column3 float NOT NULL,\n"
            "    column4 bigint NOT NULL,\n"
            "    column5 int NOT NULL\n,"
            "    index(key=column1, ts=column5)\n"
            ");";

        db.name = "db_1";
        fesql::sdk::Status status;
        fesql::sdk::ExecuteResult result;
        fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(tablet_port));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    sdk->SyncInsert("db_1", "insert into t1 values(1, 2.2, 3.3, 4, 5);",
                    insert_status);
    ASSERT_EQ(0, static_cast<int>(insert_status.code));
    {
        Query query;
        sdk::Status query_status;
        query.db = "db_1";
        query.sql = "select column1, column2 from t1 limit 1;";
        std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query, query_status);
        if (rs) {
            ASSERT_EQ(2u, rs->GetColumnCnt());
            ASSERT_EQ("column1", rs->GetColumnName(0));
            ASSERT_EQ("column2", rs->GetColumnName(1));
            std::unique_ptr<ResultSetIterator> it = rs->Iterator();
            ASSERT_TRUE(it->HasNext());
            it->Next();
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                double val = 0;
                ASSERT_TRUE(it->GetDouble(1, &val));
                ASSERT_EQ(val, 2.2);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }

    {
        Query query;
        sdk::Status query_status;
        query.db = "db_1";
        query.sql =
            "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    "
            "return d\nend\n%%sql\nSELECT column1, column2, "
            "test(column1,column5) as f1, column1 + column5 as f2 FROM t1 "
            "limit 10;";
        std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query, query_status);
        if (rs) {
            ASSERT_EQ(4u, rs->GetColumnCnt());
            ASSERT_EQ("column1", rs->GetColumnName(0));
            ASSERT_EQ("column2", rs->GetColumnName(1));
            ASSERT_EQ("f1", rs->GetColumnName(2));
            ASSERT_EQ("f2", rs->GetColumnName(3));
            std::unique_ptr<ResultSetIterator> it = rs->Iterator();
            ASSERT_TRUE(it->HasNext());
            it->Next();
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                double val = 0;
                ASSERT_TRUE(it->GetDouble(1, &val));
                ASSERT_EQ(val, 2.2);
            }
            {
                int val = 0;
                ASSERT_TRUE(it->GetInt32(2, &val));
                ASSERT_EQ(val, 7);
            }
            {
                int val = 0;
                ASSERT_TRUE(it->GetInt32(3, &val));
                ASSERT_EQ(val, 6);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }

    {
        Query query;
        sdk::Status query_status;
        query.db = "db_1";
        query.sql =
            "select column1, column2, column3, column4, column5 from t1 limit "
            "1;";
        std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query, query_status);
        if (rs) {
            ASSERT_EQ(5u, rs->GetColumnCnt());
            ASSERT_EQ("column1", rs->GetColumnName(0));
            ASSERT_EQ("column2", rs->GetColumnName(1));
            ASSERT_EQ("column3", rs->GetColumnName(2));
            ASSERT_EQ("column4", rs->GetColumnName(3));
            ASSERT_EQ("column5", rs->GetColumnName(4));
            std::unique_ptr<ResultSetIterator> it = rs->Iterator();
            ASSERT_TRUE(it->HasNext());
            it->Next();
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                double val = 0;
                ASSERT_TRUE(it->GetDouble(1, &val));
                ASSERT_EQ(val, 2.2);
            }
            {
                float val = 0;
                ASSERT_TRUE(it->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(it->GetInt64(3, &val));
                ASSERT_EQ(val, 4L);
            }
            {
                int val = 0;
                ASSERT_TRUE(it->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
    //
    delete dbms;
    delete dbms_sdk;
    delete tablet;
}

TEST_F(TabletSdkTest, test_udf_query) {
    // prepare servive
    brpc::Server server;
    brpc::Server tablet_server;
    int tablet_port = 8301;
    int port = 9501;

    tablet::TabletServerImpl* tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    if (0 !=
        tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }
    tablet_server.Start(tablet_port, &options);

    ::fesql::dbms::DBMSServerImpl* dbms = new ::fesql::dbms::DBMSServerImpl();
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }
    server.Start(port, &options);
    dbms->SetTabletEndpoint("127.0.0.1:" + std::to_string(tablet_port));

    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    ::fesql::sdk::DBMSSdk* dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    // create database db1
    {
        fesql::sdk::Status status;
        DatabaseDef db;
        db.name = "db_1";
        dbms_sdk->CreateDatabase(db, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    {
        // create table db1
        DatabaseDef db;
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
        db.name = "db_1";
        fesql::sdk::Status status;
        fesql::sdk::ExecuteResult result;
        fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk =
        CreateTabletSdk("127.0.0.1:" + std::to_string(tablet_port));
    if (sdk) {
        ASSERT_TRUE(true);
    } else {
        ASSERT_FALSE(true);
    }

    ::fesql::sdk::Status insert_status;
    sdk->SyncInsert("db_1",
                    "insert into t1 values(1, 2, 3.3, 4, 5, \"hello\");",
                    insert_status);
    if (0 != insert_status.code) {
        std::cout << insert_status.msg << std::endl;
    }
    ASSERT_EQ(0, static_cast<int>(insert_status.code));

    {
        Query query;
        sdk::Status query_status;
        query.db = "db_1";
        query.sql =
            "select column1, column2, column3, column4, column5, column6 from "
            "t1 limit "
            "1;";
        std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query, query_status);
        if (rs) {
            ASSERT_EQ(6u, rs->GetColumnCnt());
            ASSERT_EQ("column1", rs->GetColumnName(0));
            ASSERT_EQ("column2", rs->GetColumnName(1));
            ASSERT_EQ("column3", rs->GetColumnName(2));
            ASSERT_EQ("column4", rs->GetColumnName(3));
            ASSERT_EQ("column5", rs->GetColumnName(4));
            ASSERT_EQ("column6", rs->GetColumnName(5));
            std::unique_ptr<ResultSetIterator> it = rs->Iterator();
            ASSERT_TRUE(it->HasNext());
            it->Next();
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                float val = 0;
                ASSERT_TRUE(it->GetFloat(2, &val));
                ASSERT_EQ(val, 3.3f);
            }
            {
                int64_t val = 0;
                ASSERT_TRUE(it->GetInt64(3, &val));
                ASSERT_EQ(val, 4L);
            }
            {
                int val = 0;
                ASSERT_TRUE(it->GetInt32(4, &val));
                ASSERT_EQ(val, 5);
            }
            {
                char* val = NULL;
                uint32_t size = 0;
                ASSERT_TRUE(it->GetString(5, &val, &size));
                ASSERT_EQ(size, 5);
                std::string str(val, 5);
                ASSERT_EQ(str, "hello");
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
    {
        Query query;
        sdk::Status query_status;
        query.db = "db_1";
        query.sql =
            "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    "
            "return d\nend\n%%sql\nSELECT column1, column2, "
            "test(column1,column5) as f1 FROM t1 limit 10;";

        std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query, query_status);
        if (rs) {
            ASSERT_EQ(3u, rs->GetColumnCnt());
            ASSERT_EQ("column1", rs->GetColumnName(0));
            ASSERT_EQ("column2", rs->GetColumnName(1));
            std::cout << rs->GetColumnName(2) << std::endl;
            std::unique_ptr<ResultSetIterator> it = rs->Iterator();
            ASSERT_TRUE(it->HasNext());
            it->Next();
            {
                int32_t val = 0;
                ASSERT_TRUE(it->GetInt32(0, &val));
                ASSERT_EQ(val, 1);
            }
            {
                int val = 0;
                ASSERT_TRUE(it->GetInt32(1, &val));
                ASSERT_EQ(val, 2);
            }
            {
                int val = 0;
                ASSERT_TRUE(it->GetInt32(2, &val));
                ASSERT_EQ(val, 7);
            }
        } else {
            ASSERT_TRUE(false);
        }
    }
    //
    delete dbms;
    delete dbms_sdk;
    delete tablet;
}

}  // namespace sdk
}  // namespace fesql

int main(int argc, char** argv) {
    InitLLVM X(argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
