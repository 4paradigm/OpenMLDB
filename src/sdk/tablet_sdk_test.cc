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
#include "sdk/dbms_sdk.h"

#include "dbms/dbms_server_impl.h"
#include "tablet/tablet_server_impl.h"
#include "tablet/tablet_internal_sdk.h"
#include "gtest/gtest.h"
#include "brpc/server.h"

#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"

using namespace llvm;
using namespace llvm::orc;

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
        column->set_name("col15");
    }
    common::Status status;
    interal_sdk.CreateTable(&req, status);
    ASSERT_EQ(status.code(), common::kOk);
    std::unique_ptr<TabletSdk> sdk = CreateTabletSdk("127.0.0.1:8121");
    if (sdk) {
        ASSERT_TRUE(true);
    }else {
        ASSERT_FALSE(true);
    }
    Query query;
    query.db = "db1";
    query.sql = "select col1, col2 from t1 limit 1;";
    std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query);
    if (rs) {
        ASSERT_EQ(2u, rs->GetColumnCnt());
        ASSERT_EQ("col1", rs->GetColumnName(0));
        ASSERT_EQ("col2", rs->GetColumnName(1));
        ASSERT_EQ(0u, rs->GetRowCnt());
    }
}

TEST_F(TabletSdkTest, test_create_and_query) {
    //prepare servive
    brpc::Server server;
    brpc::Server tablet_server;
    int tablet_port = 8300;
    int port = 9500;

    tablet::TabletServerImpl *tablet = new tablet::TabletServerImpl();
    ASSERT_TRUE(tablet->Init());
    brpc::ServerOptions options;
    if (0 != tablet_server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(WARNING) << "Fail to add tablet service";
        exit(1);
    }
    tablet_server.Start(tablet_port, &options);

    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }
    server.Start(port, &options);
    dbms->SetTabletEndpoint("127.0.0.1:" + std::to_string(tablet_port));

    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    ::fesql::sdk::DBMSSdk *dbms_sdk = ::fesql::sdk::CreateDBMSSdk(endpoint);
    ASSERT_TRUE(nullptr != dbms_sdk);

    // create database db1
    {
        fesql::base::Status status;
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
            "    column4 bigint NOT NULL\n"
            ");";

        db.name = "db_1";
        fesql::base::Status status;
        fesql::sdk::ExecuteResult result;
        fesql::sdk::ExecuteRequst request;
        request.database = db;
        request.sql = sql;
        dbms_sdk->ExecuteScript(request, result, status);
        ASSERT_EQ(0, static_cast<int>(status.code));
    }

    std::unique_ptr<TabletSdk> sdk = CreateTabletSdk("127.0.0.1:" + std::to_string(tablet_port));
    if (sdk) {
        ASSERT_TRUE(true);
    }else {
        ASSERT_FALSE(true);
    }
    Query query;
    query.db = "db_1";
    query.sql = "select column1, column2 from t1 limit 1;";
    std::unique_ptr<ResultSet> rs = sdk->SyncQuery(query);
    if (rs) {
        ASSERT_EQ(2u, rs->GetColumnCnt());
        ASSERT_EQ("column1", rs->GetColumnName(0));
        ASSERT_EQ("column2", rs->GetColumnName(1));
        ASSERT_EQ(0u, rs->GetRowCnt());
    }
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

