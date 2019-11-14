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
    if (rs) {}
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

