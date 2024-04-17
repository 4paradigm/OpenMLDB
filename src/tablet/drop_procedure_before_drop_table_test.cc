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

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <unistd.h>

#include "client/ns_client.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "sdk/sql_router.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_bool(auto_failover);
DECLARE_uint32(system_table_replica_num);

using ::openmldb::nameserver::NameServerImpl;

namespace openmldb {
namespace tablet {

class SqlClusterTest : public ::testing::Test {
 public:
    SqlClusterTest() {}

    ~SqlClusterTest() {}
};

std::shared_ptr<openmldb::sdk::SQLRouter> GetNewSQLRouter() {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    openmldb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = FLAGS_zk_cluster;
    sql_opt.zk_path = FLAGS_zk_root_path;
    sql_opt.enable_debug = true;
    return openmldb::sdk::NewClusterSQLRouter(sql_opt);
}

void StartNameServer(brpc::Server& server) {  // NOLINT
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init("");
    ASSERT_TRUE(ok);
    brpc::ServerOptions options;
    if (server.AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    sleep(2);
}

void StartTablet(brpc::Server* server, ::openmldb::tablet::TabletImpl* tablet) {  // NOLINT
    bool ok = tablet->Init("");
    ASSERT_TRUE(ok);
    brpc::ServerOptions options;
    if (server->AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ASSERT_TRUE(tablet->RegisterZK());
    sleep(2);
}

void DropTable(::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub>& name_server_client,  // NOLINT
               const std::string& db, const std::string& table_name, bool success) {
    ::openmldb::nameserver::DropTableRequest request;
    ::openmldb::nameserver::GeneralResponse response;
    request.set_db(db);
    request.set_name(table_name);
    bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropTable, &request, &response,
                                             FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    if (success) {
        ASSERT_EQ(response.code(), 0);
    } else {
        ASSERT_NE(response.code(), 0);
    }
}

void DropProcedure(::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub>& name_server_client,  // NOLINT
                   const std::string& db, const std::string& sp_name) {
    api::DropProcedureRequest request;
    nameserver::GeneralResponse response;
    request.set_db_name(db);
    request.set_sp_name(sp_name);
    bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropProcedure, &request,
                                             &response, FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(response.code(), 0);
}

void ShowTable(::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub>& name_server_client,  // NOLINT

               const std::string& db, int32_t size) {
    ::openmldb::nameserver::ShowTableRequest request;
    ::openmldb::nameserver::ShowTableResponse response;
    request.set_db(db);
    request.set_show_all(false);
    bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTable, &request, &response,
                                             FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(response.table_info_size(), size);
}

TEST_F(SqlClusterTest, DropProcedureBeforeDropTable) {
    FLAGS_auto_failover = true;
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb4" + ::openmldb::test::GenRand();

    // tablet1
    FLAGS_endpoint = "127.0.0.1:9832";
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    brpc::Server tb_server1;
    ::openmldb::tablet::TabletImpl* tablet1 = new ::openmldb::tablet::TabletImpl();
    StartTablet(&tb_server1, tablet1);

    // ns1
    FLAGS_endpoint = "127.0.0.1:9632";
    brpc::Server ns_server;
    StartNameServer(ns_server);
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client(FLAGS_endpoint, "");
    name_server_client.Init();

    {
        FLAGS_endpoint = "127.0.0.1:9832";
        // showtablet
        ::openmldb::nameserver::ShowTabletRequest request;
        ::openmldb::nameserver::ShowTabletResponse response;
        bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTablet, &request,
                                                 &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(response.tablets_size(), 1);
        ::openmldb::nameserver::TabletStatus status = response.tablets(0);
        ASSERT_EQ(FLAGS_endpoint, status.endpoint());
        ASSERT_EQ("kHealthy", status.state());
    }

    // create table
    std::string ddl =
        "create table trans(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7))OPTIONS(partitionnum=4);";
    std::string ddl2 =
        "create table trans1(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7))OPTIONS(partitionnum=4);";
    auto router = GetNewSQLRouter();
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "test1";
    hybridse::sdk::Status status;
    ASSERT_TRUE(router->CreateDB(db, &status));
    router->ExecuteDDL(db, "drop table trans;", &status);
    ASSERT_TRUE(router->RefreshCatalog());
    if (!router->ExecuteDDL(db, ddl, &status)) {
        FAIL() << "fail to create table";
    }
    if (!router->ExecuteDDL(db, ddl2, &status)) {
        FAIL() << "fail to create table";
    }
    ASSERT_TRUE(router->RefreshCatalog());
    // insert
    std::string insert_sql = "insert into trans1 values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp";
    std::string sql =
        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS"
        " (UNION trans1 PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl = "create procedure " + sp_name +
                         " (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date" +
                         ")" + " begin " + sql + " end;";
    if (!router->ExecuteDDL(db, sp_ddl, &status)) {
        FAIL() << "fail to create procedure";
    }
    ASSERT_TRUE(router->RefreshCatalog());
    // call procedure
    auto request_row = router->GetRequestRow(db, sql, &status);
    ASSERT_TRUE(request_row);
    request_row->Init(2);
    ASSERT_TRUE(request_row->AppendString("bb"));
    ASSERT_TRUE(request_row->AppendInt32(23));
    ASSERT_TRUE(request_row->AppendInt64(33));
    ASSERT_TRUE(request_row->AppendFloat(1.5f));
    ASSERT_TRUE(request_row->AppendDouble(2.5));
    ASSERT_TRUE(request_row->AppendTimestamp(1590738994000));
    ASSERT_TRUE(request_row->AppendDate(1234));
    ASSERT_TRUE(request_row->Build());
    auto rs = router->CallProcedure(db, sp_name, request_row, &status);
    if (!rs) FAIL() << "call procedure failed";
    auto schema = rs->GetSchema();
    ASSERT_EQ(schema->GetColumnCnt(), 3);
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(rs->GetStringUnsafe(0), "bb");
    ASSERT_EQ(rs->GetInt32Unsafe(1), 23);
    ASSERT_EQ(rs->GetInt64Unsafe(2), 67);
    ASSERT_FALSE(rs->Next());
    // stop
    tb_server1.Stop(10);
    delete tablet1;
    sleep(3);
    rs = router->CallProcedure(db, sp_name, request_row, &status);
    ASSERT_FALSE(rs);
    // restart
    brpc::Server tb_server2;
    ::openmldb::tablet::TabletImpl* tablet2 = new ::openmldb::tablet::TabletImpl();
    StartTablet(&tb_server2, tablet2);
    sleep(3);
    rs = router->CallProcedure(db, sp_name, request_row, &status);
    if (!rs) FAIL() << "call procedure failed";
    schema = rs->GetSchema();
    ASSERT_EQ(schema->GetColumnCnt(), 3);
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(rs->GetStringUnsafe(0), "bb");
    ASSERT_EQ(rs->GetInt32Unsafe(1), 23);
    ASSERT_EQ(rs->GetInt64Unsafe(2), 67);
    ASSERT_FALSE(rs->Next());

    // create another procedure
    std::string sp_name1 = "sp1";
    std::string sp_ddl1 = "create procedure " + sp_name1 +
                          " (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date" +
                          ")" + " begin " + sql + " end;";
    if (!router->ExecuteDDL(db, sp_ddl1, &status)) {
        FAIL() << "fail to create procedure";
    }
    ASSERT_TRUE(router->RefreshCatalog());

    ShowTable(name_server_client, db, 2);
    // drop table fail
    DropTable(name_server_client, db, "trans", false);
    // drop procedure sp
    DropProcedure(name_server_client, db, sp_name);
    // drop table fail
    DropTable(name_server_client, db, "trans", false);
    // drop procedure sp1
    DropProcedure(name_server_client, db, sp_name1);
    // drop table success
    DropTable(name_server_client, db, "trans", true);
    // drop table success
    DropTable(name_server_client, db, "trans1", true);
    ShowTable(name_server_client, db, 0);

    tb_server2.Stop(10);
    delete tablet2;
}

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 2000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::test::InitRandomDiskFlags("drop_procedure_before_drop_table_test");
    FLAGS_system_table_replica_num = 0;
    return RUN_ALL_TESTS();
}
