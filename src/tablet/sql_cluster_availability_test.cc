//
// sql_cluster_availability_test.cc
// Copyright (C) 2020 4paradigm.com
//

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <unistd.h>
#include <timer.h>

#include "base/glog_wapper.h"
#include "client/ns_client.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "tablet/tablet_impl.h"
#include "sdk/sql_router.h"

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(request_timeout_ms);
DECLARE_bool(binlog_notify_on_put);
DECLARE_bool(auto_failover);

using ::rtidb::zk::ZkClient;
using ::rtidb::nameserver::NameServerImpl;

namespace rtidb {
namespace tablet {

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class SqlClusterTest : public ::testing::Test {
 public:
    SqlClusterTest() {}

    ~SqlClusterTest() {}
};

std::shared_ptr<rtidb::sdk::SQLRouter> GetNewSQLRouter() {
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    rtidb::sdk::SQLRouterOptions sql_opt;
    sql_opt.zk_cluster = FLAGS_zk_cluster;
    sql_opt.zk_path = FLAGS_zk_root_path;
    sql_opt.enable_debug = true;
    return rtidb::sdk::NewClusterSQLRouter(sql_opt);
}

void StartNameServer(brpc::Server& server) { //NOLINT
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

void StartTablet(brpc::Server* server, ::rtidb::tablet::TabletImpl* tablet) { //NOLINT
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

TEST_F(SqlClusterTest, RecoverProcedure) {
    FLAGS_auto_failover = true;
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb4" + GenRand();

    // ns1
    FLAGS_endpoint = "127.0.0.1:9631";
    brpc::Server ns_server;
    StartNameServer(ns_server);
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub>
        name_server_client(FLAGS_endpoint, "");
    name_server_client.Init();

    // tablet1
    FLAGS_endpoint = "127.0.0.1:9831";
    FLAGS_db_root_path = "/tmp/" + GenRand();
    brpc::Server* tb_server1 = new brpc::Server();
    ::rtidb::tablet::TabletImpl* tablet1 = new ::rtidb::tablet::TabletImpl();
    StartTablet(tb_server1, tablet1);

    {
        // showtablet
        ::rtidb::nameserver::ShowTabletRequest request;
        ::rtidb::nameserver::ShowTabletResponse response;
        bool ok = name_server_client.SendRequest(
                &::rtidb::nameserver::NameServer_Stub::ShowTablet,
                &request, &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);

        ::rtidb::nameserver::TabletStatus status =
            response.tablets(0);
        ASSERT_EQ(FLAGS_endpoint, status.endpoint());
        ASSERT_EQ("kTabletHealthy", status.state());
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
        "                   index(key=c1, ts=c7));";
    auto router = GetNewSQLRouter();
    if (!router) {
        FAIL() << "Fail new cluster sql router";
    }
    std::string db = "test";
    fesql::sdk::Status status;
    router->CreateDB(db, &status);
    router->ExecuteDDL(db, "drop table trans;", &status);
    ASSERT_TRUE(router->RefreshCatalog());
    if (!router->ExecuteDDL(db, ddl, &status)) {
        FAIL() << "fail to create table";
    }
    ASSERT_TRUE(router->RefreshCatalog());
    // insert
    std::string insert_sql =
        "insert into trans values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(router->ExecuteInsert(db, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp";
    std::string sql =
        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS"
        " (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl =
        "create procedure " + sp_name +
        " (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date" + ")" +
        " begin " + sql + " end;";
    if (!router->ExecuteDDL(db, sp_ddl, &status)) {
        FAIL() << "fail to create procedure";
    }
    // call procedure
    ASSERT_TRUE(router->RefreshCatalog());
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
    ASSERT_EQ(schema->GetColumnCnt(), 3u);
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(rs->GetStringUnsafe(0), "bb");
    ASSERT_EQ(rs->GetInt32Unsafe(1), 23);
    ASSERT_EQ(rs->GetInt64Unsafe(2), 67);
    ASSERT_FALSE(rs->Next());
    // stop
    delete tablet1;
    delete tb_server1;
    sleep(3);
    rs = router->CallProcedure(db, sp_name, request_row, &status);
    ASSERT_FALSE(rs);
    // restart
    brpc::Server* tb_server2 = new brpc::Server();
    ::rtidb::tablet::TabletImpl* tablet2 = new ::rtidb::tablet::TabletImpl();
    StartTablet(tb_server2, tablet2);
    sleep(3);
    rs = router->CallProcedure(db, sp_name, request_row, &status);
    if (!rs) FAIL() << "call procedure failed";
    schema = rs->GetSchema();
    ASSERT_EQ(schema->GetColumnCnt(), 3u);
    ASSERT_TRUE(rs->Next());
    ASSERT_EQ(rs->GetStringUnsafe(0), "bb");
    ASSERT_EQ(rs->GetInt32Unsafe(1), 23);
    ASSERT_EQ(rs->GetInt64Unsafe(2), 67);
    ASSERT_FALSE(rs->Next());
    delete tablet2;
    delete tb_server2;
}

}  // namespace tablet
}  // namespace rtidb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 2000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::rtidb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
