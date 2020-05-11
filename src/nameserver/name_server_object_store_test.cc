//
// name_server_create_remote_test.cc
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-22
//

#include <blobserver/blobserver_impl.h>
#include <timer.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "base/glog_wapper.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"

DECLARE_string(endpoint);
DECLARE_string(hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_uint32(name_server_task_max_concurrency);
DECLARE_bool(auto_failover);

using ::rtidb::zk::ZkClient;

namespace rtidb {
namespace nameserver {

inline std::string GenRand() { return std::to_string(rand() % 10000000 + 1); } // NOLINT

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};
class NameServerImplObjectStoreTest : public ::testing::Test {
 public:
    NameServerImplObjectStoreTest() {}
    ~NameServerImplObjectStoreTest() {}
};

void StartNameServer(brpc::Server* server, NameServerImpl* nameserver) {
    bool ok = nameserver->Init();
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options;
    if (server->AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
}

void StartNameServer(brpc::Server* server) {
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init();
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options;
    if (server->AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
}

void StartBlob(brpc::Server* server) {
    FLAGS_hdd_root_path = "/tmp/object_store_test/" + GenRand();
    ::rtidb::blobserver::BlobServerImpl* blob =
        new ::rtidb::blobserver::BlobServerImpl();
    bool ok = blob->Init();
    ASSERT_TRUE(ok);
    sleep(2);
    brpc::ServerOptions options1;
    if (server->AddService(blob, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options1) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ok = blob->RegisterZK();
    ASSERT_TRUE(ok);
    sleep(2);
}

TEST_F(NameServerImplObjectStoreTest, CreateTable) {
    // local ns and tablet
    // ns
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + GenRand();
    FLAGS_endpoint = "127.0.0.1:9631";

    NameServerImpl* nameserver_1 = new NameServerImpl();
    brpc::Server server;
    StartNameServer(&server, nameserver_1);
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub>
        name_server_client_1(FLAGS_endpoint);
    name_server_client_1.Init();

    FLAGS_endpoint = "127.0.0.1:9931";
    brpc::Server server1;
    StartBlob(&server1);
    ::rtidb::client::BsClient blob_client(FLAGS_endpoint);
    ASSERT_EQ(0, blob_client.Init());

    sleep(6);

    bool ok = false;
    std::string name = "test" + GenRand();
    {
        CreateTableRequest request;
        GeneralResponse response;
        TableInfo* table_info = request.mutable_table_info();
        table_info->set_name(name);
        table_info->set_table_type(::rtidb::type::kObjectStore);
        table_info->set_replica_num(1);
        ok = name_server_client_1.SendRequest(
            &::rtidb::nameserver::NameServer_Stub::CreateTable, &request,
            &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
        sleep(3);
    }
    {
        ::rtidb::nameserver::ShowTableRequest request;
        ::rtidb::nameserver::ShowTableResponse response;
        request.set_name(name);
        ok = name_server_client_1.SendRequest(
            &::rtidb::nameserver::NameServer_Stub::ShowTable, &request,
            &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, response.table_info_size());
        const auto& info = response.table_info(0);
        ASSERT_EQ(::rtidb::type::TableType::kObjectStore, info.table_type());
        uint32_t tid = info.tid();
        ::rtidb::blobserver::StoreStatus status;
        ok = blob_client.GetStoreStatus(tid, 0, &status);
        ASSERT_TRUE(ok);
        ASSERT_EQ(tid, status.tid());
        ASSERT_EQ(0u, status.pid());
    }
}

}  // namespace nameserver
}  // namespace rtidb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::rtidb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    // FLAGS_db_root_path = "/tmp/" + ::rtidb::nameserver::GenRand();
    return RUN_ALL_TESTS();
}
