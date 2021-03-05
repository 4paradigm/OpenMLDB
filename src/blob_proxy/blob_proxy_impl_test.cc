//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-25

#include "blob_proxy/blob_proxy_impl.h"

#include <gflags/gflags.h>
#include <google/protobuf/stubs/common.h>
#include <string>

#include "blobserver/blobserver_impl.h"
#include "client/bs_client.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "base/glog_wapper.h"

DECLARE_string(endpoint);
DECLARE_string(hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(zk_keep_alive_check_interval);

using rtidb::nameserver::CreateTableRequest;
using rtidb::nameserver::GeneralResponse;
using rtidb::nameserver::NameServerImpl;
using rtidb::nameserver::TableInfo;

namespace rtidb {
namespace blobproxy {

uint32_t counter = 10;

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class BlobProxyImplTest : public ::testing::Test {
 public:
    BlobProxyImplTest() {}
    ~BlobProxyImplTest() {}
};

void StartNameServer(brpc::Server* server, NameServerImpl* nameserver) {
    bool ok = nameserver->Init("");
    ASSERT_TRUE(ok);
    brpc::ServerOptions options;
    if (server->AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    sleep(2);
}

void StartBlob(brpc::Server* server) {
    FLAGS_hdd_root_path = "/tmp/object_store_test/" + GenRand();
    ::rtidb::blobserver::BlobServerImpl* blob =
        new ::rtidb::blobserver::BlobServerImpl();
    bool ok = blob->Init("");
    ASSERT_TRUE(ok);
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

void StartProxy(brpc::Server* server) {
    ::rtidb::blobproxy::BlobProxyImpl* proxy =
        new ::rtidb::blobproxy::BlobProxyImpl();
    bool ok = proxy->Init();
    ASSERT_TRUE(ok);
    sleep(2);
    brpc::ServerOptions options1;
    brpc::ServerOptions options;
    if (server->AddService(proxy, brpc::SERVER_DOESNT_OWN_SERVICE,
                           "/v1/get/* => Get") != 0) {
        PDLOG(WARNING, "fail to add service");
        exit(1);
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options1) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ASSERT_TRUE(ok);
    sleep(2);
}

TEST_F(BlobProxyImplTest, Basic_Test) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + GenRand();
    FLAGS_endpoint = "127.0.0.1:9631";

    NameServerImpl* nameserver_1 = new NameServerImpl();
    brpc::Server server;
    StartNameServer(&server, nameserver_1);
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub> name_server_client_1(FLAGS_endpoint);
    name_server_client_1.Init();

    FLAGS_endpoint = "127.0.0.1:9931";
    brpc::Server server1;
    StartBlob(&server1);
    ::rtidb::client::BsClient blob_client(FLAGS_endpoint, "");
    ASSERT_EQ(0, blob_client.Init());

    sleep(6);

    bool ok = false;
    std::string name = "test" + GenRand();
    uint32_t tid, pid = 0;
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
        tid = info.tid();
        ::rtidb::blobserver::StoreStatus status;
        ok = blob_client.GetStoreStatus(tid, 0, &status);
        ASSERT_TRUE(ok);
        ASSERT_EQ(tid, status.tid());
        ASSERT_EQ(0u, status.pid());
    }
    int64_t key = 10086;
    std::string value = "testvalue1";
    std::string err_msg;
    ok = blob_client.Put(tid, pid, key, value, &err_msg);
    ASSERT_TRUE(ok);
    std::string get_value;
    ok = blob_client.Get(tid, pid, key, &get_value, &err_msg);
    ASSERT_TRUE(ok);
    {
        int code = memcmp(value.data(), get_value.data(), value.length());
        ASSERT_EQ(0, code);
    }
    {
        FLAGS_endpoint = "127.0.0.1:9932";
        brpc::Server server;
        StartProxy(&server);

        brpc::ChannelOptions oc;
        oc.protocol = brpc::PROTOCOL_HTTP;
        brpc::Channel channel;
        int ret = channel.Init(FLAGS_endpoint.c_str(), &oc);
        ASSERT_EQ(0, ret);
        brpc::Controller cntl;
        cntl.http_request().uri() = FLAGS_endpoint + "/v1/get/" + name + "/" + std::to_string(key) + "?format=mp3";
        cntl.http_request().set_method(brpc::HTTP_METHOD_GET);
        channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_EQ(200, cntl.http_response().status_code());
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ("audio/mpeg", cntl.http_response().content_type());
        butil::IOBuf buff = cntl.response_attachment();
        std::string ss = buff.to_string();
        int code = memcmp(value.data(), ss.data(), value.length());
        ASSERT_EQ(0, code);
    }
}

}  // namespace blobproxy
}  // namespace rtidb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::rtidb::base::SetLogLevel(INFO);
    FLAGS_hdd_root_path = "/tmp/test_blobserver" + ::rtidb::blobproxy::GenRand();
    return RUN_ALL_TESTS();
}
