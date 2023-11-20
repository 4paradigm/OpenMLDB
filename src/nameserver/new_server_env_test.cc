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

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "client/ns_client.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_string(zk_auth_schema);
DECLARE_string(zk_cert);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(request_timeout_ms);
DECLARE_bool(binlog_notify_on_put);
DECLARE_bool(use_name);
DECLARE_uint32(system_table_replica_num);

using ::openmldb::zk::ZkClient;

namespace openmldb {
namespace nameserver {

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class NewServerEnvTest : public ::testing::Test {
 public:
    NewServerEnvTest() {}

    ~NewServerEnvTest() {}
};

void StartNameServer(brpc::Server& server, const std::string& real_ep) {  // NOLINT
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init(real_ep);
    ASSERT_EQ(true, nameserver->RegisterName());
    ASSERT_TRUE(ok);
    brpc::ServerOptions options;
    if (server.AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(real_ep.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    sleep(2);
}

void StartTablet(brpc::Server& server, const std::string& real_ep) {  // NOLINT
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    bool ok = tablet->Init(real_ep);
    ASSERT_TRUE(ok);
    brpc::ServerOptions options;
    if (server.AddService(tablet, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(real_ep.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ok = tablet->RegisterZK();
    ASSERT_TRUE(ok);
    sleep(2);
}

void SetSdkEndpoint(::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub>& name_server_client,  // NOLINT
                    const std::string& server_name, const std::string& sdk_endpoint) {
    ::openmldb::nameserver::SetSdkEndpointRequest request;
    request.set_server_name(server_name);
    request.set_sdk_endpoint(sdk_endpoint);
    ::openmldb::nameserver::GeneralResponse response;
    bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::SetSdkEndpoint, &request,
                                             &response, FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
}

void ShowNameServer(std::map<std::string, std::string>* map) {
    std::shared_ptr<::openmldb::zk::ZkClient> zk_client;
    zk_client = std::make_shared<::openmldb::zk::ZkClient>(FLAGS_zk_cluster, "", 1000, "", FLAGS_zk_root_path,
            FLAGS_zk_auth_schema, FLAGS_zk_cert);
    if (!zk_client->Init()) {
        ASSERT_TRUE(false);
    }
    std::string node_path = FLAGS_zk_root_path + "/leader";
    std::vector<std::string> children;
    if (!zk_client->GetChildren(node_path, children) || children.empty()) {
        ASSERT_TRUE(false);
    }
    for (auto path : children) {
        std::string endpoint;
        std::string real_path = node_path + "/" + path;
        if (!zk_client->GetNodeValue(real_path, endpoint)) {
            ASSERT_TRUE(false);
        }
        map->insert(std::make_pair(endpoint, ""));
    }
    for (auto& kv : *map) {
        std::string real_endpoint;
        std::string name = "/map/names/" + kv.first;
        if (zk_client->IsExistNode(FLAGS_zk_root_path + name) == 0) {
            if (!zk_client->GetNodeValue(FLAGS_zk_root_path + name, real_endpoint) || real_endpoint.empty()) {
                ASSERT_TRUE(false);
            }
        }
        kv.second = real_endpoint;
    }
}

TEST_F(NewServerEnvTest, ShowRealEndpoint) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb4" + ::openmldb::test::GenRand();

    // ns1
    FLAGS_use_name = true;
    FLAGS_endpoint = "ns1";
    std::string ns_real_ep = "127.0.0.1:9631";
    brpc::Server ns_server;
    StartNameServer(ns_server, ns_real_ep);
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client(ns_real_ep);
    name_server_client.Init();

    // tablet1
    FLAGS_use_name = true;
    FLAGS_endpoint = "tb1";
    std::string tb_real_ep_1 = "127.0.0.1:9831";
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    brpc::Server tb_server1;
    StartTablet(tb_server1, tb_real_ep_1);

    // tablet2
    FLAGS_use_name = true;
    FLAGS_endpoint = "tb2";
    std::string tb_real_ep_2 = "127.0.0.1:9931";
    FLAGS_db_root_path = tmp_path.GetTempPath();
    brpc::Server tb_server2;
    StartTablet(tb_server2, tb_real_ep_2);

    {
        std::map<std::string, std::string> map;
        ShowNameServer(&map);
        ASSERT_EQ(1u, map.size());
        auto it = map.find("ns1");
        if (it != map.end()) {
            ASSERT_EQ(ns_real_ep, it->second);
        } else {
            ASSERT_TRUE(false);
        }
    }
    {
        // showtablet
        ::openmldb::nameserver::ShowTabletRequest request;
        ::openmldb::nameserver::ShowTabletResponse response;
        bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTablet, &request,
                                                 &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);

        ::openmldb::nameserver::TabletStatus status = response.tablets(0);
        ASSERT_EQ("tb1", status.endpoint());
        ASSERT_EQ(tb_real_ep_1, status.real_endpoint());
        ASSERT_EQ("kHealthy", status.state());

        status = response.tablets(1);
        ASSERT_EQ("tb2", status.endpoint());
        ASSERT_EQ(tb_real_ep_2, status.real_endpoint());
        ASSERT_EQ("kHealthy", status.state());
    }
    std::string ns_sdk_ep = "127.0.0.1:8881";
    std::string tb_sdk_ep_1 = "127.0.0.1:8882";
    std::string tb_sdk_ep_2 = "127.0.0.1:8883";
    {
        // set sdkendpoint
        SetSdkEndpoint(name_server_client, "ns1", ns_sdk_ep);
        SetSdkEndpoint(name_server_client, "tb1", tb_sdk_ep_1);
        SetSdkEndpoint(name_server_client, "tb2", tb_sdk_ep_2);
    }
    {
        // show sdkendpoint
        ::openmldb::nameserver::ShowSdkEndpointRequest request;
        ::openmldb::nameserver::ShowSdkEndpointResponse response;
        bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowSdkEndpoint, &request,
                                                 &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);

        auto status = response.tablets(0);
        ASSERT_EQ("ns1", status.endpoint());
        ASSERT_EQ(ns_sdk_ep, status.real_endpoint());

        status = response.tablets(1);
        ASSERT_EQ("tb1", status.endpoint());
        ASSERT_EQ(tb_sdk_ep_1, status.real_endpoint());

        status = response.tablets(2);
        ASSERT_EQ("tb2", status.endpoint());
        ASSERT_EQ(tb_sdk_ep_2, status.real_endpoint());
    }
}

/*TEST_F(NewServerEnvTest, SyncMultiReplicaData) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb4" + GenRand();

    // ns1
    FLAGS_use_name = true;
    FLAGS_endpoint = "ns1";
    std::string ns_real_ep = "127.0.0.1:9631";
    brpc::Server ns_server;
    StartNameServer(ns_server, ns_real_ep);
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client(ns_real_ep);
    name_server_client.Init();

    // tablet1
    FLAGS_binlog_notify_on_put = true;
    FLAGS_use_name = true;
    FLAGS_endpoint = "tb1";
    std::string tb_real_ep_1 = "127.0.0.1:9831";
    FLAGS_db_root_path = "/tmp/" + ::openmldb::nameserver::GenRand();
    brpc::Server tb_server1;
    StartTablet(tb_server1, tb_real_ep_1);
    ::openmldb::RpcClient<::openmldb::api::TabletServer_Stub> tb_client_1(tb_real_ep_1);
    tb_client_1.Init();

    // tablet2
    FLAGS_use_name = true;
    FLAGS_endpoint = "tb2";
    std::string tb_real_ep_2 = "127.0.0.1:9931";
    FLAGS_db_root_path = "/tmp/" + ::openmldb::nameserver::GenRand();
    brpc::Server tb_server2;
    StartTablet(tb_server2, tb_real_ep_2);
    ::openmldb::RpcClient<::openmldb::api::TabletServer_Stub> tb_client_2(tb_real_ep_2);
    tb_client_2.Init();

    bool ok = false;
    std::string name = "test" + GenRand();
    {
        CreateTableRequest request;
        GeneralResponse response;
        TableInfo* table_info = request.mutable_table_info();
        table_info->set_name(name);
        auto column_desc = table_info->add_column_desc();
        column_desc->set_name("idx0");
        column_desc->set_data_type(::openmldb::type::kString);
        auto column_desc1 = table_info->add_column_desc();
        column_desc1->set_name("value");
        column_desc1->set_data_type(::openmldb::type::kString);
        auto column_key = table_info->add_column_key();
        column_key->set_index_name("idx0");
        column_key->add_col_name("idx0");
        ::openmldb::common::TTLSt* ttl_st = column_key->mutable_ttl();
        ttl_st->set_abs_ttl(0);
        ttl_st->set_lat_ttl(0);
        ttl_st->set_ttl_type(::openmldb::type::kAbsoluteTime);
        TablePartition* partion = table_info->add_table_partition();
        partion->set_pid(0);
        PartitionMeta* meta = partion->add_partition_meta();
        meta->set_endpoint("tb1");
        meta->set_is_leader(true);
        meta = partion->add_partition_meta();
        meta->set_endpoint("tb2");
        meta->set_is_leader(false);
        ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                            FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
    }
    uint32_t tid = 0;
    {
        ::openmldb::nameserver::ShowTableRequest request;
        ::openmldb::nameserver::ShowTableResponse response;
        request.set_name(name);
        bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTable, &request,
                                                 &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        tid = response.table_info(0).tid();
    }
    {
        ::openmldb::api::PutRequest put_request;
        ::openmldb::api::PutResponse put_response;
        put_request.set_pk("1");
        put_request.set_time(1);
        put_request.set_value("a");
        put_request.set_tid(tid);
        put_request.set_pid(0);
        ok = tb_client_1.SendRequest(&::openmldb::api::TabletServer_Stub::Put, &put_request, &put_response,
                                     FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, put_response.code());
    }
    {
        ::openmldb::api::TraverseRequest traverse_request;
        ::openmldb::api::TraverseResponse traverse_response;
        traverse_request.set_pid(0);
        traverse_request.set_tid(tid);
        ok = tb_client_1.SendRequest(&::openmldb::api::TabletServer_Stub::Traverse, &traverse_request,
                                     &traverse_response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, traverse_response.code());
        ASSERT_EQ(1u, traverse_response.count());
        ASSERT_EQ("1", traverse_response.pk());
        ASSERT_EQ(1u, traverse_response.ts());
    }
    sleep(3);
    {
        ::openmldb::api::TraverseRequest traverse_request;
        ::openmldb::api::TraverseResponse traverse_response;
        traverse_request.set_tid(tid);
        traverse_request.set_pid(0);
        ok = tb_client_2.SendRequest(&::openmldb::api::TabletServer_Stub::Traverse, &traverse_request,
                                     &traverse_response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, traverse_response.code());
        ASSERT_EQ(1u, traverse_response.count());
        ASSERT_EQ("1", traverse_response.pk());
        ASSERT_EQ(1u, traverse_response.ts());
    }
} */

}  // namespace nameserver
}  // namespace openmldb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_system_table_replica_num = 0;
    return RUN_ALL_TESTS();
}
