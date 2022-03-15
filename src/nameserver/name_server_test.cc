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
#include <sched.h>
#include <unistd.h>

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "client/ns_client.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "rpc/rpc_client.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_uint32(name_server_task_max_concurrency);
DECLARE_uint32(system_table_replica_num);
DECLARE_bool(auto_failover);
DECLARE_bool(enable_timeseries_table);

using brpc::Server;
using openmldb::tablet::TabletImpl;
using ::openmldb::zk::ZkClient;
using std::shared_ptr;
using std::string;
using std::tuple;
using std::vector;

namespace openmldb {
namespace nameserver {

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class DiskTestEnvironment : public ::testing::Environment{
    virtual void SetUp() {
        ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
    }
    virtual void TearDown() {
        ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
    }
};

class NameServerImplTest : public ::testing::Test {
 public:
    NameServerImplTest() {}
    ~NameServerImplTest() {}
    void Start(NameServerImpl* nameserver) { nameserver->running_ = true; }
    std::vector<std::list<std::shared_ptr<OPData>>>& GetTaskVec(NameServerImpl* nameserver) {
        return nameserver->task_vec_;
    }
    std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& GetTableInfo(
        NameServerImpl* nameserver) {
        return nameserver->table_info_;
    }
};

bool StartNS(const std::string& endpoint, brpc::Server* server, brpc::ServerOptions* options) {
    FLAGS_endpoint = endpoint;
    NameServerImpl* nameserver = new NameServerImpl();
    if (!nameserver->Init("")) {
        return false;
    }
    if (server->AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(endpoint.c_str(), options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    sleep(2);
    return true;
}

bool StartTablet(const std::string& endpoint, brpc::Server* server, brpc::ServerOptions* options) {
    FLAGS_endpoint = endpoint;
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    if (!tablet->Init("")) {
        return false;
    }
    if (server->AddService(tablet, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(endpoint.c_str(), options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    if (!tablet->RegisterZK()) {
        return false;
    }
    sleep(2);
    return true;
}

bool CreateDB(::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub>& name_server_client,  // NOLINT
              const std::string& db_name) {
    ::openmldb::nameserver::CreateDatabaseRequest request;
    request.set_db(db_name);
    ::openmldb::nameserver::GeneralResponse response;
    bool ret = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateDatabase, &request,
                                              &response, FLAGS_request_timeout_ms, 1);
    return ret;
}

void MakesnapshotTask(::openmldb::common::StorageMode storage_mode) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    int32_t old_offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();

    brpc::ServerOptions options;
    brpc::Server server;
    ASSERT_TRUE(StartNS("127.0.0.1:9631", &server, &options));
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client("127.0.0.1:9631", "");
    name_server_client.Init();

    brpc::ServerOptions options1;
    brpc::Server server1;
    ASSERT_TRUE(StartTablet("127.0.0.1:9530", &server1, &options1));

    CreateTableRequest request;
    GeneralResponse response;
    TableInfo* table_info = request.mutable_table_info();
    std::string name = "test" + ::openmldb::test::GenRand();
    table_info->set_name(name);
    table_info->set_storage_mode(storage_mode);
    TablePartition* partion = table_info->add_table_partition();
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
    partion->set_pid(0);
    PartitionMeta* meta = partion->add_partition_meta();
    meta->set_endpoint("127.0.0.1:9530");
    meta->set_is_leader(true);
    bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                             FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());

    MakeSnapshotNSRequest m_request;
    m_request.set_name(name);
    m_request.set_pid(0);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::MakeSnapshotNS, &m_request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);

    sleep(5);

    ZkClient zk_client(FLAGS_zk_cluster, "", 1000, FLAGS_endpoint, FLAGS_zk_root_path);
    ok = zk_client.Init();
    ASSERT_TRUE(ok);
    std::string op_index_node = FLAGS_zk_root_path + "/op/op_index";
    std::string value;
    ok = zk_client.GetNodeValue(op_index_node, value);
    ASSERT_TRUE(ok);
    std::string op_node = FLAGS_zk_root_path + "/op/op_data/" + value;
    ok = zk_client.GetNodeValue(op_node, value);
    ASSERT_FALSE(ok);

    value.clear();
    std::string table_index_node = FLAGS_zk_root_path + "/table/table_index";
    ok = zk_client.GetNodeValue(table_index_node, value);
    ASSERT_TRUE(ok);

    std::string snapshot_path;
    if (storage_mode == ::openmldb::common::kMemory) {
        snapshot_path = FLAGS_db_root_path + "/" + value + "_0/snapshot/";
    } else {
        snapshot_path = FLAGS_hdd_root_path + "/" + value + "_0/snapshot/";
    }
    std::vector<std::string> vec;
    std::vector<std::string> sub;
    int cnt = ::openmldb::base::GetFileName(snapshot_path, vec);
    int cnt2 = ::openmldb::base::GetSubDir(snapshot_path, sub);
    ASSERT_EQ(0, cnt);
    ASSERT_EQ(0, cnt2);
    ASSERT_EQ(2, (int64_t)vec.size() + (int64_t)sub.size());

    std::string table_data_node = FLAGS_zk_root_path + "/table/table_data/" + name;
    ok = zk_client.GetNodeValue(table_data_node, value);
    ASSERT_TRUE(ok);
    ::openmldb::nameserver::TableInfo table_info1;
    table_info1.ParseFromString(value);
    ASSERT_STREQ(table_info->name().c_str(), table_info1.name().c_str());
    ASSERT_EQ(table_info->table_partition_size(), table_info1.table_partition_size());

    // check drop table
    DropTableRequest drop_request;
    drop_request.set_name(name);
    response.Clear();
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropTable, &drop_request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());
    ok = zk_client.GetNodeValue(table_data_node, value);
    ASSERT_FALSE(ok);

    // snapshot with db
    std::string db = "db" + ::openmldb::test::GenRand();
    CreateDatabaseRequest db_request;
    db_request.set_db(db);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateDatabase, &db_request,
                                        &response, FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());

    table_info->set_db(db);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());

    m_request.set_db(db);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::MakeSnapshotNS, &m_request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);

    sleep(5);

    ShowTableRequest sr_request;
    ShowTableResponse sr_response;
    sr_request.set_name(name);
    sr_request.set_db(db);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTable, &sr_request, &sr_response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1, sr_response.table_info_size());
    const TableInfo& table = sr_response.table_info(0);

    op_index_node = FLAGS_zk_root_path + "/op/op_index";
    value.clear();
    ok = zk_client.GetNodeValue(op_index_node, value);
    ASSERT_TRUE(ok);
    op_node = FLAGS_zk_root_path + "/op/op_data/" + value;
    ok = zk_client.GetNodeValue(op_node, value);
    ASSERT_FALSE(ok);

    value.clear();
    table_index_node = FLAGS_zk_root_path + "/table/table_index";
    ok = zk_client.GetNodeValue(table_index_node, value);
    ASSERT_TRUE(ok);
    if (storage_mode == ::openmldb::common::kMemory) {
        snapshot_path = FLAGS_db_root_path + "/" + value + "_0/snapshot/";
    } else {
        snapshot_path = FLAGS_hdd_root_path + "/" + value + "_0/snapshot/";
    }
    vec.clear();
    sub.clear();
    cnt = ::openmldb::base::GetFileName(snapshot_path, vec);
    cnt2 = ::openmldb::base::GetSubDir(snapshot_path, sub);
    ASSERT_EQ(0, cnt);
    ASSERT_EQ(0, cnt2);
    ASSERT_EQ(2, (int64_t)vec.size() + (int64_t)sub.size());

    table_data_node = FLAGS_zk_root_path + "/table/db_table_data/" + std::to_string(table.tid());
    ok = zk_client.GetNodeValue(table_data_node, value);
    ASSERT_TRUE(ok);
    table_info1.ParseFromString(value);
    ASSERT_STREQ(table_info->name().c_str(), table_info1.name().c_str());
    ASSERT_EQ(table_info->table_partition_size(), table_info1.table_partition_size());

    drop_request.set_db(db);
    response.Clear();
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::DropTable, &drop_request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());
    ok = zk_client.GetNodeValue(table_data_node, value);
    ASSERT_FALSE(ok);

    FLAGS_make_snapshot_threshold_offset = old_offset;
    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_0");
}

TEST_F(NameServerImplTest, MakesnapshotTaskDisk) {
    MakesnapshotTask(::openmldb::common::kHDD);
}

TEST_F(NameServerImplTest, MakesnapshotTaskMem) {
    MakesnapshotTask(::openmldb::common::kMemory);
}

TEST_F(NameServerImplTest, ConfigGetAndSet) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();

    std::string endpoint = "127.0.0.1:9631";
    FLAGS_endpoint = endpoint;
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init("");
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options;
    brpc::Server server;
    if (server.AddService(nameserver, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }

    std::string endpoint1 = "127.0.0.1:9632";
    FLAGS_endpoint = endpoint1;
    NameServerImpl* nameserver1 = new NameServerImpl();
    ok = nameserver1->Init("");
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options1;
    brpc::Server server1;
    if (server1.AddService(nameserver1, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server1.Start(FLAGS_endpoint.c_str(), &options1) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ::openmldb::client::NsClient name_server_client(endpoint, "");
    name_server_client.Init();
    std::string key = "auto_failover";
    std::string msg;
    std::map<std::string, std::string> conf_map;
    bool ret = name_server_client.ConfGet(key, conf_map, msg);
    ASSERT_TRUE(ret);
    ASSERT_STREQ(conf_map[key].c_str(), "false");
    ret = name_server_client.ConfSet(key, "true", msg);
    ASSERT_TRUE(ret);
    conf_map.clear();
    ret = name_server_client.ConfGet(key, conf_map, msg);
    ASSERT_TRUE(ret);
    ASSERT_STREQ(conf_map[key].c_str(), "true");
    ret = name_server_client.DisConnectZK(msg);
    sleep(5);
    ::openmldb::client::NsClient name_server_client1(endpoint1, "");
    name_server_client1.Init();
    ret = name_server_client1.ConfGet(key, conf_map, msg);
    ASSERT_TRUE(ret);
    ASSERT_STREQ(conf_map[key].c_str(), "true");
    delete nameserver;
    delete nameserver1;
}

void CreateTable(::openmldb::common::StorageMode storage_mode) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();

    FLAGS_endpoint = "127.0.0.1:9632";
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init("");
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options;
    brpc::Server server;
    if (server.AddService(nameserver, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client(FLAGS_endpoint, "");
    name_server_client.Init();

    FLAGS_endpoint = "127.0.0.1:9531";
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    ok = tablet->Init("");
    ASSERT_TRUE(ok);
    sleep(2);

    brpc::ServerOptions options1;
    brpc::Server server1;
    if (server1.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server1.Start(FLAGS_endpoint.c_str(), &options1) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ok = tablet->RegisterZK();
    ASSERT_TRUE(ok);

    sleep(2);

    CreateTableRequest request;
    GeneralResponse response;
    TableInfo* table_info = request.mutable_table_info();
    std::string name = "test" + ::openmldb::test::GenRand();
    table_info->set_name(name);
    table_info->set_storage_mode(storage_mode);
    TablePartition* partion = table_info->add_table_partition();
    partion->set_pid(1);
    PartitionMeta* meta = partion->add_partition_meta();
    meta->set_endpoint("127.0.0.1:9531");
    meta->set_is_leader(true);
    TablePartition* partion1 = table_info->add_table_partition();
    partion1->set_pid(2);
    PartitionMeta* meta1 = partion1->add_partition_meta();
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
    meta1->set_endpoint("127.0.0.1:9531");
    meta1->set_is_leader(true);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(307, response.code());

    TablePartition* partion2 = table_info->add_table_partition();
    partion2->set_pid(0);
    PartitionMeta* meta2 = partion2->add_partition_meta();
    meta2->set_endpoint("127.0.0.1:9531");
    meta2->set_is_leader(true);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());
    delete nameserver;
    delete tablet;

    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_0");
    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_1");
    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_2");
}

TEST_F(NameServerImplTest, CreateTableDisk) {
    CreateTable(::openmldb::common::kHDD);
}

TEST_F(NameServerImplTest, CreateTableMem) {
    CreateTable(::openmldb::common::kMemory);
}

void Offline(openmldb::common::StorageMode storage_mode) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();
    FLAGS_auto_failover = true;
    FLAGS_endpoint = "127.0.0.1:9633";
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init("");
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options;
    brpc::Server server;
    if (server.AddService(nameserver, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client(FLAGS_endpoint, "");
    name_server_client.Init();

    FLAGS_endpoint = "127.0.0.1:9533";
    std::string old_db_root_path = FLAGS_db_root_path;
    std::string old_hdd_root_path = FLAGS_hdd_root_path;
    FLAGS_db_root_path = "/tmp/" + ::openmldb::test::GenRand();
    FLAGS_hdd_root_path = "/tmp/" + ::openmldb::test::GenRand();
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    ok = tablet->Init("");
    ASSERT_TRUE(ok);
    sleep(2);

    brpc::ServerOptions options1;
    brpc::Server server1;
    if (server1.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server1.Start(FLAGS_endpoint.c_str(), &options1) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ok = tablet->RegisterZK();
    ASSERT_TRUE(ok);

    FLAGS_endpoint = "127.0.0.1:9534";
    std::string tmp_hdd_root_path = FLAGS_hdd_root_path;
    FLAGS_hdd_root_path = "/tmp/" + ::openmldb::test::GenRand();
    FLAGS_db_root_path = "/tmp/" + ::openmldb::test::GenRand();
    ::openmldb::tablet::TabletImpl* tablet2 = new ::openmldb::tablet::TabletImpl();
    ok = tablet2->Init("");
    ASSERT_TRUE(ok);
    sleep(2);

    brpc::ServerOptions options2;
    brpc::Server server2;
    if (server2.AddService(tablet2, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server2.Start(FLAGS_endpoint.c_str(), &options2) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ok = tablet2->RegisterZK();
    ASSERT_TRUE(ok);

    sleep(2);
    CreateTableRequest request;
    GeneralResponse response;
    TableInfo* table_info = request.mutable_table_info();
    std::string name = "test" + ::openmldb::test::GenRand();
    table_info->set_name(name);
    table_info->set_storage_mode(storage_mode);
    TablePartition* partion = table_info->add_table_partition();
    partion->set_pid(1);
    PartitionMeta* meta = partion->add_partition_meta();
    meta->set_endpoint("127.0.0.1:9534");
    meta->set_is_leader(true);
    meta = partion->add_partition_meta();
    meta->set_endpoint("127.0.0.1:9533");
    meta->set_is_leader(false);
    TablePartition* partion1 = table_info->add_table_partition();
    partion1->set_pid(2);
    PartitionMeta* meta1 = partion1->add_partition_meta();
    meta1->set_endpoint("127.0.0.1:9534");
    meta1->set_is_leader(true);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(307, response.code());

    TablePartition* partion2 = table_info->add_table_partition();
    partion2->set_pid(0);
    PartitionMeta* meta2 = partion2->add_partition_meta();
    meta2->set_endpoint("127.0.0.1:9534");
    meta2->set_is_leader(true);
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());
    sleep(2);
    {
        ::openmldb::api::ConnectZKRequest request;
        ::openmldb::api::GeneralResponse response;
        MockClosure closure;
        tablet->ConnectZK(NULL, &request, &response, &closure);
        ASSERT_EQ(0, response.code());
    }
    sleep(6);
    {
        ::openmldb::nameserver::ShowTableRequest request;
        ::openmldb::nameserver::ShowTableResponse response;
        ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowTable, &request, &response,
                                            FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
    }
    delete nameserver;
    delete tablet;
    delete tablet2;

    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
    ::openmldb::base::RemoveDirRecursive(tmp_hdd_root_path);
    FLAGS_hdd_root_path = old_hdd_root_path;
}

TEST_F(NameServerImplTest, OfflineDisk) {
    Offline(openmldb::common::kHDD);
}

TEST_F(NameServerImplTest, OfflineMem) {
    Offline(openmldb::common::kMemory);
}

void SetTablePartition(openmldb::common::StorageMode storage_mode) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();

    FLAGS_endpoint = "127.0.0.1:9632";
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init("");
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options;
    brpc::Server server;
    if (server.AddService(nameserver, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client(FLAGS_endpoint, "");
    name_server_client.Init();

    FLAGS_endpoint = "127.0.0.1:9531";
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    ok = tablet->Init("");
    ASSERT_TRUE(ok);
    sleep(2);

    brpc::ServerOptions options1;
    brpc::Server server1;
    if (server1.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server1.Start(FLAGS_endpoint.c_str(), &options1) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ok = tablet->RegisterZK();
    ASSERT_TRUE(ok);

    sleep(2);
    std::string msg;
    ConfSetRequest conf_request;
    GeneralResponse conf_response;
    ::openmldb::nameserver::Pair* conf = conf_request.mutable_conf();
    conf->set_key("auto_failover");
    conf->set_value("false");
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ConfSet, &conf_request,
                                        &conf_response, FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);

    CreateTableRequest request;
    GeneralResponse response;
    TableInfo* table_info = request.mutable_table_info();
    std::string name = "test" + ::openmldb::test::GenRand();
    table_info->set_name(name);
    table_info->set_storage_mode(storage_mode);
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
    TablePartition* partion = table_info->add_table_partition();
    partion->set_pid(1);
    PartitionMeta* meta = partion->add_partition_meta();
    meta->set_endpoint("127.0.0.1:9531");
    meta->set_is_leader(true);
    TablePartition* partion1 = table_info->add_table_partition();
    partion1->set_pid(2);
    PartitionMeta* meta1 = partion1->add_partition_meta();
    meta1->set_endpoint("127.0.0.1:9531");
    meta1->set_is_leader(true);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(307, response.code());

    TablePartition* partion2 = table_info->add_table_partition();
    partion2->set_pid(0);
    PartitionMeta* meta2 = partion2->add_partition_meta();
    meta2->set_endpoint("127.0.0.1:9531");
    meta2->set_is_leader(true);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request, &response,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, response.code());

    ::openmldb::nameserver::GetTablePartitionRequest get_request;
    ::openmldb::nameserver::GetTablePartitionResponse get_response;
    get_request.set_name(name);
    get_request.set_pid(0);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::GetTablePartition, &get_request,
                                        &get_response, FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, get_response.code());
    ::openmldb::nameserver::TablePartition table_partition;
    table_partition.CopyFrom(get_response.table_partition());
    ASSERT_EQ(1, table_partition.partition_meta_size());
    ASSERT_TRUE(table_partition.partition_meta(0).is_leader());

    ::openmldb::nameserver::PartitionMeta* partition_meta = table_partition.mutable_partition_meta(0);
    partition_meta->set_is_leader(false);
    ::openmldb::nameserver::SetTablePartitionRequest set_request;
    ::openmldb::nameserver::GeneralResponse set_response;
    set_request.set_name(name);
    ::openmldb::nameserver::TablePartition* cur_table_partition = set_request.mutable_table_partition();
    cur_table_partition->CopyFrom(table_partition);
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::SetTablePartition, &set_request,
                                        &set_response, FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, set_response.code());

    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::GetTablePartition, &get_request,
                                        &get_response, FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(0, get_response.code());
    ASSERT_FALSE(get_response.table_partition().partition_meta(0).is_leader());

    delete nameserver;
    delete tablet;

    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_0");
    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_1");
    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_2");
}

TEST_F(NameServerImplTest, SetTablePartitionDisk) {
    SetTablePartition(openmldb::common::kHDD);
}

TEST_F(NameServerImplTest, SetTablePartitionMem) {
    SetTablePartition(openmldb::common::kMemory);
}

TEST_F(NameServerImplTest, CancelOP) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();

    FLAGS_endpoint = "127.0.0.1:9632";
    NameServerImpl* nameserver = new NameServerImpl();
    bool ok = nameserver->Init("");
    ASSERT_TRUE(ok);
    sleep(4);
    brpc::ServerOptions options;
    brpc::Server server;
    if (server.AddService(nameserver, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }

    ConfSetRequest conf_request;
    GeneralResponse conf_response;
    MockClosure closure;
    ::openmldb::nameserver::Pair* conf = conf_request.mutable_conf();
    conf->set_key("auto_failover");
    conf->set_value("false");
    nameserver->ConfSet(NULL, &conf_request, &conf_response, &closure);
    ASSERT_EQ(0, conf_response.code());

    CancelOPRequest request;
    GeneralResponse response;
    request.set_op_id(11);
    nameserver->CancelOP(NULL, &request, &response, &closure);
    ASSERT_EQ(312, response.code());

    std::vector<std::list<std::shared_ptr<OPData>>>& task_vec = GetTaskVec(nameserver);
    task_vec.resize(FLAGS_name_server_task_max_concurrency);
    std::shared_ptr<OPData> op_data = std::make_shared<OPData>();
    uint64_t op_id = 10;
    op_data->op_info_.set_op_id(op_id);
    op_data->op_info_.set_op_type(::openmldb::api::OPType::kDelReplicaOP);
    op_data->op_info_.set_task_index(0);
    op_data->op_info_.set_data("");
    op_data->op_info_.set_task_status(::openmldb::api::kInited);
    op_data->op_info_.set_name("test");
    op_data->op_info_.set_pid(0);
    op_data->op_info_.set_parent_id(UINT64_MAX);
    task_vec[0].push_back(op_data);

    request.set_op_id(10);
    response.Clear();
    nameserver->CancelOP(NULL, &request, &response, &closure);
    ASSERT_EQ(0, response.code());
    ASSERT_TRUE(op_data->op_info_.task_status() == ::openmldb::api::kCanceled);
    delete nameserver;
}

bool InitRpc(Server* server, google::protobuf::Service* general_svr) {
    brpc::ServerOptions options;
    if (server->AddService(general_svr, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Failed to add service");
        return false;
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        return false;
    }
    return true;
}

void InitTablet(int port, vector<Server*> services, vector<shared_ptr<TabletImpl>*> impls, vector<string*> eps) {
    if (services.size() != impls.size()) {
        PDLOG(WARNING, "services and impls size not equal");
        exit(1);
    }
    if (services.size() != eps.size()) {
        PDLOG(WARNING, "services and eps size not equal");
        exit(1);
    }
    for (uint64_t i = 0; i < services.size(); i++) {
        FLAGS_db_root_path = "/tmp/test4" + ::openmldb::test::GenRand();
        FLAGS_hdd_root_path = "/tmp/hdd/test4" + openmldb::test::GenRand();
        port += 500;
        FLAGS_endpoint = "127.0.0.1:" + std::to_string(port);

        shared_ptr<TabletImpl> tb = std::make_shared<TabletImpl>();
        if (!tb->Init("")) {
            PDLOG(WARNING, "failed to init tablet");
            exit(1);
        }

        if (!InitRpc(services[i], tb.get())) {
            exit(1);
        }
        if (!tb->RegisterZK()) {
            PDLOG(WARNING, "failed register tablet to zk");
            exit(1);
        }
        *eps[i] = FLAGS_endpoint;
        *impls[i] = tb;
    }
    return;
}

void InitNs(int port, vector<Server*> services, vector<shared_ptr<NameServerImpl>*> impls, vector<string*> eps) {
    if (services.size() != impls.size()) {
        PDLOG(WARNING, "services and impls size not equal");
        exit(1);
    }
    if (services.size() != eps.size()) {
        PDLOG(WARNING, "services and eps size not equal");
        exit(1);
    }
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();
    FLAGS_endpoint = "127.0.0.1:" + std::to_string(port);
    for (uint64_t i = 0; i < services.size(); i++) {
        shared_ptr<NameServerImpl> ns = std::make_shared<NameServerImpl>();
        if (!ns->Init("")) {
            PDLOG(WARNING, "failed to init ns");
            exit(1);
        }
        sleep(4);
        if (!InitRpc(services[i], ns.get())) {
            PDLOG(WARNING, "init rpc failed");
            exit(1);
        }
        *impls[i] = ns;
        *eps[i] = FLAGS_endpoint;
        port += 100;
        FLAGS_endpoint = "127.0.0.1:" + std::to_string(port);
    }
    return;
}

void AddAndRemoveReplicaCluster(openmldb::common::StorageMode storage_mode) {
    std::string old_hdd_root_path = FLAGS_hdd_root_path;

    std::shared_ptr<NameServerImpl> m1_ns1, m1_ns2, f1_ns1, f1_ns2, f2_ns1, f2_ns2;
    std::shared_ptr<TabletImpl> m1_t1, m1_t2, f1_t1, f1_t2, f2_t1, f2_t2;
    Server m1_ns1_svr, m1_ns2_svr, m1_t1_svr, m1_t2_svr;
    Server f1_ns1_svr, f1_ns2_svr, f1_t1_svr, f1_t2_svr;
    Server f2_ns1_svr, f2_ns2_svr, f2_t1_svr, f2_t2_svr;
    string m1_ns1_ep, m1_ns2_ep, m1_t1_ep,
        m1_t2_ep;  // ep == endpoint t_ep = tablet endpoint
    string f1_ns1_ep, f1_ns2_ep, f1_t1_ep, f1_t2_ep;
    string f2_ns1_ep, f2_ns2_ep, f2_t1_ep, f2_t2_ep;
    string m1_zkpath, f1_zkpath, f2_zkpath;

    vector<Server*> svrs = {&m1_ns1_svr, &m1_ns2_svr};
    vector<shared_ptr<NameServerImpl>*> ns_vector = {&m1_ns1, &m1_ns2};
    vector<shared_ptr<TabletImpl>*> tb_vector = {&m1_t1, &m1_t2};
    vector<string*> endpoints = {&m1_ns1_ep, &m1_ns2_ep};

    FLAGS_zk_cluster = "127.0.0.1:6181";
    int port = 9632;
    InitNs(port, svrs, ns_vector, endpoints);
    m1_zkpath = FLAGS_zk_root_path;

    svrs = {&m1_t1_svr, &m1_t2_svr};
    endpoints = {&m1_t1_ep, &m1_t2_ep};

    InitTablet(port, svrs, tb_vector, endpoints);

    port++;

    svrs = {&f1_ns1_svr, &f1_ns2_svr};
    ns_vector = {&f1_ns1, &f1_ns2};
    endpoints = {&f1_ns1_ep, &f1_ns2_ep};

    InitNs(port, svrs, ns_vector, endpoints);
    f1_zkpath = FLAGS_zk_root_path;

    svrs = {&f1_t1_svr, &f1_t2_svr};
    endpoints = {&f1_t1_ep, &f1_t2_ep};
    tb_vector = {&f1_t1, &f1_t2};

    InitTablet(port, svrs, tb_vector, endpoints);

    port++;

    svrs = {&f2_ns1_svr, &f2_ns2_svr};
    ns_vector = {&f2_ns1, &f2_ns2};
    endpoints = {&f2_ns1_ep, &f2_ns2_ep};

    InitNs(port, svrs, ns_vector, endpoints);
    f2_zkpath = FLAGS_zk_root_path;

    svrs = {&f2_t1_svr, &f2_t2_svr};
    endpoints = {&f2_t1_ep, &f2_t2_ep};
    tb_vector = {&f2_t1, &f2_t2};

    InitTablet(port, svrs, tb_vector, endpoints);

    // disable autoconf
    ConfSetRequest conf_set_request;
    GeneralResponse general_response;
    Pair* p = conf_set_request.mutable_conf();
    p->set_key("auto_failover");
    p->set_value("false");
    std::vector<shared_ptr<NameServerImpl>> nss{m1_ns1, f1_ns1, f2_ns1};
    MockClosure closure;
    for (auto& i : nss) {
        i->ConfSet(NULL, &conf_set_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }
    // test add leader cluster as follower role
    SwitchModeRequest switch_mode_request;
    switch_mode_request.set_sm(kLEADER);
    // switch to leader mode before add replica cluster
    m1_ns1->SwitchMode(NULL, &switch_mode_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();
    f1_ns1->SwitchMode(NULL, &switch_mode_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();

    ClusterAddress cd;
    cd.set_zk_endpoints(FLAGS_zk_cluster);
    cd.set_alias("f1");
    cd.set_zk_path(f1_zkpath);
    // except add leader cluster ad replica, that is failed.
    m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
    ASSERT_EQ(300, general_response.code());
    general_response.Clear();
    // switch normal mode, then add as replica cluster
    switch_mode_request.set_sm(kNORMAL);
    f1_ns1->SwitchMode(NULL, &switch_mode_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();
    m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();

    // replica cluster has table, add as follower cluster failed
    CreateTableRequest create_table_request;
    TableInfo* table_info = create_table_request.mutable_table_info();
    string name = "test" + ::openmldb::test::GenRand();
    table_info->set_name(name);
    table_info->set_partition_num(1);
    table_info->set_replica_num(1);
    table_info->set_storage_mode(storage_mode);
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
    f2_ns1->CreateTable(NULL, &create_table_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();

    ShowTableRequest show_table_request;
    ShowTableResponse show_table_response;
    f2_ns1->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
    ASSERT_EQ(1, show_table_response.table_info_size());
    show_table_response.Clear();

    cd.set_alias("f2");
    cd.set_zk_path(f2_zkpath);
    m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
    // failed, because leader cluster table is empty, but replica cluster have
    // tables;
    ASSERT_EQ(567, general_response.code());
    general_response.Clear();

    DropTableRequest drop_table_request;
    drop_table_request.set_name(name);
    f2_ns1->DropTable(NULL, &drop_table_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    show_table_response.Clear();
    f2_ns1->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
    ASSERT_EQ(0, show_table_response.table_info_size());
    general_response.Clear();

    // success, because replica do not have tables;
    m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());

    GeneralRequest general_request;
    ShowReplicaClusterResponse show_replica_cluster_response;
    m1_ns1->ShowReplicaCluster(NULL, &general_request, &show_replica_cluster_response, &closure);
    ASSERT_EQ(2, show_replica_cluster_response.replicas_size());
    show_replica_cluster_response.Clear();

    // readd replica cluster
    RemoveReplicaOfRequest remove_replica_of_request;
    remove_replica_of_request.set_alias("f2");
    {
        m1_ns1->RemoveReplicaCluster(NULL, &remove_replica_of_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
        m1_ns1->ShowReplicaCluster(NULL, &general_request, &show_replica_cluster_response, &closure);
        ASSERT_EQ(1, show_replica_cluster_response.replicas_size());
        show_replica_cluster_response.Clear();
    }

    cd.set_zk_path(f2_zkpath);
    cd.set_alias("f2");
    {
        m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
        m1_ns1->ShowReplicaCluster(NULL, &general_request, &show_replica_cluster_response, &closure);
        ASSERT_EQ(2, show_replica_cluster_response.replicas_size());
        show_replica_cluster_response.Clear();
    }

    ::openmldb::base::RemoveDirRecursive("/tmp/hdd/test4");
    FLAGS_hdd_root_path = old_hdd_root_path;
}

TEST_F(NameServerImplTest, AddAndRemoveReplicaClusterDisk) {
    AddAndRemoveReplicaCluster(openmldb::common::kHDD);
}

TEST_F(NameServerImplTest, AddAndRemoveReplicaClusterMem) {
    AddAndRemoveReplicaCluster(openmldb::common::kMemory);
}

void SyncTableReplicaCluster(openmldb::common::StorageMode storage_mode) {
    std::string old_hdd_root_path = FLAGS_hdd_root_path;

    std::shared_ptr<NameServerImpl> m1_ns1, m1_ns2, f1_ns1, f1_ns2, f2_ns1, f2_ns2;
    std::shared_ptr<TabletImpl> m1_t1, m1_t2, f1_t1, f1_t2, f2_t1, f2_t2;
    Server m1_ns1_svr, m1_ns2_svr, m1_t1_svr, m1_t2_svr;
    Server f1_ns1_svr, f1_ns2_svr, f1_t1_svr, f1_t2_svr;
    Server f2_ns1_svr, f2_ns2_svr, f2_t1_svr, f2_t2_svr;
    string m1_ns1_ep, m1_ns2_ep, m1_t1_ep,
        m1_t2_ep;  // ep == endpoint t_ep = tablet endpoint
    string f1_ns1_ep, f1_ns2_ep, f1_t1_ep, f1_t2_ep;
    string f2_ns1_ep, f2_ns2_ep, f2_t1_ep, f2_t2_ep;
    string m1_zkpath, f1_zkpath, f2_zkpath;

    vector<Server*> svrs = {&m1_ns1_svr, &m1_ns2_svr};
    vector<shared_ptr<NameServerImpl>*> ns_vector = {&m1_ns1, &m1_ns2};
    vector<shared_ptr<TabletImpl>*> tb_vector = {&m1_t1, &m1_t2};
    vector<string*> endpoints = {&m1_ns1_ep, &m1_ns2_ep};

    FLAGS_zk_cluster = "127.0.0.1:6181";
    int port = 9642;
    InitNs(port, svrs, ns_vector, endpoints);
    m1_zkpath = FLAGS_zk_root_path;

    svrs = {&m1_t1_svr, &m1_t2_svr};
    endpoints = {&m1_t1_ep, &m1_t2_ep};

    InitTablet(port, svrs, tb_vector, endpoints);

    port++;

    svrs = {&f1_ns1_svr, &f1_ns2_svr};
    ns_vector = {&f1_ns1, &f1_ns2};
    endpoints = {&f1_ns1_ep, &f1_ns2_ep};

    InitNs(port, svrs, ns_vector, endpoints);
    f1_zkpath = FLAGS_zk_root_path;

    svrs = {&f1_t1_svr, &f1_t2_svr};
    endpoints = {&f1_t1_ep, &f1_t2_ep};
    tb_vector = {&f1_t1, &f1_t2};

    InitTablet(port, svrs, tb_vector, endpoints);

    port++;

    svrs = {&f2_ns1_svr, &f2_ns2_svr};
    ns_vector = {&f2_ns1, &f2_ns2};
    endpoints = {&f2_ns1_ep, &f2_ns2_ep};

    InitNs(port, svrs, ns_vector, endpoints);
    f2_zkpath = FLAGS_zk_root_path;

    svrs = {&f2_t1_svr, &f2_t2_svr};
    endpoints = {&f2_t1_ep, &f2_t2_ep};
    tb_vector = {&f2_t1, &f2_t2};

    InitTablet(port, svrs, tb_vector, endpoints);

    // disable autoconf
    ConfSetRequest conf_set_request;
    GeneralResponse general_response;
    Pair* p = conf_set_request.mutable_conf();
    p->set_key("auto_failover");
    p->set_value("false");
    std::vector<shared_ptr<NameServerImpl>> nss{m1_ns1, f1_ns1, f2_ns1};
    MockClosure closure;
    for (auto& i : nss) {
        i->ConfSet(NULL, &conf_set_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }
    SwitchModeRequest switch_mode_request;
    switch_mode_request.set_sm(kLEADER);
    // switch to leader mode before add replica cluster
    m1_ns1->SwitchMode(NULL, &switch_mode_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();

    ClusterAddress cd;
    cd.set_zk_endpoints(FLAGS_zk_cluster);
    cd.set_alias("f1");
    vector<string> replica_names{"f1", "f2"};

    vector<string> replica_zk_path{f1_zkpath, f2_zkpath};
    for (uint64_t i = 0; i < replica_names.size(); i++) {
        cd.set_alias(replica_names[i]);
        cd.set_zk_path(replica_zk_path[i]);
        m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }

    GeneralRequest general_request;
    ShowReplicaClusterResponse show_replica_cluster_response;
    m1_ns1->ShowReplicaCluster(NULL, &general_request, &show_replica_cluster_response, &closure);
    ASSERT_EQ(2, show_replica_cluster_response.replicas_size());
    show_replica_cluster_response.Clear();

    CreateTableRequest create_table_request;
    TableInfo* table_info = create_table_request.mutable_table_info();
    string name = "test" + ::openmldb::test::GenRand();
    table_info->set_name(name);
    table_info->set_storage_mode(storage_mode);
    table_info->set_partition_num(1);
    table_info->set_replica_num(1);
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);

    m1_ns1->CreateTable(NULL, &create_table_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    ShowTableRequest show_table_request;
    ShowTableResponse show_table_response;
    sleep(10);
    for (auto& ns : nss) {
        ns->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
        ASSERT_EQ(1, show_table_response.table_info_size());
        ASSERT_EQ(name, show_table_response.table_info(0).name());
        show_table_response.Clear();
    }
    DropTableRequest drop_table_request;
    drop_table_request.set_name(name);
    m1_ns1->DropTable(NULL, &drop_table_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();
    drop_table_request.Clear();
    sleep(8);
    for (auto& ns : nss) {
        ns->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
        ASSERT_EQ(0, show_table_response.table_info_size());
        show_table_response.Clear();
    }
    RemoveReplicaOfRequest remove_replica_of_request;
    for (auto s : replica_names) {
        remove_replica_of_request.set_alias(s);
        m1_ns1->RemoveReplicaCluster(NULL, &remove_replica_of_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }
    name = "test" + ::openmldb::test::GenRand();
    table_info->set_name(name);

    m1_ns1->CreateTable(NULL, &create_table_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());

    for (uint64_t i = 0; i < replica_names.size(); i++) {
        cd.set_alias(replica_names[i]);
        cd.set_zk_path(replica_zk_path[i]);
        m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }
    sleep(4);
    for (auto& ns : nss) {
        ns->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
        ASSERT_EQ(1, show_table_response.table_info_size());
        ASSERT_EQ(name, show_table_response.table_info(0).name());
        show_table_response.Clear();
    }

    ::openmldb::base::RemoveDirRecursive("/tmp/hdd/test4");
    FLAGS_hdd_root_path = old_hdd_root_path;
}

TEST_F(NameServerImplTest, SyncTableReplicaClusterDisk) {
    SyncTableReplicaCluster(openmldb::common::kHDD);
}

TEST_F(NameServerImplTest, SyncTableReplicaClusterMem) {
    SyncTableReplicaCluster(openmldb::common::kMemory);
}

/* TEST_F(NameServerImplTest, DataSyncReplicaCluster) {
    std::shared_ptr<NameServerImpl> m1_ns1, m1_ns2, f1_ns1, f1_ns2, f2_ns1, f2_ns2;
    std::shared_ptr<TabletImpl> m1_t1, m1_t2, f1_t1, f1_t2, f2_t1, f2_t2;
    Server m1_ns1_svr, m1_ns2_svr, m1_t1_svr, m1_t2_svr;
    Server f1_ns1_svr, f1_ns2_svr, f1_t1_svr, f1_t2_svr;
    Server f2_ns1_svr, f2_ns2_svr, f2_t1_svr, f2_t2_svr;
    string m1_ns1_ep, m1_ns2_ep, m1_t1_ep,
        m1_t2_ep;  // ep == endpoint t_ep = tablet endpoint
    string f1_ns1_ep, f1_ns2_ep, f1_t1_ep, f1_t2_ep;
    string f2_ns1_ep, f2_ns2_ep, f2_t1_ep, f2_t2_ep;
    string m1_zkpath, f1_zkpath, f2_zkpath;

    vector<Server*> svrs = {&m1_ns1_svr, &m1_ns2_svr};
    vector<shared_ptr<NameServerImpl>*> ns_vector = {&m1_ns1, &m1_ns2};
    vector<shared_ptr<TabletImpl>*> tb_vector = {&m1_t1, &m1_t2};
    vector<string*> endpoints = {&m1_ns1_ep, &m1_ns2_ep};

    FLAGS_zk_cluster = "127.0.0.1:6181";
    int port = 9642;
    InitNs(port, svrs, ns_vector, endpoints);
    m1_zkpath = FLAGS_zk_root_path;

    svrs = {&m1_t1_svr, &m1_t2_svr};
    endpoints = {&m1_t1_ep, &m1_t2_ep};

    InitTablet(port, svrs, tb_vector, endpoints);

    port++;

    svrs = {&f1_ns1_svr, &f1_ns2_svr};
    ns_vector = {&f1_ns1, &f1_ns2};
    endpoints = {&f1_ns1_ep, &f1_ns2_ep};

    InitNs(port, svrs, ns_vector, endpoints);
    f1_zkpath = FLAGS_zk_root_path;

    svrs = {&f1_t1_svr, &f1_t2_svr};
    endpoints = {&f1_t1_ep, &f1_t2_ep};
    tb_vector = {&f1_t1, &f1_t2};

    InitTablet(port, svrs, tb_vector, endpoints);

    port++;

    svrs = {&f2_ns1_svr, &f2_ns2_svr};
    ns_vector = {&f2_ns1, &f2_ns2};
    endpoints = {&f2_ns1_ep, &f2_ns2_ep};

    InitNs(port, svrs, ns_vector, endpoints);
    f2_zkpath = FLAGS_zk_root_path;

    svrs = {&f2_t1_svr, &f2_t2_svr};
    endpoints = {&f2_t1_ep, &f2_t2_ep};
    tb_vector = {&f2_t1, &f2_t2};

    InitTablet(port, svrs, tb_vector, endpoints);

    // disable autoconf
    ConfSetRequest conf_set_request;
    GeneralResponse general_response;
    Pair* p = conf_set_request.mutable_conf();
    p->set_key("auto_failover");
    p->set_value("false");
    std::vector<shared_ptr<NameServerImpl>> nss{m1_ns1, f1_ns1, f2_ns1};
    MockClosure closure;
    for (auto& i : nss) {
        i->ConfSet(NULL, &conf_set_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }
    vector<shared_ptr<NameServerImpl>> follower_nss{f1_ns1, f2_ns1};
    {
        CreateTableRequest create_table_request;
        TableInfo* table_info = create_table_request.mutable_table_info();
        string name = "test" + ::openmldb::test::GenRand();
        table_info->set_name(name);
        table_info->set_partition_num(1);
        table_info->set_replica_num(2);
        ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
        f2_ns1->CreateTable(NULL, &create_table_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        ShowTableRequest show_table_request;
        ShowTableResponse show_table_response;
        f2_ns1->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
        ASSERT_EQ(1, show_table_response.table_info_size());
        show_table_response.Clear();
        DropTableRequest drop_table_request;
        drop_table_request.set_name(name);
        f2_ns1->DropTable(NULL, &drop_table_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        show_table_response.Clear();
        f2_ns1->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
        ASSERT_EQ(0, show_table_response.table_info_size());
        general_response.Clear();
    }
    SwitchModeRequest switch_mode_request;
    switch_mode_request.set_sm(kLEADER);
    // switch to leader mode before add replica cluster
    m1_ns1->SwitchMode(NULL, &switch_mode_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();

    ClusterAddress cd;
    cd.set_zk_endpoints(FLAGS_zk_cluster);
    cd.set_alias("f1");
    vector<string> replica_names{"f1", "f2"};

    vector<string> replica_zk_path{f1_zkpath, f2_zkpath};
    for (uint64_t i = 0; i < replica_names.size(); i++) {
        cd.set_alias(replica_names[i]);
        cd.set_zk_path(replica_zk_path[i]);
        m1_ns1->AddReplicaCluster(NULL, &cd, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }

    GeneralRequest general_request;
    ShowReplicaClusterResponse show_replica_cluster_response;
    m1_ns1->ShowReplicaCluster(NULL, &general_request, &show_replica_cluster_response, &closure);
    ASSERT_EQ(2, show_replica_cluster_response.replicas_size());
    show_replica_cluster_response.Clear();

    CreateTableRequest create_table_request;
    string name = "test" + ::openmldb::test::GenRand();
    TableInfo* table_info = create_table_request.mutable_table_info();
    table_info->set_name(name);
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
    TablePartition* partition = table_info->add_table_partition();
    partition->set_pid(0);
    PartitionMeta* meta1 = partition->add_partition_meta();
    meta1->set_is_leader(true);
    meta1->set_endpoint(m1_t1_ep);
    PartitionMeta* meta2 = partition->add_partition_meta();
    meta2->set_is_leader(false);
    meta2->set_endpoint(m1_t2_ep);
    m1_ns1->CreateTable(NULL, &create_table_request, &general_response, &closure);
    ASSERT_EQ(0, general_response.code());
    general_response.Clear();

    for (auto& ns : follower_nss) {
        ns->CreateTable(NULL, &create_table_request, &general_response, &closure);
        ASSERT_EQ(501, general_response.code());
        general_response.Clear();
    }

    sleep(8);
    ShowTableRequest show_table_request;
    ShowTableResponse show_table_response;

    m1_ns1->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
    ASSERT_EQ(1, show_table_response.table_info_size());
    ASSERT_EQ(name, show_table_response.table_info(0).name());
    uint32_t tid = show_table_response.table_info().begin()->tid();
    show_table_response.Clear();
    for (auto& ns : nss) {
        ns->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
        ASSERT_EQ(1, show_table_response.table_info_size());
        ASSERT_EQ(2, show_table_response.table_info(0).table_partition(0).partition_meta_size());
        ASSERT_EQ(name, show_table_response.table_info(0).name());
        show_table_response.Clear();
    }
    ::openmldb::api::PutRequest put_request;
    ::openmldb::api::PutResponse put_response;
    string pk = "1";
    put_request.set_pk(pk);
    put_request.set_time(1);
    put_request.set_value("a");
    put_request.set_tid(tid);
    put_request.set_pid(0);
    m1_t1->Put(NULL, &put_request, &put_response, &closure);
    ASSERT_EQ(0, put_response.code());
    sleep(4);
    std::vector<shared_ptr<TabletImpl>> tablets{m1_t1, m1_t2, f1_t1, f1_t2};
    std::vector<shared_ptr<TabletImpl>> f2_tablets{f2_t1, f2_t2};
    {
        ::openmldb::api::TraverseRequest traverse_request;
        ::openmldb::api::TraverseResponse traverse_response;
        traverse_request.set_pid(0);
        traverse_request.set_tid(tid);
        for (auto& tablet : tablets) {
            tablet->Traverse(NULL, &traverse_request, &traverse_response, &closure);
            ASSERT_EQ(0, traverse_response.code());
            ASSERT_EQ(1, (int64_t)traverse_response.count());
            traverse_response.Clear();
        }
    }
    {
        ::openmldb::api::TraverseRequest traverse_request;
        ::openmldb::api::TraverseResponse traverse_response;
        traverse_request.set_pid(0);
        traverse_request.set_tid(tid + 1);
        for (auto& tablet : f2_tablets) {
            tablet->Traverse(NULL, &traverse_request, &traverse_response, &closure);
            ASSERT_EQ(0, traverse_response.code());
            ASSERT_EQ(1, (int64_t)traverse_response.count());
            traverse_response.Clear();
        }
    }
    ::openmldb::api::ScanRequest scan_request;
    scan_request.set_pk(pk);
    scan_request.set_st(0);
    scan_request.set_et(0);
    scan_request.set_tid(tid);
    scan_request.set_pid(0);
    ::openmldb::api::ScanResponse* scan_response = new ::openmldb::api::ScanResponse();
    sleep(4);
    for (auto& tablet : tablets) {
        tablet->Scan(NULL, &scan_request, scan_response, &closure);
        ASSERT_EQ(0, scan_response->code());
        ASSERT_EQ(1, (int64_t)scan_response->count());
        scan_response->Clear();
    }
    scan_request.set_tid(tid + 1);
    for (auto& tablet : f2_tablets) {
        tablet->Scan(NULL, &scan_request, scan_response, &closure);
        ASSERT_EQ(0, scan_response->code());
        ASSERT_EQ(1, (int64_t)scan_response->count());
        scan_response->Clear();
    }
    {
        ChangeLeaderRequest change_leader_request;
        change_leader_request.set_name(name);
        change_leader_request.set_pid(0);
        change_leader_request.set_candidate_leader(m1_t2_ep);
        m1_ns1->ChangeLeader(NULL, &change_leader_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        sleep(6);
        general_response.Clear();
        RecoverTableRequest recover_table_request;
        recover_table_request.set_name(name);
        recover_table_request.set_pid(0);
        recover_table_request.set_endpoint(m1_t1_ep);
        m1_ns1->RecoverTable(NULL, &recover_table_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }
    sleep(6);
    put_request.set_pk(pk);
    put_request.set_time(2);
    put_request.set_value("b");
    put_request.set_tid(tid);
    put_request.set_pid(0);
    m1_t2->Put(NULL, &put_request, &put_response, &closure);
    ASSERT_EQ(0, put_response.code());
    sleep(8);
    scan_request.set_tid(tid);
    for (auto& tablet : tablets) {
        tablet->Scan(NULL, &scan_request, scan_response, &closure);
        ASSERT_EQ(0, scan_response->code());
        ASSERT_EQ(2, (int64_t)scan_response->count());
        scan_response->Clear();
    }
    scan_request.set_tid(tid + 1);
    for (auto& tablet : f2_tablets) {
        tablet->Scan(NULL, &scan_request, scan_response, &closure);
        ASSERT_EQ(0, scan_response->code());
        ASSERT_EQ(2, (int64_t)scan_response->count());
        scan_response->Clear();
    }
    for (auto& i : follower_nss) {
        i->ShowTable(NULL, &show_table_request, &show_table_response, &closure);
        ASSERT_EQ(0, show_table_response.code());
        ASSERT_EQ(1, show_table_response.table_info_size());
        ASSERT_EQ(2, show_table_response.table_info(0).table_partition(0).partition_meta_size());
        ASSERT_EQ(name, show_table_response.table_info(0).name());
        string leader_ep, follower_ep;
        for (auto part : show_table_response.table_info(0).table_partition(0).partition_meta()) {
            if (part.is_leader() && part.is_alive()) {
                leader_ep = part.endpoint();
            } else {
                follower_ep = part.endpoint();
            }
        }
        if (leader_ep.empty() || follower_ep.empty()) {
            PDLOG(WARNING, "endpoint is empty");
            exit(1);
        }
        ChangeLeaderRequest change_leader_request;
        change_leader_request.set_name(name);
        change_leader_request.set_pid(0);
        change_leader_request.set_candidate_leader(follower_ep);
        i->ChangeLeader(NULL, &change_leader_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
        show_table_response.Clear();
        sleep(6);
        RecoverTableRequest recover_table_request;
        recover_table_request.set_name(name);
        recover_table_request.set_pid(0);
        recover_table_request.set_endpoint(leader_ep);
        i->RecoverTable(NULL, &recover_table_request, &general_response, &closure);
        ASSERT_EQ(0, general_response.code());
        general_response.Clear();
    }
    sleep(6);
    put_request.set_pk(pk);
    put_request.set_time(3);
    put_request.set_value("c");
    put_request.set_tid(tid);
    put_request.set_pid(0);
    m1_t2->Put(NULL, &put_request, &put_response, &closure);
    ASSERT_EQ(0, put_response.code());
    sleep(18);
    scan_request.set_tid(tid);
    for (auto& tablet : tablets) {
        tablet->Scan(NULL, &scan_request, scan_response, &closure);
        ASSERT_EQ(0, scan_response->code());
        ASSERT_EQ(3, (int64_t)scan_response->count());
        scan_response->Clear();
    }
    scan_request.set_tid(tid + 1);
    for (auto& tablet : f2_tablets) {
        tablet->Scan(NULL, &scan_request, scan_response, &closure);
        ASSERT_EQ(0, scan_response->code());
        ASSERT_EQ(3, (int64_t)scan_response->count());
        scan_response->Clear();
    }
} */

void ShowCatalogVersion(::openmldb::common::StorageMode storage_mode) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();

    brpc::ServerOptions options;
    brpc::Server server;
    ASSERT_TRUE(StartNS("127.0.0.1:9634", &server, &options));
    ::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub> name_server_client("127.0.0.1:9634", "");
    name_server_client.Init();

    brpc::ServerOptions options1;
    brpc::Server server1;
    ASSERT_TRUE(StartTablet("127.0.0.1:9535", &server1, &options1));

    brpc::ServerOptions options2;
    brpc::Server server2;
    ASSERT_TRUE(StartTablet("127.0.0.1:9536", &server2, &options2));
    std::string db_name = "db1";
    ASSERT_TRUE(CreateDB(name_server_client, db_name));

    {
        CreateTableRequest request;
        GeneralResponse response;
        TableInfo* table_info = request.mutable_table_info();
        std::string name = "test" + ::openmldb::test::GenRand();
        table_info->set_name(name);
        table_info->set_db(db_name);
        table_info->set_storage_mode(storage_mode);
        ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
        TablePartition* partion = table_info->add_table_partition();
        partion->set_pid(0);
        PartitionMeta* meta = partion->add_partition_meta();
        meta->set_endpoint("127.0.0.1:9535");
        meta->set_is_leader(true);
        partion = table_info->add_table_partition();
        partion->set_pid(1);
        meta = partion->add_partition_meta();
        meta->set_endpoint("127.0.0.1:9536");
        meta->set_is_leader(true);
        bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request,
                                                 &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
    }
    sleep(1);

    std::map<std::string, uint64_t> version_map;
    ShowCatalogRequest request;
    ShowCatalogResponse response;
    bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowCatalog, &request, &response,
                                             FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(response.catalog_size(), 2);
    for (const auto& cur_catalog : response.catalog()) {
        version_map.emplace(cur_catalog.endpoint(), cur_catalog.version());
        PDLOG(INFO, "endpoint %s version %lu", cur_catalog.endpoint().c_str(), cur_catalog.version());
    }

    {
        CreateTableRequest request;
        GeneralResponse response;
        TableInfo* table_info = request.mutable_table_info();
        std::string name = "test" + ::openmldb::test::GenRand();
        table_info->set_name(name);
        table_info->set_db(db_name);
        table_info->set_storage_mode(storage_mode);
        ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
        TablePartition* partion = table_info->add_table_partition();
        partion->set_pid(0);
        PartitionMeta* meta = partion->add_partition_meta();
        meta->set_endpoint("127.0.0.1:9535");
        meta->set_is_leader(true);
        partion = table_info->add_table_partition();
        partion->set_pid(1);
        meta = partion->add_partition_meta();
        meta->set_endpoint("127.0.0.1:9536");
        meta->set_is_leader(true);
        bool ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateTable, &request,
                                                 &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
    }
    sleep(2);
    ShowCatalogRequest request1;
    ShowCatalogResponse response1;
    ok = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::ShowCatalog, &request1, &response1,
                                        FLAGS_request_timeout_ms, 1);
    ASSERT_TRUE(ok);
    ASSERT_EQ(response1.catalog_size(), 2);
    for (const auto& cur_catalog : response1.catalog()) {
        ASSERT_EQ(cur_catalog.version(), version_map[cur_catalog.endpoint()] + 1);
        PDLOG(INFO, "endpoint %s version %lu", cur_catalog.endpoint().c_str(), cur_catalog.version());
    }

    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_0");
    ::openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path + "/2_1");
}

TEST_F(NameServerImplTest, ShowCatalogVersionDisk) {
    ShowCatalogVersion(openmldb::common::kHDD);
}

TEST_F(NameServerImplTest, ShowCatalogVersionMem) {
    ShowCatalogVersion(openmldb::common::kMemory);
}

}  // namespace nameserver
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::AddGlobalTestEnvironment(new ::openmldb::nameserver::DiskTestEnvironment);
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    // FLAGS_db_root_path = "/tmp/" + ::openmldb::test::GenRand();
    // FLAGS_ssd_root_path = "/tmp/ssd/" + ::openmldb::test::GenRand();
    // FLAGS_hdd_root_path = "/tmp/hdd/" + ::openmldb::test::GenRand();
    FLAGS_db_root_path = "/tmp/1";
    FLAGS_ssd_root_path = "/tmp/ssd/1";
    FLAGS_hdd_root_path = "/tmp/hdd/1";
    FLAGS_system_table_replica_num = 0;
    return RUN_ALL_TESTS();
}
