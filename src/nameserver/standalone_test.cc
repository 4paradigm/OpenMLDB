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

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_uint32(name_server_task_max_concurrency);
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
class StandaloneTest : public ::testing::Test {
 public:
    StandaloneTest() {}
    ~StandaloneTest() {}
    void Start(NameServerImpl* nameserver) { nameserver->running_ = true; }
    std::vector<std::list<std::shared_ptr<OPData>>>& GetTaskVec(NameServerImpl* nameserver) {
        return nameserver->task_vec_;
    }
    std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>& GetTableInfo(
        NameServerImpl* nameserver) {
        return nameserver->table_info_;
    }
};


bool CreateDB(::openmldb::RpcClient<::openmldb::nameserver::NameServer_Stub>& name_server_client,  // NOLINT
              const std::string& db_name) {
    ::openmldb::nameserver::CreateDatabaseRequest request;
    request.set_db(db_name);
    ::openmldb::nameserver::GeneralResponse response;
    bool ret = name_server_client.SendRequest(&::openmldb::nameserver::NameServer_Stub::CreateDatabase, &request,
                                              &response, FLAGS_request_timeout_ms, 1);
    return ret;
}

TEST_F(StandaloneTest, MakesnapshotTask) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    int32_t old_offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
    FLAGS_zk_root_path = "/rtidb3" + GenRand();

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
    std::string name = "test" + GenRand();
    table_info->set_name(name);
    TablePartition* partion = table_info->add_table_partition();
    AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, table_info);
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
    std::string snapshot_path = FLAGS_db_root_path + "/" + value + "_0/snapshot/";
    std::vector<std::string> vec;
    int cnt = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, cnt);
    ASSERT_EQ(2, (int64_t)vec.size());

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
    std::string db = "db" + GenRand();
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
    snapshot_path = FLAGS_db_root_path + "/" + value + "_0/snapshot/";
    vec.clear();
    cnt = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, cnt);
    ASSERT_EQ(2, (int64_t)vec.size());

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
}


}  // namespace nameserver
}  // namespace openmldb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::openmldb::nameserver::GenRand();
    return RUN_ALL_TESTS();
}
