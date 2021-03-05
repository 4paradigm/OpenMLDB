//
// name_server_create_remote_test.cc
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-22
//

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <sched.h>
#include <timer.h>
#include <unistd.h>

#include "blobserver/blobserver_impl.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "base/glog_wapper.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "tablet/tablet_impl.h"

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_uint32(name_server_task_max_concurrency);
DECLARE_bool(auto_failover);

using ::rtidb::zk::ZkClient;
using google::protobuf::RepeatedPtrField;

namespace rtidb {
namespace nameserver {

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1); // NOLINT
}

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
    bool ok = nameserver->Init("");
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
    bool ok = nameserver->Init("");
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
    FLAGS_hdd_root_path = "/tmp/object_store_test" + GenRand();
    ::rtidb::blobserver::BlobServerImpl* blob =
        new ::rtidb::blobserver::BlobServerImpl();
    bool ok = blob->Init("");
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

void StartTablet(brpc::Server* server, rtidb::tablet::TabletImpl** tb) {
    FLAGS_db_root_path = "/tmp/test" + GenRand();
    FLAGS_hdd_root_path = "/tmp/test" + GenRand();
    FLAGS_ssd_root_path = "/tmp/test" + GenRand();
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    bool ok = tablet->Init("");
    ASSERT_TRUE(ok);
    sleep(2);
    brpc::ServerOptions options1;
    if (server->AddService(tablet, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (server->Start(FLAGS_endpoint.c_str(), &options1) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    ok = tablet->RegisterZK();
    ASSERT_TRUE(ok);
    *tb = tablet;
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
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub> name_server_client_1(FLAGS_endpoint, "");
    name_server_client_1.Init();

    FLAGS_endpoint = "127.0.0.1:9931";
    brpc::Server server1;
    StartBlob(&server1);
    ::rtidb::client::BsClient blob_client(FLAGS_endpoint, "");
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

TEST_F(NameServerImplObjectStoreTest, CreateTableWithBlobField) {
    // local ns and tablet
    // ns
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb3" + GenRand();
    FLAGS_endpoint = "127.0.0.1:9631";

    NameServerImpl* nameserver = new NameServerImpl();
    brpc::Server server;
    StartNameServer(&server, nameserver);
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub> name_server_client(FLAGS_endpoint, "");
    ASSERT_EQ(0, name_server_client.Init());

    FLAGS_endpoint = "127.0.0.1:9731";
    brpc::Server tablet_svr;
    ::rtidb::tablet::TabletImpl* tablet;
    StartTablet(&tablet_svr, &tablet);
    ::rtidb::client::TabletClient tablet_client(FLAGS_endpoint, "");
    ASSERT_EQ(0, tablet_client.Init());

    FLAGS_endpoint = "127.0.0.1:9931";
    brpc::Server server1;
    StartBlob(&server1);
    ::rtidb::client::BsClient blob_client(FLAGS_endpoint, "");
    ASSERT_EQ(0, blob_client.Init());

    sleep(6);

    bool ok = false;
    std::string name = "test" + GenRand();
    Schema schema;
    {
        CreateTableRequest request;
        GeneralResponse response;
        TableInfo* table_info = request.mutable_table_info();
        table_info->set_table_type(::rtidb::type::kRelational);
        table_info->set_name(name);
        TablePartition* partion = table_info->add_table_partition();
        partion->set_pid(0);
        PartitionMeta* meta = partion->add_partition_meta();
        meta->set_endpoint("127.0.0.1:9731");
        meta->set_is_leader(true);
        ::rtidb::common::ColumnDesc* col0 = table_info->add_column_desc_v1();
        col0->set_name("card");
        col0->set_data_type(::rtidb::type::kBigInt);
        col0->set_not_null(true);
        ::rtidb::common::ColumnDesc* col1 = table_info->add_column_desc_v1();
        col1->set_name("mcc");
        col1->set_data_type(::rtidb::type::kVarchar);
        ::rtidb::common::ColumnDesc* col2 = table_info->add_column_desc_v1();
        col2->set_name("img");
        col2->set_data_type(::rtidb::type::kBlob);
        ::rtidb::common::ColumnKey* ck = table_info->add_column_key();
        ck->set_index_name("card");
        ck->add_col_name("card");
        ck->set_index_type(::rtidb::type::kPrimaryKey);
        table_info->set_replica_num(1);
        ok = name_server_client.SendRequest(
            &::rtidb::nameserver::NameServer_Stub::CreateTable, &request,
            &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
        sleep(3);
        schema = table_info->column_desc_v1();
    }
    {
        ::rtidb::nameserver::ShowTableRequest request;
        ::rtidb::nameserver::ShowTableResponse response;
        request.set_name(name);
        ok = name_server_client.SendRequest(
            &::rtidb::nameserver::NameServer_Stub::ShowTable, &request,
            &response, FLAGS_request_timeout_ms, 1);
        ASSERT_TRUE(ok);
        ASSERT_EQ(0, response.code());
        ASSERT_EQ(1, response.table_info_size());
        const auto& info = response.table_info(0);
        ASSERT_EQ(1, info.blob_info().blob_partition_size());
        uint32_t tid = info.tid();
        ::rtidb::blobserver::StoreStatus blob_status;
        ok = blob_client.GetStoreStatus(tid, 0, &blob_status);
        ASSERT_TRUE(ok);
        ASSERT_EQ(tid, blob_status.tid());
        ASSERT_EQ(0u, blob_status.pid());
        ::rtidb::api::GetTableStatusResponse resp;
        ok = tablet_client.GetTableStatus(resp);
        ASSERT_TRUE(ok);
        ASSERT_EQ(1, resp.all_table_status_size());
        ::rtidb::api::TableStatus table_status = resp.all_table_status(0);
        ASSERT_EQ(tid, table_status.tid());
        ASSERT_EQ(0u, table_status.pid());
        std::string err_msg;
        int64_t blob_key;
        std::string blob_data = "this is blob data";
        ok = blob_client.Put(tid, 0, blob_data, &blob_key, &err_msg);
        ASSERT_TRUE(ok);
        rtidb::codec::RowBuilder builder(schema);
        uint32_t size = builder.CalTotalLength(4);
        std::string row;
        row.resize(size);
        builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
        ASSERT_TRUE(builder.AppendInt64(10l));
        std::string mcc_data = "mcc0";
        ASSERT_TRUE(builder.AppendString(mcc_data.data(), mcc_data.size()));
        ASSERT_TRUE(builder.AppendBlob(blob_key));
        ok = tablet_client.Put(tid, 0, "", 0, row);
        ASSERT_TRUE(ok);
        std::vector<std::string> keys{"10"};
        std::string data;
        uint32_t count;
        RepeatedPtrField<rtidb::api::ReadOption> ros;
        {
            auto ro = ros.Add();
            auto index = ro->add_index();
            index->add_name("card");
            std::string* val = index->mutable_value();
            ok = rtidb::codec::Convert("10", rtidb::type::kBigInt, val);
            ASSERT_TRUE(ok);
        }
        ok = tablet_client.BatchQuery(tid, 0, ros, &data, &count, &err_msg);
        ASSERT_TRUE(ok);
        ASSERT_EQ((int32_t)count, 1);
        rtidb::codec::RowView view(schema);
        ok = view.Reset(reinterpret_cast<int8_t*>(&data[0]+4),
                        size);
        ASSERT_TRUE(ok);
        int64_t val = 0;
        int ret = view.GetInt64(0, &val);
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(val, 10l);
        char* ch = NULL;
        uint32_t length = 0;
        ASSERT_EQ(view.GetString(1, &ch, &length), 0);
        std::string get_data(ch, length);
        ASSERT_STREQ(mcc_data.data(), get_data.c_str());
        ASSERT_EQ(view.GetBlob(2, &val), 0);
        ASSERT_EQ(val, blob_key);
        data.clear();
        ok = blob_client.Get(tid, 0, blob_key, &data, &err_msg);
        ASSERT_TRUE(ok);
        ret = memcmp(blob_data.c_str(), data.c_str(), blob_data.size());
        ASSERT_EQ(ret, 0);
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
