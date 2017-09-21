//
// snapshot_replica_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-08-16
//

#include "replica/log_replicator.h"
#include "replica/replicate_node.h"
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/atomic.hpp>
#include <boost/bind.hpp>
#include <stdio.h>
#include "proto/tablet.pb.h"
#include "logging.h"
#include "thread_pool.h"
#include <sofa/pbrpc/pbrpc.h>
#include "storage/table.h"
#include "storage/segment.h"
#include "storage/ticket.h"
#include "timer.h"
#include "tablet/tablet_impl.h"
#include "client/tablet_client.h"
#include <gflags/gflags.h>
#include "base/file_util.h"

using ::baidu::common::ThreadPool;
using ::rtidb::storage::Table;
using ::rtidb::storage::Ticket;
using ::rtidb::storage::DataBlock;
using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::INFO;
using ::baidu::common::DEBUG;
using ::rtidb::tablet::TabletImpl;

DECLARE_string(binlog_root_path);
DECLARE_string(snapshot_root_path);

namespace rtidb {
namespace replica {

class SnapshotReplicaTest : public ::testing::Test {

public:
    SnapshotReplicaTest() {}

    ~SnapshotReplicaTest() {}
};

TEST_F(SnapshotReplicaTest, AddReplicate) {
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    tablet->Init();
    sofa::pbrpc::Servlet webservice =
            sofa::pbrpc::NewPermanentExtClosure(tablet, &rtidb::tablet::TabletImpl::WebService);
    if (!rpc_server.RegisterService(tablet)) {
       LOG(WARNING, "fail to register tablet rpc service");
       exit(1);
    }
    rpc_server.RegisterWebServlet("/tablet", webservice);
    std::string leader_point = "127.0.0.1:18529";
    if (!rpc_server.Start(leader_point)) {
        LOG(WARNING, "fail to listen port %s", leader_point.c_str());
        exit(1);
    }

    uint32_t tid = 2;
    uint32_t pid = 123;

    ::rtidb::client::TabletClient client(leader_point);
    std::vector<std::string> endpoints;
    bool ret = client.CreateTable("table1", tid, pid, 100000, true, endpoints);
    ASSERT_TRUE(ret);

    std::string end_point = "127.0.0.1:18530";
    ret = client.AddReplica(tid, pid, end_point);
    ASSERT_TRUE(ret);
    sleep(1);

    ::rtidb::api::TableStatus table_status;
    if (client.GetTableStatus(tid, pid, table_status) < 0) {
        ASSERT_TRUE(0);
    }
    ASSERT_EQ(::rtidb::api::kTableNormal, table_status.state());

    ret = client.DelReplica(tid, pid, end_point);
    ASSERT_TRUE(ret);
}

TEST_F(SnapshotReplicaTest, LeaderAndFollower) {
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    tablet->Init();
    sofa::pbrpc::Servlet webservice =
            sofa::pbrpc::NewPermanentExtClosure(tablet, &rtidb::tablet::TabletImpl::WebService);
    if (!rpc_server.RegisterService(tablet)) {
       LOG(WARNING, "fail to register tablet rpc service");
       exit(1);
    }
    rpc_server.RegisterWebServlet("/tablet", webservice);
    std::string leader_point = "127.0.0.1:18529";
    if (!rpc_server.Start(leader_point)) {
        LOG(WARNING, "fail to listen port %s", leader_point.c_str());
        exit(1);
    }

    uint32_t tid = 1;
    uint32_t pid = 123;

    ::rtidb::client::TabletClient client(leader_point);
    std::vector<std::string> endpoints;
    bool ret = client.CreateTable("table1", tid, pid, 100000, true, endpoints);
    ASSERT_TRUE(ret);
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    ret = client.Put(tid, pid, "testkey", cur_time, "value1");
    ASSERT_TRUE(ret);

    uint32_t count = 0;
    while (count < 10) {
        count++;
        char key[100];
        snprintf(key, 100, "test%u", count);
        client.Put(tid, pid, key, cur_time, key);
    }

    // TODO. test new add replicate case
}

}
}

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_snapshot_root_path = "/tmp/" + ::GenRand();
    FLAGS_binlog_root_path = "/tmp/" + ::GenRand();
    return RUN_ALL_TESTS();
}

