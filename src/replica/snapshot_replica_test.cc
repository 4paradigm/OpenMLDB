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

DECLARE_string(db_root_path);
DECLARE_string(endpoint);

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}

namespace rtidb {
namespace replica {

class MockClosure : public ::google::protobuf::Closure {

public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}

};

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

    FLAGS_db_root_path = "/tmp/" + ::GenRand();
    FLAGS_endpoint = "127.0.0.1:18530";
    sofa::pbrpc::RpcServerOptions options1;
    sofa::pbrpc::RpcServer rpc_server1(options1);
    ::rtidb::tablet::TabletImpl* tablet1 = new ::rtidb::tablet::TabletImpl();
    tablet1->Init();
    sofa::pbrpc::Servlet webservice1 =
            sofa::pbrpc::NewPermanentExtClosure(tablet1, &rtidb::tablet::TabletImpl::WebService);
    if (!rpc_server1.RegisterService(tablet1)) {
       LOG(WARNING, "fail to register tablet rpc service");
       exit(1);
    }
    rpc_server1.RegisterWebServlet("/tablet", webservice1);
    std::string follower_point = "127.0.0.1:18530";
    if (!rpc_server1.Start(follower_point)) {
        LOG(WARNING, "fail to listen port %s", follower_point.c_str());
        exit(1);
    }
    ::rtidb::client::TabletClient client1(follower_point);
    ret = client1.CreateTable("table1", tid, pid, 14400, false, endpoints, 8);
    ASSERT_TRUE(ret);
    client.AddReplica(tid, pid, follower_point);
    sleep(3);
	
	::rtidb::api::ScanRequest sr;
	MockClosure closure;
    sr.set_tid(tid);
    sr.set_pid(pid);
    sr.set_pk("testkey");
    sr.set_st(cur_time + 1);
    sr.set_et(cur_time - 1);
    sr.set_limit(10);
    ::rtidb::api::ScanResponse srp;
    tablet1->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(1, srp.count());
    ASSERT_EQ(0, srp.code());
	
    ret = client.Put(tid, pid, "newkey", cur_time, "value2");
    ASSERT_TRUE(ret);
	sleep(2);
    sr.set_pk("newkey");
    tablet1->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(1, srp.count());
    ASSERT_EQ(0, srp.code());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::GenRand();
    return RUN_ALL_TESTS();
}

