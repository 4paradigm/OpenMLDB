//
// name_server_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong 
// Date 2017-09-07
//

#include "gtest/gtest.h"
#include "logging.h"
#include "timer.h"
#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>
#include "tablet/tablet_impl.h"
#include "proto/tablet.pb.h"
#include "proto/name_server.pb.h"
#include <boost/lexical_cast.hpp>
#include "name_server_impl.h"
#include "rpc/rpc_client.h"

DECLARE_string(snapshot_root_path);
DECLARE_string(binlog_root_path);
DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

using ::rtidb::zk::ZkClient;


namespace rtidb {
namespace nameserver {


uint32_t counter = 10;
static int32_t endpoint_size = 1;


inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}

class MockClosure : public ::google::protobuf::Closure {

public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}

};
class NameServerImplTest : public ::testing::Test {

public:
    NameServerImplTest() {}
    ~NameServerImplTest() {}
};


TEST_F(NameServerImplTest, CreateTable) {
    FLAGS_endpoint="127.0.0.1:9530";
    FLAGS_zk_cluster="127.0.0.1:12181";
    FLAGS_zk_root_path="/rtidb3";
    ZkClient zk_client(FLAGS_zk_cluster, 1000, FLAGS_endpoint, FLAGS_zk_root_path);
    bool ok = zk_client.Init();
    ASSERT_TRUE(ok);
    ok = zk_client.Mkdir(FLAGS_zk_root_path + "/nodes");
    ASSERT_TRUE(ok);

    FLAGS_endpoint = "127.0.0.1:9531";
    NameServerImpl* nameserver = new NameServerImpl();
    ok = nameserver->Init();
    ASSERT_TRUE(ok);
    endpoint_size++;
    sleep(4);
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    sofa::pbrpc::Servlet webservice =
            sofa::pbrpc::NewPermanentExtClosure(nameserver, &rtidb::nameserver::NameServerImpl::WebService);
    if (!rpc_server.RegisterService(nameserver)) {
       LOG(WARNING, "fail to register nameserver rpc service");
       exit(1);
    }
    rpc_server.RegisterWebServlet("/nameserver", webservice);
    if (!rpc_server.Start(FLAGS_endpoint)) {
        LOG(WARNING, "fail to listen port %s", FLAGS_endpoint.c_str());
        exit(1);
    }
    ::rtidb::RpcClient name_server_client;
    ::rtidb::nameserver::NameServer_Stub *stub = NULL;
    name_server_client.GetStub(FLAGS_endpoint, &stub);

    FLAGS_endpoint="127.0.0.1:9530";
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    ok = tablet->Init();
    ASSERT_TRUE(ok);
    sleep(2);
    sofa::pbrpc::RpcServerOptions options1;
    sofa::pbrpc::RpcServer rpc_server1(options1);
    sofa::pbrpc::Servlet webservice1 =
            sofa::pbrpc::NewPermanentExtClosure(tablet, &rtidb::tablet::TabletImpl::WebService);
    if (!rpc_server1.RegisterService(tablet)) {
       LOG(WARNING, "fail to register nameserver rpc service");
       exit(1);
    }
    rpc_server1.RegisterWebServlet("/tablet", webservice1);
    if (!rpc_server1.Start(FLAGS_endpoint)) {
        LOG(WARNING, "fail to listen port %s", FLAGS_endpoint.c_str());
        exit(1);
    }
    sleep(2);

    
    CreateTableRequest request;
    GeneralResponse response;

    TableMeta *meta = request.mutable_table_meta();
    meta->set_name("test" + GenRand());
    TablePartition* partion = meta->add_table_partition();
    partion->set_endpoint("127.0.0.1:9530");
    partion->set_is_leader(true);
    partion->set_pid(0);
    ok = name_server_client.SendRequest(stub,
            &::rtidb::nameserver::NameServer_Stub::CreateTable,
            &request, &response, 12, 1);
    ASSERT_TRUE(ok);

    FLAGS_endpoint = "127.0.0.1:9532";
    NameServerImpl* nameserver2 = new NameServerImpl();
    ok = nameserver2->Init();
    ASSERT_TRUE(ok);
    sleep(3);
    
    CreateTableRequest request1;
    GeneralResponse response1;

    TableMeta *meta1 = request1.mutable_table_meta();
    meta1->set_name("test" + GenRand());
    TablePartition* partion1 = meta1->add_table_partition();
    partion1->set_endpoint("127.0.0.1:9530");
    partion1->set_is_leader(true);
    partion1->set_pid(0);
    MockClosure closure;
    nameserver2->CreateTable(NULL, &request1, &response1, &closure);
    ASSERT_EQ(-1, response1.code());

}

}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_snapshot_root_path = "/tmp/" + ::rtidb::nameserver::GenRand();
    FLAGS_binlog_root_path = "/tmp/" + ::rtidb::nameserver::GenRand();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



