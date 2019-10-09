//
// binlog_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-01
//

#include "replica/log_replicator.h"
#include "replica/replicate_node.h"
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include "proto/tablet.pb.h"
#include "logging.h"
#include "thread_pool.h"
#include <brpc/server.h>
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
DECLARE_int32(binlog_single_file_max_size);
DECLARE_int32(binlog_delete_interval);
DECLARE_int32(make_snapshot_threshold_offset);
namespace rtidb {
namespace replica {

class BinlogTest : public ::testing::Test {

public:
    BinlogTest() {}

    ~BinlogTest() {}
};

TEST_F(BinlogTest, DeleteBinlog) {
    FLAGS_binlog_single_file_max_size = 1;
    FLAGS_binlog_delete_interval = 500;
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    tablet->Init();
    int offset = FLAGS_make_snapshot_threshold_offset;
    FLAGS_make_snapshot_threshold_offset = 0;
	brpc::Server server;
    if (server.AddService(tablet, brpc::SERVER_OWNS_SERVICE) != 0) {
       PDLOG(WARNING, "fail to register tablet rpc service");
       exit(1);
    }
    brpc::ServerOptions options;
    std::string leader_point = "127.0.0.1:18529";
    if (server.Start(leader_point.c_str(), &options) != 0) {
        PDLOG(WARNING, "fail to start server %s", leader_point.c_str());
        exit(1);
    }

    uint32_t tid = 2;
    uint32_t pid = 123;

    ::rtidb::client::TabletClient client(leader_point);
    client.Init();
    std::vector<std::string> endpoints;
    bool ret = client.CreateTable("table1", tid, pid, 100000, true, endpoints,
                    ::rtidb::api::TTLType::kAbsoluteTime, 16, 0, ::rtidb::api::CompressType::kNoCompress);
    ASSERT_TRUE(ret);
    
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    int count = 1000;
    while(count) {
        char key[20];
        snprintf(key, 20, "testkey_%d", count);
        ret = client.Put(tid, pid, key, cur_time, std::string(10 * 1024, 'a'));
        count--;
    }
    ret = client.MakeSnapshot(tid, pid);
    ASSERT_TRUE(ret);
    sleep(2);
    std::vector<std::string> vec;
    std::string binlog_path = FLAGS_db_root_path + "/2_123/binlog";
    ::rtidb::base::GetFileName(binlog_path, vec);
    ASSERT_EQ(1, vec.size());
    std::string file_name = binlog_path + "/00000004.log";
    ASSERT_STREQ(file_name.c_str(), vec[0].c_str());
    FLAGS_make_snapshot_threshold_offset = offset;
}

}
}

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::GenRand();
    return RUN_ALL_TESTS();
}

