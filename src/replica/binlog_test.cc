//
// binlog_test.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-01
//

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sched.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "base/file_util.h"
#include "client/tablet_client.h"
#include "base/glog_wapper.h" // NOLINT
#include "proto/tablet.pb.h"
#include "replica/log_replicator.h"
#include "replica/replicate_node.h"
#include "storage/segment.h"
#include "storage/table.h"
#include "storage/ticket.h"
#include "tablet/tablet_impl.h"
#include "thread_pool.h" // NOLINT
#include "timer.h" // NOLINT
#include "config.h" // NOLINT

using ::baidu::common::ThreadPool;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::rtidb::storage::DataBlock;
using ::rtidb::storage::Table;
using ::rtidb::storage::Ticket;
using ::rtidb::tablet::TabletImpl;

DECLARE_string(db_root_path);
DECLARE_int32(binlog_single_file_max_size);
DECLARE_int32(binlog_delete_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_string(snapshot_compression);

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
    tablet->Init("");
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

    ::rtidb::client::TabletClient client(leader_point, "");
    client.Init();
    std::vector<std::string> endpoints;
    bool ret =
        client.CreateTable("table1", tid, pid, 100000, 0, true, endpoints,
                           ::rtidb::api::TTLType::kAbsoluteTime, 16, 0,
                           ::rtidb::api::CompressType::kNoCompress);
    ASSERT_TRUE(ret);

    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    int count = 1000;
    while (count) {
        char key[20];
        snprintf(key, sizeof(key), "testkey_%d", count);
        ret = client.Put(tid, pid, key, cur_time, std::string(10 * 1024, 'a'));
        count--;
    }
    ret = client.MakeSnapshot(tid, pid, 0);
    std::string binlog_path = FLAGS_db_root_path + "/2_123/binlog";
    std::vector<std::string> vec;
    ASSERT_TRUE(ret);
    for (int i = 0; i < 50; i++) {
        vec.clear();
        ::rtidb::base::GetFileName(binlog_path, vec);
        if (vec.size() == 1) {
            break;
        }
        sleep(2);
    }
    vec.clear();
    ::rtidb::base::GetFileName(binlog_path, vec);
    ASSERT_EQ(1, (int64_t)vec.size());
    std::string file_name = binlog_path + "/00000004.log";
    ASSERT_STREQ(file_name.c_str(), vec[0].c_str());
    FLAGS_make_snapshot_threshold_offset = offset;
}

}  // namespace replica
}  // namespace rtidb

inline std::string GenRand() { return std::to_string(rand() % 10000000 + 1); } // NOLINT

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::rtidb::base::SetLogLevel(DEBUG);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    int ret = 0;
    std::vector<std::string> vec{"off", "zlib", "snappy", "pz"};
    for (size_t i = 0; i < vec.size(); i++) {
#ifndef PZFPGA_ENABLE
        if (vec[i] == "pz") continue;
#endif
        std::cout << "compress type: " << vec[i] << std::endl;
        FLAGS_db_root_path = "/tmp/" + GenRand();
        FLAGS_snapshot_compression = vec[i];
        ret += RUN_ALL_TESTS();
    }
    return ret;
    // return RUN_ALL_TESTS();
}
