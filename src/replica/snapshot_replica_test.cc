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
#include <gtest/gtest.h>
#include <sched.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "client/tablet_client.h"
#include "common/thread_pool.h"
#include "common/timer.h"
#include "proto/tablet.pb.h"
#include "replica/log_replicator.h"
#include "replica/replicate_node.h"
#include "storage/segment.h"
#include "storage/table.h"
#include "storage/ticket.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

using ::baidu::common::ThreadPool;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::openmldb::storage::DataBlock;
using ::openmldb::storage::Table;
using ::openmldb::storage::Ticket;
using ::openmldb::tablet::TabletImpl;

DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(endpoint);
DECLARE_int32(make_snapshot_threshold_offset);

inline std::string GenRand() { return std::to_string(rand() % 10000000 + 1); }  // NOLINT

namespace openmldb {
namespace replica {

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class SnapshotReplicaTest : public ::testing::TestWithParam<::openmldb::common::StorageMode> {
 public:
    SnapshotReplicaTest() {}

    ~SnapshotReplicaTest() {}
};

TEST_P(SnapshotReplicaTest, AddReplicate) {
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    tablet->Init("");
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

    auto storage_mode = GetParam();
    ::openmldb::client::TabletClient client(leader_point, "");
    client.Init();
    std::vector<std::string> endpoints;
    bool ret =
        client.CreateTable("table1", tid, pid, 100000, 0, true, endpoints, ::openmldb::type::TTLType::kAbsoluteTime, 16,
                           0, ::openmldb::type::CompressType::kNoCompress, storage_mode);
    ASSERT_TRUE(ret);

    std::string end_point = "127.0.0.1:18530";
    ret = client.AddReplica(tid, pid, end_point);
    ASSERT_TRUE(ret);
    sleep(1);

    ::openmldb::api::TableStatus table_status;
    ASSERT_TRUE(client.GetTableStatus(tid, pid, table_status));
    ASSERT_EQ(::openmldb::api::kTableNormal, table_status.state());

    ret = client.DelReplica(tid, pid, end_point);
    ASSERT_TRUE(ret);
}

TEST_P(SnapshotReplicaTest, LeaderAndFollower) {
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    tablet->Init("");
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
    // server.RunUntilAskedToQuit();

    uint32_t tid = 1;
    uint32_t pid = 123;

    auto storage_mode = GetParam();
    ::openmldb::client::TabletClient client(leader_point, "");
    client.Init();
    std::vector<std::string> endpoints;
    bool ret =
        client.CreateTable("table1", tid, pid, 100000, 0, true, endpoints, ::openmldb::type::TTLType::kAbsoluteTime, 16,
                           0, ::openmldb::type::CompressType::kNoCompress, storage_mode);
    ASSERT_TRUE(ret);
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    ret = client.Put(tid, pid, "testkey", cur_time, ::openmldb::test::EncodeKV("testkey", "value1"));
    ASSERT_TRUE(ret);

    uint32_t count = 0;
    while (count < 10) {
        count++;
        std::string key = "test" + std::to_string(count);
        client.Put(tid, pid, key, cur_time, ::openmldb::test::EncodeKV(key, key));
    }

    sleep(3);
    FLAGS_db_root_path = "/tmp/" + ::GenRand();
    FLAGS_hdd_root_path = "/tmp/hdd_" + GenRand();
    FLAGS_endpoint = "127.0.0.1:18530";
    ::openmldb::tablet::TabletImpl* tablet1 = new ::openmldb::tablet::TabletImpl();
    tablet1->Init("");
    brpc::Server server1;
    if (server1.AddService(tablet1, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "fail to register tablet rpc service");
        exit(1);
    }
    std::string follower_point = "127.0.0.1:18530";
    if (server1.Start(follower_point.c_str(), &options) != 0) {
        PDLOG(WARNING, "fail to start server %s", follower_point.c_str());
        exit(1);
    }
    // server.RunUntilAskedToQuit();
    ::openmldb::client::TabletClient client1(follower_point, "");
    client1.Init();
    ret = client1.CreateTable("table1", tid, pid, 14400, 0, false, endpoints, ::openmldb::type::TTLType::kAbsoluteTime,
                              16, 0, ::openmldb::type::CompressType::kNoCompress, storage_mode);
    ASSERT_TRUE(ret);
    client.AddReplica(tid, pid, follower_point);
    sleep(3);

    ::openmldb::api::ScanRequest sr;
    MockClosure closure;
    sr.set_tid(tid);
    sr.set_pid(pid);
    sr.set_pk("testkey");
    sr.set_st(cur_time + 1);
    sr.set_et(cur_time - 1);
    sr.set_limit(10);
    ::openmldb::api::ScanResponse srp;
    tablet1->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(1, (int64_t)srp.count());
    ASSERT_EQ(0, srp.code());

    tablet->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(1, (int64_t)srp.count());
    ASSERT_EQ(0, srp.code());

    ret = client.Put(tid, pid, "newkey", cur_time, ::openmldb::test::EncodeKV("newkey", "value2"));
    ASSERT_TRUE(ret);
    sleep(2);
    sr.set_pk("newkey");
    tablet1->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(1, (int64_t)srp.count());
    ASSERT_EQ(0, srp.code());
    {
        ::openmldb::api::GetTableFollowerRequest gr;
        ::openmldb::api::GetTableFollowerResponse grs;
        gr.set_tid(tid);
        gr.set_pid(pid);
        tablet->GetTableFollower(NULL, &gr, &grs, &closure);
        ASSERT_EQ(0, grs.code());
        ASSERT_EQ(12, (int64_t)grs.offset());
        ASSERT_EQ(1, grs.follower_info_size());
        ASSERT_STREQ(follower_point.c_str(), grs.follower_info(0).endpoint().c_str());
        ASSERT_EQ(12, (int64_t)grs.follower_info(0).offset());
    }
    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(tid);
    dr.set_pid(pid);
    ::openmldb::api::DropTableResponse drs;
    tablet->DropTable(NULL, &dr, &drs, &closure);
    sleep(2);
}

TEST_P(SnapshotReplicaTest, SendSnapshot) {
    FLAGS_make_snapshot_threshold_offset = 0;
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    MockClosure closure;
    tablet->Init("");
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
    auto storage_mode = GetParam();
    ::openmldb::client::TabletClient client(leader_point, "");
    client.Init();
    std::vector<std::string> endpoints;
    bool ret =
        client.CreateTable("table1", tid, pid, 100000, 0, true, endpoints, ::openmldb::type::TTLType::kAbsoluteTime, 16,
                           0, ::openmldb::type::CompressType::kNoCompress, storage_mode);
    ASSERT_TRUE(ret);
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    ret = client.Put(tid, pid, "testkey", cur_time, ::openmldb::test::EncodeKV("testkey", "value1"));
    ASSERT_TRUE(ret);

    uint32_t count = 0;
    while (count < 10) {
        count++;
        std::string key = "test";
        client.Put(tid, pid, key, cur_time + count, ::openmldb::test::EncodeKV(key, key));
    }
    ::openmldb::api::GeneralRequest grq;
    grq.set_tid(tid);
    grq.set_pid(pid);
    grq.set_storage_mode(storage_mode);
    ::openmldb::api::GeneralResponse grp;
    grp.set_code(-1);
    tablet->MakeSnapshot(NULL, &grq, &grp, &closure);

    sleep(3);
    FLAGS_db_root_path = "/tmp/follower_" + ::GenRand();
    FLAGS_hdd_root_path = "/tmp/follower_hdd_" + GenRand();
    FLAGS_endpoint = "127.0.0.1:18530";
    ::openmldb::tablet::TabletImpl* tablet1 = new ::openmldb::tablet::TabletImpl();
    tablet1->Init("");
    brpc::Server server1;
    if (server1.AddService(tablet1, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "fail to register tablet rpc service");
        exit(1);
    }
    std::string follower_point = "127.0.0.1:18530";
    if (server1.Start(follower_point.c_str(), &options) != 0) {
        PDLOG(WARNING, "fail to start server %s", follower_point.c_str());
        exit(1);
    }
    ::openmldb::client::TabletClient client1(follower_point, "");
    client1.Init();

    // send snapshot to the follower
    client.PauseSnapshot(tid, pid);
    client.SendSnapshot(tid, tid, pid, follower_point);
    sleep(10);

    // load table in the follower
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_format_version(1);
    table_meta.set_name("table1");
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_storage_mode(storage_mode);
    client1.LoadTable(table_meta, nullptr);
    sleep(5);

    ::openmldb::api::ScanRequest sr;
    sr.set_tid(tid);
    sr.set_pid(pid);
    sr.set_pk("testkey");
    sr.set_st(cur_time + 1);
    sr.set_et(cur_time - 1);
    sr.set_limit(10);
    ::openmldb::api::ScanResponse srp;
    tablet1->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(1, (int64_t)srp.count());
    ASSERT_EQ(0, srp.code());

    sr.set_tid(tid);
    sr.set_pid(pid);
    sr.set_pk("test");
    sr.set_st(cur_time + 20);
    sr.set_et(cur_time - 20);
    sr.set_limit(20);
    tablet1->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(10, (int64_t)srp.count());
    ASSERT_EQ(0, srp.code());

    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(tid);
    dr.set_pid(pid);
    ::openmldb::api::DropTableResponse drs;
    tablet->DropTable(NULL, &dr, &drs, &closure);
    tablet1->DropTable(NULL, &dr, &drs, &closure);
    sleep(2);
}

TEST_P(SnapshotReplicaTest, IncompleteSnapshot) {
    FLAGS_make_snapshot_threshold_offset = 0;
    uint32_t tid = 2;
    uint32_t pid = 123;
    auto storage_mode = GetParam();
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    {
        ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
        MockClosure closure;
        tablet->Init("");
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

        ::openmldb::client::TabletClient client(leader_point, "");
        client.Init();
        std::vector<std::string> endpoints;
        bool ret =
            client.CreateTable("table1", tid, pid, 100000, 0, true, endpoints, ::openmldb::type::TTLType::kAbsoluteTime,
                               16, 0, ::openmldb::type::CompressType::kNoCompress, storage_mode);
        ASSERT_TRUE(ret);
        ret = client.Put(tid, pid, "testkey", cur_time, ::openmldb::test::EncodeKV("testkey", "value1"));
        ASSERT_TRUE(ret);

        uint32_t count = 0;
        while (count < 10) {
            count++;
            std::string key = "test";
            client.Put(tid, pid, key, cur_time + count, ::openmldb::test::EncodeKV(key, key));
        }
        ::openmldb::api::GeneralRequest grq;
        grq.set_tid(tid);
        grq.set_pid(pid);
        grq.set_storage_mode(storage_mode);
        ::openmldb::api::GeneralResponse grp;
        grp.set_code(-1);
        tablet->MakeSnapshot(NULL, &grq, &grp, &closure);
        sleep(5);
    }

    {
        ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
        MockClosure closure;
        tablet->Init("");
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

        ::openmldb::client::TabletClient client(leader_point, "");
        client.Init();

        // load table
        ::openmldb::api::TableMeta table_meta;
        table_meta.set_format_version(1);
        table_meta.set_name("table1");
        table_meta.set_tid(tid);
        table_meta.set_pid(pid);
        table_meta.set_storage_mode(storage_mode);
        client.LoadTable(table_meta, nullptr);
        sleep(5);

        ::openmldb::api::ScanRequest sr;
        sr.set_tid(tid);
        sr.set_pid(pid);
        sr.set_pk("testkey");
        sr.set_st(cur_time + 1);
        sr.set_et(cur_time - 1);
        sr.set_limit(10);
        ::openmldb::api::ScanResponse srp;
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(1, (int64_t)srp.count());
        ASSERT_EQ(0, srp.code());

        sr.set_tid(tid);
        sr.set_pid(pid);
        sr.set_pk("test");
        sr.set_st(cur_time + 20);
        sr.set_et(cur_time - 20);
        sr.set_limit(20);
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(10, (int64_t)srp.count());
        ASSERT_EQ(0, srp.code());

        std::string key = "test2";
        ASSERT_TRUE(client.Put(tid, pid, key, cur_time, ::openmldb::test::EncodeKV(key, key)));

        sr.set_tid(tid);
        sr.set_pid(pid);
        sr.set_pk("test2");
        sr.set_st(cur_time + 20);
        sr.set_et(cur_time - 20);
        sr.set_limit(20);
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(1, (int64_t)srp.count());
        ASSERT_EQ(0, srp.code());
    }

    // remove the snapshot file, only keeping the MANIFEST
    // i.e., corrupt the snapshot
    std::string db_root_path;
    if (storage_mode == common::kSSD) {
        db_root_path = FLAGS_ssd_root_path;
    } else if (storage_mode == common::kHDD) {
        db_root_path = FLAGS_hdd_root_path;
    } else {
        db_root_path = FLAGS_db_root_path;
    }
    std::string snapshot_path = absl::StrCat(db_root_path, "/", tid, "_", pid, "/snapshot/");
    std::vector<std::string> sub_dirs;
    ::openmldb::base::GetSubDir(snapshot_path, sub_dirs);
    for (const auto& dir : sub_dirs) {
        auto sub_path = absl::StrCat(snapshot_path, dir);
        DLOG(INFO) << "remove snapshot path: " << sub_path;
        ASSERT_TRUE(::openmldb::base::RemoveDir(sub_path));
    }
    sleep(2);

    {
        ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
        MockClosure closure;
        tablet->Init("");
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

        ::openmldb::client::TabletClient client(leader_point, "");
        client.Init();

        // load table
        ::openmldb::api::TableMeta table_meta;
        table_meta.set_format_version(1);
        table_meta.set_name("table1");
        table_meta.set_tid(tid);
        table_meta.set_pid(pid);
        table_meta.set_storage_mode(storage_mode);
        client.LoadTable(table_meta, nullptr);
        sleep(5);

        ::openmldb::api::ScanRequest sr;
        sr.set_tid(tid);
        sr.set_pid(pid);
        sr.set_pk("testkey");
        sr.set_st(cur_time + 1);
        sr.set_et(cur_time - 1);
        sr.set_limit(10);
        ::openmldb::api::ScanResponse srp;
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(1, (int64_t)srp.count());
        ASSERT_EQ(0, srp.code());

        sr.set_tid(tid);
        sr.set_pid(pid);
        sr.set_pk("test");
        sr.set_st(cur_time + 20);
        sr.set_et(cur_time - 20);
        sr.set_limit(20);
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(10, (int64_t)srp.count());
        ASSERT_EQ(0, srp.code());

        sr.set_tid(tid);
        sr.set_pid(pid);
        sr.set_pk("test2");
        sr.set_st(cur_time + 20);
        sr.set_et(cur_time - 20);
        sr.set_limit(20);
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(1, (int64_t)srp.count());
        ASSERT_EQ(0, srp.code());

        uint32_t count = 0;
        while (count < 10) {
            count++;
            std::string key = "test3";
            client.Put(tid, pid, key, cur_time + count, ::openmldb::test::EncodeKV(key, key));
        }

        sr.set_tid(tid);
        sr.set_pid(pid);
        sr.set_pk("test3");
        sr.set_st(cur_time + 20);
        sr.set_et(cur_time - 20);
        sr.set_limit(20);
        tablet->Scan(NULL, &sr, &srp, &closure);
        ASSERT_EQ(10, (int64_t)srp.count());
        ASSERT_EQ(0, srp.code());

        ::openmldb::api::DropTableRequest dr;
        dr.set_tid(tid);
        dr.set_pid(pid);
        ::openmldb::api::DropTableResponse drs;
        tablet->DropTable(NULL, &dr, &drs, &closure);
        sleep(2);
    }
}

TEST_P(SnapshotReplicaTest, LeaderAndFollowerTS) {
    auto storage_mode = GetParam();
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    tablet->Init("");
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
    uint32_t tid = 1;
    uint32_t pid = 123;
    ::openmldb::client::TabletClient client(leader_point, "");
    client.Init();
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_format_version(1);
    table_meta.set_name("test");
    table_meta.set_tid(tid);
    table_meta.set_pid(pid);
    table_meta.set_seg_cnt(8);
    table_meta.set_storage_mode(storage_mode);
    ::openmldb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("card");
    column_desc1->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("mcc");
    column_desc2->set_data_type(::openmldb::type::kString);
    ::openmldb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("amt");
    column_desc3->set_data_type(::openmldb::type::kDouble);
    ::openmldb::common::ColumnDesc* column_desc4 = table_meta.add_column_desc();
    column_desc4->set_name("ts1");
    column_desc4->set_data_type(::openmldb::type::kBigInt);
    ::openmldb::common::ColumnDesc* column_desc5 = table_meta.add_column_desc();
    column_desc5->set_name("ts2");
    column_desc5->set_data_type(::openmldb::type::kBigInt);
    auto column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("card");
    column_key1->set_ts_name("ts1");
    auto ttl = column_key1->mutable_ttl();
    ttl->set_abs_ttl(0);
    auto column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("card1");
    column_key2->add_col_name("card");
    column_key2->set_ts_name("ts2");
    ttl = column_key2->mutable_ttl();
    ttl->set_abs_ttl(0);
    auto column_key3 = table_meta.add_column_key();
    column_key3->set_index_name("mcc");
    column_key3->set_ts_name("ts1");
    ttl = column_key3->mutable_ttl();
    ttl->set_abs_ttl(0);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    bool ret = client.CreateTable(table_meta);
    ASSERT_TRUE(ret);
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    std::vector<std::pair<std::string, uint32_t>> dimensions;
    dimensions.push_back(std::make_pair("card0", 0));
    dimensions.push_back(std::make_pair("mcc0", 1));
    ::openmldb::codec::SDKCodec sdk_codec(table_meta);
    std::vector<std::string> row = {"card0", "mcc0", "1.3", std::to_string(cur_time), std::to_string(cur_time - 100)};
    std::string value;
    sdk_codec.EncodeRow(row, &value);
    ret = client.Put(tid, pid, cur_time, value, dimensions);
    ASSERT_TRUE(ret);

    sleep(3);
    FLAGS_db_root_path = "/tmp/" + ::GenRand();
    FLAGS_hdd_root_path = "/tmp/hdd_" + GenRand();
    FLAGS_endpoint = "127.0.0.1:18530";
    ::openmldb::tablet::TabletImpl* tablet1 = new ::openmldb::tablet::TabletImpl();
    tablet1->Init("");
    brpc::Server server1;
    if (server1.AddService(tablet1, brpc::SERVER_OWNS_SERVICE) != 0) {
        PDLOG(WARNING, "fail to register tablet rpc service");
        exit(1);
    }
    std::string follower_point = "127.0.0.1:18530";
    if (server1.Start(follower_point.c_str(), &options) != 0) {
        PDLOG(WARNING, "fail to start server %s", follower_point.c_str());
        exit(1);
    }
    ::openmldb::client::TabletClient client1(follower_point, "");
    client1.Init();
    table_meta.set_mode(::openmldb::api::TableMode::kTableFollower);
    ret = client1.CreateTable(table_meta);
    ASSERT_TRUE(ret);
    client.AddReplica(tid, pid, follower_point);
    sleep(3);

    ::openmldb::api::ScanRequest sr;
    MockClosure closure;
    sr.set_tid(tid);
    sr.set_pid(pid);
    sr.set_pk("card0");
    sr.set_idx_name("card1");
    sr.set_st(cur_time + 1);
    sr.set_et(0);
    ::openmldb::api::ScanResponse srp;
    tablet1->Scan(NULL, &sr, &srp, &closure);
    ASSERT_EQ(1, (int64_t)srp.count());
    ASSERT_EQ(0, srp.code());

    ::openmldb::api::DropTableRequest dr;
    dr.set_tid(tid);
    dr.set_pid(pid);
    ::openmldb::api::DropTableResponse drs;
    tablet->DropTable(NULL, &dr, &drs, &closure);
    tablet1->DropTable(NULL, &dr, &drs, &closure);
    sleep(1);
}

INSTANTIATE_TEST_SUITE_P(DBSDK, SnapshotReplicaTest,
                         testing::Values(::openmldb::common::kMemory, ::openmldb::common::kHDD));

}  // namespace replica
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + GenRand();
    FLAGS_hdd_root_path = "/tmp/hdd_" + GenRand();
    return RUN_ALL_TESTS();
}
