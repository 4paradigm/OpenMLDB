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

#include <vector>

#include "codec/sdk_codec.h"
#include "datacollector/data_collector.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "vm/engine.h"

using openmldb::client::TabletClient;
namespace openmldb::datacollector {

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() override {}
};

class DataCollectorTest : public ::testing::Test {
 public:
    DataCollectorTest() {}
    ~DataCollectorTest() {}

    api::TableStatus GetTableStatus(uint32_t tid, uint32_t pid) {
        std::shared_ptr<TabletClient> tablet_client = std::make_shared<TabletClient>("172.24.4.27:7126", "");
        tablet_client->Init();
        api::TableStatus table_status;
        tablet_client->GetTableStatus(tid, pid, table_status);
        LOG(INFO) << "table status: " << table_status.ShortDebugString();
        return table_status;
    }

    api::TableMeta GetTableSchema(uint32_t tid, uint32_t pid) {
        std::shared_ptr<TabletClient> tablet_client = std::make_shared<TabletClient>("172.24.4.27:7126", "");
        tablet_client->Init();
        api::TableMeta table_meta;
        tablet_client->GetTableSchema(tid, pid, table_meta);
        LOG(INFO) << "table meta: " << table_meta.ShortDebugString();
        return table_meta;
    }

    std::string VecToString(std::vector<std::string> vec) {
        std::string str;
        for (auto& s : vec) {
            str += s + " ";
        }
        return str;
    }

    void TraverseAndPrint(storage::TraverseIterator* it, api::TableMeta meta, uint64_t limit = 0,
                          uint64_t start_ts = 0) {
        codec::SDKCodec codec(meta);
        std::vector<std::string> vrow;
        uint64_t i = 0;
        while (it->Valid() && (limit == 0 || i++ < limit)) {
            if (start_ts != 0 && it->GetKey() < start_ts) {
                // ts is lower than start_ts, skip this pk
                it->NextPK();
                continue;
            }
            vrow.clear();
            codec.DecodeRow(it->GetValue().ToString(), &vrow);
            LOG(INFO) << "pk " << it->GetPK() << ", ts " << it->GetKey() << ", value: " << VecToString(vrow);
            it->Next();
        }
    }
};

TEST_F(DataCollectorTest, getTableDataPath) {
    // try get tid-pid 6-0 data paths
    // must be the disk table
    auto table_status = GetTableStatus(6, 0);

    ASSERT_NE(table_status.storage_mode(), common::StorageMode::kMemory);
    ASSERT_FALSE(table_status.snapshot_path().empty());
    ASSERT_FALSE(table_status.binlog_path().empty());
    ASSERT_TRUE(std::filesystem::path(table_status.snapshot_path()).is_absolute());
    ASSERT_TRUE(std::filesystem::path(table_status.binlog_path()).is_absolute());
}

TEST_F(DataCollectorTest, readSnapshot) {
    auto snapshot_path = GetTableStatus(6, 0).snapshot_path();
    // hard link, notice hard link can't cross different disk
    std::string data_collector_tmp_path = "/home/huangwei/tmp/data_collector_test/";
    auto snapshot_hardlink_path = data_collector_tmp_path + "6_0/";
    ASSERT_TRUE(HardLinkSnapshot(snapshot_path, snapshot_hardlink_path, nullptr));
    // the snapshot has the data offset [0, offset_in_manifest]
    // read the manifest to get the offset
    api::Manifest manifest;
    std::string manifest_file = snapshot_hardlink_path + "MANIFEST";
    ASSERT_TRUE(storage::Snapshot::GetLocalManifest(manifest_file, manifest) == 0) << "get manifest failed";
    LOG(INFO) << "manifest: " << manifest.ShortDebugString();
    // mock table
    auto meta = GetTableSchema(6, 0);
    // mapping empty? ttl&ttl type, get from namserver?
    storage::DiskTable mock_table(meta, snapshot_hardlink_path);
    mock_table.Init();
    LOG(INFO) << mock_table.GetOffset();
    // traverse will go through all data, not only current pk
    auto it = mock_table.NewTraverseIterator(0);
    // assume start sync point is [snapshot, no pk&ts(means start from 0)]
    it->SeekToFirst();
    TraverseAndPrint(it, meta);

    // if request start sync point is [snapshot, pk&ts], seek to it, read include it
    // assume start sync point is [snapshot, pk&ts]
    // order is [big, small]: 3, 2, 1, so print 2, 1
    auto pk = "a";
    uint64_t ts = 2;  // == key
    LOG(INFO) << "seek to " << pk << ", " << ts;
    it->Seek(pk, ts);
    LOG(INFO) << "read two row";
    TraverseAndPrint(it, meta, 2);
    // after read in limited, the current it is not read, or it may be invalid
    if (it->Valid()) {
        LOG(INFO) << "has next value in snapshot, point(next to read) is " << it->GetPK() << ", " << it->GetKey();
    }

    // mode 2 every pk start from ts, filter all ts >= start ts
    it->SeekToFirst();
    LOG(INFO) << "mode 2, start ts " << ts;
    LOG(INFO) << "read two row in mode 2";
    TraverseAndPrint(it, meta, 2, ts);
    if (it->Valid()) {
        LOG(INFO) << "has next value in snapshot, point(next to read) is " << it->GetPK() << ", " << it->GetKey()
                  << ". In mode 2, it may be filtered";
        it->Seek(it->GetPK(), it->GetKey());
        LOG(INFO) << "read all left row in mode 2";
        TraverseAndPrint(it, meta, UINT64_MAX, ts);
    }

    if (!it->Valid()) {
        // [binlog, start_offset(offset in snapshot and +1)]
        LOG(INFO) << "snapshot end, read binlog, next to read(include me) is " << manifest.offset() + 1;
    }
    base::RemoveDir(data_collector_tmp_path);
}

TEST_F(DataCollectorTest, readBinlog) {
    auto binglog_path = GetTableStatus(6, 0).binlog_path();
    // assume start sync point is [binlog, 3]
    auto replicator = genLogReplicatorFromBinlog(binglog_path);
    // Set start offset only seek to the right log part, e.g. 3 is in log 0 [0, 100]
    uint64_t start_offset = 3;
    bool no_binlog = false;
    auto reader = getLogReader(replicator, 3, &no_binlog);
    ASSERT_TRUE(reader) << "can't gen log reader";
    // TODO(hw): make some binlog files to test
    LOG(INFO) << "binlog min offset: " << reader->GetMinOffset();

    ::openmldb::api::LogEntry entry;
    std::string buffer;
    ::openmldb::base::Slice record;
    // this binlog file may have some records before start_offset, so we still need to filter
    int cur_log_index = reader->GetLogIndex();

    auto meta = GetTableSchema(6, 0);
    codec::SDKCodec codec(meta);
    std::vector<std::string> vrow;
    int i = 0;
    while (true) {
        buffer.clear();
        LOG(INFO) << "read " << i;
        ::openmldb::log::Status status = reader->ReadNextRecord(&record, &buffer);
        if (status.IsEof()) {
            // should read next binlog file
            if (reader->GetLogIndex() != cur_log_index) {
                cur_log_index = reader->GetLogIndex();
                continue;
            }
            // last binlog file has been read
            break;
        }
        // may meet a unfinished binlog file
        if (status.IsWaitRecord()) {
            // wait for next record
            break;
        }

        ASSERT_TRUE(status.ok()) << i << ": " << status.ToString();
        entry.ParseFromString(record.ToString());
        // parse entry
        vrow.clear();
        codec.DecodeRow(entry.value(), &vrow);
        LOG(INFO) << i << " value: " << VecToString(vrow);
        if (entry.log_index() >= start_offset) {
            // TODO(hw): which is key in entry? dim0 ts?
            LOG(INFO) << "match >= start_offset: " << VecToString(vrow);
        }
        i++;
        // what it in mode 2, start_ts = 2, filter all ts >= 2
    }
    LOG(INFO) << "read binlog entry num: " << i;
    // TODO(hw): what if read when writing?
}

// init.sh and use playground disk_test.sql to create tables
// and trigger tid 6 to create snapshot
TEST_F(DataCollectorTest, taskAndRate) {
    // test about tasks, no need to init zk
    DataCollectorImpl dc;
    MockClosure closure;
    datasync::AddSyncTaskRequest request;
    request.set_tid(6);
    request.set_pid(0);
    request.set_mode(datasync::SyncMode::kFull);
    auto sync_point = request.mutable_sync_point();
    sync_point->set_type(datasync::SyncType::kSNAPSHOT);
    // no pk&ts, means start from 0
    request.set_tablet_endpoint("172.24.4.27:7126");
    datasync::GeneralResponse response;
    dc.AddSyncTask(nullptr, &request, &response, &closure);
    ASSERT_EQ(response.code(), 0);
    auto task = dc.GetTaskInfo(6, 0);
    LOG(INFO) << task.ShortDebugString();
    ASSERT_EQ(task.tid(), 6);
    ASSERT_EQ(task.pid(), 0);
    // sleep to call SyncOnce more
    sleep(1);
    // new token task but snapshot in db doesn't change, so the snapshot env is not changed
    request.set_mode(datasync::SyncMode::kIncrementalByTimestamp);
    request.set_start_ts(3);
    request.set_token("newer_one");
    dc.AddSyncTask(nullptr, &request, &response, &closure);
    sleep(1);

    // a table doesn't have snapshot, make sure the snapshot path in db is empty
    request.set_tid(7);
    request.set_des_endpoint("");
    dc.AddSyncTask(nullptr, &request, &response, &closure);
    sleep(1);
    ASSERT_EQ(response.code(), 0);

    // a table doesn't exist
    request.set_tid(10086);
    dc.AddSyncTask(nullptr, &request, &response, &closure);
    sleep(1);
    ASSERT_EQ(response.code(), -1);
    LOG(INFO) << "got resp: " << response.msg();
}
}  // namespace openmldb::datacollector

int main(int argc, char** argv) {
    // init google test first for gtest_xxx flags
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::openmldb::base::SetupGlog(true);

    // FLAGS_zk_session_timeout = 100000;
    // ::openmldb::sdk::MiniCluster mc(6181);
    // ::openmldb::sdk::mc_ = &mc;
    // FLAGS_enable_distsql = true;
    // int ok = ::openmldb::sdk::mc_->SetUp(3);
    // sleep(5);

    // ::openmldb::sdk::router_ = ::openmldb::sdk::GetNewSQLRouter();
    // if (nullptr == ::openmldb::sdk::router_) {
    //     LOG(ERROR) << "Test failed with NULL SQL router";
    //     return -1;
    // }

    srand(time(nullptr));
    // TODO(hw): skip this test, cuz it needs sync tool. It's better to mock the sync tool
    int ok = 0;
    // ok = RUN_ALL_TESTS();
    // ::openmldb::sdk::mc_->Close();
    return ok;
}
