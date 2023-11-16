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

#include "datacollector/data_collector.h"

#include <fstream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_join.h"
#include "boost/algorithm/string/predicate.hpp"
#include "gflags/gflags.h"
#include "google/protobuf/util/message_differencer.h"

#include "base/response_util.h"
#include "base/status.h"
#include "replica/replicate_node.h"

DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_string(zk_auth_schema);
DECLARE_string(zk_cert);
DECLARE_int32(thread_pool_size);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DEFINE_int32(task_thread_pool_size, 32, "thread pool size for exec sync task");
DEFINE_string(collector_datadir, "/tmp/openmldb/datacollector",
              "data collector dir for meta data and snapshot hard link");
DEFINE_uint64(max_pack_size, 1 * 1024 * 1024,
              "need < 64M(brpc max_body_size), max size of one pack, != 64M cuz of meta overhead");
DEFINE_uint64(sync_task_long_interval_ms, 5000,
              "interval of one sync task, if binlog meet end, use this one, should be smaller than sync tool "
              "sync_task.check_period. Note that it should be much less than binlog_delete_interval, The frequency we "
              "fetch binlog is the same as the frequency of sync task execution");
DEFINE_uint64(sync_task_short_interval_ms, 1000,
              "interval of one sync task, if has next data to send, use this one, should be smaller than sync tool "
              "sync_task.check_period");

namespace fs = std::filesystem;
namespace openmldb::datacollector {
// not thread safe
std::string LogPartsToString(replica::LogParts* log_parts) {
    std::stringstream ss;
    ss << "[";
    auto it = log_parts->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        ss << "(" << it->GetKey() << ", " << it->GetValue() << "),";
        it->Next();
    }
    ss << "] (size: " << log_parts->GetSize() << ")";
    return ss.str();
}

std::shared_ptr<replica::LogReplicator> genLogReplicatorFromParent(const std::string& binlog_parent) {
    LOG(INFO) << "use binlog parent path: " << binlog_parent;
    std::map<std::string, std::string> real_ep_map;
    // set follower to avoid redundant init of leader, tid-pid is not used, use GetLogPath() to check
    auto replicator = std::make_shared<replica::LogReplicator>(0, 0, binlog_parent, real_ep_map,
                                                               replica::ReplicatorRole::kFollowerNode);
    bool ok = replicator->Init();
    if (!ok) {
        LOG(ERROR) << "init log replicator failed";
        return {};
    }
    // debug info
    LOG(INFO) << "log parts: " << LogPartsToString(replicator->GetLogPart());
    return replicator;
}

std::shared_ptr<replica::LogReplicator> genLogReplicatorFromBinlog(const std::string& binlog_path) {
    // binlog_path is '<path>/binlog/', replicator needs '<path>'
    auto no_end_slash = binlog_path.back() == '/' ? binlog_path.substr(0, binlog_path.size() - 1) : binlog_path;
    auto binlog_parent = fs::path(no_end_slash).parent_path();
    return genLogReplicatorFromParent(binlog_parent);
}

// LogPart is static, so we generate the LogReader in any time, it can read the latest binlog
std::shared_ptr<log::LogReader> getLogReader(std::shared_ptr<replica::LogReplicator> replicator, uint64_t start_offset,
                                             bool* no_binlog_file) {
    // not own LogParts, and never meet compressed binlog
    auto reader = std::make_shared<log::LogReader>(replicator->GetLogPart(), replicator->GetLogPath(), false);
    if (reader->GetMinOffset() == UINT64_MAX) {
        LOG(INFO) << "no binlog file currently";
        *no_binlog_file = true;
        return {};
    }
    if (!reader->SetOffset(start_offset)) {
        LOG(ERROR) << "set offset failed, check the binlog files";
        return {};
    }
    LOG(INFO) << "set offset to " << start_offset << " success";
    return reader;
}

// Manifest location: <snapshot_path>/MANIFEST
// You can use `has_name()` to check if the manifest is valid(otherwise, it does not exist or is invalid)
api::Manifest GetManifest(const std::string& snapshot_path) {
    api::Manifest manifest;
    std::string manifest_file = snapshot_path + "MANIFEST";
    if (int ret = (storage::Snapshot::GetLocalManifest(manifest_file, manifest)); ret != 0) {
        LOG(WARNING) << "get manifest failed, ret " << ret;
        return {};
    }
    return manifest;
}

bool HardLinkSnapshot(const std::string& snapshot_path, const std::string& dest, uint64_t* snapshot_offset) {
    // HardLinkDir is not recursive, hard link the manifest file for debug
    auto ret = base::HardLinkDir(snapshot_path, dest);
    if (ret) {
        LOG(WARNING) << " hard link manifest failed, ret " << ret << ", err: " << errno << " " << strerror(errno);
        return false;
    }
    // to hard link the snapshot dir, read dir name from MANIFEST
    auto manifest = GetManifest(snapshot_path);
    if (!manifest.has_name()) {
        LOG(WARNING) << "no manifest name failed";
        return false;
    }
    // <snapshot_hardlink_path>/data
    ret = base::HardLinkDir(snapshot_path + manifest.name(), dest + "data");
    if (ret) {
        LOG(WARNING) << " hard link snapshot data failed, ret " << ret << ", err: " << errno << " " << strerror(errno);
        return false;
    }
    if (snapshot_offset) {
        *snapshot_offset = manifest.offset();
    }
    return true;
}

bool SaveTaskInfoInDisk(const datasync::AddSyncTaskRequest* info, const std::string& path) {
    std::ofstream output(path + "task.progress", std::ios::out | std::ios::binary);
    // Write the message to the file
    auto ret = info->SerializeToOstream(&output);
    // Close the file
    output.close();
    return ret;
}

bool LoadTaskInfoFromDisk(const std::string& path, datasync::AddSyncTaskRequest* info) {
    auto file = path + "task.progress";
    std::ifstream input(file, std::ios::in | std::ios::binary);
    if (!input.is_open()) {
        LOG(WARNING) << "open task progress file [" << file << "] failed, " << std::strerror(errno);
        return false;
    }
    // Read the message from the file
    auto ret = info->ParseFromIstream(&input);
    // Close the file
    input.close();
    return ret;
}

DataCollectorImpl::DataCollectorImpl()
    : zk_client_(nullptr), keep_alive_pool_(1), task_pool_(FLAGS_task_thread_pool_size) {}

DataCollectorImpl::~DataCollectorImpl() {
    keep_alive_pool_.Stop(true);
    task_pool_.Stop(true);
}

bool DataCollectorImpl::Init(const std::string& endpoint) {
    return Init(FLAGS_zk_cluster, FLAGS_zk_root_path, endpoint);
}
bool DataCollectorImpl::Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint) {
    zk_client_ = std::make_shared<zk::ZkClient>(zk_cluster, FLAGS_zk_session_timeout, endpoint, zk_path,
                                                zk_path + kDataCollectorRegisterPath,
                                                FLAGS_zk_auth_schema, FLAGS_zk_cert);
    if (!zk_client_->Init()) {
        LOG(WARNING) << "fail to init zk client";
        return false;
    }
    LOG(INFO) << "init zk client success";
    // recover tasks from FLAGS_collector_datadir
    Recover();
    return true;
}

bool DataCollectorImpl::RegisterZK() {
    if (zk_client_ == nullptr) {
        LOG(WARNING) << "zk client is null, please init first";
        return false;
    }
    // no use_name logic
    // no need to startup_
    if (!zk_client_->Register(false)) {
        LOG(WARNING) << "fail to register zk";
        return false;
    }

    keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, std::bind(&DataCollectorImpl::CheckZkClient, this));
    return true;
}

void DataCollectorImpl::CheckZkClient() {
    if (zk_client_ == nullptr) {
        LOG(WARNING) << "zk client is null, weird";
        return;
    }
    if (!zk_client_->IsConnected()) {
        LOG(WARNING) << "reconnect zk";
        if (zk_client_->Reconnect() && zk_client_->Register()) {
            LOG(WARNING) << "reconnect zk ok";
        }
    } else if (!zk_client_->IsRegisted()) {
        LOG(WARNING) << "registe zk";
        if (zk_client_->Register()) {
            LOG(WARNING) << "registe zk ok";
        }
    }
    keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, std::bind(&DataCollectorImpl::CheckZkClient, this));
}

std::string DataCollectorImpl::GetWorkDir(const std::string& name) {
    return FLAGS_collector_datadir + "/" + name + "/";
}

void DataCollectorImpl::CreateTaskEnv(const datasync::AddSyncTaskRequest* request,
                                      datasync::GeneralResponse* response) {
    auto tid = request->tid();
    auto pid = request->pid();
    auto name = EncodeId(tid, pid);
    // create env for task
    {
        std::lock_guard<std::mutex> lock(process_map_mutex_);
        // two situation:
        // 1. task is not existing, add it
        // 2. task exists, maybe the token is new, update it
        if (!AddTaskInfoUnlocked(request)) {
            SET_RESP_AND_WARN(response, -1, "why send a same task?");
            return;
        }
        auto tablet_endpoint = request->tablet_endpoint();
        if (tablet_client_map_.find(tablet_endpoint) == tablet_client_map_.end()) {
            tablet_client_map_[tablet_endpoint] =
                std::make_shared<client::TabletClient>(request->tablet_endpoint(), "");
            if (tablet_client_map_[tablet_endpoint]->Init() != 0) {
                SET_RESP_AND_WARN(response, -1, "init tablet client failed");
                return;
            }
        }
        auto tablet_client = tablet_client_map_[tablet_endpoint];
        api::TableStatus table_status;
        if (auto st = tablet_client->GetTableStatus(tid, pid, table_status); !st.OK()) {
            SET_RESP_AND_WARN(response, -1, "get table status from tablet server failed, maybe table doesn't exist: " + st.GetMsg());
            return;
        }
        if (!ValidateTableStatus(table_status)) {
            SET_RESP_AND_WARN(response, -1, "table status is not valid: " + table_status.ShortDebugString());
            return;
        }

        // datasync::AddSyncTaskRequest adjusted_request = *request;
        if (request->sync_point().type() == datasync::SyncType::kSNAPSHOT) {
            // if no snapshot, we should let sync tool know the change
            if (auto manifest = GetManifest(table_status.snapshot_path());
                !manifest.has_name() || manifest.count() == 0) {
                LOG(INFO) << "no snapshot or snapshot is empty count in db, switch to binlog in the first sync once";
            } else {
                // has snapshot, create snapshot env
                api::TableMeta table_meta;
                tablet_client->GetTableSchema(tid, pid, table_meta);
                // create or update mock table for read snapshot
                if (!CreateSnapshotEnvUnlocked(name, table_meta, table_status.snapshot_path())) {
                    SET_RESP_AND_WARN(response, -1, "create snapshot env failed");
                    return;
                }
            }
        }

        // create binlog replicator, in any case, we need to read binlog(no matter binlog files exist or not now)
        // we should hardlink again when add new binlog file in db
        // NOTE: don't need to rollback snapshot env? maybe use while break

        // link to work dir. We'll create a replicator, no need to update it
        if (!FetchBinlogUnlocked(name, table_status.binlog_path(), nullptr)) {
            SET_RESP_AND_WARN(response, -1, "hardlink binlog failed");
            return;
        }
        // binlog path in db may changed later, we cache this in data collector(sync tasks request it frequently)
        {
            std::lock_guard<std::mutex> lock(cache_mutex_);
            binlog_path_map_[name] = table_status.binlog_path();
        }
        // read binlog from hardlink dir
        auto replicator = genLogReplicatorFromParent(GetWorkDir(name));
        if (replicator == nullptr) {
            SET_RESP_AND_WARN(response, -1, "create binlog env failed");
            return;
        }
        replicator_map_[name] = replicator;
        // save task info in disk
        SaveTaskInfoInDisk(request, GetWorkDir(name));
    }

    // add task to task pool, delay a long time to avoid sync tool not ready(adding tasks are parallel), and it's ok to
    // do a failed SendData when sync tool not ready, data collector will retry later
    // TODO(hw): add a random delay to avoid all tasks send data at the same time? rpc can't hold so much data.
    task_pool_.DelayTask(FLAGS_sync_task_long_interval_ms, std::bind(&DataCollectorImpl::SyncOnce, this, tid, pid));
}

void DataCollectorImpl::AddSyncTask(::google::protobuf::RpcController* controller,
                                    const ::openmldb::datasync::AddSyncTaskRequest* request,
                                    ::openmldb::datasync::GeneralResponse* response,
                                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    LOG(INFO) << "get AddSyncTask request: " << request->ShortDebugString();

    CreateTaskEnv(request, response);
}

bool DataCollectorImpl::ValidateTableStatus(const api::TableStatus& table_status) {
    if (table_status.storage_mode() == common::StorageMode::kMemory) {
        LOG(WARNING) << "storage mode is memory, not disk";
        return false;
    }
    // it may be empty in tablet server of old version
    if (table_status.snapshot_path().empty()) {
        LOG(WARNING) << "snapshot path is empty";
        return false;
    }
    if (table_status.binlog_path().empty()) {
        LOG(WARNING) << "binlog path is empty";
        return false;
    }
    if (!fs::path(table_status.snapshot_path()).is_absolute()) {
        LOG(WARNING) << "snapshot path is not absolute";
        return false;
    }
    if (!fs::path(table_status.binlog_path()).is_absolute()) {
        LOG(WARNING) << "binlog path is not absolute";
        return false;
    }
    return true;
}

bool DataCollectorImpl::CreateSnapshotEnvUnlocked(const std::string& name, const api::TableMeta& meta,
                                                  const std::string& snapshot_path) {
    auto snapshot_hardlink_path = GetWorkDir(name) + "snapshot/";
    auto it = snapshot_map_.find(name);
    if (it != snapshot_map_.end()) {
        LOG(INFO) << "snapshot env already exists, name: " << name;
        // diff the snapshot in db and hardlink
        auto manifest_in_db = GetManifest(snapshot_path);
        auto manifest_in_hardlink = GetManifest(snapshot_hardlink_path);
        if (!manifest_in_db.has_name() || !manifest_in_hardlink.has_name()) {
            // still create snapshot env
            LOG(WARNING) << "manifest in db or hardlink is empty, but we've created snapshot env before. Still "
                            "recreate env. db: "
                         << manifest_in_db.ShortDebugString() << ", hl: " << manifest_in_hardlink.ShortDebugString();
        } else if (google::protobuf::util::MessageDifferencer::Equals(manifest_in_db, manifest_in_hardlink)) {
            LOG(INFO) << "manifest is same, no need to update in snapshot env";
            return true;
        }

        LOG(INFO) << "remove old snapshot env";
        // old dir can be removed by HardLinkSnapshot
        snapshot_map_.erase(it);
    }
    // create new snapshot env

    // AddSyncTask is called by SyncTool, we don't need to check the old snapshot
    // Just hard link the newest snapshot
    uint64_t offset = 0;
    if (!HardLinkSnapshot(snapshot_path, snapshot_hardlink_path, &offset)) {
        LOG(WARNING) << "hard link snapshot failed, probably the collector_datadir is bad";
        return false;
    }
    snapshot_map_[name] = std::make_shared<storage::DiskTable>(meta, snapshot_hardlink_path);
    snapshot_map_[name]->Init();
    // set snapshot offset, so we can know the next offset when switch to binlog
    snapshot_map_[name]->SetOffset(offset);
    return true;
}

// not thread safe, don't call it in multi thread
bool DataCollectorImpl::FetchBinlogUnlocked(const std::string& name, const std::string& binlog_path, bool* updated) {
    // binlog_path is <>/binlog/, just files in it
    fs::path binlog_dir(binlog_path);
    // even empty, binlog dir should exist, if not, sth is wrong
    if (!fs::exists(binlog_dir)) {
        LOG(ERROR) << "binlog dir not exist, path: " << binlog_path;
        return false;
    }

    auto binlog_hardlink_path = GetWorkDir(name) + "binlog/";
    if (!fs::exists(binlog_hardlink_path)) {
        std::error_code ec;
        fs::create_directories(binlog_hardlink_path, ec);
        if (ec) {
            LOG(ERROR) << "create binlog hardlink dir failed, path: " << binlog_hardlink_path
                       << ", err: " << ec.message();
            return false;
        }
    }

    for (auto& p : fs::directory_iterator(binlog_dir)) {
        if (fs::is_regular_file(p)) {
            auto file_name = p.path().filename().string();
            if (!boost::ends_with(file_name, ".log")) {
                continue;
            }
            auto src = binlog_path + file_name;
            auto dst = binlog_hardlink_path + file_name;
            if (fs::exists(dst)) {
                // hardlinked before, skip. TODO(hw): check inode?
                continue;
            }
            std::error_code ec;
            fs::create_hard_link(src, dst, ec);
            // link failed, may miss some binlog, stop the task
            if (ec) {
                LOG(WARNING) << "create hard link failed, src: " << src << ", dst: " << dst
                             << ", err: " << ec.message();
                return false;
            }
            if (updated) {
                *updated = true;
            }
        }
    }
    return true;
}

// for one tid-pid, SyncOnce is serial(supported by add next task logic), so we don't need to lock for one tid-pid
void DataCollectorImpl::SyncOnce(uint32_t tid, uint32_t pid) {
    // read the progress
    auto task = GetTaskInfo(tid, pid);
    if (!task.has_tid()) {
        LOG(WARNING) << "task not exist(deleted), tid: " << tid << ", pid: " << pid << ", reject task";
        return;
    }
    LOG(INFO) << "sync once task: " << task.ShortDebugString();
    auto name = EncodeId(tid, pid);
    auto start_point = task.sync_point();
    auto mode = task.mode();

    // TODO(hw): fetch binlog in a work thread
    // we fetch binlog even when we read snapshot, cuz we don't want lost any binlog file, should fetch in time
    std::string binlog_path_in_db;
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        binlog_path_in_db = binlog_path_map_[name];
    }
    bool updated = false;
    if (FetchBinlogUnlocked(name, binlog_path_in_db, &updated) == false) {
        LOG(WARNING) << "fail to fetch binlog, may lost data, reject task: " << task.ShortDebugString();
        return;
    }
    // to avoid too much used binlog
    auto replicator = GetReplicator(name);
    if (replicator == nullptr) {
        LOG(WARNING) << "fail to get replicator, reject task: " << task.ShortDebugString();
        return;
    }
    // only update log part when fetch new, and no need to delete old log files when there is no new log file
    if (updated) {
        replicator->Recover();
        LOG(INFO) << "log parts: " << LogPartsToString(replicator->GetLogPart());
        if (start_point.type() == datasync::SyncType::kBINLOG &&
            replicator->GetSnapshotLastOffset() < start_point.offset()) {
            replicator->SetSnapshotLogPartIndex(start_point.offset());
            // delete old log file if SetSnapshotLogPartIndex
            // delete will update log parts, it's ok
            replicator->DeleteBinlog();
        }
    }

    // fullfil the request: io_buf, count, next_point, finished(for mode 0)
    butil::IOBuf io_buf;
    uint64_t count = 0;
    decltype(start_point) next_point;
    bool meet_binlog_end = false;
    // we can do switch in it
    if (!PackData(task, &io_buf, &count, &next_point, &meet_binlog_end)) {
        LOG(WARNING) << "fail to pack data, reject task: " << task.ShortDebugString();
        return;
    }

    VLOG(1) << "pack cnt:" << count << "-size:" << io_buf.size() << ", start " << start_point.ShortDebugString()
              << ", next: " << next_point.ShortDebugString();

    bool delay_longer = meet_binlog_end;
    bool is_finished = false;
    // even count is 0(all data in snapshot, binlog is empty)
    if (meet_binlog_end && mode == datasync::SyncMode::kFull) {
        is_finished = true;
        LOG(INFO) << "Mode FULL and meet binlog end, set finished flag";
    }
    bool need_update = true;
    // validations
    if (count == 0) {
        if (io_buf.size() != 0) {
            LOG(ERROR) << "no data but io_buf not empty, reject task" << task.ShortDebugString();
            return;
        }
        // it's ok to have no data but next_point is not equal to start_point, cuz we may have ts filter in mode 1
        // or many delete entries in binlog
        // check next_point >= start_point?

        // validate about status, if failed, reject task
        if (next_point.type() == datasync::SyncType::kSNAPSHOT) {
            LOG(ERROR) << "why snapshot can't get any data and can't switch to binlog? reject task: "
                       << task.ShortDebugString();
            return;
        }

        // validate meet_binlog_end if start_point is binlog(don't check by next_point, cuz it won't update
        // meet_binlog_end when snapshot->binlog, just check start_point)
        if (start_point.type() == datasync::SyncType::kBINLOG && !meet_binlog_end) {
            LOG(ERROR) << "why binlog can't get any data and can't meet binlog end? reject task: "
                       << task.ShortDebugString();
            return;
        }

        if (google::protobuf::util::MessageDifferencer::Equals(start_point, next_point)) {
            LOG(INFO) << "sync point not changed, skip update in file: " << task.ShortDebugString();
            need_update = false;
        }
    }
    // send sync rpc(not async, we should retry in the next turn if failed)
    decltype(task) updated_task;
    // DO NOT use io_buf after this line, because io_buf may be moved
    datasync::SendDataResponse response;
    LOG(INFO) << "send data, task: " << task.ShortDebugString() << ", count: " << count
              << ", is_finished: " << is_finished << ", next_point: " << next_point.ShortDebugString()
              << ", io_buf size: " << io_buf.size();
    auto ok = SendDataUnlock(&task, io_buf, count, next_point, is_finished, &response, &updated_task);
    // if failed, retry later
    if (!ok) {
        LOG(WARNING) << "send data failed(rpc), retry later, task: " << task.ShortDebugString();
        delay_longer = true;
    } else {
        bool destroy_env = false;
        // got sync tool response to delete outdated task
        if (response.has_delete_task() && response.delete_task()) {
            destroy_env = true;
        }
        if (response.response().code() != 0) {
            // retry or delete below
            LOG(WARNING) << "send data failed(sync), task: " << task.ShortDebugString()
                         << ", response: " << response.ShortDebugString();
        }

        // if sync_point field will changed, even is_finished, store the last change
        if (response.response().code() == 0 && need_update) {
            // update progress if send success
            {
                std::lock_guard<std::mutex> lock(process_map_mutex_);
                // update task info in memory, replace the old one(set replace to avoid failed when the same token)
                AddTaskInfoUnlocked(&updated_task, true);
                // save task info in disk
                SaveTaskInfoInDisk(&updated_task, GetWorkDir(name));
            }
        }
        // (count == 0 && is_finished == true) is possible, so we do independent check for is_finished
        if (destroy_env || is_finished) {
            if (destroy_env) {
                LOG(INFO) << "task fault and need to be deleted, tid: " << tid << ", pid: " << pid;
            } else if (is_finished) {
                LOG(INFO) << "task finished, tid: " << tid << ", pid: " << pid;
            }
            CleanTaskEnv(name);
            return;
        }
    }

    // Add next task.For each task, the sync progress is serial, so we add the next task to the task pool after the
    // current task is done.
    if (delay_longer) {
        // if meet binlog end, we can delay the next sync longer TODO(hw): param
        task_pool_.DelayTask(FLAGS_sync_task_long_interval_ms, std::bind(&DataCollectorImpl::SyncOnce, this, tid, pid));
    } else {
        task_pool_.DelayTask(FLAGS_sync_task_short_interval_ms,
                             std::bind(&DataCollectorImpl::SyncOnce, this, tid, pid));
    }
}

bool DataCollectorImpl::PackData(const datasync::AddSyncTaskRequest& task, butil::IOBuf* io_buf, uint64_t* count,
                                 datasync::SyncPoint* next_point, bool* meet_binlog_end) {
    auto name = EncodeId(task.tid(), task.pid());
    auto start_point = task.sync_point();
    auto mode = task.mode();
    auto start_ts = task.start_ts();

    auto pack_with_ts = [io_buf, count, &start_ts](uint64_t ts, const base::Slice& slice) {
        if (ts < start_ts) {
            return true;
        }
        if (io_buf->size() + slice.size() > FLAGS_max_pack_size) {
            return false;
        }
        auto ret = io_buf->append(slice.data(), slice.size());
        if (ret != 0) {
            LOG(WARNING) << "fail to append slice to io_buf";
            return false;
        }
        *count += 1;
        return true;
    };
    // ts is not used in mode 0/2
    auto pack = [io_buf, count](uint64_t ts, const base::Slice& slice) {
        if (io_buf->size() + slice.size() > FLAGS_max_pack_size) {
            return false;
        }
        auto ret = io_buf->append(slice.data(), slice.size());
        if (ret != 0) {
            LOG(WARNING) << "fail to append slice to io_buf";
            return false;
        }
        *count += 1;
        return true;
    };

    if (start_point.type() == datasync::SyncType::kSNAPSHOT) {
        // read snapshot
        std::shared_ptr<storage::TraverseIterator> iter;
        uint64_t snapshot_offset = 0;  // snapshot end offset
        {
            std::lock_guard<std::mutex> lock(process_map_mutex_);
            auto it = snapshot_map_.find(name);
            if (it == snapshot_map_.end()) {
                LOG(INFO) << "snapshot env does not exist, switch to binlog";
                next_point->set_type(datasync::SyncType::kBINLOG);
                // start read from 1(include)
                next_point->set_offset(1);
                return true;
            }
            // 0 is the base index
            iter.reset(it->second->NewTraverseIterator(0));
            snapshot_offset = it->second->GetOffset();
        }
        if (iter == nullptr) {
            LOG(WARNING) << "fail to get snapshot iterator of index 0";
            return false;
        }
        if (!start_point.has_pk()) {
            LOG(INFO) << "read snapshot from begin";
            iter->SeekToFirst();
        } else {
            LOG(INFO) << "read snapshot from pk: " << start_point.pk() << " ts: " << start_point.ts();
            iter->Seek(start_point.pk(), start_point.ts());
        }

        // pack Slices, include start_point, exclude next_point
        bool finished = false;
        if (mode == datasync::SyncMode::kIncrementalByTimestamp) {
            // to parse the ts, we need the table meta
            finished = PackSNAPSHOT(iter, pack_with_ts, next_point);
        } else {
            finished = PackSNAPSHOT(iter, pack, next_point);
        }
        // if snapshot finished, we should set the next offset to snapshot_offset(manifest's offset) + 1
        if (finished) {
            next_point->set_type(datasync::SyncType::kBINLOG);
            next_point->set_offset(snapshot_offset + 1);
        }
    } else {
        std::shared_ptr<openmldb::log::LogReader> reader;
        // we read the binlog path in db, it can be shared
        // DO NOT write in the binlog path
        // it's ok to check no_binlog before reader
        {
            std::lock_guard<std::mutex> lock(process_map_mutex_);
            if (replicator_map_.find(name) == replicator_map_.end()) {
                LOG(WARNING) << "replicator not exist, task " << task.ShortDebugString();
                return false;
            }
            auto replicator = replicator_map_[name];
            // if no binlog file(partition is empty), we can check it
            bool no_binlog = false;
            reader = getLogReader(replicator, start_point.offset(), &no_binlog);
            if (no_binlog) {
                LOG(INFO) << "no binlog file, task " << task.ShortDebugString();
                // buf stay the same
                *count = 0;
                *next_point = start_point;
                *meet_binlog_end = true;
                return true;
            }
        }
        if (reader == nullptr) {
            LOG(WARNING) << "fail to get binlog reader, task " << task.ShortDebugString();
            return false;
        }
        bool ok = false;
        if (mode == datasync::SyncMode::kIncrementalByTimestamp) {
            ok = PackBINLOG(reader, start_point.offset(), pack_with_ts, next_point, meet_binlog_end);
        } else {
            ok = PackBINLOG(reader, start_point.offset(), pack, next_point, meet_binlog_end);
        }
        if (!ok) {
            LOG(WARNING) << "fail to pack binlog, task " << task.ShortDebugString();
            return false;
        }
    }
    return true;
}

// snapshot iterator is simple, no where to know if error, so the return value is used to indicate if finished
// return true: read snapshot finished, false: still has snapshot to read
// If finished, don't forget to switch to binlog and set offset later
template <typename Func>
bool DataCollectorImpl::PackSNAPSHOT(std::shared_ptr<storage::TraverseIterator> it, Func pack_func,
                                     datasync::SyncPoint* next_point) {
    while (it->Valid()) {
        if (!pack_func(it->GetKey(), it->GetValue())) {
            break;
        }
        it->Next();
    }

    // still has next to read
    if (it->Valid()) {
        next_point->set_type(datasync::SyncType::kSNAPSHOT);
        next_point->set_pk(it->GetPK());
        next_point->set_ts(it->GetKey());
    }
    return !it->Valid();
}

// TODO(hw): what if SchedDelBinlog is called in tablet server when we are reading binlog?
// return the next offset to read?
template <typename Func>
int PackRecords(std::shared_ptr<log::LogReader> reader, uint64_t start_offset, Func&& pack_func, uint64_t* next_offset,
                bool* meet_binlog_end) {
    api::LogEntry entry;
    std::string buffer;
    base::Slice record;
    log::Status status;
    int cur_log_index = reader->GetLogIndex();
    *meet_binlog_end = false;
    *next_offset = start_offset;
    // TODO(hw): refactor this
    while (true) {
        status = reader->ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            if (!entry.ParseFromString(record.ToString())) {
                LOG(ERROR) << "parse log entry failed, skip it. " << base::DebugString(record.ToString());
                continue;
            }
            // TODO(hw): just skip delete record?
            if (entry.has_method_type() && entry.method_type() == api::MethodType::kDelete) {
                continue;
            }
            // TODO(hw): need check index in order?
            if (entry.log_index() >= start_offset) {
                if (!pack_func(entry.ts(), entry.value())) {
                    // stop pack, current entry is exclude, so read in next.
                    // and meet_binlog_end is false
                    *next_offset = entry.log_index();
                    return 0;
                }
                *next_offset = entry.log_index() + 1;
            }
        } else if (status.IsWaitRecord()) {
            // handle unfinished binlog file
            // ref Binlog::RecoverFromBinlog, but don't use it directly, cuz we can't modify the binlog in db
            // it isn't an error status, just wait for next record
            int end_log_index = reader->GetEndLogIndex();
            int cur_log_index = reader->GetLogIndex();
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                reader->RollRLogFile();
                continue;
            }
            // read the opening binlog file, meet the last record, it's ok
            *meet_binlog_end = true;
            return 0;
        } else if (status.IsEof()) {
            // should read next binlog file
            if (reader->GetLogIndex() != cur_log_index) {
                cur_log_index = reader->GetLogIndex();
                continue;
            }
            // last binlog file has been read
            *meet_binlog_end = true;
            return 0;
        } else {
            // TODO(hw): should abort when read failed?
            LOG(ERROR) << "read record failed, skip it, status: " << status.ToString();
            continue;
        }
    }
    return -1;
}

template <typename Func>
bool DataCollectorImpl::PackBINLOG(std::shared_ptr<log::LogReader> reader, uint64_t start_offset, Func pack_func,
                                   datasync::SyncPoint* next_point, bool* meet_binlog_end) {
    uint64_t next_offset = 0;
    auto ret = PackRecords(reader, start_offset, std::move(pack_func), &next_offset, meet_binlog_end);
    if (ret != 0) {
        LOG(WARNING) << "PackRecords failed, ret: " << ret << ". maybe binlog is break or no data";
        return false;
    }
    next_point->set_type(datasync::SyncType::kBINLOG);
    next_point->set_offset(next_offset);
    return true;
}

// use response and update_task iff status == true (the response may be not ok, and should delete the task)
bool DataCollectorImpl::SendDataUnlock(const datasync::AddSyncTaskRequest* task, butil::IOBuf& data, uint64_t count,
                                       const datasync::SyncPoint& next_point, bool is_finished,
                                       datasync::SendDataResponse* reponse, datasync::AddSyncTaskRequest* update_task) {
    // pack send data request
    datasync::SendDataRequest send_data_request;
    send_data_request.set_tid(task->tid());
    send_data_request.set_pid(task->pid());
    send_data_request.mutable_start_point()->CopyFrom(task->sync_point());
    send_data_request.set_count(count);
    send_data_request.set_token(task->token());
    send_data_request.mutable_next_point()->CopyFrom(next_point);
    send_data_request.set_finished(is_finished);

    // send to sync tool
    auto sync_tool_addr = task->des_endpoint();
    SyncToolClient client(sync_tool_addr);
    if (!client.Init()) {
        LOG(WARNING) << "fail to init sync tool client, task " << task->ShortDebugString();
        return false;
    }

    if (!client.SendData(&send_data_request, data, reponse)) {
        LOG(WARNING) << "fail to send data to sync tool, task " << task->ShortDebugString();
        return false;
    }

    // update, if response is not ok, this new update task shouldn't be added to task map
    if (update_task) {
        update_task->CopyFrom(*task);
        update_task->mutable_sync_point()->CopyFrom(next_point);
    }

    return true;
}

// recover from disk, report the failed tasks, but it's not mean the data collector shouldn't run
void DataCollectorImpl::Recover() {
    // read sync task info from disk, get binlog path in the runtime(if tablet is invalid, no need to recover sync
    // task) and do some validations
    LOG(INFO) << "start to recover sync task";
    // walk through the sync task dir FLAGS_collector_datadir
    if (!fs::exists(FLAGS_collector_datadir)) {
        LOG(INFO) << "collector data dir not exist, skip recover. dir: " << FLAGS_collector_datadir;
        return;
    }
    // datadir/<tid-pid>/task.progress
    std::vector<std::string> failed_tasks;
    for (const auto& entry : fs::directory_iterator(FLAGS_collector_datadir)) {
        if (!entry.is_directory()) {
            LOG(WARNING) << "irrelavant file? " << entry.path();
            continue;
        }
        LOG(INFO) << "try recover: " << entry.path();
        // get the last directory name, parse tid-pid
        auto name = entry.path().filename();

        // read task.progress
        datasync::AddSyncTaskRequest recovered_task;
        if (!LoadTaskInfoFromDisk(entry.path().string() + "/", &recovered_task)) {
            LOG(WARNING) << "fail to load task info from disk, skip. task dir: " << entry.path();
            failed_tasks.push_back(name);
            continue;
        }
        // create task backup
        datasync::GeneralResponse response;
        CreateTaskEnv(&recovered_task, &response);
        if (response.code() != 0) {
            LOG(WARNING) << "fail to create task env for recovery, skip. task dir: " << entry.path();
            failed_tasks.push_back(name);
        }
    }
    if (!failed_tasks.empty()) {
        LOG(WARNING) << "fail to recover some tasks, please check the log. failed tasks: "
                     << absl::StrJoin(failed_tasks, ",");
    }
}

void DataCollectorImpl::CleanTaskEnv(const std::string& name) {
    // destroy the task env
    {
        std::lock_guard<std::mutex> lock(process_map_mutex_);
        process_map_.erase(name);
        snapshot_map_.erase(name);
        replicator_map_.erase(name);
    }
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        binlog_path_map_.erase(name);
    }
    // TODO(hw): store some history data?
    fs::remove_all(GetWorkDir(name));
}
}  // namespace openmldb::datacollector
