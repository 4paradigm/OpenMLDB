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
#ifndef SRC_DATACOLLECTOR_DATA_COLLECTOR_H_
#define SRC_DATACOLLECTOR_DATA_COLLECTOR_H_

#include <filesystem>
#include <map>
#include <memory>
#include <string>

#include "common/thread_pool.h"

#include "base/file_util.h"
#include "client/tablet_client.h"
#include "log/log_reader.h"
#include "proto/data_sync.pb.h"
#include "proto/tablet.pb.h"
#include "replica/log_replicator.h"
#include "storage/disk_table.h"
#include "storage/snapshot.h"
#include "zk/zk_client.h"

DECLARE_int32(request_timeout_ms);
namespace openmldb::datacollector {

static const char kDataCollectorRegisterPath[] = "/sync_tool/collector";

// To hold log parts, we get log reader from log replicator
std::shared_ptr<replica::LogReplicator> genLogReplicatorFromBinlog(const std::string& binlog_path);

std::shared_ptr<log::LogReader> getLogReader(std::shared_ptr<replica::LogReplicator> replicator, uint64_t start_offset,
                                             bool* no_binlog_file);

// two path need end with '/'
bool HardLinkSnapshot(const std::string& snapshot_path, const std::string& dest, uint64_t* snapshot_offset);

bool SaveTaskInfoInDisk(const datasync::AddSyncTaskRequest* info, const std::string& path);
class SyncToolClient {
 public:
    explicit SyncToolClient(const std::string& endpoint) : client_(endpoint) {}
    ~SyncToolClient() {}
    bool Init() {
        if (client_.Init() != 0) return false;
        return true;
    }
    // if ok, check repsonse, otherwise do not use response
    bool SendData(const datasync::SendDataRequest* request, butil::IOBuf& data,  // NOLINT
                  datasync::SendDataResponse* response) {
        // TODO(hw): IOBufAppender is better?
        auto st = client_.SendRequestSt(&datasync::SyncTool_Stub::SendData,
                                        [&data](brpc::Controller* cntl) { cntl->request_attachment().swap(data); },
                                        request, response, FLAGS_request_timeout_ms, 1);
        if (!st.OK()) {
            LOG(WARNING) << "send data rpc failed, " << st.GetCode() << ", " << st.GetMsg();
            return false;
        }
        return true;
    }

 private:
    RpcClient<datasync::SyncTool_Stub> client_;
};

class DataCollectorImpl : public datasync::DataCollector {
 public:
    DataCollectorImpl();
    ~DataCollectorImpl();
    // setup zk client, recover if process exist
    bool Init(const std::string& endpoint);
    bool Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint);

    // should call after Init(setup zk client)
    bool RegisterZK();
    void CheckZkClient();

    void AddSyncTask(::google::protobuf::RpcController* controller,
                     const ::openmldb::datasync::AddSyncTaskRequest* request,
                     ::openmldb::datasync::GeneralResponse* response, ::google::protobuf::Closure* done) override;
    // limited by rpc size, so we need to split task
    void SyncOnce(uint32_t tid, uint32_t pid);

    // if not exist, return empty, you can check it by has_tid()
    datasync::AddSyncTaskRequest GetTaskInfo(uint32_t tid, uint32_t pid) {
        std::lock_guard<std::mutex> lock(process_map_mutex_);
        auto name = EncodeId(tid, pid);
        // warning if not exist
        if (process_map_.find(name) == process_map_.end()) {
            LOG(WARNING) << "task not exist, tid: " << tid << ", pid: " << pid;
            return {};
        }
        return process_map_[name];
    }

 private:
    void Recover();

    // check status by using response
    void CreateTaskEnv(const datasync::AddSyncTaskRequest* request, datasync::GeneralResponse* response);

    void CleanTaskEnv(const std::string& name);

    std::string EncodeId(uint32_t tid, uint32_t pid) { return std::to_string(tid) + "-" + std::to_string(pid); }

    std::string GetWorkDir(const std::string& name);

    bool AddTaskInfoUnlocked(const datasync::AddSyncTaskRequest* request, bool replace = false) {
        // "tid-pid" as key
        auto name = EncodeId(request->tid(), request->pid());
        auto it = process_map_.find(name);
        if (it != process_map_.end()) {
            LOG(INFO) << "task replace, old: " << it->second.ShortDebugString()
                      << ", new: " << request->ShortDebugString();
            if (!replace && it->second.token() == request->token()) {
                LOG(ERROR) << "token is same, ignore";
                return false;
            }
        }
        process_map_.insert_or_assign(name, *request);
        return true;
    }

    bool ValidateTableStatus(const api::TableStatus& table_status);
    bool CreateSnapshotEnvUnlocked(const std::string& name, const api::TableMeta& meta,
                                   const std::string& snapshot_path);
    bool FetchBinlogUnlocked(const std::string& name, const std::string& binlog_path, bool* updated);

    bool PackData(const datasync::AddSyncTaskRequest& task, butil::IOBuf* io_buf, uint64_t* count,
                  datasync::SyncPoint* next_point, bool* meet_binlog_end);

    template <typename Func>
    bool PackSNAPSHOT(std::shared_ptr<storage::TraverseIterator> it, Func pack_func, datasync::SyncPoint* next_point);
    template <typename Func>
    bool PackBINLOG(std::shared_ptr<log::LogReader> reader, uint64_t start_offset, Func pack_func,
                    datasync::SyncPoint* next_point, bool* meet_binlog_end);

    // data will be cleared, whether send success or not
    bool SendDataUnlock(const datasync::AddSyncTaskRequest* task, butil::IOBuf& data, uint64_t count,  // NOLINT
                        const datasync::SyncPoint& next_point, bool is_finished, datasync::SendDataResponse* reponse,
                        datasync::AddSyncTaskRequest* update_task);

    std::shared_ptr<replica::LogReplicator> GetReplicator(const std::string& name) {
        std::lock_guard<std::mutex> lock(process_map_mutex_);
        auto it = replicator_map_.find(name);
        if (it == replicator_map_.end()) {
            LOG(WARNING) << "replicator not exist, name: " << name;
            return nullptr;
        }
        return it->second;
    }

 private:
    std::shared_ptr<zk::ZkClient> zk_client_;
    // zk background thread
    baidu::common::ThreadPool keep_alive_pool_;
    // protect 4 maps
    std::mutex process_map_mutex_;
    // tid-pid -> ..., we use tid-pid as key cuz data collector doesn't need to know the status of whole tid
    std::map<std::string, datasync::AddSyncTaskRequest> process_map_;
    std::map<std::string, std::shared_ptr<storage::DiskTable>> snapshot_map_;
    // store LogRepliactor, so we can get LogParts to seek and LogReader to read
    std::map<std::string, std::shared_ptr<replica::LogReplicator>> replicator_map_;
    // tablet_endpoint -> tablet_client
    std::map<std::string, std::shared_ptr<client::TabletClient>> tablet_client_map_;

    // smaller lock, cache the binlog path, don't use the big lock process_map_mutex_ to lock it
    std::mutex cache_mutex_;
    std::map<std::string, std::string> binlog_path_map_;
    baidu::common::ThreadPool task_pool_;
};
}  // namespace openmldb::datacollector

#endif  // SRC_DATACOLLECTOR_DATA_COLLECTOR_H_
