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

#ifndef SRC_NAMESERVER_TASK_H_
#define SRC_NAMESERVER_TASK_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/function.hpp"
#include "proto/common.pb.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"

namespace openmldb {
namespace nameserver {

using TaskFun = boost::function<void()>;
constexpr uint64_t INVALID_PARENT_ID = UINT64_MAX;

class Task {
 public:
    Task(std::string endpoint, std::shared_ptr<::openmldb::api::TaskInfo> task_info)
        : endpoint_(std::move(endpoint)), task_info_(std::move(task_info)) {}
    bool IsFinished() const;
    void UpdateTaskStatus(const ::google::protobuf::RepeatedPtrField<::openmldb::api::TaskInfo>& tasks,
            const std::string& endpoint, bool is_recover);
    void UpdateStatusFromSubTask();
    std::string GetReadableType() const { return ::openmldb::api::TaskType_Name(task_info_->task_type()); }
    api::TaskType GetType() const { return task_info_->task_type(); }
    void SetStatus(api::TaskStatus status) { task_info_->set_status(status); }
    api::TaskStatus GetStatus() const { return task_info_->status(); }
    std::string GetReadableStatus() const { return ::openmldb::api::TaskStatus_Name(task_info_->status()); }
    static std::string GetReadableStatus(const ::openmldb::api::TaskInfo& task_info) {
        return ::openmldb::api::TaskStatus_Name(task_info.status());
    }
    uint64_t GetOpId() const { return task_info_->op_id(); }
    std::string GetReadableOpType() const { return ::openmldb::api::OPType_Name(task_info_->op_type()); }
    std::string GetAdditionalMsg();  // for log info
    static std::string GetAdditionalMsg(const ::openmldb::api::TaskInfo& task_info);

    std::string endpoint_;
    std::shared_ptr<::openmldb::api::TaskInfo> task_info_;
    std::list<std::shared_ptr<Task>> sub_task_;
    TaskFun fun_;

 private:
    bool UpdateStatus(const ::openmldb::api::TaskInfo& task_info, const std::string& endpoint);
    void SetState(const std::string& endpoint, bool is_recover);

 private:
    int check_num_ = 0;
    bool traversed_ = false;
};

class OPData {
 public:
     uint64_t GetOpId() const { return op_info_.op_id(); }
     api::OPType GetType() const { return op_info_.op_type(); }
     std::string GetReadableType() const { return ::openmldb::api::OPType_Name(op_info_.op_type()); }
     void SetTaskStatus(api::TaskStatus status) { op_info_.set_task_status(status); }
     api::TaskStatus GetTaskStatus() const { return op_info_.task_status(); }

    ::openmldb::api::OPInfo op_info_;
    std::list<std::shared_ptr<Task>> task_list_;
};

class TaskMeta {
 public:
    TaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, ::openmldb::api::TaskType task_type,
            const std::string& endpoint) {
        task_info = std::make_shared<::openmldb::api::TaskInfo>();
        task_info->set_op_id(op_id);
        task_info->set_op_type(op_type);
        task_info->set_task_type(task_type);
        task_info->set_status(::openmldb::api::TaskStatus::kInited);
        if (!endpoint.empty()) {
            task_info->set_endpoint(endpoint);
        }
    }
    virtual ~TaskMeta() {}
    std::shared_ptr<::openmldb::api::TaskInfo> task_info;
};

class MakeSnapshotTaskMeta : public TaskMeta {
 public:
    MakeSnapshotTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, uint64_t end_offset_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kMakeSnapshot, endpoint),
        tid(tid_i), pid(pid_i), end_offset(end_offset_i) {}
    uint32_t tid;
    uint32_t pid;
    uint64_t end_offset;
};

class PauseSnapshotTaskMeta : public TaskMeta {
 public:
    PauseSnapshotTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kPauseSnapshot, endpoint),
        tid(tid_i), pid(pid_i) {}
    uint32_t tid;
    uint32_t pid;
};

class RecoverSnapshotTaskMeta : public TaskMeta {
 public:
    RecoverSnapshotTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kRecoverSnapshot, endpoint),
        tid(tid_i), pid(pid_i) {}
    uint32_t tid;
    uint32_t pid;
};

class SendSnapshotTaskMeta : public TaskMeta {
 public:
    SendSnapshotTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t remote_tid_i, uint32_t pid_i, const std::string& des_endpoint_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kSendSnapshot, endpoint),
        tid(tid_i), remote_tid(remote_tid_i), pid(pid_i), des_endpoint(des_endpoint_i) {}
    uint32_t tid;
    uint32_t remote_tid;
    uint32_t pid;
    std::string des_endpoint;
};

class LoadTableTaskMeta : public TaskMeta {
 public:
    LoadTableTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            const std::string& name_i, uint32_t tid_i, uint32_t pid_i, uint32_t seg_cnt_i,
            bool is_leader_i, ::openmldb::common::StorageMode storage_mode_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kLoadTable, endpoint),
        name(name_i), tid(tid_i), pid(pid_i), seg_cnt(seg_cnt_i),
        is_leader(is_leader_i), storage_mode(storage_mode_i) {}
    std::string name;
    uint32_t tid;
    uint32_t pid;
    uint32_t seg_cnt;
    bool is_leader;
    ::openmldb::common::StorageMode storage_mode;
};

class AddReplicaTaskMeta : public TaskMeta {
 public:
    AddReplicaTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, const std::string& des_endpoint_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddReplica, endpoint),
        tid(tid_i), pid(pid_i), des_endpoint(des_endpoint_i) {}
    AddReplicaTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, const std::string& des_endpoint_i,
            uint32_t remote_tid_i, uint64_t task_id_i = INVALID_PARENT_ID) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddReplica, endpoint),
        tid(tid_i), pid(pid_i), des_endpoint(des_endpoint_i),
        is_remote(true), remote_tid(remote_tid_i), task_id(task_id_i) {}
    uint32_t tid;
    uint32_t pid;
    std::string des_endpoint;
    bool is_remote = false;
    uint32_t remote_tid;
    uint64_t task_id = INVALID_PARENT_ID;
};

class DelReplicaTaskMeta : public TaskMeta {
 public:
    DelReplicaTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, const std::string& des_endpoint_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kDelReplica, endpoint),
        tid(tid_i), pid(pid_i), des_endpoint(des_endpoint_i) {}
    uint32_t tid;
    uint32_t pid;
    std::string des_endpoint;
};

class DropTableTaskMeta : public TaskMeta {
 public:
    DropTableTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kDropTable, endpoint),
        tid(tid_i), pid(pid_i) {}
    uint32_t tid;
    uint32_t pid;
};

class SendIndexRequestTaskMeta : public TaskMeta {
 public:
    SendIndexRequestTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, const std::map<uint32_t, std::string>& pid_endpoint_map_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kSendIndexRequest, endpoint),
        tid(tid_i), pid(pid_i), pid_endpoint_map(pid_endpoint_map_i) {
        task_info->set_tid(tid);
        task_info->set_pid(pid);
    }
    uint32_t tid;
    uint32_t pid;
    std::map<uint32_t, std::string> pid_endpoint_map;
};

class SendIndexDataTaskMeta : public TaskMeta {
 public:
    SendIndexDataTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            uint32_t tid_i, const std::map<uint32_t, std::string>& pid_endpoint_map_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kSendIndexData, ""),
        tid(tid_i), pid_endpoint_map(pid_endpoint_map_i) {
        task_info->set_tid(tid);
    }
    uint32_t tid;
    std::map<uint32_t, std::string> pid_endpoint_map;
};

class LoadIndexRequestTaskMeta : public TaskMeta {
 public:
    LoadIndexRequestTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, uint32_t partition_num_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kLoadIndexRequest, endpoint),
        tid(tid_i), pid(pid_i), partition_num(partition_num_i) {
        task_info->set_tid(tid);
        task_info->set_pid(pid);
    }
    uint32_t tid;
    uint32_t pid;
    uint32_t partition_num;
};

class LoadIndexDataTaskMeta : public TaskMeta {
 public:
    LoadIndexDataTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            uint32_t tid_i, uint32_t partition_num_i,
            const std::map<uint32_t, std::string>& pid_endpoint_map_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kLoadIndexData, ""),
        tid(tid_i), pid_endpoint_map(pid_endpoint_map_i) {
        task_info->set_tid(tid);
    }
    uint32_t tid;
    std::map<uint32_t, std::string> pid_endpoint_map;
};

class ExtractIndexRequestTaskMeta : public TaskMeta {
 public:
    ExtractIndexRequestTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, uint32_t partition_num_i,
            const std::vector<::openmldb::common::ColumnKey>& column_key_i,
            uint64_t offset_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kExtractIndexRequest, endpoint),
        tid(tid_i), pid(pid_i), partition_num(partition_num_i), column_key(column_key_i), offset(offset_i) {
        task_info->set_tid(tid);
        task_info->set_pid(pid);
    }
    uint32_t tid;
    uint32_t pid;
    uint32_t partition_num;
    std::vector<::openmldb::common::ColumnKey> column_key;
    uint64_t offset;
};

class ExtractIndexDataTaskMeta : public TaskMeta {
 public:
    ExtractIndexDataTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            uint32_t tid_i, uint32_t partition_num_i,
            const std::vector<::openmldb::common::ColumnKey>& column_key_i,
            const std::map<uint32_t, uint64_t>& pid_offset_map_i,
            const std::map<uint32_t, std::string>& pid_endpoint_map_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kExtractIndexData, ""),
        tid(tid_i), partition_num(partition_num_i), column_key(column_key_i),
        pid_offset_map(pid_offset_map_i), pid_endpoint_map(pid_endpoint_map_i) {
        task_info->set_tid(tid);
    }
    uint32_t tid;
    uint32_t partition_num;
    std::vector<::openmldb::common::ColumnKey> column_key;
    std::map<uint32_t, uint64_t> pid_offset_map;
    std::map<uint32_t, std::string> pid_endpoint_map;
};

class AddIndexToTabletTaskMeta : public TaskMeta {
 public:
    AddIndexToTabletTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const ::openmldb::nameserver::TableInfo& table_info_i,
            const std::vector<::openmldb::common::ColumnKey>& column_key_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddIndexToTablet, ""),
        table_info(table_info_i), column_key(column_key_i) {
        task_info->set_tid(table_info.tid());
    }
    ::openmldb::nameserver::TableInfo table_info;
    std::vector<::openmldb::common::ColumnKey> column_key;
};

class AddIndexToTabletRequestTaskMeta : public TaskMeta {
 public:
    AddIndexToTabletRequestTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i,
            const std::vector<::openmldb::common::ColumnKey>& column_key_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddIndexToTabletRequest, endpoint),
        tid(tid_i), pid(pid_i), column_key(column_key_i) {
        task_info->set_tid(tid);
        task_info->set_pid(pid);
    }
    uint32_t tid;
    uint32_t pid;
    std::vector<::openmldb::common::ColumnKey> column_key;
};

class AddIndexToTableInfoTaskMeta : public TaskMeta {
 public:
    AddIndexToTableInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& table_name, const std::string& db_name,
            const std::vector<::openmldb::common::ColumnKey>& column_key_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddIndexToTableInfo, ""),
        name(table_name), db(db_name), column_key(column_key_i) {}
    std::string name;
    std::string db;
    std::vector<::openmldb::common::ColumnKey> column_key;
};

class AddTableInfoTaskMeta : public TaskMeta {
 public:
    AddTableInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,  uint32_t pid_i, const std::string& endpoint_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddTableInfo, ""),
        name(name_i), db(db_i), pid(pid_i), endpoint(endpoint_i) {}

    AddTableInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,  uint32_t pid_i, const std::string& endpoint_i,
            const std::string& alias_i, uint32_t remote_tid_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddTableInfo, ""),
        name(name_i), db(db_i), pid(pid_i), endpoint(endpoint_i),
        is_remote(true), alias(alias_i), remote_tid(remote_tid_i) {}
    std::string name;
    std::string db;
    uint32_t pid;
    std::string endpoint;
    bool is_remote = false;
    std::string alias;
    uint32_t remote_tid;
};

class DelTableInfoTaskMeta : public TaskMeta {
 public:
    DelTableInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,  uint32_t pid_i, const std::string& endpoint_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kDelTableInfo, ""),
        name(name_i), db(db_i), pid(pid_i), endpoint(endpoint_i) {}

    DelTableInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,  uint32_t pid_i,
            const std::string& endpoint_i, uint32_t flag_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kDelTableInfo, ""),
        name(name_i), db(db_i), pid(pid_i), endpoint(endpoint_i), has_flag(true), flag(flag_i) {}
    std::string name;
    std::string db;
    uint32_t pid;
    std::string endpoint;
    bool has_flag = false;
    uint32_t flag;
};

class UpdateTableInfoTaskMeta : public TaskMeta {
 public:
    UpdateTableInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,  uint32_t pid_i,
            const std::string& src_endpoint_i, const std::string& des_endpoint_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kUpdateTableInfo, ""),
        name(name_i), db(db_i), pid(pid_i), src_endpoint(src_endpoint_i), des_endpoint(des_endpoint_i) {}
    std::string name;
    std::string db;
    uint32_t pid;
    std::string src_endpoint;
    std::string des_endpoint;
};

class CheckBinlogSyncProgressTaskMeta : public TaskMeta {
 public:
    CheckBinlogSyncProgressTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,  uint32_t pid_i,
            const std::string& follower_i, uint64_t offset_delta_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kCheckBinlogSyncProgress, ""),
        name(name_i), db(db_i), pid(pid_i), follower(follower_i), offset_delta(offset_delta_i) {}
    std::string name;
    std::string db;
    uint32_t pid;
    std::string follower;
    uint64_t offset_delta;
};

class ChangeLeaderTaskMeta : public TaskMeta {
 public:
    ChangeLeaderTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kChangeLeader, "") {}
};

class UpdateLeaderInfoTaskMeta : public TaskMeta {
 public:
    UpdateLeaderInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kUpdateLeaderInfo, "") {}
};

class SelectLeaderTaskMeta : public TaskMeta {
 public:
    SelectLeaderTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i, uint32_t tid_i, uint32_t pid_i,
            const std::vector<std::string>& follower_endpoint_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kSelectLeader, ""),
        name(name_i), db(db_i), tid(tid_i), pid(pid_i), follower_endpoint(follower_endpoint_i) {}
    std::string name;
    std::string db;
    uint32_t tid;
    uint32_t pid;
    std::vector<std::string> follower_endpoint;
};

class UpdatePartitionStatusTaskMeta : public TaskMeta {
 public:
    UpdatePartitionStatusTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i, uint32_t pid_i,
            const std::string& endpoint_i, bool is_leader_i, bool is_alive_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kUpdatePartitionStatus, ""),
        name(name_i), db(db_i), pid(pid_i), endpoint(endpoint_i), is_leader(is_leader_i), is_alive(is_alive_i) {}
    std::string name;
    std::string db;
    uint32_t pid;
    std::string endpoint;
    bool is_leader;
    bool is_alive;
};

class RecoverTableTaskMeta : public TaskMeta {
 public:
    RecoverTableTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i, uint32_t pid_i,
            const std::string& endpoint_i, uint64_t offset_delta_i, uint32_t concurrency_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kRecoverTable, ""),
        name(name_i), db(db_i), pid(pid_i), endpoint(endpoint_i),
        offset_delta(offset_delta_i), concurrency(concurrency_i) {}
    std::string name;
    std::string db;
    uint32_t pid;
    std::string endpoint;
    uint64_t offset_delta;
    uint32_t concurrency;
};

class CreateTableRemoteTaskMeta : public TaskMeta {
 public:
    CreateTableRemoteTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const ::openmldb::nameserver::TableInfo& table_info_i, const std::string& alias_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kCreateTableRemote, ""),
        table_info(table_info_i), alias(alias_i) {}
    ::openmldb::nameserver::TableInfo table_info;
    std::string alias;
};

class DropTableRemoteTaskMeta : public TaskMeta {
 public:
    DropTableRemoteTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i, const std::string& alias_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kDropTableRemote, ""),
        name(name_i), db(db_i), alias(alias_i) {}
    std::string name;
    std::string db;
    std::string alias;
};

class AddReplicaNSRemoteTaskMeta : public TaskMeta {
 public:
    AddReplicaNSRemoteTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& alias_i,
            const std::vector<std::string>& endpoint_vec_i, uint32_t pid_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddReplicaNSRemote, ""),
        name(name_i), alias(alias_i), endpoint_vec(endpoint_vec_i), pid(pid_i) {}
    std::string name;
    std::string alias;
    std::vector<std::string> endpoint_vec;
    uint32_t pid;
};

class AddMultiTableIndexTaskMeta : public TaskMeta {
 public:
    AddMultiTableIndexTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const ::google::protobuf::RepeatedPtrField<nameserver::TableIndex>& table_index_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddMultiTableIndex, ""), table_index(table_index_i) {}
    ::google::protobuf::RepeatedPtrField<nameserver::TableIndex> table_index;
};

class AddTableIndexTaskMeta : public TaskMeta {
 public:
    AddTableIndexTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,
            const std::vector<::openmldb::common::ColumnKey>& column_key_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddTableIndex, ""),
        name(name_i), db(db_i), column_key(column_key_i) {}
    std::string name;
    std::string db;
    std::vector<::openmldb::common::ColumnKey> column_key;
};

class CreateProcedureTaskMeta : public TaskMeta {
 public:
    CreateProcedureTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const api::ProcedureInfo& sp_info_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kCreateProcedure, ""), sp_info(sp_info_i) {}
    api::ProcedureInfo sp_info;
};

}  // namespace nameserver
}  // namespace openmldb
#endif  // SRC_NAMESERVER_TASK_H_
