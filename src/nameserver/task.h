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

#include <map>
#include <memory>
#include <string>

#include "boost/bind.hpp"
#include "proto/common.pb.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"

namespace openmldb {
namespace nameserver {

using TaskFun = boost::function<void()>;

struct Task {
    Task(std::string endpoint, std::shared_ptr<::openmldb::api::TaskInfo> task_info)
        : endpoint_(std::move(endpoint)), task_info_(std::move(task_info)) {}
    ~Task() = default;
    std::string endpoint_;
    std::shared_ptr<::openmldb::api::TaskInfo> task_info_;
    std::vector<std::shared_ptr<Task>> sub_task_;
    TaskFun fun_;
};

struct OPData {
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
    uint32_t tid;
    uint32_t pid;
    std::string des_endpoint;
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

class TableSyncTaskMeta : public TaskMeta {
  public:
    TableSyncTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, uint32_t tid_i, boost::function<bool()> fun_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kTableSyncTask, ""), tid(tid_i), fun(fun_i) {}
    uint32_t tid;
    boost::function<bool()> fun;
};

class DumpIndexDataTaskMeta : public TaskMeta {
  public:
    DumpIndexDataTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, uint32_t partition_num_i,
            const ::openmldb::common::ColumnKey& column_key_i, uint32_t idx_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kDumpIndexData, endpoint),
        tid(tid_i), pid(pid_i), partition_num(partition_num_i), column_key(column_key_i), idx(idx_i) {}
    uint32_t tid;
    uint32_t pid;
    uint32_t partition_num;
    ::openmldb::common::ColumnKey column_key;
    uint32_t idx;
};

class SendIndexDataTaskMeta : public TaskMeta {
  public:
    SendIndexDataTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, const std::map<uint32_t, std::string>& pid_endpoint_map_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kSendIndexData, endpoint),
        tid(tid_i), pid(pid_i), pid_endpoint_map(pid_endpoint_map_i) {}
    uint32_t tid;
    uint32_t pid;
    std::map<uint32_t, std::string> pid_endpoint_map;
};

class LoadIndexDataTaskMeta : public TaskMeta {
  public:
    LoadIndexDataTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type, const std::string& endpoint,
            uint32_t tid_i, uint32_t pid_i, uint32_t partition_num_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kLoadIndexData, endpoint),
        tid(tid_i), pid(pid_i), partition_num(partition_num_i) {}
    uint32_t tid;
    uint32_t pid;
    uint32_t partition_num;
};

class ExtractIndexDataTaskMeta : public TaskMeta {
  public:
    ExtractIndexDataTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            uint32_t tid_i, uint32_t pid_i, const std::vector<std::string>& endpoints_i,
            uint32_t partition_num_i, const ::openmldb::common::ColumnKey& column_key_i, uint32_t idx_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kExtractIndexData, ""),
        tid(tid_i), pid(pid_i), endpoints(endpoints_i), partition_num(partition_num_i), column_key(column_key_i), idx(idx_i) {}
    uint32_t tid;
    uint32_t pid;
    std::vector<std::string> endpoints;
    uint32_t partition_num;
    ::openmldb::common::ColumnKey column_key;
    uint32_t idx;
};

class AddIndexToTabletTaskMeta : public TaskMeta {
  public:
    AddIndexToTabletTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            uint32_t tid_i, uint32_t pid_i, const std::vector<std::string>& endpoints_i,
            const ::openmldb::common::ColumnKey& column_key_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kAddIndexToTablet, ""),
        tid(tid_i), pid(pid_i), endpoints(endpoints_i), column_key(column_key_i) {}
    uint32_t tid;
    uint32_t pid;
    std::vector<std::string> endpoints;
    ::openmldb::common::ColumnKey column_key;
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
        name(name_i), db(db_i), pid(pid_i), endpoint(endpoint_i), is_remote(true), alias(alias_i), remote_tid(remote_tid_i) {}
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
        name(name_i), db(db_i), endpoint(endpoint_i) {}
    DelTableInfoTaskMeta(uint64_t op_id, ::openmldb::api::OPType op_type,
            const std::string& name_i, const std::string& db_i,  uint32_t pid_i,
            const std::string& endpoint_i, uint32_t flag_i) :
        TaskMeta(op_id, op_type, ::openmldb::api::TaskType::kDelTableInfo, ""),
        name(name_i), db(db_i), endpoint(endpoint_i), has_flag(true), flag(flag_i) {}
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
        name(name_i), db(db_i), src_endpoint(src_endpoint_i), des_endpoint(des_endpoint_i) {}
    std::string name;
    std::string db;
    uint32_t pid;
    std::string src_endpoint;
    std::string des_endpoint;
};
}  // namespace nameserver
}  // namespace openmldb
#endif  // SRC_NAMESERVER_TASK_H_
