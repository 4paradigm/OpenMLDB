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

}  // namespace nameserver
}  // namespace openmldb
#endif  // SRC_NAMESERVER_TASK_H_
