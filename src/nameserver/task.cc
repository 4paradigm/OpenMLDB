/*
 * Copyright 2022 4Paradigm
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

#include "nameserver/task.h"

#include "absl/strings/str_cat.h"
#include "base/glog_wrapper.h"

namespace openmldb {
namespace nameserver {

bool Task::IsFinished() const {
    return task_info_->status() == ::openmldb::api::kDone ||
        task_info_->status() == ::openmldb::api::kFailed || task_info_->status() == ::openmldb::api::kCanceled;
}

void Task::UpdateTaskStatus(const ::google::protobuf::RepeatedPtrField<::openmldb::api::TaskInfo>& tasks,
        const std::string& endpoint, bool is_recover) {
    if (IsFinished()) {
        return;
    }
    for (const auto& cur_task : tasks) {
        UpdateStatus(cur_task, endpoint);
    }
    SetState(endpoint, is_recover);
    check_num_++;
}

bool Task::UpdateStatus(const ::openmldb::api::TaskInfo& task, const std::string& endpoint) {
    if (GetOpId() != task.op_id()) {
        return false;
    }
    if (IsFinished()) {
        return false;
    }
    if (!seq_task_.empty()) {
        seq_task_.front()->UpdateStatus(task, endpoint);
    } else if (!sub_task_.empty()) {
        for (auto& cur_task : sub_task_) {
            cur_task->UpdateStatus(task, endpoint);
        }
    } else {
        if (task_info_->task_type() != task.task_type()) {
            return false;
        }
        if (task_info_->has_endpoint() && task_info_->endpoint() != endpoint) {
            return false;
        }
        if (task_info_->has_tid() && (!task.has_tid() || task.tid() != task_info_->tid())) {
            return false;
        }
        if (task_info_->has_pid() && (!task.has_pid() || task.pid() != task_info_->pid())) {
            return false;
        }
        if (task.status() != ::openmldb::api::kInited) {
            if (task_info_->status() != task.status()) {
                PDLOG(INFO, "update task status from %s to %s. op_id %lu task_type %s %s",
                        GetReadableStatus().c_str(), GetReadableStatus(task).c_str(),
                        task.op_id(), GetReadableType().c_str(), GetAdditionalMsg().c_str());
                task_info_->set_status(task.status());
            }
        }
        traversed_ = true;
        return true;
    }
    return false;
}

void Task::SetState(const std::string& endpoint, bool is_recover) {
    if (IsFinished()) {
        return;
    }
    if (!seq_task_.empty()) {
        seq_task_.front()->SetState(endpoint, is_recover);
        return;
    }
    if (sub_task_.empty()) {
        if (endpoint_ == endpoint && !traversed_ && (is_recover || (task_info_->is_rpc_send() && check_num_ > 5))) {
            // if the task does not exist in tablet, set to failed
            task_info_->set_status(::openmldb::api::kFailed);
        }
        traversed_ = false;
        return;
    }
    uint32_t done_cnt = 0;
    for (auto& cur_task : sub_task_) {
        cur_task->SetState(endpoint, is_recover);
        if (cur_task->GetStatus() == ::openmldb::api::kDone) {
            done_cnt++;
        } else if (cur_task->GetStatus() == ::openmldb::api::kFailed) {
            PDLOG(INFO, "update task status from %s to kFailed. op_id %lu task_type %s %s",
                    GetReadableStatus().c_str(), GetOpId(), GetReadableType().c_str(), GetAdditionalMsg().c_str());
            SetStatus(::openmldb::api::kFailed);
            break;
        } else if (cur_task->GetStatus() == ::openmldb::api::kCanceled) {
            PDLOG(INFO, "update task status from %s to kCanceled. op_id %lu task_type %s %s",
                    GetReadableStatus().c_str(), GetOpId(), GetReadableType().c_str(), GetAdditionalMsg().c_str());
            SetStatus(::openmldb::api::kCanceled);
            break;
        }
    }
    if (done_cnt == sub_task_.size()) {
        PDLOG(INFO, "update task status from %s to kDone. op_id %lu task_type %s %s",
                GetReadableStatus().c_str(), GetOpId(), GetReadableType().c_str(), GetAdditionalMsg().c_str());
        SetStatus(::openmldb::api::kDone);
    }
}

void Task::UpdateStatusFromSubTask() {
    if (IsFinished()) {
        return;
    }
    if (!seq_task_.empty()) {
        return seq_task_.front()->UpdateStatusFromSubTask();
    }
    if (sub_task_.empty()) {
        return;
    }
    uint32_t done_cnt = 0;
    for (auto& cur_task : sub_task_) {
        cur_task->UpdateStatusFromSubTask();
        if (cur_task->GetStatus() == ::openmldb::api::kDone) {
            done_cnt++;
        } else if (cur_task->GetStatus() == ::openmldb::api::kFailed) {
            PDLOG(INFO, "update task status from %s to kFailed. op_id %lu task_type %s %s",
                    GetReadableStatus().c_str(), GetOpId(), GetReadableType().c_str(), GetAdditionalMsg().c_str());
            SetStatus(::openmldb::api::kFailed);
            return;
        } else if (cur_task->GetStatus() == ::openmldb::api::kCanceled) {
            PDLOG(INFO, "update task status from %s to kCanceled. op_id %lu task_type %s %s",
                    GetReadableStatus().c_str(), GetOpId(), GetReadableType().c_str(), GetAdditionalMsg().c_str());
            SetStatus(::openmldb::api::kCanceled);
            return;
        }
    }
    if (done_cnt == sub_task_.size()) {
        PDLOG(INFO, "update task status from %s to kDone. op_id %lu task_type %s %s",
                GetReadableStatus().c_str(), GetOpId(), GetReadableType().c_str(), GetAdditionalMsg().c_str());
        SetStatus(::openmldb::api::kDone);
    }
}

std::string Task::GetAdditionalMsg(const ::openmldb::api::TaskInfo& task_info) {
    std::string additional_msg;
    if (task_info.has_tid()) {
        absl::StrAppend(&additional_msg, "tid ", task_info.tid());
    }
    if (task_info.has_pid()) {
        absl::StrAppend(&additional_msg, " pid ", task_info.pid());
    }
    return additional_msg;
}

std::string Task::GetAdditionalMsg() {
    return GetAdditionalMsg(*task_info_);
}

}  // namespace nameserver
}  // namespace openmldb
