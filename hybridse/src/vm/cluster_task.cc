/**
 * Copyright (c) 2023 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/cluster_task.h"

namespace hybridse {
namespace vm {
const bool RouteInfo::IsCompleted() const { return table_handler_ && !index_.empty() && index_key_.ValidKey(); }
const bool RouteInfo::EqualWith(const RouteInfo& info1, const RouteInfo& info2) {
    return info1.input_ == info2.input_ && info1.table_handler_ == info2.table_handler_ &&
           info1.index_ == info2.index_ && node::ExprEquals(info1.index_key_.keys_, info2.index_key_.keys_);
}
const std::string RouteInfo::ToString() const {
    if (IsCompleted()) {
        std::ostringstream oss;
        if (lazy_route_) {
            oss << "[LAZY]";
        }
        oss << ", routing index = " << table_handler_->GetDatabase() << "." << table_handler_->GetName() << "."
            << index_ << ", " << index_key_.ToString();
        return oss.str();
    } else {
        return "";
    }
}
const bool RouteInfo::IsCluster() const { return table_handler_ && !index_.empty(); }
void ClusterTask::Print(std::ostream& output, const std::string& tab) const {
    output << route_info_.ToString() << "\n";
    if (nullptr == root_) {
        output << tab << "NULL RUNNER\n";
    } else {
        std::set<int32_t> visited_ids;
        root_->Print(output, tab, &visited_ids);
    }
}
void ClusterTask::ResetInputs(std::shared_ptr<ClusterTask> input) {
    for (auto input_runner : input_runners_) {
        input_runner->SetProducer(0, route_info_.input_->GetRoot());
    }
    route_info_.index_key_input_runner_ = route_info_.input_->GetRoot();
    route_info_.input_ = input;
}
Runner* ClusterTask::GetInputRunner(size_t idx) const {
    return idx >= input_runners_.size() ? nullptr : input_runners_[idx];
}
const bool ClusterTask::TaskCanBeMerge(const ClusterTask& task1, const ClusterTask& task2) {
    return RouteInfo::EqualWith(task1.route_info_, task2.route_info_);
}
const ClusterTask ClusterTask::TaskMerge(Runner* root, const ClusterTask& task1, const ClusterTask& task2) {
    return TaskMergeToLeft(root, task1, task2);
}
const ClusterTask ClusterTask::TaskMergeToLeft(Runner* root, const ClusterTask& task1, const ClusterTask& task2) {
    std::vector<Runner*> input_runners;
    for (auto runner : task1.input_runners_) {
        input_runners.push_back(runner);
    }
    for (auto runner : task2.input_runners_) {
        input_runners.push_back(runner);
    }
    return ClusterTask(root, input_runners, task1.route_info_);
}
const ClusterTask ClusterTask::TaskMergeToRight(Runner* root, const ClusterTask& task1, const ClusterTask& task2) {
    std::vector<Runner*> input_runners;
    for (auto runner : task1.input_runners_) {
        input_runners.push_back(runner);
    }
    for (auto runner : task2.input_runners_) {
        input_runners.push_back(runner);
    }
    return ClusterTask(root, input_runners, task2.route_info_);
}
const Runner* ClusterTask::GetRequestInput(const ClusterTask& task) {
    if (!task.IsValid()) {
        return nullptr;
    }
    auto input_task = task.GetInput();
    if (input_task) {
        return input_task->GetRoot();
    }
    return nullptr;
}
ClusterTask ClusterJob::GetTask(int32_t id) {
    if (id < 0 || id >= static_cast<int32_t>(tasks_.size())) {
        LOG(WARNING) << "fail get task: task " << id << " not exist";
        return ClusterTask();
    }
    return tasks_[id];
}
int32_t ClusterJob::AddTask(const ClusterTask& task) {
    if (!task.IsValid()) {
        LOG(WARNING) << "fail to add invalid task";
        return -1;
    }
    tasks_.push_back(task);
    return tasks_.size() - 1;
}
bool ClusterJob::AddRunnerToTask(Runner* runner, const int32_t id) {
    if (id < 0 || id >= static_cast<int32_t>(tasks_.size())) {
        LOG(WARNING) << "fail update task: task " << id << " not exist";
        return false;
    }
    runner->AddProducer(tasks_[id].GetRoot());
    tasks_[id].SetRoot(runner);
    return true;
}
void ClusterJob::Print(std::ostream& output, const std::string& tab) const {
    if (tasks_.empty()) {
        output << "EMPTY CLUSTER JOB\n";
        return;
    }
    for (size_t i = 0; i < tasks_.size(); i++) {
        if (main_task_id_ == static_cast<int32_t>(i)) {
            output << "MAIN TASK ID " << i;
        } else {
            output << "TASK ID " << i;
        }
        tasks_[i].Print(output, tab);
        output << "\n";
    }
}
void ClusterJob::Print() const { this->Print(std::cout, "    "); }
}  // namespace vm
}  // namespace hybridse
