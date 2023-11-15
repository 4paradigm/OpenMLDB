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

#ifndef HYBRIDSE_SRC_VM_CLUSTER_TASK_H_
#define HYBRIDSE_SRC_VM_CLUSTER_TASK_H_

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "vm/catalog.h"
#include "vm/physical_op.h"
#include "vm/runner.h"

namespace hybridse {
namespace vm {

class ClusterTask;

class RouteInfo {
 public:
    RouteInfo()
        : index_(),
          index_key_(),
          index_key_input_runner_(nullptr),
          input_(),
          table_handler_() {}
    RouteInfo(const std::string index,
              std::shared_ptr<TableHandler> table_handler)
        : index_(index),
          index_key_(),
          index_key_input_runner_(nullptr),
          input_(),
          table_handler_(table_handler) {}
    RouteInfo(const std::string index, const Key& index_key,
              std::shared_ptr<ClusterTask> input,
              std::shared_ptr<TableHandler> table_handler)
        : index_(index),
          index_key_(index_key),
          index_key_input_runner_(nullptr),
          input_(input),
          table_handler_(table_handler) {}
    ~RouteInfo() {}
    const bool IsCompleted() const;
    const bool IsCluster() const;
    static const bool EqualWith(const RouteInfo& info1, const RouteInfo& info2);

    const std::string ToString() const;
    std::string index_;
    Key index_key_;
    Runner* index_key_input_runner_;
    std::shared_ptr<ClusterTask> input_;
    std::shared_ptr<TableHandler> table_handler_;

    // if true: generate the complete ClusterTask only when requires
    bool lazy_route_ = false;
};

// task info of cluster job
// partitoin/index info
// index key generator
// request generator
class ClusterTask {
 public:
    // common tasks
    ClusterTask() : root_(nullptr), input_runners_(), route_info_() {}
    explicit ClusterTask(Runner* root)
        : root_(root), input_runners_(), route_info_() {}

    // cluster task with explicit routeinfo
    ClusterTask(Runner* root, const std::shared_ptr<TableHandler> table_handler,
                std::string index)
        : root_(root), input_runners_(), route_info_(index, table_handler) {}
    ClusterTask(Runner* root, const std::vector<Runner*>& input_runners,
                const RouteInfo& route_info)
        : root_(root), input_runners_(input_runners), route_info_(route_info) {}
    ~ClusterTask() {}

    void Print(std::ostream& output, const std::string& tab) const;

    friend std::ostream& operator<<(std::ostream& os, const ClusterTask& output) {
        output.Print(os, "");
        return os;
    }

    void ResetInputs(std::shared_ptr<ClusterTask> input);
    Runner* GetRoot() const { return root_; }
    void SetRoot(Runner* root) { root_ = root; }
    Runner* GetInputRunner(size_t idx) const;
    Runner* GetIndexKeyInput() const {
        return route_info_.index_key_input_runner_;
    }
    std::shared_ptr<ClusterTask> GetInput() const { return route_info_.input_; }
    Key GetIndexKey() const { return route_info_.index_key_; }
    void SetIndexKey(const Key& key) { route_info_.index_key_ = key; }
    void SetInput(std::shared_ptr<ClusterTask> input) {
        route_info_.input_ = input;
    }

    const bool IsValid() const { return nullptr != root_; }

    const bool IsCompletedClusterTask() const {
        return IsValid() && route_info_.IsCompleted();
    }
    const bool IsUnCompletedClusterTask() const {
        return IsClusterTask() && !route_info_.IsCompleted();
    }
    const bool IsClusterTask() const { return route_info_.IsCluster(); }
    const std::string& index() { return route_info_.index_; }
    std::shared_ptr<TableHandler> table_handler() {
        return route_info_.table_handler_;
    }

    // Cluster tasks with same input runners and index keys can be merged
    static const bool TaskCanBeMerge(const ClusterTask& task1, const ClusterTask& task2);
    static const ClusterTask TaskMerge(Runner* root, const ClusterTask& task1, const ClusterTask& task2);
    static const ClusterTask TaskMergeToLeft(Runner* root, const ClusterTask& task1, const ClusterTask& task2);
    static const ClusterTask TaskMergeToRight(Runner* root, const ClusterTask& task1, const ClusterTask& task2);
    static const Runner* GetRequestInput(const ClusterTask& task);

    const RouteInfo& GetRouteInfo() const { return route_info_; }

 protected:
    Runner* root_;
    std::vector<Runner*> input_runners_;
    RouteInfo route_info_;
};

class ClusterJob {
 public:
    ClusterJob()
        : tasks_(), main_task_id_(-1), sql_(""), common_column_indices_() {}
    explicit ClusterJob(const std::string& sql, const std::string& db,
                        const std::set<size_t>& common_column_indices)
        : tasks_(),
          main_task_id_(-1),
          sql_(sql),
          db_(db),
          common_column_indices_(common_column_indices) {}
    ClusterTask GetTask(int32_t id);

    ClusterTask GetMainTask() { return GetTask(main_task_id_); }
    int32_t AddTask(const ClusterTask& task);
    bool AddRunnerToTask(Runner* runner, const int32_t id);

    void AddMainTask(const ClusterTask& task) { main_task_id_ = AddTask(task); }
    void Reset() { tasks_.clear(); }
    const size_t GetTaskSize() const { return tasks_.size(); }
    const bool IsValid() const { return !tasks_.empty(); }
    const int32_t main_task_id() const { return main_task_id_; }
    const std::string& sql() const { return sql_; }
    const std::string& db() const { return db_; }
    const std::set<size_t>& common_column_indices() const { return common_column_indices_; }
    void Print(std::ostream& output, const std::string& tab) const;
    void Print() const;

 private:
    std::vector<ClusterTask> tasks_;
    int32_t main_task_id_;
    std::string sql_;
    std::string db_;
    std::set<size_t> common_column_indices_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_CLUSTER_TASK_H_
