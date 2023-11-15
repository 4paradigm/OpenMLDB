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

#ifndef HYBRIDSE_SRC_VM_RUNNER_BUILDER_H_
#define HYBRIDSE_SRC_VM_RUNNER_BUILDER_H_

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "node/node_manager.h"
#include "vm/cluster_task.h"
#include "vm/runner.h"

namespace hybridse {
namespace vm {

class RunnerBuilder {
    enum TaskBiasType { kLeftBias, kRightBias, kNoBias };

 public:
    explicit RunnerBuilder(node::NodeManager* nm, const std::string& sql, const std::string& db,
                           bool support_cluster_optimized, const std::set<size_t>& common_column_indices,
                           const std::set<size_t>& batch_common_node_set)
        : nm_(nm),
          support_cluster_optimized_(support_cluster_optimized),
          id_(0),
          cluster_job_(sql, db, common_column_indices),
          task_map_(),
          proxy_runner_map_(),
          batch_common_node_set_(batch_common_node_set) {}
    virtual ~RunnerBuilder() {}
    ClusterTask RegisterTask(PhysicalOpNode* node, ClusterTask task);
    ClusterTask Build(PhysicalOpNode* node,                            // NOLINT
                      Status& status);                                 // NOLINT
    ClusterJob BuildClusterJob(PhysicalOpNode* node, Status& status);  // NOLINT

    template <typename Op, typename... Args>
    Op* CreateRunner(Args&&... args) {
        return nm_->MakeNode<Op>(std::forward<Args>(args)...);
    }

 private:
    ClusterTask MultipleInherit(const std::vector<const ClusterTask*>& children, Runner* runner, const Key& index_key,
                                const TaskBiasType bias);
    ClusterTask BinaryInherit(const ClusterTask& left, const ClusterTask& right, Runner* runner, const Key& index_key,
                              const TaskBiasType bias = kNoBias);
    ClusterTask BuildLocalTaskForBinaryRunner(const ClusterTask& left, const ClusterTask& right, Runner* runner);
    ClusterTask BuildClusterTaskForBinaryRunner(const ClusterTask& left, const ClusterTask& right, Runner* runner,
                                                const Key& index_key, const TaskBiasType bias);
    ClusterTask BuildProxyRunnerForClusterTask(const ClusterTask& task);
    ClusterTask InvalidTask() { return ClusterTask(); }
    ClusterTask CommonTask(Runner* runner) { return ClusterTask(runner); }
    ClusterTask UnCompletedClusterTask(Runner* runner, const std::shared_ptr<TableHandler> table_handler,
                                       std::string index);
    ClusterTask BuildRequestTask(RequestRunner* runner);
    ClusterTask UnaryInheritTask(const ClusterTask& input, Runner* runner);
    ClusterTask BuildRequestAggUnionTask(PhysicalOpNode* node, Status& status);  // NOLINT

 private:
    node::NodeManager* nm_;
    // only set for request mode
    bool support_cluster_optimized_;
    int32_t id_;
    ClusterJob cluster_job_;

    std::unordered_map<::hybridse::vm::PhysicalOpNode*, ::hybridse::vm::ClusterTask> task_map_;
    std::shared_ptr<ClusterTask> request_task_;
    std::unordered_map<hybridse::vm::Runner*, ::hybridse::vm::Runner*> proxy_runner_map_;
    std::set<size_t> batch_common_node_set_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_RUNNER_BUILDER_H_
