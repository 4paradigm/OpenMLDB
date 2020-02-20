/*
 * batch_planner.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_PLAN_BATCH_PLANNER_H_
#define SRC_PLAN_BATCH_PLANNER_H_

#include <string>
#include "analyser/analyser.h"
#include "base/status.h"
#include "glog/logging.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "parser/parser.h"
#include "proto/type.pb.h"

namespace fesql {
namespace plan {

class BatchPlanner {
 public:
    explicit BatchPlanner(node::NodeManager* mgr);
    ~BatchPlanner();

    int32_t CreateTree(const node::NodePointVector& parse_trees,
                       node::BatchPlanTree* batch_plan_tree,
                       base::Status& status);  // NOLINT
 private:
    node::NodeManager* mgr_;
};

}  // namespace plan
}  // namespace fesql
#endif  // SRC_PLAN_BATCH_PLANNER_H_
