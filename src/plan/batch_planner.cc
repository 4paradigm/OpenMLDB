/*
 * batch_planner.cc
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

#include "plan/batch_planner.h"

namespace fesql {
namespace plan {

BatchPlanner::BatchPlanner(node::NodeManager* mgr):mgr_(mgr) {}

BatchPlanner::~BatchPlanner() {}

// create select statement batch plan
// the following features are not supported currently
// 1. nested sql
// 2. left join
// 3. window
int32_t BatchPlanner::CreateTree(const node::NodePointVector& parse_trees,
        node::BatchPlanTree* batch_plan_tree, base::Status& status) { // NOLINT

    if (batch_plan_tree == NULL) {
        LOG(WARNING) << "batch plan tree is null pointer";
        return -1;
    }

    if (parse_trees.empty()) {
        status.msg = "fail to create plan tree: parser trees is empty";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return status.code;
    }

    auto sql_node = parse_trees.at(0);
    if (sql_node == NULL || sql_node->GetType() != node::kSelectStmt) {
        status.msg = "invalid statement , select statement is required";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return status.code;
    }

    const node::SelectStmt *stmt = reinterpret_cast<const node::SelectStmt*>(sql_node);
    const node::NodePointVector& tables = stmt->GetTableRefList();
    const node::TableNode* tn = reinterpret_cast<const node::TableNode*>(tables.at(0));
    node::DatasetNode* db = mgr_->MakeDataset(tn->GetOrgTableName());
    node::MapNode* mn = mgr_->MakeMapNode(stmt->GetSelectList());
    db->AddChild(mn);
    batch_plan_tree->SetRoot(db);
    return 0;
}

}  // namespace plan
}  // namespace fesql



