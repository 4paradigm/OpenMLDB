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

#include "plan/planner.h"
#include <map>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "parser/parser.h"
#include "plan/plan_api.h"
#include "proto/fe_common.pb.h"

namespace hybridse {
namespace plan {

bool Planner::CreateQueryPlan(const node::QueryNode *root, PlanNode **plan_tree,
                              Status &status) {
    if (nullptr == root) {
        status.msg = "can not create query plan node with null query node";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    switch (root->query_type_) {
        case node::kQuerySelect:
            if (!CreateSelectQueryPlan(
                    dynamic_cast<const node::SelectQueryNode *>(root),
                    plan_tree, status)) {
                return false;
            }
            break;
        case node::kQueryUnion:
            if (!CreateUnionQueryPlan(
                    dynamic_cast<const node::UnionQueryNode *>(root), plan_tree,
                    status)) {
                return false;
            }
            break;
        default: {
            status.msg =
                "can not create query plan node with invalid query type " +
                node::QueryTypeName(root->query_type_);
            status.code = common::kPlanError;
            return false;
        }
    }
    return true;
}
bool Planner::CreateSelectQueryPlan(const node::SelectQueryNode *root,
                                    PlanNode **plan_tree, Status &status) {
    const node::NodePointVector &table_ref_list =
        nullptr == root->GetTableRefList() ? std::vector<SqlNode *>()
                                           : root->GetTableRefList()->GetList();
    std::vector<node::PlanNode *> relation_nodes;
    for (node::SqlNode *node : table_ref_list) {
        node::PlanNode *table_ref_plan = nullptr;
        if (nullptr == node) {
            status.msg =
                "can not create select plan node: table reference node is "
                "null";
            status.code = common::kPlanError;
            return false;
        }
        if (node::kTableRef != node->GetType()) {
            status.msg =
                "can not create select plan node: table reference node "
                "type is "
                "invalid" +
                node::NameOfSqlNodeType(node->GetType());
            status.code = common::kSqlError;
            return false;
        }
        if (!CreateTableReferencePlanNode(
                dynamic_cast<node::TableRefNode *>(node), &table_ref_plan,
                status)) {
            return false;
        }
        relation_nodes.push_back(table_ref_plan);
    }

    std::string table_name = "";
    node::PlanNode *current_node = nullptr;
    // from tables
    if (!relation_nodes.empty()) {
        auto iter = relation_nodes.cbegin();
        current_node = *iter;
        iter++;
        // cross product if there are multi tables
        for (; iter != relation_nodes.cend(); iter++) {
            current_node = node_manager_->MakeJoinNode(
                current_node, *iter, node::JoinType::kJoinTypeFull, nullptr,
                nullptr);
        }

        // TODO(chenjing): 处理子查询
        table_name = MakeTableName(current_node);
    }
    // where condition
    if (nullptr != root->where_clause_ptr_) {
        current_node = node_manager_->MakeFilterPlanNode(
            current_node, root->where_clause_ptr_);
    }

    // group by
    bool group_by_agg = false;
    if (nullptr != root->group_clause_ptr_) {
        current_node = node_manager_->MakeGroupPlanNode(
            current_node, root->group_clause_ptr_);
        group_by_agg = true;
    }

    // select target_list
    if (nullptr == root->GetSelectList() ||
        root->GetSelectList()->GetList().empty()) {
        status.msg =
            "fail to create select query plan: select expr list is null or "
            "empty";
        status.code = common::kPlanError;
        return false;
    }
    const node::NodePointVector &select_expr_list =
        root->GetSelectList()->GetList();

    // prepare window list
    std::map<const node::WindowDefNode *, node::ProjectListNode *>
        project_list_map;
    // prepare window def
    int w_id = 1;
    std::map<std::string, const node::WindowDefNode *> windows;
    if (nullptr != root->GetWindowList() && !root->GetWindowList()->IsEmpty()) {
        for (auto node : root->GetWindowList()->GetList()) {
            const node::WindowDefNode *w =
                dynamic_cast<node::WindowDefNode *>(node);
            if (windows.find(w->GetName()) != windows.cend()) {
                status.msg = "fail to resolve window, window name duplicate: " +
                             w->GetName();
                status.code = common::kPlanError;
                return false;
            }
            if (!CheckWindowFrame(w, status)) {
                return false;
            }
            windows[w->GetName()] = w;
        }
    }

    for (uint32_t pos = 0u; pos < select_expr_list.size(); pos++) {
        auto expr = select_expr_list[pos];
        std::string project_name;
        node::ExprNode *project_expr;
        switch (expr->GetType()) {
            case node::kResTarget: {
                const node::ResTarget *target_ptr =
                    (const node::ResTarget *)expr;
                project_name = target_ptr->GetName();
                if (project_name.empty()) {
                    project_name =
                        target_ptr->GetVal()->GenerateExpressionName();
                }
                project_expr = target_ptr->GetVal();
                break;
            }
            default: {
                status.msg = "can not create project plan node with type " +
                             node::NameOfSqlNodeType(root->GetType());
                status.code = common::kPlanError;
                LOG(WARNING) << status;
                return false;
            }
        }

        const node::WindowDefNode *w_ptr = nullptr;
        if (!node::WindowOfExpression(windows, project_expr, &w_ptr)) {
            status.msg = "fail to resolved window";
            status.code = common::kPlanError;
            return false;
        }

        if (project_list_map.find(w_ptr) == project_list_map.end()) {
            if (w_ptr == nullptr) {
                project_list_map[w_ptr] =
                    node_manager_->MakeProjectListPlanNode(nullptr,
                                                           group_by_agg);

            } else {
                node::WindowPlanNode *w_node_ptr =
                    node_manager_->MakeWindowPlanNode(w_id++);
                if (!CreateWindowPlanNode(w_ptr, w_node_ptr, status)) {
                    return false;
                }
                project_list_map[w_ptr] =
                    node_manager_->MakeProjectListPlanNode(w_node_ptr, true);
            }
        }
        node::ProjectNode *project_node_ptr =
            nullptr == w_ptr
                ? node_manager_->MakeRowProjectNode(pos, project_name,
                                                    project_expr)
                : node_manager_->MakeAggProjectNode(
                      pos, project_name, project_expr, w_ptr->GetFrame());

        project_list_map[w_ptr]->AddProject(project_node_ptr);
    }

    // merge window map
    std::map<const node::WindowDefNode *, node::ProjectListNode *>
        merged_project_list_map;
    if (!MergeProjectMap(project_list_map, &merged_project_list_map, status)) {
        LOG(WARNING) << "Fail to merge window project";
        return false;
    }
    // add MergeNode if multi ProjectionLists exist
    PlanNodeList project_list_vec(w_id);
    for (auto &v : merged_project_list_map) {
        node::ProjectListNode *project_list = v.second;
        int pos =
            nullptr == project_list->GetW() ? 0 : project_list->GetW()->GetId();
        project_list_vec[pos] = project_list;
    }

    // merge simple project with 1st window project
    if (nullptr != project_list_vec[0] && project_list_vec.size() > 1) {
        auto simple_project =
            dynamic_cast<node::ProjectListNode *>(project_list_vec[0]);
        auto first_window_project =
            dynamic_cast<node::ProjectListNode *>(project_list_vec[1]);
        node::ProjectListNode *merged_project =
            node_manager_->MakeProjectListPlanNode(first_window_project->GetW(),
                                                   true);
        if (!is_cluster_optimized_ && !enable_batch_window_parallelization_ &&
            node::ProjectListNode::MergeProjectList(
                simple_project, first_window_project, merged_project)) {
            project_list_vec[0] = nullptr;
            project_list_vec[1] = merged_project;
        }
    }

    PlanNodeList project_list_without_null;
    std::vector<std::pair<uint32_t, uint32_t>> pos_mapping(
        select_expr_list.size());
    int project_list_id = 0;
    for (auto &v : project_list_vec) {
        if (nullptr == v) {
            continue;
        }
        auto project_list =
            dynamic_cast<node::ProjectListNode *>(v)->GetProjects();
        int project_pos = 0;
        for (auto project : project_list) {
            pos_mapping[dynamic_cast<node::ProjectNode *>(project)->GetPos()] =
                std::make_pair(project_list_id, project_pos);
            project_pos++;
        }
        project_list_without_null.push_back(v);
        project_list_id++;
    }

    current_node = node_manager_->MakeProjectPlanNode(
        current_node, table_name, project_list_without_null, pos_mapping);

    // distinct
    if (root->distinct_opt_) {
        current_node = node_manager_->MakeDistinctPlanNode(current_node);
    }
    // having
    if (nullptr != root->having_clause_ptr_) {
        current_node = node_manager_->MakeFilterPlanNode(
            current_node, root->having_clause_ptr_);
    }
    // order
    if (nullptr != root->order_clause_ptr_) {
        current_node = node_manager_->MakeSortPlanNode(current_node,
                                                       root->order_clause_ptr_);
    }
    // limit
    if (nullptr != root->GetLimit()) {
        const node::LimitNode *limit_ptr = (node::LimitNode *)root->GetLimit();
        current_node = node_manager_->MakeLimitPlanNode(
            current_node, limit_ptr->GetLimitCount());
    }
    current_node = node_manager_->MakeSelectPlanNode(current_node);
    *plan_tree = current_node;
    return true;
}

bool Planner::CreateUnionQueryPlan(const node::UnionQueryNode *root,
                                   PlanNode **plan_tree, Status &status) {
    if (nullptr == root) {
        status.msg = "can not create query plan node with null query node";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    node::PlanNode *left_plan = nullptr;
    node::PlanNode *right_plan = nullptr;
    if (!CreateQueryPlan(root->left_, &left_plan, status)) {
        status.msg =
            "can not create union query plan left query: " + status.str();
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    if (!CreateQueryPlan(root->right_, &right_plan, status)) {
        status.msg =
            "can not create union query plan right query: " + status.str();
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    *plan_tree =
        node_manager_->MakeUnionPlanNode(left_plan, right_plan, root->is_all_);
    return true;
}
bool Planner::CheckWindowFrame(const node::WindowDefNode *w_ptr,
                               base::Status &status) {
    if (nullptr == w_ptr->GetFrame()) {
        status.code = common::kPlanError;
        status.msg =
            "fail to create project list node: frame "
            "can't be unbound ";
        LOG(WARNING) << status;
        return false;
    }

    if (w_ptr->GetFrame()->frame_type() == node::kFrameRows) {
        auto extent =
            dynamic_cast<node::FrameExtent *>(w_ptr->GetFrame()->frame_rows());
        if ((extent->start()->bound_type() == node::kPreceding ||
             extent->start()->bound_type() == node::kFollowing) &&
            extent->start()->is_time_offset()) {
            status.code = common::kPlanError;
            status.msg = "Fail Make Rows Frame Node: time offset un-support";
            LOG(WARNING) << status;
            return false;
        }
        if ((extent->end()->bound_type() == node::kPreceding ||
             extent->end()->bound_type() == node::kFollowing) &&
            extent->end()->is_time_offset()) {
            status.code = common::kPlanError;
            status.msg = "Fail Make Rows Frame Node: time offset un-support";
            LOG(WARNING) << status;
            return false;
        }

        if (w_ptr->GetFrame()->frame_maxsize() > 0) {
            status.code = common::kPlanError;
            status.msg =
                "Fail Make Rows Window: MAXSIZE non-support for Rows Window";
            LOG(WARNING) << status;
            return false;
        }
    }
    return true;
}
bool Planner::CreateWindowPlanNode(
    const node::WindowDefNode *w_ptr, node::WindowPlanNode *w_node_ptr,
    Status &status) {  // NOLINT (runtime/references)

    if (nullptr != w_ptr) {
        // Prepare Window Frame
        if (!CheckWindowFrame(w_ptr, status)) {
            return false;
        }
        node::FrameNode *frame =
            dynamic_cast<node::FrameNode *>(w_ptr->GetFrame());
        w_node_ptr->set_frame_node(frame);

        // Prepare Window Name
        if (w_ptr->GetName().empty()) {
            w_node_ptr->SetName(
                PlanAPI::GenerateName("anonymous_w", w_node_ptr->GetId()));
        } else {
            w_node_ptr->SetName(w_ptr->GetName());
        }

        // Prepare Window partitions and orders
        w_node_ptr->SetKeys(w_ptr->GetPartitions());
        w_node_ptr->SetOrders(w_ptr->GetOrders());

        // Prepare Window Union Info
        if (nullptr != w_ptr->union_tables() &&
            !w_ptr->union_tables()->GetList().empty()) {
            for (auto node : w_ptr->union_tables()->GetList()) {
                node::PlanNode *table_plan = nullptr;
                if (!CreateTableReferencePlanNode(
                        dynamic_cast<node::TableRefNode *>(node), &table_plan,
                        status)) {
                    return false;
                }
                w_node_ptr->AddUnionTable(table_plan);
            }
        }
        w_node_ptr->set_instance_not_in_window(w_ptr->instance_not_in_window());
        w_node_ptr->set_exclude_current_time(w_ptr->exclude_current_time());
    }
    return true;
}

bool Planner::CreateCreateTablePlan(
    const node::SqlNode *root, node::PlanNode **output,
    Status &status) {  // NOLINT (runtime/references)
    const node::CreateStmt *create_tree = (const node::CreateStmt *)root;
    *output = node_manager_->MakeCreateTablePlanNode(
        create_tree->GetTableName(), create_tree->GetReplicaNum(),
        create_tree->GetPartitionNum(), create_tree->GetColumnDefList(),
        create_tree->GetDistributionList());
    return true;
}

bool Planner::IsTable(node::PlanNode *node) {
    if (nullptr == node) {
        return false;
    }

    switch (node->type_) {
        case node::kPlanTypeTable: {
            return true;
        }
        case node::kPlanTypeRename: {
            return IsTable(node->GetChildren()[0]);
        }
        case node::kPlanTypeQuery: {
            return IsTable(
                dynamic_cast<node::QueryPlanNode *>(node)->GetChildren()[0]);
        }
        case node::kPlanTypeProject: {
            if ((dynamic_cast<node::ProjectPlanNode *>(node))
                    ->IsSimpleProjectPlan()) {
                return IsTable(node->GetChildren()[0]);
            }
        }
        default: {
            return false;
        }
    }
    return false;
}
bool Planner::ValidatePrimaryPath(node::PlanNode *node, node::PlanNode **output,
                                  base::Status &status) {
    if (nullptr == node) {
        status.msg = "primary path validate fail: node or output is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    switch (node->type_) {
        case node::kPlanTypeJoin:
        case node::kPlanTypeUnion: {
            auto binary_op = dynamic_cast<node::BinaryPlanNode *>(node);
            node::PlanNode *left_primary_table = nullptr;
            if (!ValidatePrimaryPath(binary_op->GetLeft(), &left_primary_table,
                                     status)) {
                status.msg =
                    "primary path validate fail: left path isn't valid";
                status.code = common::kPlanError;
                LOG(WARNING) << status;
                return false;
            }

            if (IsTable(binary_op->GetRight())) {
                *output = left_primary_table;
                return true;
            }

            node::PlanNode *right_primary_table = nullptr;
            if (!ValidatePrimaryPath(binary_op->GetRight(),
                                     &right_primary_table, status)) {
                status.msg =
                    "primary path validate fail: right path isn't valid";
                status.code = common::kPlanError;
                LOG(WARNING) << status;
                return false;
            }

            if (node::PlanEquals(left_primary_table, right_primary_table)) {
                *output = left_primary_table;
                return true;
            } else {
                status.msg =
                    "primary path validate fail: left path and right path has "
                    "different source";
                status.code = common::kPlanError;
                LOG(WARNING) << status;
                return false;
            }
        }
        case node::kPlanTypeTable: {
            *output = node;
            return true;
        }
        case node::kPlanTypeCreate:
        case node::kPlanTypeInsert:
        case node::kPlanTypeCmd:
        case node::kPlanTypeWindow:
        case node::kProjectList:
        case node::kProjectNode: {
            status.msg =
                "primary path validate fail: invalid node of primary path";
            status.code = common::kPlanError;
            LOG(WARNING) << status;
            return false;
        }
        default: {
            auto unary_op = dynamic_cast<const node::UnaryPlanNode *>(node);
            return ValidatePrimaryPath(unary_op->GetDepend(), output, status);
        }
    }
}

int SimplePlanner::CreatePlanTree(
    const NodePointVector &parser_trees, PlanNodeList &plan_trees,
    Status &status) {  // NOLINT (runtime/references)
    if (parser_trees.empty()) {
        status.msg = "fail to create plan tree: parser trees is empty";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return status.code;
    }

    for (auto parser_tree : parser_trees) {
        switch (parser_tree->GetType()) {
            case node::kQuery: {
                PlanNode *query_plan = nullptr;
                if (!CreateQueryPlan(
                        dynamic_cast<node::QueryNode *>(parser_tree),
                        &query_plan, status)) {
                    return status.code;
                }

                if (!is_batch_mode_) {
                    // return false if Primary path check fail
                    ::hybridse::node::PlanNode *primary_node;
                    if (!ValidatePrimaryPath(query_plan, &primary_node,
                                             status)) {
                        DLOG(INFO) << "primay check fail, logical plan:\n"
                                   << *query_plan;
                        return status.code;
                    }
                    dynamic_cast<node::TablePlanNode *>(primary_node)
                        ->SetIsPrimary(true);
                    DLOG(INFO) << "plan after primary check:\n" << *query_plan;
                }

                plan_trees.push_back(query_plan);
                break;
            }
            case node::kCreateStmt: {
                PlanNode *create_plan = nullptr;
                if (!CreateCreateTablePlan(parser_tree, &create_plan, status)) {
                    return status.code;
                }
                plan_trees.push_back(create_plan);
                break;
            }
            case node::kCreateSpStmt: {
                PlanNode *create_sp_plan = nullptr;
                PlanNodeList inner_plan_node_list;
                const node::CreateSpStmt *create_sp_tree =
                    (const node::CreateSpStmt *)parser_tree;
                if (CreatePlanTree(create_sp_tree->GetInnerNodeList(),
                                   inner_plan_node_list,
                                   status) != common::StatusCode::kOk) {
                    return status.code;
                }
                if (!CreateCreateProcedurePlan(parser_tree,
                                               inner_plan_node_list,
                                               &create_sp_plan, status)) {
                    return status.code;
                }
                plan_trees.push_back(create_sp_plan);
                break;
            }
            case node::kCmdStmt: {
                node::PlanNode *cmd_plan = nullptr;
                if (!CreateCmdPlan(parser_tree, &cmd_plan, status)) {
                    return status.code;
                }
                plan_trees.push_back(cmd_plan);
                break;
            }
            case node::kInsertStmt: {
                node::PlanNode *insert_plan = nullptr;
                if (!CreateInsertPlan(parser_tree, &insert_plan, status)) {
                    return status.code;
                }
                plan_trees.push_back(insert_plan);
                break;
            }
            case ::hybridse::node::kFnDef: {
                node::PlanNode *fn_plan = nullptr;
                if (!CreateFuncDefPlan(parser_tree, &fn_plan, status)) {
                    return status.code;
                }
                plan_trees.push_back(fn_plan);
                break;
            }
            default: {
                status.msg = "can not handle tree type " +
                             node::NameOfSqlNodeType(parser_tree->GetType());
                status.code = common::kPlanError;
                LOG(WARNING) << status;
                return status.code;
            }
        }
    }

    return status.code;
}

/***
 * Create function def plan node
 * 1. check indent
 * 2. construct sub blocks
 *      if_then_else block
 *
 * @param root
 * @param plan
 * @param status
 */
bool Planner::CreateFuncDefPlan(
    const SqlNode *root, node::PlanNode **output,
    Status &status) {  // NOLINT (runtime/references)
    if (nullptr == root) {
        status.msg =
            "fail to create func def plan node: query tree node it null";
        status.code = common::kSqlError;
        LOG(WARNING) << status;
        return false;
    }

    if (root->GetType() != node::kFnDef) {
        status.code = common::kSqlError;
        status.msg =
            "fail to create cmd plan node: query tree node it not function "
            "def "
            "type";
        LOG(WARNING) << status;
        return false;
    }
    *output = node_manager_->MakeFuncPlanNode(
        dynamic_cast<node::FnNodeFnDef *>(const_cast<SqlNode *>(root)));
    return true;
}

bool Planner::CreateInsertPlan(const node::SqlNode *root,
                               node::PlanNode **output,
                               Status &status) {  // NOLINT (runtime/references)
    if (nullptr == root) {
        status.msg = "fail to create cmd plan node: query tree node it null";
        status.code = common::kSqlError;
        LOG(WARNING) << status;
        return false;
    }

    if (root->GetType() != node::kInsertStmt) {
        status.msg =
            "fail to create cmd plan node: query tree node it not insert "
            "type";
        status.code = common::kSqlError;
        return false;
    }
    *output = node_manager_->MakeInsertPlanNode(
        dynamic_cast<const node::InsertStmt *>(root));
    return true;
}

bool Planner::CreateCmdPlan(const SqlNode *root, node::PlanNode **output,
                            Status &status) {  // NOLINT (runtime/references)
    if (nullptr == root) {
        status.msg = "fail to create cmd plan node: query tree node it null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    if (root->GetType() != node::kCmdStmt) {
        status.msg =
            "fail to create cmd plan node: query tree node it not cmd type";
        status.code = common::kPlanError;
        return false;
    }
    *output = node_manager_->MakeCmdPlanNode(
        dynamic_cast<const node::CmdNode *>(root));
    return true;
}

bool Planner::CreateCreateProcedurePlan(
    const node::SqlNode *root, const PlanNodeList &inner_plan_node_list,
    node::PlanNode **output, Status &status) {  // NOLINT (runtime/references)
    const node::CreateSpStmt *create_sp_tree = (const node::CreateSpStmt *)root;
    *output = node_manager_->MakeCreateProcedurePlanNode(
        create_sp_tree->GetSpName(), create_sp_tree->GetInputParameterList(),
        inner_plan_node_list);
    return true;
}

std::string Planner::MakeTableName(const PlanNode *node) const {
    switch (node->GetType()) {
        case node::kPlanTypeTable: {
            const node::TablePlanNode *table_node =
                dynamic_cast<const node::TablePlanNode *>(node);
            return table_node->table_;
        }
        case node::kPlanTypeRename: {
            const node::RenamePlanNode *table_node =
                dynamic_cast<const node::RenamePlanNode *>(node);
            return table_node->table_;
        }
        case node::kPlanTypeJoin: {
            return "";
        }
        case node::kPlanTypeQuery: {
            return "";
        }
        default: {
            LOG(WARNING) << "fail to get or generate table name for given plan "
                            "node type "
                         << node::NameOfPlanNodeType(node->GetType());
            return "";
        }
    }
    return "";
}
bool Planner::CreateTableReferencePlanNode(const node::TableRefNode *root,
                                           node::PlanNode **output,
                                           Status &status) {
    node::PlanNode *plan_node = nullptr;
    switch (root->ref_type_) {
        case node::kRefTable: {
            const node::TableNode *table_node =
                dynamic_cast<const node::TableNode *>(root);
            plan_node =
                node_manager_->MakeTablePlanNode(table_node->org_table_name_);
            if (!table_node->alias_table_name_.empty()) {
                *output = node_manager_->MakeRenamePlanNode(
                    plan_node, table_node->alias_table_name_);
            } else {
                *output = plan_node;
            }
            break;
        }
        case node::kRefJoin: {
            const node::JoinNode *join_node =
                dynamic_cast<const node::JoinNode *>(root);
            node::PlanNode *left = nullptr;
            node::PlanNode *right = nullptr;
            if (!CreateTableReferencePlanNode(join_node->left_, &left,
                                              status)) {
                return false;
            }
            if (!CreateTableReferencePlanNode(join_node->right_, &right,
                                              status)) {
                return false;
            }
            plan_node = node_manager_->MakeJoinNode(
                left, right, join_node->join_type_, join_node->orders_,
                join_node->condition_);
            if (!join_node->alias_table_name_.empty()) {
                *output = node_manager_->MakeRenamePlanNode(
                    plan_node, join_node->alias_table_name_);
            } else {
                *output = plan_node;
            }
            break;
        }
        case node::kRefQuery: {
            const node::QueryRefNode *sub_query_node =
                dynamic_cast<const node::QueryRefNode *>(root);
            if (!CreateQueryPlan(sub_query_node->query_, &plan_node, status)) {
                return false;
            }
            if (!sub_query_node->alias_table_name_.empty()) {
                *output = node_manager_->MakeRenamePlanNode(
                    plan_node, sub_query_node->alias_table_name_);
            } else {
                *output = plan_node;
            }
            break;
        }
        default: {
            status.msg =
                "fail to create table reference node, unrecognized type " +
                node::NameOfSqlNodeType(root->GetType());
            status.code = common::kPlanError;
            LOG(WARNING) << status;
            return false;
        }
    }

    return true;
}
bool Planner::MergeWindows(
    const std::map<const node::WindowDefNode *, node::ProjectListNode *> &map,
    std::vector<const node::WindowDefNode *> *windows_ptr) {
    if (nullptr == windows_ptr) {
        return false;
    }
    bool has_window_merged = false;
    auto &windows = *windows_ptr;

    std::vector<std::pair<const node::WindowDefNode *, int32_t>>
        window_id_pairs;
    for (auto it = map.begin(); it != map.end(); it++) {
        window_id_pairs.push_back(std::make_pair(
            it->first,
            nullptr == it->second->GetW() ? 0 : it->second->GetW()->GetId()));
    }
    std::sort(
        window_id_pairs.begin(), window_id_pairs.end(),
        [](const std::pair<const node::WindowDefNode *, int32_t> &p1,
           const std::pair<const node::WindowDefNode *, int32_t> &p2) -> bool {
            return p1.second < p2.second;
        });

    // Merge Rows Frames First
    for (auto iter = window_id_pairs.cbegin(); iter != window_id_pairs.cend();
         iter++) {
        if (nullptr != iter->first &&
            iter->first->GetFrame()->IsRowsRangeLikeFrame()) {
            // skip handling range like frames
            continue;
        }
        if (windows.empty()) {
            windows.push_back(iter->first);
            continue;
        }
        bool can_be_merged = false;
        for (auto iter_w = windows.begin(); iter_w != windows.end(); iter_w++) {
            if (node::SqlEquals(iter->first, *iter_w)) {
                can_be_merged = true;
                has_window_merged = true;
                break;
            }
            if (nullptr == *iter_w) {
                continue;
            }
            if (iter->first->CanMergeWith(*iter_w,
                                          enable_window_maxsize_merged_)) {
                can_be_merged = true;
                *iter_w = node_manager_->MergeWindow(iter->first, *iter_w);
                has_window_merged = true;
                break;
            }
        }

        if (!can_be_merged) {
            windows.push_back(iter->first);
        }
    }

    for (auto iter = window_id_pairs.cbegin(); iter != window_id_pairs.cend();
         iter++) {
        if (nullptr == iter->first ||
            !iter->first->GetFrame()->IsRowsRangeLikeFrame()) {
            // skip handling rows frames
            continue;
        }
        if (windows.empty()) {
            windows.push_back(iter->first);
            continue;
        }
        bool can_be_merged = false;
        for (auto iter_w = windows.begin(); iter_w != windows.end(); iter_w++) {
            if (node::SqlEquals(iter->first, *iter_w)) {
                can_be_merged = true;
                has_window_merged = true;
                break;
            }
            if (nullptr == *iter_w) {
                continue;
            }
            if (iter->first->CanMergeWith(*iter_w,
                                          enable_window_maxsize_merged_)) {
                can_be_merged = true;
                *iter_w = node_manager_->MergeWindow(iter->first, *iter_w);
                has_window_merged = true;
                break;
            }
        }

        if (!can_be_merged) {
            windows.push_back(iter->first);
        }
    }

    return has_window_merged;
}

bool Planner::MergeProjectMap(
    const std::map<const node::WindowDefNode *, node::ProjectListNode *> &map,
    std::map<const node::WindowDefNode *, node::ProjectListNode *> *output,
    Status &status) {
    if (map.empty()) {
        DLOG(INFO) << "Nothing to merge, project list map is empty";
        *output = map;
        return true;
    }
    std::vector<const node::WindowDefNode *> windows;
    bool flag_merge = MergeWindows(map, &windows);
    bool flag_expand = ExpandCurrentHistoryWindow(&windows);
    if (!flag_merge && !flag_expand) {
        DLOG(INFO) << "No window can be merged or expand";
        *output = map;
        return true;
    }

    int32_t w_id = 1;
    for (auto iter = windows.cbegin(); iter != windows.cend(); iter++) {
        if (nullptr == *iter) {
            output->insert(std::make_pair(
                nullptr,
                node_manager_->MakeProjectListPlanNode(nullptr, false)));
            continue;
        }
        node::WindowPlanNode *w_node_ptr =
            node_manager_->MakeWindowPlanNode(w_id++);
        if (!CreateWindowPlanNode(*iter, w_node_ptr, status)) {
            return false;
        }
        output->insert(std::make_pair(
            *iter, node_manager_->MakeProjectListPlanNode(w_node_ptr, true)));
    }

    for (auto map_iter = map.cbegin(); map_iter != map.cend(); map_iter++) {
        bool merge_ok = false;
        for (auto iter = output->begin(); iter != output->end(); iter++) {
            if (node::SqlEquals(map_iter->first, iter->first) ||
                (nullptr != map_iter->first &&
                 map_iter->first->CanMergeWith(iter->first))) {
                auto frame = iter->second->GetW();
                node::ProjectListNode *merged_project =
                    node_manager_->MakeProjectListPlanNode(frame,
                                                           frame != nullptr);
                node::ProjectListNode::MergeProjectList(
                    iter->second, map_iter->second, merged_project);
                iter->second = merged_project;
                merge_ok = true;
                break;
            }
        }
        if (!merge_ok) {
            status.msg = "Fail to merge project list";
            status.code = common::kPlanError;
            LOG(WARNING) << status;
            return false;
        }
    }

    return true;
}
bool Planner::ExpandCurrentHistoryWindow(
    std::vector<const node::WindowDefNode *> *windows_ptr) {
    if (nullptr == windows_ptr) {
        return false;
    }
    auto &windows = *windows_ptr;
    bool has_window_expand = false;
    // merge big history window with current history window
    for (auto iter = windows.begin(); iter != windows.end(); iter++) {
        const node::WindowDefNode *w_ptr = *iter;
        if (nullptr != w_ptr && nullptr != w_ptr->GetFrame() &&
            !w_ptr->GetFrame()->IsRowsRangeLikeFrame() &&
            w_ptr->GetFrame()->IsPureHistoryFrame()) {
            node::FrameNode *current_frame =
                node_manager_->MergeFrameNodeWithCurrentHistoryFrame(
                    w_ptr->GetFrame());
            *iter = dynamic_cast<node::WindowDefNode *>(
                node_manager_->MakeWindowDefNode(
                    w_ptr->union_tables(), w_ptr->GetPartitions(),
                    w_ptr->GetOrders(), current_frame,
                    w_ptr->exclude_current_time(),
                    w_ptr->instance_not_in_window()));
            has_window_expand = true;
        }
    }
    return has_window_expand;
}

bool Planner::TransformTableDef(
    const std::string &table_name, const NodePointVector &column_desc_list,
    type::TableDef *table,
    Status &status) {  // NOLINT (runtime/references)
    std::set<std::string> index_names;
    std::set<std::string> column_names;

    for (auto column_desc : column_desc_list) {
        switch (column_desc->GetType()) {
            case node::kColumnDesc: {
                node::ColumnDefNode *column_def =
                    (node::ColumnDefNode *)column_desc;
                type::ColumnDef *column = table->add_columns();

                if (column_names.find(column_def->GetColumnName()) !=
                    column_names.end()) {
                    status.msg = "CREATE common: COLUMN NAME " +
                                 column_def->GetColumnName() + " duplicate";
                    status.code = common::kSqlError;
                    LOG(WARNING) << status;
                    return false;
                }
                column->set_name(column_def->GetColumnName());
                column->set_is_not_null(column_def->GetIsNotNull());
                column_names.insert(column_def->GetColumnName());
                switch (column_def->GetColumnType()) {
                    case node::kBool:
                        column->set_type(type::Type::kBool);
                        break;
                    case node::kInt16:
                        column->set_type(type::Type::kInt16);
                        break;
                    case node::kInt32:
                        column->set_type(type::Type::kInt32);
                        break;
                    case node::kInt64:
                        column->set_type(type::Type::kInt64);
                        break;
                    case node::kFloat:
                        column->set_type(type::Type::kFloat);
                        break;
                    case node::kDouble:
                        column->set_type(type::Type::kDouble);
                        break;
                    case node::kTimestamp: {
                        column->set_type(type::Type::kTimestamp);
                        break;
                    }
                    case node::kDate: {
                        column->set_type(type::Type::kDate);
                        break;
                    }
                    case node::kVarchar:
                        column->set_type(type::Type::kVarchar);
                        break;
                    default: {
                        status.msg =
                            "CREATE common: column type " +
                            node::DataTypeName(column_def->GetColumnType()) +
                            " is not supported";
                        status.code = common::kSqlError;
                        LOG(WARNING) << status;
                        return false;
                    }
                }
                break;
            }

            case node::kColumnIndex: {
                node::ColumnIndexNode *column_index =
                    (node::ColumnIndexNode *)column_desc;

                if (column_index->GetName().empty()) {
                    column_index->SetName(
                        PlanAPI::GenerateName("INDEX", table->indexes_size()));
                }
                if (index_names.find(column_index->GetName()) !=
                    index_names.end()) {
                    status.msg = "CREATE common: INDEX NAME " +
                                 column_index->GetName() + " duplicate";
                    status.code = common::kSqlError;
                    LOG(WARNING) << status;
                    return false;
                }
                index_names.insert(column_index->GetName());
                type::IndexDef *index = table->add_indexes();
                index->set_name(column_index->GetName());

                // TODO(chenjing): set ttl per key
                if (column_index->GetAbsTTL() >= 0) {
                    index->add_ttl(column_index->GetAbsTTL());
                } else {
                    index->add_ttl(0);
                }
                if (column_index->GetLatTTL() >= 0) {
                    index->add_ttl(column_index->GetLatTTL());
                } else {
                    index->add_ttl(0);
                }

                for (auto key : column_index->GetKey()) {
                    index->add_first_keys(key);
                }

                if (!column_index->GetTs().empty()) {
                    index->set_second_key(column_index->GetTs());
                }
                break;
            }
            default: {
                status.msg = "can not support " +
                             node::NameOfSqlNodeType(column_desc->GetType()) +
                             " when CREATE TABLE";
                status.code = common::kSqlError;
                LOG(WARNING) << status;
                return false;
            }
        }
    }
    table->set_name(table_name);
    return true;
}

}  // namespace plan
}  // namespace hybridse
