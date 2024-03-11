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

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "plan/plan_api.h"
#include "proto/fe_common.pb.h"
#include "udf/default_udf_library.h"
#include "vm/engine.h"

namespace hybridse {
namespace plan {

// whether the window function is relative to current row instead of window frame bound
inline bool IsCurRowRelativeWinFun(absl::string_view fn_name) {
    return absl::EqualsIgnoreCase("lag", fn_name) || absl::EqualsIgnoreCase("at", fn_name) ||
           absl::EqualsIgnoreCase("lead", fn_name);
}

Planner::Planner(node::NodeManager *manager, const bool is_batch_mode, const bool is_cluster_optimized,
        const bool enable_batch_window_parallelization,
        const std::unordered_map<std::string, std::string>* extra_options)
    : is_batch_mode_(is_batch_mode),
      is_cluster_optimized_(is_cluster_optimized),
      enable_window_maxsize_merged_(true),
      enable_batch_window_parallelization_(enable_batch_window_parallelization),
      node_manager_(manager),
      extra_options_(extra_options) {
    if (extra_options_ && extra_options_->count(vm::LONG_WINDOWS)) {
        std::vector<std::string> tokens;
        boost::split(tokens, extra_options_->at(vm::LONG_WINDOWS), boost::is_any_of(","));
        for (auto& w : tokens) {
            std::vector<std::string> window_info;
            boost::split(window_info, w, boost::is_any_of(":"));
            boost::trim(window_info[0]);
            long_windows_.insert(window_info[0]);
        }
    }
}

base::Status Planner::CreateQueryPlan(const node::QueryNode *root, node::QueryPlanNode **plan_tree) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "can not create query plan node with null query node");

    auto out = node_manager_->MakeNode<node::QueryPlanNode>();

    if (!root->with_clauses_.empty()) {
        auto with_list = node_manager_->MakeList<node::WithClauseEntryPlanNode>();
        for (auto q : root->with_clauses_) {
            node::QueryPlanNode *with = nullptr;
            // CHECK_TRUE(q->query_->query_type_ == node::kQuerySelect, common::kPlanError,
            //            "only support select query as with clause entry");
            CHECK_STATUS(CreateQueryPlan(q->query_, &with));

            auto with_entry = node_manager_->MakeNode<node::WithClauseEntryPlanNode>(q->alias_, with);

            with_list->data_.push_back(with_entry);
        }
        out->with_clauses_ = absl::MakeSpan(with_list->data_);
    }

    if (root->config_options_ != nullptr) {
        out->config_options_ = root->config_options_;
    }

    switch (root->query_type_) {
        case node::kQuerySelect: {
            node::PlanNode* query_input = nullptr;
            CHECK_STATUS(CreateSelectQueryPlan(dynamic_cast<const node::SelectQueryNode *>(root), &query_input));
            out->AddChild(query_input);
            break;
        }
        case node::kQuerySetOperation: {
            node::SetOperationPlanNode* un = nullptr;
            CHECK_STATUS(CreateSetOperationPlan(dynamic_cast<const node::SetOperationNode *>(root), &un));
            out->AddChild(un);
            break;
        }
        default: {
            FAIL_STATUS(common::kPlanError, "can not create query plan node with invalid query type " +
                                                node::QueryTypeName(root->query_type_));
        }
    }

    *plan_tree = out;
    return base::Status::OK();
}
// TODO(chenjing): refactor SELECT query logical plan
// Deal with group by clause, order clause, having clause in physical plan instead of logical plan, since we need
// schema context for column resolve.
base::Status Planner::CreateSelectQueryPlan(const node::SelectQueryNode *root, node::PlanNode **plan_tree) {
    const node::NodePointVector &table_ref_list =
        nullptr == root->GetTableRefList() ? std::vector<SqlNode *>() : root->GetTableRefList()->GetList();
    std::vector<node::PlanNode *> relation_nodes;
    for (node::SqlNode *node : table_ref_list) {
        node::PlanNode *table_ref_plan = nullptr;
        CHECK_TRUE(nullptr != node, common::kPlanError,
                   "can not create select plan node: table reference node is null");
        CHECK_TRUE(node::kTableRef == node->GetType(), common::kPlanError,
                   "can not create select plan node: table reference node type is invalid ",
                   node::NameOfSqlNodeType(node->GetType()))
        CHECK_STATUS(CreateTableReferencePlanNode(dynamic_cast<node::TableRefNode *>(node), &table_ref_plan))
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
            current_node =
                node_manager_->MakeJoinNode(current_node, *iter, node::JoinType::kJoinTypeFull, nullptr, nullptr);
        }
        // TODO(chenjing): 处理子查询
        table_name = MakeTableName(current_node);
    }
    // group by
    if (nullptr != root->group_clause_ptr_) {
        if (!root->group_clause_ptr_->IsEmpty()) {
            for (size_t i = 0; i < root->group_clause_ptr_->GetChildNum(); i++) {
                CHECK_TRUE(root->group_clause_ptr_->GetChild(i)->GetExprType() == node::kExprColumnRef,
                           common::kUnsupportSql, "Only support GROUP BY column EXPRESSION currently, but #",
                           i, " EXPRESSION in GROUP BY is " ,
                           node::ExprTypeName(root->group_clause_ptr_->GetChild(i)->GetExprType()), " expression ")
            }
            current_node = node_manager_->MakeGroupPlanNode(current_node, root->group_clause_ptr_);
        }
    }

    // where condition
    if (nullptr != root->where_clause_ptr_) {
        current_node = node_manager_->MakeFilterPlanNode(current_node, root->where_clause_ptr_);
    }

    // select target_list
    CHECK_TRUE(nullptr != root->GetSelectList() && !root->GetSelectList()->GetList().empty(), common::kPlanError,
               "fail to create select query plan: select expr list is null or empty")

    // prepare window def
    int w_id = 1;
    std::map<std::string, const node::WindowDefNode *> windows;
    if (nullptr != root->GetWindowList() && !root->GetWindowList()->IsEmpty()) {
        for (auto node : root->GetWindowList()->GetList()) {
            const node::WindowDefNode *w = dynamic_cast<node::WindowDefNode *>(node);
            CHECK_TRUE(windows.find(w->GetName()) == windows.cend(), common::kPlanError,
                       "fail to resolve window, window name duplicate: ", w->GetName())
            CHECK_STATUS(CheckWindowFrame(w))
            windows[w->GetName()] = w;
        }
    }

    // mapping for window projects
    std::map<const node::WindowDefNode *, node::ProjectListNode *> window_project_list_map;
    // standalone `ProjectList` holds non-window projects
    node::ProjectListNode *table_project_list = node_manager_->MakeProjectListPlanNode(nullptr, false);
    const udf::UdfLibrary *lib = udf::DefaultUdfLibrary::get();

    const node::NodePointVector &select_expr_list = root->GetSelectList()->GetList();
    for (uint32_t pos = 0u; pos < select_expr_list.size(); pos++) {
        auto& expr = select_expr_list[pos];
        std::string project_name;
        node::ExprNode *project_expr;
        switch (expr->GetType()) {
            case node::kResTarget: {
                const node::ResTarget *target_ptr = static_cast<const node::ResTarget *>(expr);
                project_name = target_ptr->GetName();
                if (project_name.empty()) {
                    project_name = target_ptr->GetVal()->GenerateExpressionName();
                }
                project_expr = target_ptr->GetVal();
                break;
            }
            default: {
                FAIL_STATUS(common::kPlanError, "can not create project plan node with type ",
                            node::NameOfSqlNodeType(root->GetType()))
            }
        }

        // get the window for the expr
        const node::WindowDefNode *w_ptr = nullptr;
        CHECK_TRUE(node::WindowOfExpression(windows, project_expr, &w_ptr), common::kPlanError,
                   "fail to resolved window")
        // expand window frame for lag funtions early
        if (project_expr->GetExprType() == node::kExprCall) {
            auto *call_expr = dynamic_cast<node::CallExprNode *>(project_expr);
            if (call_expr != nullptr && call_expr->GetOver() != nullptr &&
                IsCurRowRelativeWinFun(call_expr->GetFnDef()->GetName())) {
                // current row window constructed only for `lag(col, 1) over w`,
                // not for nested window aggregation from kids,
                //   like `lag(split_by_key(count_cate_where(col, ...) over w, ",", ":"), 1)`
                auto s = ConstructWindowForLag(w_ptr, call_expr);
                CHECK_TRUE(s.ok(), common::kUnsupportSql, s.status().ToString());
                w_ptr = s.value();
            }
        }

        // deal with row project / table aggregation project
        if (w_ptr == nullptr) {
            if (node::IsAggregationExpression(lib, project_expr)) {
                // table aggregation project
                table_project_list->AddProject(
                    node_manager_->MakeAggProjectNode(pos, project_name, project_expr, nullptr));
            } else {
                // row project
                table_project_list->AddProject(node_manager_->MakeRowProjectNode(pos, project_name, project_expr));
            }
            continue;
        }

        // deal with window project
        auto it = window_project_list_map.find(w_ptr);
        if (it == window_project_list_map.end()) {
            // save the newly found window to (window -> project list) map
            node::WindowPlanNode *w_node_ptr = node_manager_->MakeWindowPlanNode(w_id++);
            CHECK_STATUS(FillInWindowPlanNode(w_ptr, w_node_ptr))
            // and create initial project list node for that new window
            auto res = window_project_list_map.emplace(w_ptr, node_manager_->MakeProjectListPlanNode(w_node_ptr, true));
            it = res.first;
        }
        it->second->AddProject(
            node_manager_->MakeAggProjectNode(pos, project_name, project_expr, w_ptr->GetFrame()));
    }

    // Rule 1: Can't support group clause and window clause simultaneously
    CHECK_TRUE(!(nullptr != root->group_clause_ptr_ && !window_project_list_map.empty()), common::kPlanError,
               "Can't support group clause and window clause simultaneously")
    // Rule 2: Can't support having clause and window clause simultaneously
    CHECK_TRUE(!(nullptr != root->having_clause_ptr_ && !window_project_list_map.empty()), common::kPlanError,
               "Can't support having clause and window clause simultaneously")
    // Rule 3: Can't support table aggregation and window aggregation simultaneously
    CHECK_TRUE(!(table_project_list->HasAggProject() && !window_project_list_map.empty()), common::kPlanError,
               "Can't support table aggregation and window aggregation simultaneously")

    // Add table projects into project map beforehand
    // Thus we can merge project list based on window frame when it is necessary.
    if (!table_project_list->GetProjects().empty()) {
        // (nullptr, project list) pair contains all projects that is not window related
        table_project_list->SetHavingCondition(root->having_clause_ptr_);
        window_project_list_map[nullptr] = table_project_list;
    }
    // merge window map
    bool long_window_exist = false;
    // only support long-window optimization for request-mode
    if (!is_batch_mode_ && !long_windows_.empty()) {
        for (const auto &it : window_project_list_map) {
            if (it.first == nullptr) continue;

            if (long_windows_.count(it.first->GetName())) {
                long_window_exist = true;
                DLOG(INFO) << it.first->GetName() << " is long window. Disable project merge";
                break;
            }
        }
    }

    std::map<const node::WindowDefNode *, node::ProjectListNode *> merged_project_list_map;
    if (long_window_exist) {
        merged_project_list_map = window_project_list_map;
    } else {
        CHECK_STATUS(MergeProjectMap(window_project_list_map, &merged_project_list_map))
    }

    // add MergeNode if multi ProjectionLists exist
    PlanNodeList project_list_vec(w_id);
    for (auto &v : merged_project_list_map) {
        node::ProjectListNode *project_list = v.second;
        int pos = nullptr == project_list->GetW() ? 0 : project_list->GetW()->GetId();
        project_list_vec[pos] = project_list;
    }

    // merge simple project with 1st window project
    if (!long_window_exist && nullptr != project_list_vec[0] && project_list_vec.size() > 1) {
        auto simple_project = dynamic_cast<node::ProjectListNode *>(project_list_vec[0]);
        auto first_window_project = dynamic_cast<node::ProjectListNode *>(project_list_vec[1]);
        node::ProjectListNode *merged_project =
            node_manager_->MakeProjectListPlanNode(first_window_project->GetW(), true);
        if  (!is_cluster_optimized_ && !enable_batch_window_parallelization_ &&
            node::ProjectListNode::MergeProjectList(simple_project, first_window_project, merged_project)) {
            project_list_vec[0] = nullptr;
            project_list_vec[1] = merged_project;
        }
    }

    PlanNodeList project_list_without_null;
    std::vector<std::pair<uint32_t, uint32_t>> pos_mapping(select_expr_list.size());
    int project_list_id = 0;
    for (auto &v : project_list_vec) {
        if (nullptr == v) {
            continue;
        }
        auto project_list = dynamic_cast<node::ProjectListNode *>(v)->GetProjects();
        int project_pos = 0;
        for (auto project : project_list) {
            pos_mapping[dynamic_cast<node::ProjectNode *>(project)->GetPos()] =
                std::make_pair(project_list_id, project_pos);
            project_pos++;
        }
        project_list_without_null.push_back(v);
        project_list_id++;
    }

    current_node = node_manager_->MakeNode<node::ProjectPlanNode>(current_node, table_name, project_list_without_null,
                                                                  pos_mapping);

    // distinct
    if (root->distinct_opt_) {
        current_node = node_manager_->MakeDistinctPlanNode(current_node);
    }
    // order
    if (nullptr != root->order_clause_ptr_) {
        current_node = node_manager_->MakeSortPlanNode(current_node, root->order_clause_ptr_);
    }
    // limit
    if (nullptr != root->GetLimit()) {
        const node::LimitNode *limit_ptr = static_cast<const node::LimitNode *>(root->GetLimit());
        current_node = node_manager_->MakeLimitPlanNode(current_node, limit_ptr->GetLimitCount());
    }

    *plan_tree = current_node;
    return base::Status::OK();
}

base::Status Planner::CreateSetOperationPlan(const node::SetOperationNode *root,
                                             node::SetOperationPlanNode **plan_tree) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "can not create query plan node with null query node")

    auto list = node_manager_->MakeList<node::QueryPlanNode>();
    for (auto n : root->inputs()) {
        node::QueryPlanNode* query = nullptr;
        CHECK_STATUS(CreateQueryPlan(n, &query));
        list->data_.push_back(query);
    }
    auto span = absl::MakeSpan(list->data_);
    *plan_tree = node_manager_->MakeNode<node::SetOperationPlanNode>(root->op_type(), span, root->distinct());
    return base::Status::OK();
}

base::Status Planner::CheckWindowFrame(const node::WindowDefNode *w_ptr) {
    CHECK_TRUE(nullptr != w_ptr->GetFrame(), common::kPlanError,
               "fail to create project list node: frame can't be unbound ")

    if (w_ptr->GetFrame()->frame_type() == node::kFrameRows) {
        auto extent = dynamic_cast<node::FrameExtent *>(w_ptr->GetFrame()->frame_rows());
        if ((extent->start()->bound_type() == node::kPreceding || extent->start()->bound_type() == node::kFollowing) &&
            extent->start()->is_time_offset()) {
            FAIL_STATUS(common::kPlanError, "Fail Make Rows Frame Node: time offset un-support")
        }
        if ((extent->end()->bound_type() == node::kPreceding || extent->end()->bound_type() == node::kFollowing) &&
            extent->end()->is_time_offset()) {
            FAIL_STATUS(common::kPlanError, "Fail Make Rows Frame Node: time offset un-support")
        }

        if (w_ptr->GetFrame()->frame_maxsize() > 0) {
            FAIL_STATUS(common::kPlanError, "Fail Make Rows Window: MAXSIZE non-support for Rows Window")
        }
    }
    return base::Status::OK();
}
base::Status Planner::FillInWindowPlanNode(const node::WindowDefNode *w_ptr, node::WindowPlanNode *w_node_ptr) {
    if (nullptr != w_ptr) {
        // Prepare Window Frame
        CHECK_STATUS(CheckWindowFrame(w_ptr))
        node::FrameNode *frame = dynamic_cast<node::FrameNode *>(w_ptr->GetFrame());
        w_node_ptr->set_frame_node(frame);

        // Prepare Window Name
        if (w_ptr->GetName().empty()) {
            w_node_ptr->SetName(PlanAPI::GenerateName("anonymous_w", w_node_ptr->GetId()));
        } else {
            w_node_ptr->SetName(w_ptr->GetName());
        }

        // Prepare Window partitions and orders
        w_node_ptr->SetKeys(w_ptr->GetPartitions());
        w_node_ptr->SetOrders(w_ptr->GetOrders());

        // Prepare Window Union Info
        if (nullptr != w_ptr->union_tables() && !w_ptr->union_tables()->GetList().empty()) {
            for (auto node : w_ptr->union_tables()->GetList()) {
                node::PlanNode *table_plan = nullptr;
                CHECK_STATUS(CreateTableReferencePlanNode(dynamic_cast<node::TableRefNode *>(node), &table_plan))
                w_node_ptr->AddUnionTable(table_plan);
            }
        }
        w_node_ptr->set_instance_not_in_window(w_ptr->instance_not_in_window());
        w_node_ptr->set_exclude_current_time(w_ptr->exclude_current_time());
    }
    return base::Status::OK();
}

base::Status Planner::CreateDeployPlanNode(const node::DeployNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create deploy plan with null node");
    *output = node_manager_->MakeDeployPlanNode(root->Name(), root->Stmt(), root->StmtStr(),
                                                root->Options(), root->IsIfNotExists());
    return base::Status::OK();
}

base::Status Planner::CreateLoadDataPlanNode(const node::LoadDataNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create load data plan with null node");
    *output = node_manager_->MakeLoadDataPlanNode(root->File(), root->Db(), root->Table(), root->Options(),
                                                  root->ConfigOptions());
    return base::Status::OK();
}

base::Status Planner::CreateCreateFunctionPlanNode(const node::CreateFunctionNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create create function plan with null node");
    *output = node_manager_->MakeCreateFunctionPlanNode(root->Name(), root->GetReturnType(), root->GetArgsType(),
                                                        root->IsAggregate(), root->Options());
    return base::Status::OK();
}

base::Status Planner::CreateSelectIntoPlanNode(const node::SelectIntoNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create select into plan with null node");
    node::QueryPlanNode *query = nullptr;
    CHECK_STATUS(CreateQueryPlan(root->Query(), &query))
    *output = node_manager_->MakeSelectIntoPlanNode(query, root->QueryStr(), root->OutFile(), root->Options(),
                                                    root->ConfigOptions());
    return base::Status::OK();
}

base::Status Planner::CreateSetPlanNode(const node::SetNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create set plan with null node");
    *output = node_manager_->MakeSetPlanNode(root);
    return base::Status::OK();
}

base::Status Planner::CreateCreateTablePlan(const node::SqlNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create table plan with null node")
    auto create_tree = dynamic_cast<const node::CreateStmt *>(root);
    auto* out = node_manager_->MakeCreateTablePlanNode(create_tree->GetDbName(), create_tree->GetTableName(),
                                                     create_tree->GetColumnDefList(), create_tree->GetTableOptionList(),
                                                     create_tree->GetOpIfNotExist());
    out->like_clause_ = create_tree->like_clause_;
    *output = out;
    return base::Status::OK();
}

/// Check if current plan node is depend on a (table|simple select table/rename table/sub query table)
/// Store TablePlanNode into output if true
absl::StatusOr<node::TablePlanNode*> Planner::IsTable(node::PlanNode *node) {
    if (nullptr == node) {
        return absl::InvalidArgumentError("null node");
    }

    switch (node->type_) {
        case node::kPlanTypeTable: {
            return dynamic_cast<node::TablePlanNode*>(node);
        }
        case node::kPlanTypeRename: {
            return IsTable(node->GetChildren()[0]);
        }
        case node::kPlanTypeQuery: {
            return IsTable(dynamic_cast<node::QueryPlanNode *>(node)->GetChildren()[0]);
        }
        case node::kPlanTypeProject: {
            if ((dynamic_cast<node::ProjectPlanNode *>(node))->IsSimpleProjectPlan()) {
                return IsTable(node->GetChildren()[0]);
            }
            break;
        }
        default:
            break;
    }
    return absl::NotFoundError("not found");
}

// Validate online serving op with given plan tree
// - Support Ops:
//   - TABLE
//   - SELECT
//   - JOIN
//   - WINDOW
//   - CONST PROJECT
// - UnSupport Ops::
//   - CREATE TABLE
//   - INSERT TABLE
//   - GROUP BY
//   - HAVING clause
//   - FILTER
//
// - Not Impl
//   - Order By
base::Status Planner::ValidateOnlineServingOp(node::PlanNode *node) {
    if (node == nullptr) {
        // null is fine, e.g the const project
        return {};
    }
    switch (node->type_) {
        case node::kPlanTypeProject: {
            auto project_node = dynamic_cast<node::ProjectPlanNode *>(node);

            for (auto &each : project_node->project_list_vec_) {
                node::ProjectListNode *project_list = dynamic_cast<node::ProjectListNode *>(each);
                CHECK_TRUE(nullptr == project_list->GetHavingCondition(), common::kPlanError,
                           "Non-support HAVING Op in online serving")
                CHECK_TRUE(!(nullptr == project_list->GetW() && project_list->HasAggProject()), common::kPlanError,
                           "Aggregate over a table cannot be supported in online serving")
            }

            break;
        }
        case node::kPlanTypeTable:
        case node::kPlanTypeRename:
        case node::kPlanTypeLimit:
        case node::kPlanTypeWindow:
        case node::kPlanTypeQuery:
        case node::kPlanTypeFilter:
        case node::kPlanTypeSetOperation:
        case node::kPlanTypeJoin: {
            break;
        }
        default: {
            FAIL_STATUS(common::kPlanError, "Non-support ", node->GetTypeName(), " Op in online serving");
            break;
        }
    }

    for (auto *child : node->GetChildren()) {
        CHECK_STATUS(ValidateOnlineServingOp(child));
    }

    return base::Status::OK();
}

// Get the limit count of given SQL query
int Planner::GetPlanTreeLimitCount(node::PlanNode *node) {
    if (nullptr == node) {
        return 0;
    }

    int limit_cnt = 0;
    switch (node->type_) {
        case node::kPlanTypeTable: {
            return 0;
        }
        case node::kPlanTypeLimit: {
            auto limit_node = dynamic_cast<node::LimitPlanNode *>(node);
            limit_cnt = limit_node->GetLimitCnt();
            break;
        }
        default:
            break;
    }

    if (node->GetChildrenSize() > 0) {
        int cnt = GetPlanTreeLimitCount(node->GetChildren()[0]);
        if (cnt > 0) {
            if (limit_cnt == 0) {
                limit_cnt = cnt;
            } else {
                limit_cnt = std::min(cnt, limit_cnt);
            }
        }
    }

    return limit_cnt;
}

base::Status Planner::PreparePlanForRequestMode(node::PlanNode *node) { return ValidateOnlineServingOp(node); }

// Un-support Ops:
// - Last Join
//
// Not Impl:
// - Order By
base::Status Planner::ValidateClusterOnlineTrainingOp(node::PlanNode *node) {
    if (node == nullptr) {
        return base::Status::OK();
    }
    switch (node->type_) {
        case node::kPlanTypeProject:
        case node::kPlanTypeGroup:
        case node::kPlanTypeTable:
        case node::kPlanTypeLoadData:
        case node::kPlanTypeRename:
        case node::kPlanTypeLimit:
        case node::kPlanTypeFilter:
        case node::kPlanTypeSetOperation:
        case node::kPlanTypeQuery: {
            break;
        }
        default: {
            FAIL_STATUS(common::kPlanError, "Non-support ", node->GetTypeName(), " Op in cluster online training");
            break;
        }
    }

    for (auto *child : node->GetChildren()) {
        CHECK_STATUS(ValidateClusterOnlineTrainingOp(child));
    }

    return base::Status::OK();
}

// extract request table plan node( always the first table plan node visited / the first LeafNode by DFS )
// also set else table plan node requested by the request node
base::Status Planner::PrepareRequestTable(node::PlanNode *node, std::vector<node::TablePlanNode *> &outputs) {
    CHECK_TRUE(nullptr != node, common::kNullInputPointer,
               "Fail to validate request table: input node is "
               "null")

    switch (node->type_) {
        case node::kPlanTypeJoin:
        case node::kPlanTypeSetOperation: {
            auto binary_op = dynamic_cast<node::BinaryPlanNode *>(node);
            CHECK_TRUE(nullptr != binary_op->GetLeft(), common::kPlanError, "Left child of ", node->GetTypeName(),
                       " is null")
            CHECK_STATUS(PrepareRequestTable(binary_op->GetLeft(), outputs))
            CHECK_TRUE(!outputs.empty(), common::kPlanError, "PLAN error: No request/primary table exist in left tree");

            // If right side is a table|simple select table|rename table
            // It isn't necessary to be validate request
            auto res = IsTable(binary_op->GetRight());
            if (!res.ok()) {
                CHECK_STATUS(PrepareRequestTable(binary_op->GetRight(), outputs))
            }
            return base::Status::OK();
        }
        case node::kPlanTypeTable: {
            // nodes inside with clause not verify, those nodes are checked later during physical plan transforming
            // here we just mark the table node (probably reference to a CTE in WITH clause)
            auto request_node = dynamic_cast<node::TablePlanNode *>(node);
            if (outputs.empty()) {
                // first and only request node
                outputs.push_back(request_node);
            }
            return base::Status::OK();
        }
        case node::kPlanTypeCreate:
        case node::kPlanTypeInsert:
        case node::kPlanTypeCmd:
        case node::kPlanTypeWindow:
        case node::kProjectList:
        case node::kProjectNode: {
            FAIL_STATUS(common::kPlanError, "Fail to infer a request table with invalid node", node->GetTypeName())
        }
        default: {
            CHECK_TRUE(node->GetChildrenSize() > 0, common::kPlanError, "node do not have any kid");
            CHECK_STATUS(PrepareRequestTable(node->GetChildren()[0], outputs));
            return base::Status::OK();
        }
    }
}

base::Status SimplePlanner::CreatePlanTree(const NodePointVector &parser_trees, PlanNodeList &plan_trees) {
    for (auto parser_tree : parser_trees) {
        switch (parser_tree->GetType()) {
            case node::kQuery: {
                node::QueryPlanNode *query_plan = nullptr;
                CHECK_STATUS(CreateQueryPlan(dynamic_cast<node::QueryNode *>(parser_tree), &query_plan));

                if (!is_batch_mode_) {
                    // Validate there is one and only request table in the SQL
                    CHECK_STATUS(PreparePlanForRequestMode(query_plan));
                } else {
                    if (is_cluster_optimized_) {
                        CHECK_STATUS(ValidateClusterOnlineTrainingOp(query_plan));
                    }
                }

                plan_trees.push_back(query_plan);
                break;
            }
            case node::kCreateStmt: {
                CHECK_TRUE(is_batch_mode_, common::kPlanError, "Non-support CREATE TABLE Op in online serving");
                PlanNode *create_plan = nullptr;
                CHECK_STATUS(CreateCreateTablePlan(parser_tree, &create_plan));
                plan_trees.push_back(create_plan);
                break;
            }
            case node::kCreateSpStmt: {
                PlanNode *create_sp_plan = nullptr;
                PlanNodeList inner_plan_node_list;
                const node::CreateSpStmt *create_sp_tree = static_cast<const node::CreateSpStmt *>(parser_tree);
                CHECK_STATUS(CreatePlanTree(create_sp_tree->GetInnerNodeList(), inner_plan_node_list))
                CHECK_STATUS(CreateCreateProcedurePlan(parser_tree, inner_plan_node_list, &create_sp_plan))
                plan_trees.push_back(create_sp_plan);
                break;
            }
            case node::kCmdStmt: {
                CHECK_TRUE(is_batch_mode_, common::kPlanError, "Non-support Command Op in online serving");
                node::PlanNode *cmd_plan = nullptr;
                CHECK_STATUS(CreateCmdPlan(parser_tree, &cmd_plan))
                plan_trees.push_back(cmd_plan);
                break;
            }
            case node::kInsertStmt: {
                // CHECK_TRUE(is_batch_mode_, common::kPlanError, "Non-support INSERT Op in online serving");
                node::PlanNode *insert_plan = nullptr;
                CHECK_STATUS(CreateInsertPlan(parser_tree, &insert_plan))
                plan_trees.push_back(insert_plan);
                break;
            }
            case ::hybridse::node::kFnDef: {
                node::PlanNode *fn_plan = nullptr;
                CHECK_STATUS(CreateFuncDefPlan(parser_tree, &fn_plan))
                plan_trees.push_back(fn_plan);
                break;
            }
            case ::hybridse::node::kExplainStmt: {
                node::PlanNode *explan_plan = nullptr;
                CHECK_STATUS(CreateExplainPlan(parser_tree, &explan_plan))
                plan_trees.push_back(explan_plan);
                break;
            }
            case ::hybridse::node::kCreateIndexStmt: {
                CHECK_TRUE(is_batch_mode_, common::kPlanError, "Non-support CREATE INDEX Op in online serving");
                node::PlanNode *create_index_plan = nullptr;
                CHECK_STATUS(CreateCreateIndexPlan(parser_tree, &create_index_plan))
                plan_trees.push_back(create_index_plan);
                break;
            }
            case ::hybridse::node::kSelectIntoStmt: {
                CHECK_TRUE(is_batch_mode_, common::kPlanError,
                           "Non-support SELECT INTO Op in online serving");
                node::PlanNode *select_into_plan_node = nullptr;
                CHECK_STATUS(CreateSelectIntoPlanNode(dynamic_cast<node::SelectIntoNode *>(parser_tree),
                                                      &select_into_plan_node));
                plan_trees.push_back(select_into_plan_node);
                break;
            }
            case ::hybridse::node::kLoadDataStmt: {
                CHECK_TRUE(is_batch_mode_, common::kPlanError,
                           "Non-support LOAD DATA Op in online serving");
                node::PlanNode *load_data_plan_node = nullptr;
                CHECK_STATUS(
                    CreateLoadDataPlanNode(dynamic_cast<node::LoadDataNode *>(parser_tree), &load_data_plan_node));
                plan_trees.push_back(load_data_plan_node);
                break;
            }
            case ::hybridse::node::kDeployStmt: {
                node::PlanNode *deploy_plan_node = nullptr;
                CHECK_STATUS(CreateDeployPlanNode(dynamic_cast<node::DeployNode *>(parser_tree), &deploy_plan_node));
                plan_trees.push_back(deploy_plan_node);
                break;
            }
            case ::hybridse::node::kCreateUserStmt: {
                auto node = dynamic_cast<node::CreateUserNode *>(parser_tree);
                auto create_user_plan_node = node_manager_->MakeNode<node::CreateUserPlanNode>(node->Name(),
                        node->IfNotExists(), node->Options());
                plan_trees.push_back(create_user_plan_node);
                break;
            }
            case ::hybridse::node::kAlterUserStmt: {
                auto node = dynamic_cast<node::AlterUserNode *>(parser_tree);
                auto alter_user_plan_node = node_manager_->MakeNode<node::AlterUserPlanNode>(node->Name(),
                        node->IfExists(), node->Options());
                plan_trees.push_back(alter_user_plan_node);
                break;
            }
            case ::hybridse::node::kSetStmt: {
                CHECK_TRUE(is_batch_mode_, common::kPlanError,
                           "Non-support SET Op in online serving");
                node::PlanNode *set_plan_node = nullptr;
                CHECK_STATUS(CreateSetPlanNode(dynamic_cast<node::SetNode *>(parser_tree), &set_plan_node));
                plan_trees.push_back(set_plan_node);
                break;
            }
            case ::hybridse::node::kDeleteStmt: {
                auto delete_node = dynamic_cast<const node::DeleteNode*>(parser_tree);
                CHECK_TRUE(delete_node != nullptr, common::kPlanError, "not an DeleteNode");
                node::PlanNode *delete_plan_node = node_manager_->MakeDeletePlanNode(delete_node);
                plan_trees.push_back(delete_plan_node);
                break;
            }
            case ::hybridse::node::kShowStmt: {
                auto show_node = dynamic_cast<const node::ShowNode*>(parser_tree);
                CHECK_TRUE(show_node != nullptr, common::kPlanError, "not an ShowNode");
                plan_trees.push_back(node_manager_->MakeNode<node::ShowPlanNode>(show_node->GetShowType(),
                            show_node->GetTarget(), show_node->GetLikeStr()));
                break;
            }
            case ::hybridse::node::kCreateFunctionStmt: {
                node::PlanNode *create_function_plan_node = nullptr;
                CHECK_STATUS(CreateCreateFunctionPlanNode(dynamic_cast<node::CreateFunctionNode *>(parser_tree),
                            &create_function_plan_node));
                plan_trees.push_back(create_function_plan_node);
                break;
            }
            case ::hybridse::node::kAlterTableStmt: {
                node::AlterTableStmtPlanNode* out = nullptr;
                CHECK_STATUS(ConvertGuard<node::AlterTableStmt>(
                    parser_tree, &out, [this](const node::AlterTableStmt *from, node::AlterTableStmtPlanNode **out) {
                        *out = node_manager_->MakeNode<node::AlterTableStmtPlanNode>(from->db_, from->table_,
                                                                                     from->actions_);
                        return base::Status::OK();
                    }));
                plan_trees.push_back(out);
                break;
            }
            default: {
                FAIL_STATUS(common::kPlanError, "Non-support Op ",
                            node::NameOfSqlNodeType(parser_tree->GetType()))
            }
        }
    }
    return base::Status::OK();
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
base::Status Planner::CreateFuncDefPlan(const SqlNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create func def plan node: query tree node it null")

    CHECK_TRUE(root->GetType() == node::kFnDef, common::kPlanError,
               "fail to create function plan node: query tree node it not function def type")
    *output = node_manager_->MakeFuncPlanNode(dynamic_cast<node::FnNodeFnDef *>(const_cast<SqlNode *>(root)));
    return base::Status::OK();
}

base::Status Planner::CreateInsertPlan(const node::SqlNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create cmd plan node: query tree node it null")

    CHECK_TRUE(root->GetType() == node::kInsertStmt, common::kPlanError,
               "fail to create cmd plan node: query tree node it not insert type")
    *output = node_manager_->MakeInsertPlanNode(dynamic_cast<const node::InsertStmt *>(root));
    return base::Status::OK();
}
base::Status Planner::CreateExplainPlan(const node::SqlNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create explain plan node: query tree node it null")

    CHECK_TRUE(root->GetType() == node::kExplainStmt, common::kPlanError,
               "fail to create explain plan node: query tree node it not kExplainStmt")
    *output = node_manager_->MakeExplainPlanNode(dynamic_cast<const node::ExplainNode *>(root));
    return base::Status::OK();
}
base::Status Planner::CreateCreateIndexPlan(const node::SqlNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create index plan node: query tree node it null")

    CHECK_TRUE(root->GetType() == node::kCreateIndexStmt, common::kPlanError,
               "fail to create explain plan node: query tree node it not kCreateIndexStmt")
    *output = node_manager_->MakeCreateCreateIndexPlanNode(dynamic_cast<const node::CreateIndexNode *>(root));
    return base::Status::OK();
}
base::Status Planner::CreateCmdPlan(const SqlNode *root, node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create cmd plan node: query tree node it null")
    CHECK_TRUE(root->GetType() == node::kCmdStmt, common::kPlanError,
               "fail to create cmd plan node: query tree node it not kCmdStmt")
    *output = node_manager_->MakeCmdPlanNode(dynamic_cast<const node::CmdNode *>(root));
    return base::Status::OK();
}

base::Status Planner::CreateCreateProcedurePlan(const node::SqlNode *root, const PlanNodeList &inner_plan_node_list,
                                                node::PlanNode **output) {
    CHECK_TRUE(nullptr != root, common::kPlanError, "fail to create procedure plan node: query tree node it null")
    CHECK_TRUE(root->GetType() == node::kCreateSpStmt, common::kPlanError,
               "fail to create procedure plan node: query tree node it not kCreateSpStmt")
    const node::CreateSpStmt *create_sp_tree = static_cast<const node::CreateSpStmt *>(root);
    *output = node_manager_->MakeCreateProcedurePlanNode(create_sp_tree->GetSpName(),
                                                         create_sp_tree->GetInputParameterList(), inner_plan_node_list);
    return base::Status::OK();
}

std::string Planner::MakeTableName(const PlanNode *node) const {
    switch (node->GetType()) {
        case node::kPlanTypeTable: {
            const node::TablePlanNode *table_node = dynamic_cast<const node::TablePlanNode *>(node);
            return table_node->table_;
        }
        case node::kPlanTypeRename: {
            const node::RenamePlanNode *table_node = dynamic_cast<const node::RenamePlanNode *>(node);
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
base::Status Planner::CreateTableReferencePlanNode(const node::TableRefNode *root, node::PlanNode **output) {
    node::PlanNode *plan_node = nullptr;
    switch (root->ref_type_) {
        case node::kRefTable: {
            const node::TableNode *table_node = dynamic_cast<const node::TableNode *>(root);
            plan_node = node_manager_->MakeTablePlanNode(table_node->db_, table_node->org_table_name_);
            if (!table_node->alias_table_name_.empty()) {
                *output = node_manager_->MakeRenamePlanNode(plan_node, table_node->alias_table_name_);
            } else {
                *output = plan_node;
            }
            break;
        }
        case node::kRefJoin: {
            const node::JoinNode *join_node = dynamic_cast<const node::JoinNode *>(root);
            node::PlanNode *left = nullptr;
            node::PlanNode *right = nullptr;
            CHECK_STATUS(CreateTableReferencePlanNode(join_node->left_, &left))
            CHECK_STATUS(CreateTableReferencePlanNode(join_node->right_, &right))
            plan_node = node_manager_->MakeJoinNode(left, right, join_node->join_type_, join_node->orders_,
                                                    join_node->condition_);
            if (!join_node->alias_table_name_.empty()) {
                *output = node_manager_->MakeRenamePlanNode(plan_node, join_node->alias_table_name_);
            } else {
                *output = plan_node;
            }
            break;
        }
        case node::kRefQuery: {
            const node::QueryRefNode *sub_query_node = dynamic_cast<const node::QueryRefNode *>(root);
            node::QueryPlanNode* query = nullptr;
            CHECK_STATUS(CreateQueryPlan(sub_query_node->query_, &query))
            plan_node = query;
            if (!sub_query_node->alias_table_name_.empty()) {
                *output = node_manager_->MakeRenamePlanNode(plan_node, sub_query_node->alias_table_name_);
            } else {
                *output = plan_node;
            }
            break;
        }
        default: {
            FAIL_STATUS(common::kPlanError, "fail to create table reference node, unrecognized type ",
                        node::NameOfSqlNodeType(root->GetType()))
        }
    }

    return base::Status::OK();
}
bool Planner::MergeWindows(const std::map<const node::WindowDefNode *, node::ProjectListNode *> &map,
                           std::vector<const node::WindowDefNode *> *windows_ptr) {
    if (nullptr == windows_ptr) {
        return false;
    }
    bool has_window_merged = false;

    // [ (window definition, window id or 0) ]
    std::vector<std::pair<const node::WindowDefNode *, int32_t>> window_id_pairs;
    for (auto it = map.begin(); it != map.end(); it++) {
        window_id_pairs.emplace_back(it->first, nullptr == it->second->GetW() ? 0 : it->second->GetW()->GetId());
    }
    std::sort(window_id_pairs.begin(), window_id_pairs.end(),
              [](const std::pair<const node::WindowDefNode *, int32_t> &p1,
                 const std::pair<const node::WindowDefNode *, int32_t> &p2) -> bool { return p1.second < p2.second; });

    // Merge Rows Frames First
    for (auto iter = window_id_pairs.cbegin(); iter != window_id_pairs.cend(); iter++) {
        if (nullptr != iter->first && iter->first->GetFrame()->IsRowsRangeLikeFrame()) {
            // skip handling range like frames
            continue;
        }
        if (windows_ptr->empty()) {
            windows_ptr->push_back(iter->first);
            continue;
        }
        bool can_be_merged = false;
        for (auto iter_w = windows_ptr->begin(); iter_w != windows_ptr->end(); iter_w++) {
            if (node::SqlEquals(iter->first, *iter_w)) {
                can_be_merged = true;
                has_window_merged = true;
                break;
            }
            if (nullptr == *iter_w) {
                continue;
            }
            if (iter->first->CanMergeWith(*iter_w, enable_window_maxsize_merged_)) {
                can_be_merged = true;
                *iter_w = node_manager_->MergeWindow(iter->first, *iter_w);
                has_window_merged = true;
                break;
            }
        }

        if (!can_be_merged) {
            windows_ptr->push_back(iter->first);
        }
    }

    for (auto iter = window_id_pairs.cbegin(); iter != window_id_pairs.cend(); iter++) {
        if (nullptr == iter->first || !iter->first->GetFrame()->IsRowsRangeLikeFrame()) {
            // skip handling rows frames
            continue;
        }
        if (windows_ptr->empty()) {
            windows_ptr->push_back(iter->first);
            continue;
        }
        bool can_be_merged = false;
        for (auto iter_w = windows_ptr->begin(); iter_w != windows_ptr->end(); iter_w++) {
            if (node::SqlEquals(iter->first, *iter_w)) {
                can_be_merged = true;
                has_window_merged = true;
                break;
            }
            if (nullptr == *iter_w) {
                continue;
            }
            if (iter->first->CanMergeWith(*iter_w, enable_window_maxsize_merged_)) {
                can_be_merged = true;
                *iter_w = node_manager_->MergeWindow(iter->first, *iter_w);
                has_window_merged = true;
                break;
            }
        }

        if (!can_be_merged) {
            windows_ptr->push_back(iter->first);
        }
    }

    return has_window_merged;
}

// win_id passed in for the purpose of creating possible new WindowPlanNode
base::Status Planner::MergeProjectMap(const std::map<const node::WindowDefNode *, node::ProjectListNode *> &map,
                                      std::map<const node::WindowDefNode *, node::ProjectListNode *> *output) {
    if (map.empty()) {
        DLOG(INFO) << "Nothing to merge, project list map is empty";
        *output = map;
        return base::Status::OK();
    }
    std::vector<const node::WindowDefNode *> merged_windows;
    bool flag_merge = MergeWindows(map, &merged_windows);
    bool flag_expand = ExpandCurrentHistoryWindow(&merged_windows);
    if (!flag_merge && !flag_expand) {
        DLOG(INFO) << "No window can be merged or expand";
        *output = map;
        return base::Status::OK();
    }

    int32_t w_id = 1;
    // create the after-window-merge (window->project list) map, with empty project list
    std::map<const node::WindowDefNode*, node::ProjectListNode*> merged_out;
    for (auto iter = merged_windows.cbegin(); iter != merged_windows.cend(); iter++) {
        if (nullptr == *iter) {
            // table project list or row project list
            merged_out.emplace(nullptr, node_manager_->MakeProjectListPlanNode(nullptr, false));
        } else {
            node::WindowPlanNode *w_node_ptr = node_manager_->MakeWindowPlanNode(w_id++);
            CHECK_STATUS(FillInWindowPlanNode(*iter, w_node_ptr))
            merged_out.emplace(*iter, node_manager_->MakeProjectListPlanNode(w_node_ptr, true));
        }
    }

    // add project nodes from map to merged_out, based on whether two window can merged
    for (auto map_iter = map.cbegin(); map_iter != map.cend(); map_iter++) {
        bool merge_ok = false;
        for (auto iter = merged_out.begin(); iter != merged_out.end(); iter++) {
            if (node::SqlEquals(map_iter->first, iter->first) ||
                (nullptr != map_iter->first && map_iter->first->CanMergeWith(iter->first))) {
                auto window_plan_node = iter->second->GetW();
                node::ProjectListNode *merged_project =
                    node_manager_->MakeProjectListPlanNode(window_plan_node, window_plan_node != nullptr);
                node::ProjectListNode::MergeProjectList(iter->second, map_iter->second, merged_project);
                iter->second = merged_project;
                merge_ok = true;
                break;
            }
        }
        CHECK_TRUE(merge_ok, common::kPlanError, "Fail to merge project list")
    }

    *output = merged_out;
    return base::Status::OK();
}

bool Planner::ExpandCurrentHistoryWindow(std::vector<const node::WindowDefNode *> *windows_ptr) {
    if (nullptr == windows_ptr) {
        return false;
    }
    bool has_window_expand = false;
    // merge big history window with current history window
    for (auto iter = windows_ptr->begin(); iter != windows_ptr->end(); iter++) {
        const node::WindowDefNode *w_ptr = *iter;
        if (nullptr != w_ptr && nullptr != w_ptr->GetFrame() && !w_ptr->GetFrame()->IsRowsRangeLikeFrame() &&
            w_ptr->GetFrame()->IsPureHistoryFrame()) {
            node::FrameNode *current_frame = node_manager_->MergeFrameNodeWithCurrentHistoryFrame(w_ptr->GetFrame());
            *iter = dynamic_cast<node::WindowDefNode *>(node_manager_->MakeWindowDefNode(
                w_ptr->union_tables(), w_ptr->GetPartitions(), w_ptr->GetOrders(), current_frame,
                w_ptr->exclude_current_time(), w_ptr->instance_not_in_window()));
            has_window_expand = true;
        }
    }
    return has_window_expand;
}

base::Status Planner::TransformTableDef(const std::string &table_name, const NodePointVector &column_desc_list,
                                        type::TableDef *table) {
    std::set<std::string> index_names;
    std::set<std::string> column_names;

    for (auto column_desc : column_desc_list) {
        switch (column_desc->GetType()) {
            case node::kColumnDesc: {
                node::ColumnDefNode *column_def = static_cast<node::ColumnDefNode *>(column_desc);
                type::ColumnDef *column = table->add_columns();

                CHECK_TRUE(column_names.find(column_def->GetColumnName()) == column_names.end(), common::kPlanError,
                           "CREATE common: COLUMN NAME ", column_def->GetColumnName(), " duplicate")

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
                        FAIL_STATUS(common::kPlanError, "CREATE common: column type ",
                                    node::DataTypeName(column_def->GetColumnType()), " is not supported")
                    }
                }

                column->mutable_schema()->set_base_type(column->type());
                break;
            }

            case node::kColumnIndex: {
                node::ColumnIndexNode *column_index = static_cast<node::ColumnIndexNode *>(column_desc);

                if (column_index->GetName().empty()) {
                    column_index->SetName(PlanAPI::GenerateName("INDEX", table->indexes_size()));
                }
                CHECK_TRUE(index_names.find(column_index->GetName()) == index_names.end(), common::kPlanError,
                           "CREATE common: INDEX NAME ", column_index->GetName(), " duplicate")

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
                FAIL_STATUS(common::kPlanError,
                            "can not support " + node::NameOfSqlNodeType(column_desc->GetType()) + " when CREATE TABLE")
            }
        }
    }
    table->set_name(table_name);
    return base::Status::OK();
}

// restriction rules:
// 1. offset in lag function must be constant
absl::StatusOr<node::WindowDefNode *> Planner::ConstructWindowForLag(const node::WindowDefNode *in,
                                                                     const node::CallExprNode *call) const {
    if (call->GetChildNum() <= 1) {
        return absl::InvalidArgumentError(
            absl::StrCat("expect offset as second parameter for function ", call->GetFnDef()->GetName()));
    }

    auto *offset_expr = call->GetChild(1);
    if (offset_expr->GetExprType() != node::ExprType::kExprPrimary) {
        return absl::InvalidArgumentError("offset can only be constant");
    }

    if (in->GetFrame()->frame_type() != node::FrameType::kFrameRows &&
        in->GetFrame()->frame_type() != node::FrameType::kFrameRowsRange) {
        return absl::InvalidArgumentError("input window is not a ROWS or ROWS_RANGE window");
    }

    auto *const_node = dynamic_cast<node::ConstNode *>(offset_expr);
    int64_t offset = const_node->GetAsInt64();

    auto *rows_frame_ext =
        node_manager_->MakeFrameExtent(node_manager_->MakeFrameBound(node::BoundType::kPreceding, offset),
                                       node_manager_->MakeFrameBound(node::BoundType::kCurrent));

    node::FrameNode *new_frame = in->GetFrame()->ShadowCopy(node_manager_);
    new_frame->set_frame_type(node::FrameType::kFrameRows);
    new_frame->SetFrameRows(rows_frame_ext);
    new_frame->SetFrameRange(nullptr);
    new_frame->set_frame_maxsize(0);
    // EXCLUDE CURRENT_ROW does not apply to lag
    new_frame->exclude_current_row_ = false;

    auto *new_win = in->ShadowCopy(node_manager_);
    new_win->SetFrame(new_frame);
    return new_win;
}

}  // namespace plan
}  // namespace hybridse
