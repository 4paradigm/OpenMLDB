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

#include "planv2/planner_v2.h"
#include <algorithm>
#include <map>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "planv2/ast_node_converter.h"
#include "proto/fe_common.pb.h"

namespace hybridse {
namespace plan {
int SimplePlannerV2::CreateASTScriptPlan(const zetasql::ASTScript *script, PlanNodeList &plan_trees,
                                         Status &status) {  // NOLINT (runtime/references)
    if (nullptr == script) {
        status.msg = "fail to create plan tree: ASTScript is null";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return status.code;
    }

    for (auto statement : script->statement_list()) {
        if (!statement->IsSqlStatement()) {
            status.msg = "fail to create plan tree: statement is illegal, sql statement is required";
            status.code = common::kPlanError;
            LOG(WARNING) << status;
            return status.code;
        }
        switch (statement->node_kind()) {
            case zetasql::AST_QUERY_STATEMENT: {
                const zetasql::ASTQueryStatement *query_statement =
                    statement->GetAsOrNull<zetasql::ASTQueryStatement>();
                if (query_statement == nullptr) {
                    status.msg = "fail to create plan tree: query statement is illegal";
                    status.code = common::kPlanError;
                    LOG(WARNING) << status;
                    return status.code;
                }

                PlanNode *query_plan = nullptr;

                status = CreateASTQueryPlan(query_statement->query(), &query_plan);
                if (!status.isOK()) {
                    LOG(WARNING) << status;
                    return status.code;
                }

                if (!is_batch_mode_) {
                    // return false if Primary path check fail
                    ::hybridse::node::PlanNode *primary_node;
                    if (!ValidatePrimaryPath(query_plan, &primary_node, status)) {
                        DLOG(INFO) << "primay check fail, logical plan:\n" << *query_plan;
                        return status.code;
                    }
                    dynamic_cast<node::TablePlanNode *>(primary_node)->SetIsPrimary(true);
                    DLOG(INFO) << "plan after primary check:\n" << *query_plan;
                }

                plan_trees.push_back(query_plan);
                break;
            }
                //            case node::kCreateStmt: {
                //                PlanNode *create_plan = nullptr;
                //                if (!CreateCreateTablePlan(parser_tree, &create_plan, status)) {
                //                    return status.code;
                //                }
                //                plan_trees.push_back(create_plan);
                //                break;
                //            }
                //            case node::kCreateSpStmt: {
                //                PlanNode *create_sp_plan = nullptr;
                //                PlanNodeList inner_plan_node_list;
                //                const node::CreateSpStmt *create_sp_tree =
                //                    (const node::CreateSpStmt *)parser_tree;
                //                if (CreatePlanTree(create_sp_tree->GetInnerNodeList(),
                //                                   inner_plan_node_list,
                //                                   status) != common::StatusCode::kOk) {
                //                    return status.code;
                //                }
                //                if (!CreateCreateProcedurePlan(parser_tree,
                //                                               inner_plan_node_list,
                //                                               &create_sp_plan, status)) {
                //                    return status.code;
                //                }
                //                plan_trees.push_back(create_sp_plan);
                //                break;
                //            }
                //            case node::kCmdStmt: {
                //                node::PlanNode *cmd_plan = nullptr;
                //                if (!CreateCmdPlan(parser_tree, &cmd_plan, status)) {
                //                    return status.code;
                //                }
                //                plan_trees.push_back(cmd_plan);
                //                break;
                //            }
                //            case node::kInsertStmt: {
                //                node::PlanNode *insert_plan = nullptr;
                //                if (!CreateInsertPlan(parser_tree, &insert_plan, status)) {
                //                    return status.code;
                //                }
                //                plan_trees.push_back(insert_plan);
                //                break;
                //            }
                //            case ::hybridse::node::kFnDef: {
                //                node::PlanNode *fn_plan = nullptr;
                //                if (!CreateFuncDefPlan(parser_tree, &fn_plan, status)) {
                //                    return status.code;
                //                }
                //                plan_trees.push_back(fn_plan);
                //                break;
                //            }
            default: {
                status.msg = "can not handle statement " + statement->GetNodeKindString();
                status.code = common::kPlanError;
                LOG(WARNING) << status;
                return status.code;
                //            }
            }
        }
    }

    return status.code;
}

base::Status SimplePlannerV2::CreateASTQueryPlan(const zetasql::ASTQuery *root, PlanNode **plan_tree) {
    base::Status status;
    if (nullptr == root) {
        status.msg = "can not create query plan node with null query node";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return status;
    }
    node::QueryNode *query_node = nullptr;
    CHECK_STATUS(ConvertQueryNode(root, node_manager_, &query_node))
    std::cout << *query_node << std::endl;
    if (CreateQueryPlan(query_node, plan_tree, status)) {
        return base::Status::OK();
    } else {
        return status;
    }
}

}  // namespace plan
}  // namespace hybridse
