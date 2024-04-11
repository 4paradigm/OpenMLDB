/*
 * sql_plan_factory.h
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
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
#ifndef HYBRIDSE_INCLUDE_PLAN_PLAN_API_H_
#define HYBRIDSE_INCLUDE_PLAN_PLAN_API_H_

#include <string>
#include <unordered_map>

#include "node/node_manager.h"
#include "vm/sql_ctx.h"

namespace hybridse {
namespace plan {

using hybridse::base::Status;
using hybridse::node::NodeManager;
using hybridse::node::NodePointVector;
using hybridse::node::PlanNodeList;

// TODO(someone): rm class PlanAPI
class PlanAPI {
 public:
    // parse SQL string to logic plan. ASTNode and LogicNode saved in SqlContext
    static base::Status CreatePlanTreeFromScript(vm::SqlContext* ctx);

    // deprecated, use CreatePlanTreeFromScript(vm::SqlContext*) instead
    static bool CreatePlanTreeFromScript(const std::string& sql,
                                         PlanNodeList& plan_trees,  // NOLINT
                                         NodeManager* node_manager,
                                         Status& status,  // NOLINT (runtime/references)
                                         bool is_batch_mode = true, bool is_cluster = false,
                                         bool enable_batch_window_parallelization = false,
                                         const std::unordered_map<std::string, std::string>* extra_options = nullptr);

    static const int GetPlanLimitCount(node::PlanNode* plan_trees);
    static const std::string GenerateName(const std::string prefix, int id);
};

// Parse the input str and SQL type and convert to TypeNode representation
//
// unimplemnted, reserved for later usage
absl::StatusOr<node::TypeNode*> ParseType(absl::string_view, NodeManager*);

// parse the input string as table elements and extract those element that is table_column_definition,
// then returns the corresponding proto representation.
//
// it expect input `str` joined every element by comma(,), then a CREATE TABLE SQL is created with the
// format of 'CREATE TABLE t1 ( {str} )'.
// SQL parse allows three kind of table element, which is:
// - table_column_definition
// - table_index_definition
// - table_constraint_definition
// while this method extract table_column_definition only
absl::StatusOr<codec::Schema> ParseTableColumSchema(absl::string_view str);
}  // namespace plan
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_PLAN_PLAN_API_H_
