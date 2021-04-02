/*
 * sql_plan_factory.cc.c
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
#include "plan/plan_api.h"
#include "parser/parser.h"
#include "plan/planner.h"

namespace hybridse {
namespace plan {
using hybridse::plan::SimplePlanner;
bool PlanAPI::CreatePlanTreeFromScript(const std::string &sql,
                                       PlanNodeList &plan_trees,
                                       NodeManager *node_manager,
                                       Status &status) {
    hybridse::node::NodePointVector parser_trees;
    if (!CreateSyntaxTreeFromScript(sql, parser_trees, node_manager, status)) {
        return false;
    }
    return CreatePlanTreeFromSyntaxTree(parser_trees, plan_trees, node_manager,
                                        status);
}

bool PlanAPI::CreatePlanTreeFromSyntaxTree(const NodePointVector &parser_trees,
                                           PlanNodeList &plan_trees,
                                           NodeManager *node_manager,
                                           Status &status) {
    SimplePlanner simplePlanner(node_manager);
    if (common::kOk !=
            simplePlanner.CreatePlanTree(parser_trees, plan_trees, status) ||
        !status.isOK()) {
        LOG(WARNING) << "Fail to create plan tree: " << status;
        return false;
    }
    return true;
}

bool PlanAPI::CreateSyntaxTreeFromScript(const std::string &sql,
                                         NodePointVector &parser_trees,
                                         NodeManager *node_manager,
                                         Status &status) {
    hybridse::parser::HybridSeParser parser;
    parser.parse(sql, parser_trees, node_manager, status);
    if (common::kOk != status.code) {
        LOG(WARNING) << "Fail to create syntax tree: " << status;
        return false;
    }
    return true;
}

const std::string PlanAPI::GenerateName(const std::string prefix, int id) {
    time_t t;
    time(&t);
    std::string name =
        prefix + "_" + std::to_string(id) + "_" + std::to_string(t);
    return name;
}

}  // namespace plan
}  // namespace hybridse
