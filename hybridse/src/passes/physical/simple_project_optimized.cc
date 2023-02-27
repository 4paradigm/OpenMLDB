/*
 * Copyright 2021 4paradigm
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
#include "passes/physical/simple_project_optimized.h"
#include <vector>

#include "absl/base/attributes.h"

namespace hybridse {
namespace passes {

static Status BuildColumnMapping(
    const node::ExprNode* outer_expr,
    const std::vector<node::ExprNode*>& inner_projects,
    const vm::SchemasContext* schemas_ctx, passes::ExprReplacer* replacer);

bool SimpleProjectOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;

    if (in->GetProducerCnt() > 0 && nullptr != in->producers()[0]) {
        if (in->GetOpType() == vm::kPhysicalOpSimpleProject) {
            switch (in->producers()[0]->GetOpType()) {
                case vm::kPhysicalOpSimpleProject: {
                    DLOG(INFO) << "Find consecutive simple projects";
                    auto nm = plan_ctx_->node_manager();
                    auto outer_project_op = dynamic_cast<vm::PhysicalSimpleProjectNode*>(in);
                    auto inner_project_op = dynamic_cast<vm::PhysicalSimpleProjectNode*>(in->GetProducer(0));

                    auto outer_projects = outer_project_op->project();
                    std::vector<node::ExprNode*> outer_exprs;
                    for (size_t i = 0; i < outer_projects.size(); ++i) {
                        outer_exprs.push_back(outer_projects.GetExpr(i)->DeepCopy(nm));
                    }

                    auto inner_projects = inner_project_op->project();
                    std::vector<node::ExprNode*> inner_exprs;
                    for (size_t i = 0; i < inner_projects.size(); ++i) {
                        inner_exprs.push_back(inner_projects.GetExpr(i)->DeepCopy(nm));
                    }

                    Status status;
                    passes::ExprReplacer replacer;
                    for (size_t i = 0; i < outer_projects.size(); ++i) {
                        status =
                            BuildColumnMapping(outer_exprs[i], inner_exprs, inner_project_op->schemas_ctx(), &replacer);
                        if (!status.isOK()) {
                            LOG(WARNING) << "Fail to merge simple projects: " << status;
                            return false;
                        }
                    }

                    vm::ColumnProjects new_projects;
                    for (size_t i = 0; i < outer_projects.size(); ++i) {
                        node::ExprNode* new_expr;
                        status = replacer.Replace(outer_exprs[i], &new_expr);
                        if (!status.isOK()) {
                            LOG(WARNING) << "Fail to merge simple projects: " << status;
                            return false;
                        }
                        new_projects.Add(outer_projects.GetName(i), new_expr, outer_projects.GetFrame(i));
                    }
                    vm::PhysicalSimpleProjectNode* new_project_op = nullptr;
                    status = plan_ctx_->CreateOp<vm::PhysicalSimpleProjectNode>(
                        &new_project_op, inner_project_op->GetProducer(0), new_projects);
                    if (!status.isOK()) {
                        LOG(WARNING) << "Fail to merge simple projects: " << status;
                        return false;
                    }
                    *output = new_project_op;
                    return true;
                }
                default:
                    break;
            }
        }
    }
    return false;
}

static Status BuildColumnMapping(
    const node::ExprNode* outer_expr,
    const std::vector<node::ExprNode*>& inner_projects,
    const vm::SchemasContext* schemas_ctx, passes::ExprReplacer* replacer) {
    for (size_t i = 0; i < outer_expr->GetChildNum(); ++i) {
        CHECK_STATUS(BuildColumnMapping(outer_expr->GetChild(i), inner_projects,
                                        schemas_ctx, replacer));
    }
    switch (outer_expr->GetExprType()) {
        case node::kExprColumnRef: {
            auto col_ref = dynamic_cast<const node::ColumnRefNode*>(outer_expr);
            size_t schema_idx;
            size_t col_idx;
            schemas_ctx->ResolveColumnRefIndex(col_ref, &schema_idx, &col_idx);
            CHECK_TRUE(schema_idx == 0, common::kPlanError,
                       "Simple project should output single schema");
            CHECK_TRUE(col_idx < inner_projects.size(), common::kPlanError,
                       "Column index out of bound");

            auto repl = inner_projects[col_idx];
            replacer->AddReplacement(col_ref, repl);
            break;
        }
        default:
            break;
    }
    return Status::OK();
}
}  // namespace passes
}  // namespace hybridse
