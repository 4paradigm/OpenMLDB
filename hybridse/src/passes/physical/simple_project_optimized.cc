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

template <typename ProjNode, std::enable_if_t<std::is_base_of_v<vm::PhysicalOpNode, ProjNode>, int> = 0>
ABSL_MUST_USE_RESULT Status TryRemoveUnnecessaryLastJoin(PhysicalPlanContext* plan_ctx, vm::PhysicalOpNode* in,
                                                         PhysicalOpNode* depend, PhysicalOpNode** output) {
    CHECK_TRUE(in != nullptr && depend != nullptr, common::kPhysicalPlanError, "parameter is null");

    if (depend->GetOpType() == vm::kPhysicalOpJoin) {
        auto* inner_join = dynamic_cast<vm::PhysicalJoinNode*>(depend);
        CHECK_TRUE(inner_join->join_.join_type() == node::kJoinTypeLast, common::kPhysicalPlanError,
                   "can't optimize non-lastjoin node");
    } else if (in->GetProducer(0)->GetOpType() == vm::kPhysicalOpRequestJoin) {
        auto* inner_join = dynamic_cast<vm::PhysicalRequestJoinNode*>(depend);
        CHECK_TRUE(inner_join->join_.join_type() == node::kJoinTypeLast, common::kPhysicalPlanError,
                   "can't optimize non-lastjoin node");

    } else {
        FAIL_STATUS(common::kPhysicalPlanError, "not join node");
    }

    auto* prj = dynamic_cast<ProjNode*>(in);
    CHECK_TRUE(prj != nullptr, common::kPhysicalPlanError, "unmatched prject type");

    auto* sc = depend->schemas_ctx();
    auto& prj_info = prj->project();

    std::vector<const node::ExprNode*> col_ref_list;
    for (decltype(prj_info.size()) i = 0; i < prj_info.size(); ++i) {
        CHECK_STATUS(vm::DoSearchExprDependentColumns(prj_info.GetExpr(i), &col_ref_list));
    }

    bool prj_depends_on_left_only = true;
    for (auto expr : col_ref_list) {
        switch (expr->GetExprType()) {
            case node::kExprColumnRef: {
                auto* col_ref = dynamic_cast<const node::ColumnRefNode*>(expr);

                size_t sch_idx, col_idx;
                CHECK_STATUS(sc->ResolveColumnRefIndex(col_ref, &sch_idx, &col_idx));

                if (sch_idx != 0) {
                    prj_depends_on_left_only = false;
                }
                break;
            }
            default:
                break;
        }

        if (!prj_depends_on_left_only) {
            break;
        }
    }

    if (prj_depends_on_left_only) {
        // do the optimize
        DLOG(INFO) << "removing right source of last join since non-projects depends on";

        auto* left = depend->GetProducer(0);

        ProjNode* new_project_op = nullptr;
        CHECK_STATUS(plan_ctx->CreateOp(&new_project_op, left, prj->project()));
        *output = new_project_op;
    }
    return Status::OK();
}

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
                case vm::kPhysicalOpJoin: {
                    auto s = TryRemoveUnnecessaryLastJoin<vm::PhysicalSimpleProjectNode>(plan_ctx_, in,
                                                                                         in->GetProducer(0), output);
                    if (!s.isOK()) {
                        LOG(WARNING) << s;
                        return false;
                    }

                    return true;
                }
                default:
                    break;
            }
        } else if (in->GetOpType() == vm::kPhysicalOpProject) {
            auto* prj = dynamic_cast<vm::PhysicalProjectNode*>(in);
            Status s;

            if (prj->project_type_ == vm::kRowProject) {
                    s = TryRemoveUnnecessaryLastJoin<vm::PhysicalRowProjectNode>(plan_ctx_, prj, prj->GetProducer(0),
                                                                                 output);
            } else if (prj->project_type_ == vm::kTableProject) {
                    s = TryRemoveUnnecessaryLastJoin<vm::PhysicalTableProjectNode>(plan_ctx_, prj, prj->GetProducer(0),
                                                                                   output);
            } else {
                    return false;
            }

            if (!s.isOK()) {
                LOG(WARNING) << s;
                return false;
            }

            return true;
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
