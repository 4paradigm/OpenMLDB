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
#include <string>
#include <vector>
#include "passes/physical/long_window_optimized.h"
#include "vm/engine.h"

namespace hybridse {
namespace passes {

LongWindowOptimized::LongWindowOptimized(PhysicalPlanContext* plan_ctx) : TransformUpPysicalPass(plan_ctx) {
    std::vector<std::string> windows;
    const auto* options = plan_ctx_->GetOptions();
    if (!options) {
        LOG(ERROR) << "plan_ctx option is empty";
        return;
    }

    boost::split(windows, options->at(vm::LONG_WINDOWS), boost::is_any_of(","));
    for (auto& w : windows) {
        std::vector<std::string> window_info;
        boost::split(window_info, w, boost::is_any_of(":"));
        boost::trim(window_info[0]);
        long_windows_.insert(window_info[0]);
    }
}

bool LongWindowOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    if (vm::kPhysicalOpProject != in->GetOpType()) {
        return false;
    }

    if (long_windows_.empty()) {
        LOG(ERROR) << "Long Windows is empty";
        return false;
    }

    if (vm::kPhysicalOpProject == in->GetOpType()) {
        auto project_op = dynamic_cast<vm::PhysicalProjectNode*>(in);
        if (project_op->project_type_ == vm::kAggregation) {
            auto project_aggr_op = dynamic_cast<vm::PhysicalAggrerationNode*>(project_op);
            const auto& projects = project_aggr_op->project();
            //           auto fn = projects.fn_info().fn_def();
            //           auto* expr_list = dynamic_cast<node::ExprListNode*>(fn->body());
            //           for (auto expr : expr_list->children_) {
            //               LOG(WARNING) << "fn expr " << expr->GetExprString() << "; " <<
            //               ExprTypeName(expr->GetExprType());
            //           }
            //           LOG(WARNING) << "fn = " << project_aggr_op->GetFnInfos()[0]->fn_name() << ": " <<
            //           fn->GetTreeString();
            for (int i = 0; i < projects.size(); i++) {
                const auto* expr = projects.GetExpr(i);
                if (expr->GetExprType() == node::kExprCall) {
                    LOG(WARNING) << "expr call = " << expr->GetExprString();
                    const auto* call_expr = dynamic_cast<const node::CallExprNode*>(expr);
                    const auto* window = call_expr->GetOver();
                    if (window == nullptr) continue;

                    // skip ANONYMOUS_WINDOW
                    if (!window->GetName().empty()) {
                        LOG(WARNING) << "func name = " << call_expr->GetFnDef()->GetName()
                                     << ", win name = " << window->GetName();
                    }
                } else {
                    LOG(WARNING) << "non expr call = " << expr->GetExprString() << "; "
                                 << ExprTypeName(expr->GetExprType());
                }
            }
        }
    }

    return true;
}

}  // namespace passes
}  // namespace hybridse
