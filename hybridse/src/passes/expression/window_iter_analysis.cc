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

#include "passes/expression/window_iter_analysis.h"
#include <string>
#include <unordered_map>
#include <vector>

namespace hybridse {
namespace passes {

using ::hybridse::common::kCodegenError;

Status WindowIterAnalysis::VisitFunctionLet(const node::ExprIdNode* row_arg,
                                            const node::ExprIdNode* window_arg,
                                            const node::ExprNode* body) {
    this->row_arg_ = row_arg;
    this->window_arg_ = window_arg;
    if (this->window_arg_ != nullptr) {
        SetRank(this->window_arg_, {1, true});
    }
    for (size_t i = 0; i < body->GetChildNum(); ++i) {
        WindowIterRank rank;
        CHECK_STATUS(VisitExpr(body->GetChild(i), &rank));
    }
    return Status::OK();
}

Status WindowIterAnalysis::VisitExpr(node::ExprNode* expr,
                                     WindowIterRank* rank) {
    CHECK_TRUE(expr != nullptr && rank != nullptr, kCodegenError);
    if (GetRank(expr, rank)) {
        return Status::OK();
    }

    switch (expr->GetExprType()) {
        case node::kExprColumnRef: {
            rank->rank = 1;
            rank->is_iter = true;
            break;
        }
        case node::kExprCall: {
            auto call = dynamic_cast<node::CallExprNode*>(expr);
            std::vector<WindowIterRank> arg_ranks(call->GetChildNum());
            for (size_t i = 0; i < call->GetChildNum(); ++i) {
                CHECK_STATUS(VisitExpr(expr->GetChild(i), &arg_ranks[i]));
            }
            CHECK_STATUS(VisitCall(call->GetFnDef(), arg_ranks, rank));
            break;
        }
        default: {
            size_t max_rank = 0;
            for (size_t i = 0; i < expr->GetChildNum(); ++i) {
                WindowIterRank child_rank;
                CHECK_STATUS(VisitExpr(expr->GetChild(i), &child_rank));
                max_rank =
                    child_rank.rank > max_rank ? child_rank.rank : max_rank;
            }
            rank->rank = max_rank;
            rank->is_iter = false;
        }
    }
    SetRank(expr, *rank);
    return Status::OK();
}

Status WindowIterAnalysis::VisitCall(
    node::FnDefNode* fn, const std::vector<WindowIterRank>& arg_ranks,
    WindowIterRank* rank) {
    CHECK_TRUE(arg_ranks.size() == fn->GetArgSize(), kCodegenError,
               "Incompatible arg num: ", arg_ranks.size(), ", ",
               fn->GetArgSize(), " function name ", fn->GetFlatString());
    switch (fn->GetType()) {
        case node::kUdafDef: {
            auto udaf = dynamic_cast<node::UdafDefNode*>(fn);
            WindowIterRank update_rank;
            CHECK_STATUS(VisitUdaf(udaf, &update_rank));

            bool is_window_iter = false;
            for (auto arg_rank : arg_ranks) {
                is_window_iter |= arg_rank.is_iter;
            }

            size_t max_rank = 0;
            if (is_window_iter && update_rank.rank > 0) {
                max_rank = update_rank.rank + 1;
            } else {
                max_rank = update_rank.rank;
            }

            for (auto arg_rank : arg_ranks) {
                size_t cur_rank;
                if (!is_window_iter || arg_rank.is_iter || arg_rank.rank == 0) {
                    cur_rank = arg_rank.rank;
                } else {
                    cur_rank = arg_rank.rank + 1;
                }
                max_rank = cur_rank > max_rank ? cur_rank : max_rank;
            }
            rank->rank = max_rank;
            rank->is_iter = false;
            break;
        }
        case node::kLambdaDef: {
            auto lambda = dynamic_cast<node::LambdaNode*>(fn);
            CHECK_STATUS(VisitLambdaCall(lambda, arg_ranks, rank));
            break;
        }
        default: {
            size_t max_rank = 0;
            for (auto arg_rank : arg_ranks) {
                max_rank = arg_rank.rank > max_rank ? arg_rank.rank : max_rank;
            }
            rank->rank = max_rank;
            rank->is_iter = false;
            break;
        }
    }
    return Status::OK();
}

Status WindowIterAnalysis::VisitUdaf(node::UdafDefNode* udaf,
                                     WindowIterRank* rank) {
    auto update = udaf->update_func();
    std::vector<WindowIterRank> arg_ranks(update->GetArgSize());
    return VisitCall(update, arg_ranks, rank);
}

Status WindowIterAnalysis::VisitLambdaCall(
    node::LambdaNode* lambda, const std::vector<WindowIterRank>& arg_ranks,
    WindowIterRank* rank) {
    Status status;
    EnterLambdaScope();
    for (size_t i = 0; i < arg_ranks.size(); ++i) {
        SetRank(lambda->GetArg(i), arg_ranks[i]);
    }
    status = VisitExpr(lambda->body(), rank);
    ExitLambdaScope();
    return status;
}

void WindowIterAnalysis::EnterLambdaScope() {
    scope_cache_list_.emplace_back(ScopeCache());
}

void WindowIterAnalysis::ExitLambdaScope() { scope_cache_list_.pop_back(); }

bool WindowIterAnalysis::GetRank(const node::ExprNode* expr,
                                 WindowIterRank* rank) const {
    if (expr == nullptr) {
        return false;
    }
    auto expr_id = dynamic_cast<const node::ExprIdNode*>(expr);
    if (expr_id != nullptr && expr_id->GetId() < 0) {
        return false;
    }
    for (auto iter = scope_cache_list_.rbegin();
         iter != scope_cache_list_.rend(); ++iter) {
        auto& cache = *iter;
        if (expr_id) {
            auto iter = cache.arg_dict.find(expr_id->GetId());
            if (iter != cache.arg_dict.end()) {
                *rank = iter->second;
                return true;
            }
        } else {
            auto iter = cache.expr_dict.find(expr->node_id());
            if (iter != cache.expr_dict.end()) {
                *rank = iter->second;
                return true;
            }
        }
    }
    return false;
}

void WindowIterAnalysis::SetRank(const node::ExprNode* expr,
                                 const WindowIterRank& rank) {
    auto& cache = scope_cache_list_.back();
    if (expr->GetExprType() == node::kExprId) {
        auto expr_id = dynamic_cast<const node::ExprIdNode*>(expr);
        cache.arg_dict[expr_id->GetId()] = rank;
    } else {
        cache.expr_dict[expr->node_id()] = rank;
    }
}

}  // namespace passes
}  // namespace hybridse
