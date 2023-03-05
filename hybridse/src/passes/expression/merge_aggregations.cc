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

#include "passes/expression/merge_aggregations.h"

#include <set>
#include <utility>

#include "passes/expression/window_iter_analysis.h"

namespace hybridse {
namespace passes {

using hybridse::common::kPlanError;
using hybridse::node::ExprIdNode;
using hybridse::node::ExprNode;

bool IsCandidate(const WindowIterAnalysis& window_iter_analyzer,
                 const ExprIdNode* window, ExprNode* expr) {
    if (expr->GetExprType() != node::kExprCall) {
        return false;
    }
    auto call = dynamic_cast<node::CallExprNode*>(expr);
    if (call->GetFnDef()->GetType() != node::kUdafDef) {
        return false;
    }
    auto udaf = dynamic_cast<node::UdafDefNode*>(call->GetFnDef());
    if (udaf->merge_func() != nullptr) {
        return false;
    }
    WindowIterRank rank;
    if (!window_iter_analyzer.GetRank(call, &rank)) {
        return false;
    }
    if (rank.rank != 1 || rank.is_iter) {
        return false;
    }
    bool find_window_arg = false;
    for (size_t i = 0; i < call->GetChildNum(); ++i) {
        auto arg = dynamic_cast<ExprIdNode*>(call->GetChild(i));
        if (arg != nullptr) {
            if (arg->GetId() != window->GetId()) {
                LOG(WARNING) << "Non-window argument in aggregation call";
                return false;
            }
            if (find_window_arg) {
                LOG(WARNING) << "Multiple window argument in aggregation call";
                return false;
            }
            find_window_arg = true;
        } else {
            auto dtype = call->GetChild(i)->GetOutputType();
            if (dtype == nullptr || dtype->base() == node::kList) {
                return false;
            }
        }
    }
    return find_window_arg;
}

/**
 * Find all rank-1 udaf call to be merged.
 */
Status CollectUdafCalls(const WindowIterAnalysis& window_iter_analyzer,
                        const ExprIdNode* window, ExprNode* expr,
                        std::set<size_t>* visisted,
                        std::vector<ExprNode*>* candidates) {
    if (visisted->find(expr->node_id()) != visisted->end()) {
        return Status::OK();
    }
    visisted->insert(expr->node_id());
    if (IsCandidate(window_iter_analyzer, window, expr)) {
        candidates->push_back(expr);
        return Status::OK();
    }
    for (size_t i = 0; i < expr->GetChildNum(); ++i) {
        CHECK_STATUS(CollectUdafCalls(window_iter_analyzer, window,
                                      expr->GetChild(i), visisted, candidates));
    }
    return Status::OK();
}

Status ApplyArgs(node::FnDefNode* func, const std::vector<ExprNode*>& args,
                 node::NodeManager* nm, ExprNode** output) {
    if (func->GetType() == node::kLambdaDef) {
        auto lambda = dynamic_cast<node::LambdaNode*>(func);
        CHECK_TRUE(args.size() == func->GetArgSize(), kPlanError);
        ExprReplacer replacer;
        for (size_t i = 0; i < lambda->GetArgSize(); ++i) {
            replacer.AddReplacement(lambda->GetArg(i), args[i]);
        }
        CHECK_STATUS(replacer.Replace(lambda->body(), output));
    } else {
        *output = nm->MakeFuncNode(func, args, nullptr);
    }
    return Status::OK();
}

Status MergeUdafCalls(const std::vector<ExprNode*>& calls, ExprIdNode* window,
                      node::NodeManager* nm, ExprNode** output) {
    std::vector<node::UdafDefNode*> udafs;
    for (auto expr : calls) {
        udafs.push_back(dynamic_cast<node::UdafDefNode*>(
            dynamic_cast<node::CallExprNode*>(expr)->GetFnDef()));
    }

    // udaf idx -> (sub arg idx -> new arg idx)
    std::vector<std::vector<size_t>> arg_mappings(udafs.size());

    // sub state idx range in new udaf states
    std::vector<std::pair<size_t, size_t>> state_range;

    // reserve argument position 0 for merged state, position 1 for input row
    std::vector<node::ExprIdNode*> new_update_args;
    node::TypeNode* new_state_type = nm->MakeTypeNode(node::kTuple);
    ExprIdNode* new_state = nm->MakeExprIdNode("merged_state");
    new_state->SetOutputType(new_state_type);
    new_state->SetNullable(false);
    new_update_args.push_back(new_state);

    ExprIdNode* new_row = nm->MakeExprIdNode("row");
    new_row->SetOutputType(window->GetOutputType()->GetGenericType(0));
    new_row->SetNullable(false);
    new_update_args.push_back(new_row);

    // build arguments mappings
    for (size_t i = 0; i < udafs.size(); ++i) {
        auto sub_update = udafs[i]->update_func();
        size_t arg_size = sub_update->GetArgSize();
        CHECK_TRUE(arg_size > 1 && arg_size - 1 == calls[i]->GetChildNum(),
                   kPlanError);
        auto state_type = sub_update->GetArgType(0);
        CHECK_TRUE(state_type != nullptr, kPlanError,
                   "Argument type not known");

        // collect state mappings
        size_t begin_offset = new_state_type->generics_.size();
        if (state_type->base() == node::kTuple) {
            for (size_t j = 0; j < state_type->GetGenericSize(); ++j) {
                new_state_type->AddGeneric(state_type->GetGenericType(j),
                                           state_type->IsGenericNullable(j));
            }
            state_range.push_back(std::make_pair(
                begin_offset, begin_offset + state_type->GetGenericSize()));
        } else {
            new_state_type->AddGeneric(state_type, false);
            state_range.push_back(
                std::make_pair(begin_offset, begin_offset + 1));
        }

        // collect non-state arg mappings
        size_t arg_offset = new_update_args.size();
        arg_mappings[i].resize(arg_size - 1);
        for (size_t j = 1; j < arg_size; ++j) {
            auto arg = dynamic_cast<ExprIdNode*>(calls[i]->GetChild(j - 1));
            if (arg != nullptr && arg->GetId() == window->GetId()) {
                arg_mappings[i][j - 1] = 1;
            } else {
                arg_mappings[i][j - 1] = arg_offset;
                auto new_arg = nm->MakeExprIdNode("arg_" + std::to_string(i) +
                                                  "_" + std::to_string(j));
                new_arg->SetOutputType(sub_update->GetArgType(j));
                new_arg->SetNullable(sub_update->IsArgNullable(j));
                new_update_args.push_back(new_arg);
                arg_offset += 1;
            }
        }
    }

    // build update function
    std::vector<ExprNode*> sub_update_results;
    for (size_t i = 0; i < udafs.size(); ++i) {
        std::vector<ExprNode*> sub_update_args;
        size_t state_begin_idx = state_range[i].first;
        size_t state_end_idx = state_range[i].second;
        bool is_tuple = state_end_idx - state_begin_idx > 1;
        if (is_tuple) {
            std::vector<ExprNode*> tuple;
            for (size_t j = state_begin_idx; j < state_end_idx; ++j) {
                tuple.push_back(nm->MakeGetFieldExpr(new_state, j));
            }
            sub_update_args.push_back(
                nm->MakeFuncNode("make_tuple", tuple, nullptr));
        } else {
            sub_update_args.push_back(
                nm->MakeGetFieldExpr(new_state, state_begin_idx));
        }
        auto update_func = udafs[i]->update_func();
        for (size_t j = 1; j < update_func->GetArgSize(); ++j) {
            sub_update_args.push_back(new_update_args[arg_mappings[i][j - 1]]);
        }
        ExprNode* sub_call = nullptr;
        CHECK_STATUS(ApplyArgs(update_func, sub_update_args, nm, &sub_call));
        if (is_tuple) {
            for (size_t j = state_begin_idx; j < state_end_idx; ++j) {
                sub_update_results.push_back(
                    nm->MakeGetFieldExpr(sub_call, j - state_begin_idx));
            }
        } else {
            sub_update_results.push_back(sub_call);
        }
    }
    auto update_results =
        nm->MakeFuncNode("make_tuple", sub_update_results, nullptr);
    auto new_update_func = nm->MakeLambdaNode(new_update_args, update_results);

    // build output function
    ExprIdNode* final_new_state = nm->MakeExprIdNode("merged_state");
    final_new_state->SetOutputType(new_state_type);
    final_new_state->SetNullable(false);

    std::vector<ExprNode*> sub_output_results;
    for (size_t i = 0; i < udafs.size(); ++i) {
        ExprNode* sub_output_arg;
        size_t state_begin_idx = state_range[i].first;
        size_t state_end_idx = state_range[i].second;
        bool is_tuple = state_end_idx - state_begin_idx > 1;
        if (is_tuple) {
            std::vector<ExprNode*> tuple;
            for (size_t j = state_begin_idx; j < state_end_idx; ++j) {
                tuple.push_back(nm->MakeGetFieldExpr(final_new_state, j));
            }
            sub_output_arg = nm->MakeFuncNode("make_tuple", tuple, nullptr);
        } else {
            sub_output_arg =
                nm->MakeGetFieldExpr(final_new_state, state_begin_idx);
        }
        auto output_func = udafs[i]->output_func();
        if (output_func == nullptr) {
            sub_output_results.push_back(sub_output_arg);
        } else {
            ExprNode* sub_call = nullptr;
            CHECK_STATUS(
                ApplyArgs(output_func, {sub_output_arg}, nm, &sub_call));
            sub_output_results.push_back(sub_call);
        }
    }
    auto output_results =
        nm->MakeFuncNode("make_tuple", sub_output_results, nullptr);
    auto new_output_func =
        nm->MakeLambdaNode({final_new_state}, output_results);

    // build init state
    std::vector<ExprNode*> sub_inits;
    for (size_t i = 0; i < udafs.size(); ++i) {
        size_t state_begin_idx = state_range[i].first;
        size_t state_end_idx = state_range[i].second;
        bool is_tuple = state_end_idx - state_begin_idx > 1;
        if (is_tuple) {
            for (size_t j = state_begin_idx; j < state_end_idx; ++j) {
                sub_inits.push_back(nm->MakeGetFieldExpr(udafs[i]->init_expr(),
                                                         j - state_begin_idx));
            }
        } else {
            sub_inits.push_back(udafs[i]->init_expr());
        }
    }
    auto new_init_expr = nm->MakeFuncNode("make_tuple", sub_inits, nullptr);

    // build merged call arguments
    std::vector<ExprNode*> call_args;
    call_args.push_back(window);
    for (size_t i = 0; i < udafs.size(); ++i) {
        for (size_t j = 0; j < calls[i]->GetChildNum(); ++j) {
            if (arg_mappings[i][j] != 1) {
                call_args.push_back(calls[i]->GetChild(j));
            }
        }
    }
    std::vector<const node::TypeNode*> call_arg_types;
    for (auto expr : call_args) {
        call_arg_types.push_back(expr->GetOutputType());
    }
    auto new_udaf =
        nm->MakeUdafDefNode("merged_window_agg", call_arg_types, new_init_expr,
                            new_update_func, nullptr, new_output_func);
    *output = nm->MakeFuncNode(new_udaf, call_args, nullptr);
    return Status::OK();
}

Status MergeAggregations::Apply(ExprAnalysisContext* ctx, ExprNode* expr,
                                ExprNode** out) {
    if (this->GetWindow() == nullptr) {
        *out = expr;
        return Status::OK();
    }
    WindowIterAnalysis window_iter_analyzer(ctx);
    CHECK_STATUS(window_iter_analyzer.VisitFunctionLet(
        this->GetRow(), this->GetWindow(), expr));

    // find merge candidates
    std::set<size_t> visisted;
    std::vector<ExprNode*> candidates;
    CHECK_STATUS(CollectUdafCalls(window_iter_analyzer, this->GetWindow(), expr,
                                  &visisted, &candidates));
    if (candidates.size() < 2) {
        *out = expr;
        return Status::OK();
    }

    // build merged udaf call
    ExprNode* merged_call = nullptr;
    CHECK_STATUS(MergeUdafCalls(candidates, this->GetWindow(),
                                ctx->node_manager(), &merged_call));

    // replace sub udaf calls
    ExprReplacer replacer;
    for (size_t i = 0; i < candidates.size(); ++i) {
        replacer.AddReplacement(
            candidates[i],
            ctx->node_manager()->MakeGetFieldExpr(merged_call, i));
    }
    CHECK_STATUS(replacer.Replace(expr, out));

    return Status::OK();
}

}  // namespace passes
}  // namespace hybridse
