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

#include "passes/expression/expr_pass.h"

namespace hybridse {
namespace passes {

using hybridse::base::Status;
using hybridse::common::kCodegenError;

void ExprPassGroup::AddPass(const std::shared_ptr<ExprPass>& pass) {
    if (pass == nullptr) {
        LOG(WARNING) << "Pass is null";
        return;
    }
    passes_.push_back(pass);
}

Status ExprPassGroup::Apply(node::ExprAnalysisContext* ctx,
                            node::ExprNode* expr, node::ExprNode** out) {
    node::ExprNode* cur = expr;
    for (auto pass : passes_) {
        node::ExprNode* new_expr = nullptr;
        pass->SetRow(this->GetRow());
        pass->SetWindow(this->GetWindow());
        CHECK_STATUS(pass->Apply(ctx, cur, &new_expr));
        if (new_expr != nullptr) {
            cur = new_expr;
        }
    }
    *out = cur;
    return Status::OK();
}

void ExprReplacer::AddReplacement(const node::ExprIdNode* arg,
                                  node::ExprNode* repl) {
    if (arg->IsResolved()) {
        arg_id_map_[arg->GetId()] = repl;
    } else {
        LOG(WARNING) << "Replace unresolved argument behavior is undefined";
    }
}

void ExprReplacer::AddReplacement(const node::ExprNode* expr,
                                  node::ExprNode* repl) {
    if (expr->GetExprType() == node::kExprId) {
        auto arg = dynamic_cast<const node::ExprIdNode*>(expr);
        if (arg->IsResolved()) {
            AddReplacement(arg, repl);
            return;
        }
    } else if (expr->GetExprType() == node::kExprColumnId) {
        auto column_id = dynamic_cast<const node::ColumnIdNode*>(expr);
        AddReplacement(column_id->GetColumnID(), repl);
    } else if (expr->GetExprType() == node::kExprColumnRef) {
        auto column_ref = dynamic_cast<const node::ColumnRefNode*>(expr);
        AddReplacement(column_ref->GetRelationName(),
                       column_ref->GetColumnName(), repl);
    }
    node_id_map_[expr->node_id()] = repl;
}

void ExprReplacer::AddReplacement(size_t column_id, node::ExprNode* repl) {
    column_id_map_[column_id] = repl;
}

void ExprReplacer::AddReplacement(const std::string& relation_name,
                                  const std::string& column_name,
                                  node::ExprNode* repl) {
    column_name_map_[relation_name + "." + column_name] = repl;
}

Status ExprReplacer::Replace(node::ExprNode* root,
                             node::ExprNode** output) const {
    std::unordered_set<size_t> visited;
    return DoReplace(root, &visited, output);
}

Status ExprReplacer::DoReplace(node::ExprNode* root,
                               std::unordered_set<size_t>* visited,
                               node::ExprNode** output) const {
    CHECK_TRUE(root != nullptr, kCodegenError, "Input expression is null");
    if (visited->find(root->node_id()) != visited->end()) {
        return Status::OK();
    }
    if (root->GetExprType() == node::kExprId) {
        auto arg = dynamic_cast<const node::ExprIdNode*>(root);
        if (arg->IsResolved()) {
            auto iter = arg_id_map_.find(arg->GetId());
            if (iter != arg_id_map_.end()) {
                *output = iter->second;
            } else {
                visited->insert(root->node_id());
                *output = root;
            }
        }
        return Status::OK();
    } else if (root->GetExprType() == node::kExprColumnRef) {
        auto col = dynamic_cast<const node::ColumnRefNode*>(root);
        auto name = col->GetRelationName() + "." + col->GetColumnName();
        auto iter = column_name_map_.find(name);
        if (iter != column_name_map_.end()) {
            *output = iter->second;
            return Status::OK();
        }
    } else if (root->GetExprType() == node::kExprColumnId) {
        auto col = dynamic_cast<const node::ColumnIdNode*>(root);
        auto iter = column_id_map_.find(col->GetColumnID());
        if (iter != column_id_map_.end()) {
            *output = iter->second;
            return Status::OK();
        }
    }
    auto iter = node_id_map_.find(root->node_id());
    if (iter != node_id_map_.end()) {
        *output = iter->second;
        return Status::OK();
    }
    for (size_t i = 0; i < root->GetChildNum(); ++i) {
        node::ExprNode* child = root->GetChild(i);
        node::ExprNode* new_child = nullptr;
        CHECK_STATUS(DoReplace(child, visited, &new_child));
        if (new_child != nullptr && new_child != child) {
            root->SetChild(i, new_child);
        }
    }
    visited->insert(root->node_id());
    *output = root;
    return Status::OK();
}

node::ExprIdNode* ExprPass::GetWindow() const { return window_; }

void ExprPass::SetWindow(node::ExprIdNode* window) { window_ = window; }

node::ExprIdNode* ExprPass::GetRow() const { return row_; }

void ExprPass::SetRow(node::ExprIdNode* row) { row_ = row; }

}  // namespace passes
}  // namespace hybridse
