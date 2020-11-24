/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * expr_pass.cc
 *--------------------------------------------------------------------------
 **/
#include "passes/expression/expr_pass.h"

namespace fesql {
namespace passes {

using fesql::base::Status;
using fesql::common::kCodegenError;

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
    }
    node_id_map_[expr->node_id()] = repl;
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

}  // namespace passes
}  // namespace fesql
