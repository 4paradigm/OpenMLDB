/*
 * Copyright (c) 2021 4Paradigm
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

#include "passes/resolve_udf_def.h"
#include "base/fe_status.h"
#include "node/sql_node.h"

using ::fesql::common::kCodegenError;

namespace fesql {
namespace passes {

Status ResolveUdfDef::Visit(node::FnNodeFnDef* fn_def) {
    auto& params = fn_def->header_->parameters_->GetChildren();
    for (auto p : params) {
        auto pnode = dynamic_cast<node::FnParaNode*>(p);
        std::string arg_name = pnode->GetName();
        int64_t exist_arg_id = CurrentScope()->GetVar(arg_name);
        // function def should take no duplicate arg names
        CHECK_TRUE(exist_arg_id < 0, kCodegenError, "Duplicate argument name ",
                   arg_name);

        auto expr_id = pnode->GetExprId();
        CHECK_TRUE(expr_id->IsResolved(), kCodegenError, "Expr id of argument ",
                   arg_name, " is not mark resolved");
        CHECK_TRUE(expr_id->GetOutputType() != nullptr, kCodegenError,
                   "Argument ", arg_name, "'s type is null");
        CHECK_STATUS(CurrentScope()->AddVar(arg_name, expr_id->GetId()));
    }
    return Visit(fn_def->block_);
}

Status ResolveUdfDef::Visit(node::FnNodeList* block) {
    Status status;
    for (auto node : block->GetChildren()) {
        switch (node->GetType()) {
            case node::kFnAssignStmt: {
                status = Visit(dynamic_cast<node::FnAssignNode*>(node));
                break;
            }
            case node::kFnIfElseBlock: {
                status = Visit(dynamic_cast<node::FnIfElseBlock*>(node));
                break;
            }
            case node::kFnForInBlock: {
                status = Visit(dynamic_cast<node::FnForInBlock*>(node));
                break;
            }
            case node::kFnReturnStmt: {
                status = Visit(dynamic_cast<node::FnReturnStmt*>(node));
                break;
            }
            default: {
                LOG(WARNING) << "Unknown fn node type " << node->GetType();
                break;
            }
        }
        CHECK_STATUS(status, "Error at (", node->GetLineNum(), ":",
                     node->GetLocation(), "): ", status.str());
    }
    return status;
}

Status ResolveUdfDef::Visit(node::FnAssignNode* assign) {
    CHECK_STATUS(Visit(assign->expression_));
    auto var = assign->var_;
    CHECK_TRUE(var->IsResolved(), kCodegenError, "Unresolved LHS var");
    auto exist_var_id = GetVar(var->GetName());
    if (exist_var_id >= 0) {
        // re-assign
        var->SetId(exist_var_id);
        return Status::OK();
    }
    CHECK_STATUS(CurrentScope()->AddVar(var->GetName(), var->GetId()));
    return Status::OK();
}

Status ResolveUdfDef::Visit(node::FnIfNode* node) {
    return Visit(node->expression_);
}

Status ResolveUdfDef::Visit(node::FnElifNode* node) {
    return Visit(node->expression_);
}

Status ResolveUdfDef::Visit(node::FnElseNode* node) { return Status::OK(); }

Status ResolveUdfDef::Visit(node::FnIfBlock* block) {
    CHECK_STATUS(Visit(block->if_node));
    FnScopeInfoGuard scope_guard(this);
    return Visit(block->block_);
}

Status ResolveUdfDef::Visit(node::FnElifBlock* block) {
    CHECK_STATUS(Visit(block->elif_node_));
    FnScopeInfoGuard scope_guard(this);
    return Visit(block->block_);
}

Status ResolveUdfDef::Visit(node::FnElseBlock* block) {
    FnScopeInfoGuard scope_guard(this);
    return Visit(block->block_);
}

Status ResolveUdfDef::Visit(node::FnIfElseBlock* block) {
    if (block->if_block_ != nullptr) {
        CHECK_STATUS(Visit(block->if_block_));
    }
    for (auto elif_block : block->elif_blocks_) {
        CHECK_STATUS(Visit(dynamic_cast<node::FnElifBlock*>(elif_block)));
    }
    if (block->else_block_ != nullptr) {
        CHECK_STATUS(Visit(block->else_block_));
    }
    return Status::OK();
}

Status ResolveUdfDef::Visit(node::FnForInBlock* block) {
    CHECK_STATUS(Visit(block->for_in_node_->in_expression_));
    auto var = block->for_in_node_->var_;
    FnScopeInfoGuard scope_guard(this);
    CHECK_STATUS(CurrentScope()->AddVar(var->GetName(), var->GetId()));
    return Visit(block->block_);
}

Status ResolveUdfDef::Visit(node::FnReturnStmt* node) {
    return Visit(node->return_expr_);
}

Status ResolveUdfDef::Visit(node::ExprNode* expr) {
    for (size_t i = 0; i < expr->GetChildNum(); ++i) {
        CHECK_STATUS(Visit(expr->GetChild(i)));
    }
    switch (expr->GetExprType()) {
        case node::kExprId: {
            auto expr_id = dynamic_cast<node::ExprIdNode*>(expr);
            if (!expr_id->IsResolved()) {
                auto var_name = expr_id->GetName();
                int64_t id = this->GetVar(var_name);
                CHECK_TRUE(id >= 0, kCodegenError, "Fail to find var ",
                           var_name, " in current scope");
                expr_id->SetId(id);
            }
        }
        default:
            break;
    }
    return Status::OK();
}

FnScopeInfo* ResolveUdfDef::CurrentScope() { return &scope_stack_.back(); }

int64_t ResolveUdfDef::GetVar(const std::string& var) {
    for (auto iter = scope_stack_.rbegin(); iter != scope_stack_.rend();
         ++iter) {
        int64_t res = iter->GetVar(var);
        if (res >= 0) {
            return res;
        }
    }
    return -1;
}

}  // namespace passes
}  // namespace fesql
