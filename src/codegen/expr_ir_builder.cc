/*
 * expr_ir_builder.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
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

#include "codegen/expr_ir_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

ExprIRBuilder::ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var):block_(block),
    scope_var_(scope_var) {}

ExprIRBuilder::~ExprIRBuilder() {}

bool ExprIRBuilder::Build(::fesql::node::FnNode* node,
        ::llvm::Value** output) {

    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }
    if (node->GetType()== ::fesql::node::kFnExprBinary) {
        return BuildBinaryExpr((::fesql::node::FnBinaryExpr*)node, output);
    }
    return false;
}

bool ExprIRBuilder::BuildUnaryExpr(::fesql::node::FnNode* node,
        ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }

    //TODO support more node
    ::llvm::IRBuilder<> builder(block_);
    switch (node->GetType()) {
        case ::fesql::node::kPrimary:
            {
                ::fesql::node::ConstNode* const_node = (::fesql::node::ConstNode*)node;

                switch (const_node->GetDataType()) {
                    case ::fesql::node::kTypeInt32:
                        *output = builder.getInt32(const_node->GetInt());
                        return true;
                    case ::fesql::node::kTypeInt64:
                        *output = builder.getInt64(const_node->GetLong());
                        return true;
                    default:
                        return false;
                }

            }
        case ::fesql::node::kFnId:
            {
                ::fesql::node::FnIdNode* id_node = (::fesql::node::FnIdNode*)node;
                std::string id_name(id_node->name);
                bool ok = scope_var_->FindVar(id_name, output);
                return ok;
            }
        default:
            return false;

    }
}

bool ExprIRBuilder::BuildBinaryExpr(::fesql::node::FnBinaryExpr* node,
        ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }
    if (node->children.size()  != 2) {
        LOG(WARNING) << "invalid binary expr node ";
        return false;
    }
    ::llvm::Value* left = NULL;
    bool ok = BuildUnaryExpr(node->children[0], &left);
    if (!ok) {
        LOG(WARNING) << "fail to build left node";
        return false;
    }

    ::llvm::Value* right = NULL;
    ok = BuildUnaryExpr(node->children[1], &right);
    if (!ok) {
        LOG(WARNING) << "fail to build right node";
        return false;
    }
    if (right->getType()->isIntegerTy() 
            && left->getType()->isIntegerTy()) {
        ::llvm::IRBuilder<> builder(block_);
        //TODO type check
        switch (node->op) {
            case ::fesql::node::kFnOpAdd:
                {
                    *output = builder.CreateAdd(left, right, "expr_add");
                    return true;
                }
            default:
                return false;
        }
    }else {
        LOG(WARNING) << "left mismatch right type";
        return false;
    }
}

} // namespace of codegen
} // namespace of fesql



