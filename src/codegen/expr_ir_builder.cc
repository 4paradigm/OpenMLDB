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

ExprIRBuilder::ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block), scope_var_(scope_var) {}

ExprIRBuilder::~ExprIRBuilder() {}

bool ExprIRBuilder::Build(const ::fesql::node::FnNode* node,
                          ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }
    // TODO use switch
    if (node->GetType() == ::fesql::node::kFnExprBinary) {
        return BuildBinaryExpr((::fesql::node::FnBinaryExpr*)node, output);
    } else {
        return BuildUnaryExpr(node, output);
    }
}

bool ExprIRBuilder::BuildUnaryExpr(const ::fesql::node::FnNode* node,
                                   ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }
    LOG(INFO) << "build unary " << ::fesql::node::FnNodeName(node->GetType());
    // TODO support more node
    ::llvm::IRBuilder<> builder(block_);
    switch (node->GetType()) {
        case ::fesql::node::kPrimary: {
            ::fesql::node::ConstNode* const_node =
                (::fesql::node::ConstNode*)node;

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
        case ::fesql::node::kFnId: {
            ::fesql::node::FnIdNode* id_node = (::fesql::node::FnIdNode*)node;
            ::llvm::Value* ptr = NULL;
            bool ok = scope_var_->FindVar(id_node->GetName(), &ptr);
            if (!ok || ptr == NULL) {
                LOG(WARNING) << "fail to find var " << id_node->GetName();
                return false;
            }
            if (ptr->getType()->isPointerTy()) {
                *output = builder.CreateLoad(ptr, id_node->GetName().c_str());
            } else {
                *output = ptr;
            }
            return true;
        }
        case ::fesql::node::kFnExprBinary: {
            return BuildBinaryExpr((::fesql::node::FnBinaryExpr*)node, output);
        }
        case ::fesql::node::kFnExprUnary: {
            return BuildUnaryExpr(node->children[0], output);
        }
        default:
            LOG(WARNING) << ::fesql::node::FnNodeName(node->GetType())
                         << " not support";
            return false;
    }
}

bool ExprIRBuilder::BuildBinaryExpr(const ::fesql::node::FnBinaryExpr* node,
                                    ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }

    if (node->children.size() != 2) {
        LOG(WARNING) << "invalid binary expr node ";
        return false;
    }

    LOG(INFO) << "build binary " << ::fesql::node::FnNodeName(node->GetType());
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

    if (right->getType()->isIntegerTy() && left->getType()->isIntegerTy()) {
        ::llvm::IRBuilder<> builder(block_);
        // TODO type check
        switch (node->GetOp()) {
            case ::fesql::node::kFnOpAdd: {
                *output = builder.CreateAdd(left, right, "expr_add");
                return true;
            }
            case ::fesql::node::kFnOpMulti: {
                *output = builder.CreateMul(left, right, "expr_mul");
                return true;
            }
            case ::fesql::node::kFnOpMinus: {
                *output = builder.CreateSub(left, right, "expr_sub");
                return true;
            }
            default:
                LOG(WARNING) << "invalid op ";
                return false;
        }
    } else {
        LOG(WARNING) << "left mismatch right type";
        return false;
    }
}

}  // namespace codegen
}  // namespace fesql
