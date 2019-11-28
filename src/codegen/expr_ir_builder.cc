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
#include <proto/common.pb.h>
#include "glog/logging.h"

namespace fesql {
namespace codegen {

SQLExprIRBuilder::SQLExprIRBuilder(::llvm::BasicBlock* block,
                                   ScopeVar* scope_var,
                                   BufIRBuilder* buf_ir_builder,
                                   const std::string& row_ptr_name,
                                   const std::string& row_size_name,
                                   const std::string& output_ptr_name,
                                   ::llvm::Module* module)
    : block_(block),
      sv_(scope_var),
      row_ptr_name_(row_ptr_name),
      row_size_name_(row_size_name),
      output_ptr_name_(output_ptr_name),
      buf_ir_builder_(buf_ir_builder),
      module_(module) {}

SQLExprIRBuilder::~SQLExprIRBuilder() {}

bool SQLExprIRBuilder::Build(const ::fesql::node::ExprNode* node,
                             ::llvm::Value** output, std::string& col_name) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "node or output is null";
        return false;
    }
    switch (node->GetExprType()) {
        case ::fesql::node::kExprColumnRef: {
            const ::fesql::node::ColumnRefNode* n =
                (const ::fesql::node::ColumnRefNode*)node;
            col_name.assign(n->GetColumnName());
            return BuildColumnRef(n, output);
        }
        case ::fesql::node::kExprCall: {
            const ::fesql::node::CallExprNode* fn =
                (const ::fesql::node::CallExprNode*)node;
            return BuildCallFn(fn, output);
        }
        default: {
            LOG(WARNING) << "not supported";
            return false;
        }
    }
}

bool SQLExprIRBuilder::BuildCallFn(const ::fesql::node::CallExprNode* call_fn,
                                   ::llvm::Value** output) {
    //TODO(chenjing): return status;
    common::Status status;
    if (call_fn == NULL || output == NULL) {
        LOG(WARNING) << "call fn or output is null";
        status.set_code(common::kNullPointer);
        status.set_msg("null pointer");
        return false;
    }

    ::llvm::StringRef name(call_fn->GetFunctionName());
    // TODO(wangtaize) opt function location
    ::llvm::Function* fn = module_->getFunction(name);

    if (fn == NULL) {
        status.set_code(common::kCallMethodError);
        status.set_msg("fail to find func with name " + call_fn->GetFunctionName());
        LOG(WARNING) << status.msg();
        return false;
    }

    const std::vector<::fesql::node::SQLNode*>& args = call_fn->GetArgs();
    if (args.size() != fn->arg_size()) {
        status.set_msg("Incorrect arguments passed");
        status.set_code(common::kCallMethodError);
        return false;
    }

    std::vector<::fesql::node::SQLNode*>::const_iterator it = args.begin();
    std::vector<::llvm::Value*> llvm_args;

    for (; it != args.end(); ++it) {
        const ::fesql::node::ExprNode* arg = dynamic_cast<node::ExprNode*>(*it);
        switch (arg->GetExprType()) {
            case ::fesql::node::kExprColumnRef: {
                ::llvm::Value* llvm_arg = NULL;
                const ::fesql::node::ColumnRefNode* n =
                    (const ::fesql::node::ColumnRefNode*)arg;
                bool ok = BuildColumnRef(n, &llvm_arg);
                if (!ok) {
                    return false;
                }
                llvm_args.push_back(llvm_arg);
                break;
            }
            default: {
                LOG(WARNING) << "not supported type";
                return false;
            }
        }
    }
    // TODO(wangtaize) args type check
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::ArrayRef<::llvm::Value*> array_ref(llvm_args);
    *output = builder.CreateCall(fn->getFunctionType(), fn, array_ref);
    return true;
}

bool SQLExprIRBuilder::BuildColumnRef(const ::fesql::node::ColumnRefNode* node,
                                      ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "column ref node is null";
        return false;
    }

    ::llvm::Value* row_ptr = NULL;
    bool ok = sv_->FindVar(row_ptr_name_, &row_ptr);

    if (!ok || row_ptr == NULL) {
        LOG(WARNING) << "fail to find row ptr with name " << row_ptr_name_;
        return false;
    }

    ::llvm::Value* row_size = NULL;
    ok = sv_->FindVar(row_size_name_, &row_size);
    if (!ok || row_size == NULL) {
        LOG(WARNING) << "fail to find row size with name " << row_size_name_;
        return false;
    }

    ::llvm::Value* value = NULL;
    ok = sv_->FindVar(node->GetColumnName(), &value);
    LOG(INFO) << "get table column " << node->GetColumnName();
    // not found
    if (!ok) {
        ok = buf_ir_builder_->BuildGetField(node->GetColumnName(),
                                            row_ptr,
                                            row_size,
                                            &value);
        if (!ok || value == NULL) {
            LOG(WARNING) << "fail to find column " << node->GetColumnName();
            return false;
        }

        ok = sv_->AddVar(node->GetColumnName(), value);
        if (ok) {
            *output = value;
        }

        return ok;
    } else {
        *output = value;
    }
    return true;
}

ExprIRBuilder::ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block), scope_var_(scope_var) {}

ExprIRBuilder::~ExprIRBuilder() {}

bool ExprIRBuilder::Build(const ::fesql::node::ExprNode* node,
                          ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }
    LOG(INFO) << "build unary " << ::fesql::node::ExprTypeName(node->GetExprType());
    // TODO support more node
    ::llvm::IRBuilder<> builder(block_);
    switch (node->GetExprType()) {
        case ::fesql::node::kExprPrimary: {
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
        case ::fesql::node::kExprId: {
            ::fesql::node::ExprIdNode* id_node = (::fesql::node::ExprIdNode*)node;
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
        case ::fesql::node::kExprBinary: {
            return BuildBinaryExpr((::fesql::node::BinaryExpr*)node, output);
        }
        case ::fesql::node::kExprUnary: {
            return Build(node->children[0], output);
        }
        default:
            LOG(WARNING) << ::fesql::node::FnNodeName(node->GetType())
                         << " not support";
            return false;
    }
}

bool ExprIRBuilder::BuildUnaryExpr(const ::fesql::node::UnaryExpr* node,
                                   ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }

    if (node->children.size() != 1) {
        LOG(WARNING) << "invalid unary expr node ";
        return false;
    }

    LOG(INFO) << "build unary"
              << ::fesql::node::ExprTypeName(node->GetExprType());
    ::llvm::Value* left = NULL;
    bool ok = Build(node->children[0], &left);
    if (!ok) {
        LOG(WARNING) << "fail to build unary child";
        return false;
    }
    LOG(WARNING) << "can't support unary yet";
    return false;
}
bool ExprIRBuilder::BuildBinaryExpr(const ::fesql::node::BinaryExpr* node,
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
    bool ok = Build(node->children[0], &left);
    if (!ok) {
        LOG(WARNING) << "fail to build left node";
        return false;
    }

    ::llvm::Value* right = NULL;
    ok = Build(node->children[1], &right);
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
