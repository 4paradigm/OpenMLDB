/*
 * fn_ir_builder.cc
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

#include "codegen/fn_ir_builder.h"
#include <stack>
#include "codegen/expr_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/IR/CFG.h"
#include "llvm/IR/IRBuilder.h"

namespace fesql {
namespace codegen {

FnIRBuilder::FnIRBuilder(::llvm::Module *module)
    : module_(module), variable_ir_builder_(nullptr, nullptr) {}

FnIRBuilder::~FnIRBuilder() {}
bool FnIRBuilder::Build(const ::fesql::node::FnNodeFnDef *root,
                        base::Status &status) {  // NOLINT
    if (root == NULL || root->GetType() != ::fesql::node::kFnDef) {
        status.code = common::kCodegenError;
        status.msg = "node is null";
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Function *fn = NULL;
    ::llvm::BasicBlock *block = NULL;
    std::stack<int32_t> indent_stack;
    sv_.Enter("module");

    const ::fesql::node::FnNodeFnHeander *fn_def = root->header_;
    sv_.Enter(fn_def->name_);

    bool ok = BuildFnHead(fn_def, &fn, status);
    if (!ok) {
        return false;
    }
    block = ::llvm::BasicBlock::Create(module_->getContext(), "entry", fn);
    llvm::BasicBlock *end_block =
        llvm::BasicBlock::Create(module_->getContext(), "end_block");

    ::llvm::IRBuilder<> builder(block);
    if (false == BuildBlock(root->block_, block, end_block, status)) {
        return false;
    }
    return true;
}

bool FnIRBuilder::BuildIfElseBlock(
    const ::fesql::node::FnIfElseBlock *if_else_block,
    llvm::BasicBlock *if_else_start, llvm::BasicBlock *if_else_end,
    base::Status &status) {  // NOLINE
    llvm::Function *fn = if_else_start->getParent();

    ::llvm::IRBuilder<> builder(if_else_start);

    llvm::BasicBlock *cond_true =
        llvm::BasicBlock::Create(module_->getContext(), "cond_true", fn);
    llvm::BasicBlock *cond_false =
        llvm::BasicBlock::Create(module_->getContext(), "cond_false", fn);

    builder.SetInsertPoint(if_else_start);
    ExprIRBuilder expr_builder(builder.GetInsertBlock(), &sv_);
    //进行条件的代码
    llvm::Value *cond = nullptr;
    if (false == expr_builder.Build(
                     if_else_block->if_block_->if_node->expression_, &cond)) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen condition expression";
        return false;
    }

    builder.CreateCondBr(cond, cond_true, cond_false);
    builder.SetInsertPoint(cond_true);
    if (false == BuildBlock(if_else_block->if_block_->block_, cond_true,
                            if_else_end, status)) {
        return false;
    }
    builder.SetInsertPoint(cond_false);
    if (!if_else_block->elif_blocks_.empty()) {
        for (fesql::node::FnNode *node : if_else_block->elif_blocks_) {
            llvm::BasicBlock *cond_true = llvm::BasicBlock::Create(
                module_->getContext(), "cond_true", fn);
            llvm::BasicBlock *cond_false = llvm::BasicBlock::Create(
                module_->getContext(), "cond_false", fn);

            fesql::node::FnElifBlock *elif_block =
                dynamic_cast<fesql::node::FnElifBlock *>(node);
            llvm::Value *cond = nullptr;

            ExprIRBuilder expr_builder(builder.GetInsertBlock(), &sv_);
            if (false == expr_builder.Build(elif_block->elif_node_->expression_,
                                            &cond)) {
                status.code = common::kCodegenError;
                status.msg = "fail to codegen condition expression";
                return false;
            }
            builder.CreateCondBr(cond, cond_true, cond_false);
            builder.SetInsertPoint(cond_true);
            if (false == BuildBlock(elif_block->block_,
                                    builder.GetInsertBlock(), if_else_end,
                                    status)) {
                return false;
            }
            builder.SetInsertPoint(cond_false);
        }
    }

    if (nullptr == if_else_block->else_block_) {
        builder.CreateBr(if_else_end);
    } else {
        if (false == BuildBlock(if_else_block->else_block_->block_,
                                builder.GetInsertBlock(), if_else_end,
                                status)) {
            return false;
        }
    }
    return true;
}
bool FnIRBuilder::BuildReturnStmt(const ::fesql::node::FnReturnStmt *node,
                                  ::llvm::BasicBlock *block,
                                  base::Status &status) {  // NOLINE
    if (node == nullptr || block == nullptr || node->return_expr_ == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "node or block or return expr is null";
        LOG(WARNING) << status.msg;
        return true;
    }

    ::llvm::IRBuilder<> builder(block);
    ExprIRBuilder expr_builder(block, &sv_);
    VariableIRBuilder var_ir_builder(block, &sv_);
    ::llvm::Value *value = NULL;
    bool ok = expr_builder.Build(node->return_expr_, &value);
    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen expr";
        LOG(WARNING) << status.msg;
        return false;
    }
    builder.CreateRet(value);
    return true;
}

bool FnIRBuilder::BuildAssignStmt(const ::fesql::node::FnAssignNode *node,
                                  ::llvm::BasicBlock *block,
                                  base::Status &status) {  // NOLINE
    if (node == NULL || block == NULL || node->expression_ == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "node or block is null";
        LOG(WARNING) << status.msg;
        return false;
    }
    ExprIRBuilder builder(block, &sv_);
    VariableIRBuilder variable_ir_builder(block, &sv_);
    ::llvm::Value *value = NULL;
    bool ok = builder.Build(node->expression_, &value);
    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen expr";
        LOG(WARNING) << status.msg;
        return false;
    }

    if (node->IsSSA()) {
        return variable_ir_builder.StoreValue(node->name_, value, true, status);
    } else {
        return variable_ir_builder.StoreValue(node->name_, value, false,
                                              status);
    }
}

bool FnIRBuilder::BuildBlock(const node::FnNodeList *statements,
                             llvm::BasicBlock *block,
                             llvm::BasicBlock *end_block,
                             base::Status &status) {  // NOLINT
    if (statements == NULL || block == NULL) {
        status.code = common::kCodegenError;
        status.msg = "node or block is null";
        LOG(WARNING) << status.msg;
        return false;
    }

    if (statements->children.empty()) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen block: statements is empty";
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Function *fn = block->getParent();
    ::llvm::IRBuilder<> builder(block);
    for (const node::FnNode *node : statements->children) {
        switch (node->GetType()) {
            case node::kFnAssignStmt: {
                bool ok = BuildAssignStmt(
                    dynamic_cast<const ::fesql::node::FnAssignNode *>(node),
                    builder.GetInsertBlock(), status);
                if (!ok) {
                    return false;
                }
                break;
            }
            case node::kFnReturnStmt: {
                bool ok = BuildReturnStmt(
                    dynamic_cast<const node::FnReturnStmt *>(node),
                    builder.GetInsertBlock(), status);

                return ok;
            }
            case node::kFnIfElseBlock: {
                llvm::BasicBlock *block_start = llvm::BasicBlock::Create(
                    module_->getContext(), "if_else_start", fn);
                llvm::BasicBlock *if_else_end = llvm::BasicBlock::Create(
                    module_->getContext(), "if_else_end");
                builder.CreateBr(block_start);
                builder.SetInsertPoint(block_start);
                bool ok = BuildIfElseBlock(
                    dynamic_cast<const ::fesql::node::FnIfElseBlock *>(node),
                    block_start, if_else_end, status);
                if (!ok) {
                    return false;
                }

                // stop block codegen when current block is returned
                if (::llvm::pred_empty(if_else_end)) {
                    return true;
                }
                fn->getBasicBlockList().push_back(if_else_end);
                builder.SetInsertPoint(if_else_end);
                break;
            }
            default: {
                status.code = common::kCodegenError;
                status.msg = "fail to codegen for unrecognized fn type " +
                             node::NameOfSQLNodeType(node->GetType());
                LOG(WARNING) << status.msg;
                return false;
            }
        }
    }
    builder.CreateBr(end_block);
    return true;
}
bool FnIRBuilder::BuildFnHead(const ::fesql::node::FnNodeFnHeander *fn_def,
                              ::llvm::Function **fn,
                              base::Status &status) {  // NOLINE
    if (fn_def == NULL || fn == NULL) {
        status.code = common::kCodegenError;
        status.msg = "input is null";
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::Type *ret_type = NULL;
    bool ok = ConvertFeSQLType2LLVMType(fn_def->ret_type_, module_, &ret_type);
    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to get llvm type";
        return false;
    }

    std::vector<::llvm::Type *> paras;
    if (nullptr != fn_def->parameters_) {
        bool ok = BuildParas(fn_def->parameters_, paras, status);
        if (!ok) {
            return false;
        }
    }

    ::llvm::ArrayRef<::llvm::Type *> array_ref(paras);
    ::llvm::FunctionType *fnt =
        ::llvm::FunctionType::get(ret_type, array_ref, false);
    *fn = ::llvm::Function::Create(fnt, ::llvm::Function::ExternalLinkage,
                                   fn_def->name_, module_);
    if (fn_def->parameters_) {
        bool ok = FillArgs(fn_def->parameters_, *fn, status);
        if (!ok) {
            return false;
        }
    }
    DLOG(INFO) << "build fn " << fn_def->name_ << " header done";
    return true;
}

bool FnIRBuilder::FillArgs(const ::fesql::node::FnNodeList *node,
                           ::llvm::Function *fn,
                           base::Status &status) {  // NOLINE
    if (node == NULL) {
        status.code = common::kCodegenError;
        status.msg = "node is null or node type mismatch";
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::Function::arg_iterator it = fn->arg_begin();
    uint32_t index = 0;
    for (; it != fn->arg_end() && index < node->children.size(); ++it) {
        ::fesql::node::FnParaNode *pnode =
            (::fesql::node::FnParaNode *)node->children[index];
        ::llvm::Argument *argu = &*it;
        bool ok = sv_.AddVar(pnode->GetName(), argu);
        if (!ok) {
            status.code = common::kCodegenError;
            status.msg = "fail to define var " + pnode->GetName();
            LOG(WARNING) << status.msg;
            return false;
        }
        index++;
    }
    return true;
}

bool FnIRBuilder::BuildParas(const ::fesql::node::FnNodeList *node,
                             std::vector<::llvm::Type *> &paras,
                             base::Status &status) {  // NOLINE
    if (node == NULL) {
        status.code = common::kCodegenError;
        status.msg = "node is null or node type mismatch";
        LOG(WARNING) << status.msg;
        return false;
    }

    for (uint32_t i = 0; i < node->children.size(); i++) {
        ::fesql::node::FnParaNode *pnode =
            (::fesql::node::FnParaNode *)node->children[i];
        ::llvm::Type *type = NULL;
        bool ok =
            ConvertFeSQLType2LLVMType(pnode->GetParaType(), module_, &type);
        if (!ok) {
            status.code = common::kCodegenError;
            status.msg =
                "fail to get primary type for pname " + pnode->GetName();
            LOG(WARNING) << status.msg;
            return false;
        }
        paras.push_back(type);
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
