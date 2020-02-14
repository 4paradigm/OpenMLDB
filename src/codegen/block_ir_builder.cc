/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * control_flow_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/2/12
 *--------------------------------------------------------------------------
 **/
#include "codegen/block_ir_builder.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/IR/CFG.h"
#include "llvm/IR/IRBuilder.h"

namespace fesql {
namespace codegen {

BlockIRBuilder::BlockIRBuilder(ScopeVar *scope_var) : sv_(scope_var) {}
BlockIRBuilder::~BlockIRBuilder() {}
bool fesql::codegen::BlockIRBuilder::BuildBlock(
    const fesql::node::FnNodeList *statements, llvm::BasicBlock *block,
    llvm::BasicBlock *end_block, fesql::base::Status &status) {
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
                    block->getContext(), "if_else_start", fn);
                llvm::BasicBlock *if_else_end = llvm::BasicBlock::Create(
                    block->getContext(), "if_else_end");
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

bool BlockIRBuilder::BuildIfElseBlock(
    const ::fesql::node::FnIfElseBlock *if_else_block,
    llvm::BasicBlock *if_else_start, llvm::BasicBlock *if_else_end,
    base::Status &status) {  // NOLINE
    llvm::Function *fn = if_else_start->getParent();
    llvm::LLVMContext &ctx = if_else_start->getContext();

    ::llvm::IRBuilder<> builder(if_else_start);

    llvm::BasicBlock *cond_true =
        llvm::BasicBlock::Create(ctx, "cond_true", fn);
    llvm::BasicBlock *cond_false =
        llvm::BasicBlock::Create(ctx, "cond_false", fn);

    builder.SetInsertPoint(if_else_start);
    ExprIRBuilder expr_builder(builder.GetInsertBlock(), sv_);
    //进行条件的代码
    llvm::Value *cond = nullptr;
    if (false == expr_builder.Build(
                     if_else_block->if_block_->if_node->expression_, &cond)) {
        status.code = common::kCodegenError;
        status.msg = "fail to codegen condition expression";
        LOG(WARNING) << status.msg;
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
            llvm::BasicBlock *cond_true =
                llvm::BasicBlock::Create(ctx, "cond_true", fn);
            llvm::BasicBlock *cond_false =
                llvm::BasicBlock::Create(ctx, "cond_false", fn);

            fesql::node::FnElifBlock *elif_block =
                dynamic_cast<fesql::node::FnElifBlock *>(node);
            llvm::Value *cond = nullptr;

            ExprIRBuilder expr_builder(builder.GetInsertBlock(), sv_);
            if (false == expr_builder.Build(elif_block->elif_node_->expression_,
                                            &cond)) {
                status.code = common::kCodegenError;
                status.msg = "fail to codegen condition expression";
                LOG(WARNING) << status.msg;
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
bool BlockIRBuilder::BuildReturnStmt(const ::fesql::node::FnReturnStmt *node,
                                     ::llvm::BasicBlock *block,
                                     base::Status &status) {  // NOLINE
    if (node == nullptr || block == nullptr || node->return_expr_ == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "node or block or return expr is null";
        LOG(WARNING) << status.msg;
        return true;
    }

    ::llvm::IRBuilder<> builder(block);
    ExprIRBuilder expr_builder(block, sv_);
    VariableIRBuilder var_ir_builder(block, sv_);
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

bool BlockIRBuilder::BuildAssignStmt(const ::fesql::node::FnAssignNode *node,
                                     ::llvm::BasicBlock *block,
                                     base::Status &status) {  // NOLINE
    if (node == NULL || block == NULL || node->expression_ == nullptr) {
        status.code = common::kCodegenError;
        status.msg = "node or block is null";
        LOG(WARNING) << status.msg;
        return false;
    }
    ExprIRBuilder builder(block, sv_);
    VariableIRBuilder variable_ir_builder(block, sv_);
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
}  // namespace codegen
}  // namespace fesql
