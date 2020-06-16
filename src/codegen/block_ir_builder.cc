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
#include "codegen/list_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "codegen/context.h"
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
    if (statements == NULL || block == NULL || end_block == NULL) {
        status.code = common::kCodegenError;
        status.msg = "node or block is null";
        LOG(WARNING) << status.msg;
        return false;
    }

    if (statements->children.empty()) {
        return true;
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
            case node::kFnForInBlock: {
                sv_->Enter("for_in_block");
                llvm::BasicBlock *loop_start = llvm::BasicBlock::Create(
                    block->getContext(), "loop_start", fn);
                llvm::BasicBlock *loop_end =
                    llvm::BasicBlock::Create(block->getContext(), "loop_end");
                builder.CreateBr(loop_start);
                builder.SetInsertPoint(loop_start);
                if (false ==
                    BuildForInBlock(
                        dynamic_cast<const ::fesql::node::FnForInBlock *>(node),
                        loop_start, loop_end, status)) {
                    return false;
                }

                if (::llvm::pred_empty(loop_end)) {
                    return true;
                }
                fn->getBasicBlockList().push_back(loop_end);
                builder.SetInsertPoint(loop_end);
                if (!ClearScopeValue(loop_end, status)) {
                    status.code = common::kCodegenError;
                    status.msg = "fail to clear scope value";
                    LOG(WARNING) << status.msg;
                    return false;
                }
                sv_->Exit();
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


bool BlockIRBuilder::DoBuildBranchBlock(
        const ::fesql::node::FnIfElseBlock* if_else_block,
        size_t branch_idx,
        CodeGenContext* ctx,
        ::llvm::BasicBlock* if_else_end) {
    Status status;
    ::llvm::BasicBlock* cur_block = ctx->GetCurrentBlock();

    if (branch_idx == 0) {
        // if () {}
        return BuildBlock(if_else_block->if_block_->block_,
            cur_block, if_else_end, status);

    } else if (branch_idx <= if_else_block->elif_blocks_.size()) {
        // else if () {}
        auto node = if_else_block->elif_blocks_[branch_idx - 1];
        auto elif_block = dynamic_cast<fesql::node::FnElifBlock*>(node);

        NativeValue elif_condition;
        ExprIRBuilder expr_builder(cur_block, sv_);
        bool elif_ok = expr_builder.Build(elif_block->elif_node_->expression_,
            &elif_condition, status);
        if (!elif_ok) {
            LOG(WARNING) << "fail to codegen else if condition: "
                << status.msg;
            return false;
        }

        status = ctx->CreateBranch(elif_condition, [&](){
            elif_ok = BuildBlock(elif_block->block_, ctx->GetCurrentBlock(),
                if_else_end, status);
            CHECK_TRUE(elif_ok, "fail to codegen block:", status.msg);
            return Status::OK();
        }, [&](){
            elif_ok = DoBuildBranchBlock(if_else_block,
                branch_idx + 1, ctx, if_else_end);
            CHECK_TRUE(elif_ok, "fail to codegen block:", status.msg);
            return Status::OK();
        });

    } else {
        // else {}
        if (nullptr == if_else_block->else_block_) {
            ctx->GetBuilder()->CreateBr(if_else_end);
        } else {
            bool else_ok = BuildBlock(if_else_block->else_block_->block_,
                cur_block, if_else_end, status);
            if (!else_ok) {
                LOG(WARNING) << "fail to codegen else block: " << status.msg;
                return false;
            }
        }
    }
    return true;
}


bool BlockIRBuilder::BuildIfElseBlock(
    const ::fesql::node::FnIfElseBlock *if_else_block,
    llvm::BasicBlock *if_else_start, llvm::BasicBlock *if_else_end,
    base::Status &status) {  // NOLINE
    if (if_else_block == nullptr || if_else_start == nullptr ||
        if_else_end == nullptr) {
        status.code = common::kCodegenError;
        status.msg =
            "fail to codegen if else block: "
            "node or start block or end expr is null";
        LOG(WARNING) << status.msg;
        return false;
    }
    llvm::Function *fn = if_else_start->getParent();

    CodeGenContext ctx(fn->getParent());
    FunctionScopeGuard func_guard(fn, &ctx);

    BlockGroup root_group(if_else_start, &ctx);
    BlockGroupGuard root_group_guard(&root_group);

    // first condition
    ExprIRBuilder expr_builder(ctx.GetCurrentBlock(), sv_);
    NativeValue condition;
    if (!expr_builder.Build(if_else_block->if_block_->if_node->expression_,
                            &condition, status)) {
        LOG(WARNING) << "fail to codegen condition expression: " << status.msg;
        return false;
    }

    status = ctx.CreateBranch(condition, [&]() {
        CHECK_TRUE(DoBuildBranchBlock(if_else_block, 0, &ctx, if_else_end));
        return Status::OK();
    }, [&]() {
        CHECK_TRUE(DoBuildBranchBlock(if_else_block, 1, &ctx, if_else_end));
        return Status::OK();
    });

    root_group.DropEmptyBlocks();
    root_group.ReInsertTo(fn);

    if (!status.isOK()) {
        LOG(WARNING) << "fail to codegen if else block: " << status.msg;
        return false;
    }
    return true;
}

bool BlockIRBuilder::BuildForInBlock(const ::fesql::node::FnForInBlock *node,
                                     llvm::BasicBlock *start_block,
                                     llvm::BasicBlock *end_block,
                                     base::Status &status) {
    if (node == nullptr || start_block == nullptr || end_block == nullptr) {
        status.code = common::kCodegenError;
        status.msg =
            "fail to codegen for block: node or start block or end expr is "
            "null";
        LOG(WARNING) << status.msg;
        return false;
    }
    llvm::Function *fn = start_block->getParent();
    llvm::LLVMContext &ctx = start_block->getContext();

    ::llvm::IRBuilder<> builder(start_block);
    ListIRBuilder list_ir_builder(builder.GetInsertBlock(), sv_);
    ExprIRBuilder expr_builder(builder.GetInsertBlock(), sv_);

    // loop start
    NativeValue container_value_wrapper;
    if (!expr_builder.Build(node->for_in_node_->in_expression_,
                            &container_value_wrapper, status)) {
        LOG(WARNING) << "fail to build for condition expression: "
                     << status.msg;
        return false;
    }
    llvm::Value *container_value = container_value_wrapper.GetValue(&builder);

    llvm::Value *iterator = nullptr;
    if (false ==
        list_ir_builder.BuildIterator(container_value, &iterator, status)) {
        LOG(WARNING) << "fail to build iterator expression: " << status.msg;
        return false;
    }
    sv_->AddIteratorValue(iterator);

    llvm::BasicBlock *loop_cond =
        llvm::BasicBlock::Create(ctx, "loop_cond", fn);
    llvm::BasicBlock *loop = llvm::BasicBlock::Create(ctx, "loop", fn);
    builder.CreateBr(loop_cond);
    builder.SetInsertPoint(loop_cond);
    {
        ListIRBuilder list_ir_builder(builder.GetInsertBlock(), sv_);
        // loop condition
        llvm::Value *condition;
        if (!list_ir_builder.BuildIteratorHasNext(iterator, &condition,
                                                  status)) {
            LOG(WARNING) << "fail to build iterator has next expression: "
                         << status.msg;
            return false;
        }

        builder.CreateCondBr(condition, loop, end_block);
    }

    builder.SetInsertPoint(loop);
    {
        ListIRBuilder list_ir_builder(builder.GetInsertBlock(), sv_);
        VariableIRBuilder var_ir_builder(builder.GetInsertBlock(), sv_);
        // loop step
        llvm::Value *next;
        if (false ==
            list_ir_builder.BuildIteratorNext(iterator, &next, status)) {
            LOG(WARNING) << "fail to build iterator next expression: "
                         << status.msg;
            return false;
        }
        if (!var_ir_builder.StoreValue(node->for_in_node_->var_name_,
                NativeValue::Create(next), false, status)) {
            return false;
        }
        // loop body
        if (!BuildBlock(node->block_, loop, loop_cond, status)) {
            LOG(WARNING) << "fail to codegen block: " << status.msg;
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
        return false;
    }

    ::llvm::IRBuilder<> builder(block);
    ExprIRBuilder expr_builder(block, sv_);
    VariableIRBuilder var_ir_builder(block, sv_);
    NativeValue value_wrapper;
    bool ok = expr_builder.Build(node->return_expr_, &value_wrapper, status);
    if (!ok) {
        LOG(WARNING) << "fail to codegen return expression: " << status.msg;
        return false;
    }
    ::llvm::Value *value = value_wrapper.GetValue(&builder);

    if (!ClearAllScopeValues(block, status)) {
        LOG(WARNING) << "fail to clear all scopes values : " << status.msg;
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
    NativeValue value;
    bool ok = builder.Build(node->expression_, &value, status);
    if (!ok) {
        LOG(WARNING) << "fail to codegen expr" << status.msg;
        return false;
    }
    return variable_ir_builder.StoreValue(node->name_, value, false, status);
}

bool BlockIRBuilder::ClearScopeValue(llvm::BasicBlock *block,
                                     base::Status &status) {
    llvm::Value *ret_delete = nullptr;
    ListIRBuilder list_ir_builder_delete(block, sv_);
    auto delete_values = sv_->GetScopeIteratorValues();
    if (nullptr != delete_values) {
        for (auto iter = delete_values->cbegin(); iter != delete_values->cend();
             iter++) {
            if (!list_ir_builder_delete.BuildIteratorDelete(*iter, &ret_delete,
                                                            status)) {
                LOG(WARNING) << "fail to build iterator delete expression: "
                             << status.msg;
                return false;
            }
        }
    }
    return true;
}
bool BlockIRBuilder::ClearAllScopeValues(llvm::BasicBlock *block,
                                         base::Status &status) {
    auto values_vec = sv_->GetIteratorValues();
    llvm::Value *ret_delete = nullptr;
    ListIRBuilder list_ir_builder_delete(block, sv_);
    for (auto iter = values_vec.cbegin(); iter != values_vec.cend(); iter++) {
        auto delete_values = *iter;
        if (nullptr != delete_values) {
            for (auto iter = delete_values->cbegin();
                 iter != delete_values->cend(); iter++) {
                if (!list_ir_builder_delete.BuildIteratorDelete(
                        *iter, &ret_delete, status)) {
                    LOG(WARNING) << "fail to build iterator delete expression: "
                                 << status.msg;
                    return false;
                }
            }
        }
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
