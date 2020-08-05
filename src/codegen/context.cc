/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * context.cc
 *
 * Author: chenjing
 * Date: 2020/2/12
 *--------------------------------------------------------------------------
 **/
#include "codegen/context.h"
#include <memory>
#include "glog/logging.h"

namespace fesql {
namespace codegen {

BlockGroup::BlockGroup(CodeGenContext* ctx) : ctx_(ctx), name_("") {
    blocks_.push_back(ctx->AppendNewBlock(name_));
}

BlockGroup::BlockGroup(const std::string& name, CodeGenContext* ctx)
    : ctx_(ctx), name_(name) {
    blocks_.push_back(ctx->AppendNewBlock(name_));
}

BlockGroup::BlockGroup(::llvm::BasicBlock* entry, CodeGenContext* ctx)
    : ctx_(ctx), name_("") {
    blocks_.push_back(entry);
}

void BlockGroup::DropEmptyBlocks() {
    size_t pos = 0;
    for (size_t i = 0; i < blocks_.size(); ++i) {
        auto block = blocks_[i];
        if (!block->empty() && block->hasNPredecessorsOrMore(1)) {
            blocks_[pos] = block;
            pos += 1;
        } else {
            if (block->getParent() != nullptr) {
                block->eraseFromParent();
            }
        }
    }
    blocks_.resize(pos);
}

void BlockGroup::ReInsertTo(::llvm::Function* fn) {
    for (auto block : blocks_) {
        if (block->getParent() != nullptr) {
            block->removeFromParent();
        }
        if (block->empty()) {
            BlockGuard guard(block, ctx_);
            ctx_->GetBuilder()->CreateUnreachable();
        }
        block->insertInto(fn);
    }
}

llvm::BasicBlock* BlockGroup::first() const { return blocks_[0]; }

llvm::BasicBlock* BlockGroup::last() const { return blocks_.back(); }

const llvm::Instruction* BlockGroup::last_inst() const {
    for (auto iter = blocks_.rbegin(); iter != blocks_.rend(); iter++) {
        llvm::BasicBlock* block = *iter;
        if (block->empty()) {
            return nullptr;
        } else {
            return &(*block->rbegin());
        }
    }
    return nullptr;
}

void BlockGroup::Add(const BlockGroup& sub) {
    for (auto block : sub.blocks()) {
        blocks_.push_back(block);
    }
    auto last = sub.last();
    if (ctx_->GetCurrentBlockGroup() == this) {
        ctx_->SetCurrentBlock(last);
        ctx_->GetBuilder()->SetInsertPoint(last);
    }
}

void BlockGroup::Add(llvm::BasicBlock* block) {
    blocks_.push_back(block);
    if (ctx_->GetCurrentBlockGroup() == this) {
        ctx_->SetCurrentBlock(block);
        ctx_->GetBuilder()->SetInsertPoint(block);
    }
}

BlockGroupGuard::BlockGroupGuard(BlockGroup* group)
    : ctx_(group->ctx()), prev_(ctx_->GetCurrentBlockGroup()) {
    ctx_->SetCurrentBlockGroup(group);
    auto cur = group->last();
    ctx_->SetCurrentBlock(cur);
    ctx_->GetBuilder()->SetInsertPoint(cur);
}

BlockGroupGuard::~BlockGroupGuard() {
    ctx_->SetCurrentBlockGroup(prev_);
    if (prev_ != nullptr) {
        auto cur = prev_->last();
        ctx_->SetCurrentBlock(cur);
        ctx_->GetBuilder()->SetInsertPoint(cur);
    }
}

BlockGuard::BlockGuard(llvm::BasicBlock* block, CodeGenContext* ctx)
    : ctx_(ctx), prev_(ctx->GetCurrentBlock()) {
    ctx_->SetCurrentBlock(block);
    if (block != nullptr) {
        ctx_->GetBuilder()->SetInsertPoint(block);
    }
}

BlockGuard::~BlockGuard() {
    ctx_->SetCurrentBlock(prev_);
    if (prev_ != nullptr) {
        ctx_->GetBuilder()->SetInsertPoint(prev_);
    }
}

FunctionScopeGuard::FunctionScopeGuard(llvm::Function* function,
                                       CodeGenContext* ctx)
    : ctx_(ctx), prev_function_(ctx->GetCurrentFunction()) {
    ctx_->SetCurrentFunction(function);
}

FunctionScopeGuard::~FunctionScopeGuard() {
    ctx_->SetCurrentFunction(prev_function_);
}

CodeGenContext::CodeGenContext(::llvm::Module* module)
    : llvm_ctx_(&module->getContext()),
      llvm_module_(module),
      llvm_ir_builder_(*llvm_ctx_) {}

::llvm::Function* CodeGenContext::GetCurrentFunction() const {
    return current_llvm_function_;
}

void CodeGenContext::SetCurrentFunction(::llvm::Function* function) {
    this->current_llvm_function_ = function;
}

BlockGroup* CodeGenContext::GetCurrentBlockGroup() const {
    return current_block_group_;
}

void CodeGenContext::SetCurrentBlockGroup(BlockGroup* group) {
    this->current_block_group_ = group;
}

::llvm::BasicBlock* CodeGenContext::GetCurrentBlock() const {
    return current_llvm_block_;
}

void CodeGenContext::SetCurrentBlock(::llvm::BasicBlock* block) {
    this->current_llvm_block_ = block;
}

::llvm::IRBuilder<>* CodeGenContext::GetBuilder() { return &llvm_ir_builder_; }

::llvm::BasicBlock* CodeGenContext::AppendNewBlock(const std::string& name) {
    if (GetCurrentFunction() == nullptr) {
        LOG(WARNING) << "Create block out side of any llvm function, "
                        "this may cause ir builder errors";
    }
    return ::llvm::BasicBlock::Create(*llvm_ctx_, name, GetCurrentFunction());
}

Status CodeGenContext::CreateBranch(const NativeValue& cond,
                                    const std::function<Status()>& left,
                                    const std::function<Status()>& right) {
    return CreateBranchImpl(cond.GetValue(this), &left, &right);
}

Status CodeGenContext::CreateBranch(::llvm::Value* cond,
                                    const std::function<Status()>& left,
                                    const std::function<Status()>& right) {
    return CreateBranchImpl(cond, &left, &right);
}

Status CodeGenContext::CreateBranch(const NativeValue& cond,
                                    const std::function<Status()>& left) {
    return CreateBranchImpl(cond.GetValue(this), &left, nullptr);
}

Status CodeGenContext::CreateBranch(::llvm::Value* cond,
                                    const std::function<Status()>& left) {
    return CreateBranchImpl(cond, &left, nullptr);
}

Status CodeGenContext::CreateBranchNot(const NativeValue& cond,
                                       const std::function<Status()>& right) {
    return CreateBranchImpl(cond.GetValue(this), nullptr, &right);
}

Status CodeGenContext::CreateBranchNot(::llvm::Value* cond,
                                       const std::function<Status()>& right) {
    return CreateBranchImpl(cond, nullptr, &right);
}

Status CodeGenContext::CreateBranchImpl(llvm::Value* cond,
                                        const std::function<Status()>* left,
                                        const std::function<Status()>* right) {
    auto builder = this->GetBuilder();
    std::unique_ptr<BlockGroup> if_body = nullptr;
    if (left != nullptr) {
        if_body = std::unique_ptr<BlockGroup>(
            new BlockGroup("__true_branch__", this));
    }

    std::unique_ptr<BlockGroup> else_body = nullptr;
    if (right != nullptr) {
        else_body = std::unique_ptr<BlockGroup>(
            new BlockGroup("__false_branch__", this));
    }

    // current scope blocks
    auto cur_block_group = this->GetCurrentBlockGroup();

    // exit point
    ::llvm::BasicBlock* exit_block = AppendNewBlock("__branch_exit__");

    // get condition
    auto bool_ty = ::llvm::Type::getInt1Ty(*llvm_ctx_);
    if (cond->getType() != bool_ty) {
        cond = builder->CreateIntCast(cond, bool_ty, true);
    }
    if (left == nullptr) {
        builder->CreateCondBr(cond, exit_block, else_body->first());
    } else if (right == nullptr) {
        builder->CreateCondBr(cond, if_body->first(), exit_block);
    } else {
        builder->CreateCondBr(cond, if_body->first(), else_body->first());
    }

    // goto exit point if current branch does not ends with terminal
    auto do_end_block = [this, builder, exit_block]() {
        auto last_inst = GetCurrentBlockGroup()->last_inst();
        if (last_inst == nullptr || !last_inst->isTerminator()) {
            builder->CreateBr(exit_block);
        }
    };

    if (left != nullptr) {
        BlockGroupGuard guard(if_body.get());
        CHECK_STATUS((*left)());
        do_end_block();
        cur_block_group->Add(*if_body);
    }

    if (right != nullptr) {
        BlockGroupGuard guard(else_body.get());
        CHECK_STATUS((*right)());
        do_end_block();
        cur_block_group->Add(*else_body);
    }

    cur_block_group->Add(exit_block);
    return Status::OK();
}

}  // namespace codegen
}  // namespace fesql
