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

#include "codegen/context.h"
#include <memory>
#include "glog/logging.h"

DECLARE_bool(enable_spark_unsaferow_format);

namespace hybridse {
namespace codegen {

BlockGroup::BlockGroup(CodeGenContextBase* ctx) : ctx_(ctx), name_("") {
    blocks_.push_back(ctx->AppendNewBlock(name_));
}

BlockGroup::BlockGroup(const std::string& name, CodeGenContextBase* ctx)
    : ctx_(ctx), name_(name) {
    blocks_.push_back(ctx->AppendNewBlock(name_));
}

BlockGroup::BlockGroup(::llvm::BasicBlock* entry, CodeGenContextBase* ctx)
    : ctx_(ctx), name_("") {
    blocks_.push_back(entry);
}

CodeScope::CodeScope(CodeGenContextBase* ctx, const std::string& name,
                     CodeScope* parent)
    : blocks_(name, ctx),
      sv_(parent == nullptr ? nullptr : parent->sv()),
      parent_(parent) {}

CodeScope::CodeScope(CodeGenContextBase* ctx, ::llvm::BasicBlock* entry)
    : blocks_(entry, ctx), sv_(), parent_(nullptr) {}

void BlockGroup::DropEmptyBlocks() {
    size_t pos = 0;
    for (size_t i = 0; i < blocks_.size(); ++i) {
        auto block = blocks_[i];
        if (block == &block->getParent()->getEntryBlock() ||
            (!block->empty() && block->hasNPredecessorsOrMore(1))) {
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
    if (ctx_->GetCurrentScope()->blocks() == this) {
        ctx_->SetCurrentBlock(last);
        ctx_->GetBuilder()->SetInsertPoint(last);
    }
}

void BlockGroup::Add(llvm::BasicBlock* block) {
    blocks_.push_back(block);
    if (ctx_->GetCurrentScope()->blocks() == this) {
        ctx_->SetCurrentBlock(block);
        ctx_->GetBuilder()->SetInsertPoint(block);
    }
}

CodeScopeGuard::CodeScopeGuard(CodeScope* new_scope)
    : ctx_(new_scope->ctx()), prev_(ctx_->GetCurrentScope()) {
    ctx_->SetCurrentScope(new_scope);
    auto cur = new_scope->blocks()->last();
    ctx_->SetCurrentBlock(cur);
    ctx_->GetBuilder()->SetInsertPoint(cur);
}

CodeScopeGuard::~CodeScopeGuard() {
    ctx_->SetCurrentScope(prev_);
    if (prev_ != nullptr) {
        auto cur = prev_->blocks()->last();
        ctx_->SetCurrentBlock(cur);
        ctx_->GetBuilder()->SetInsertPoint(cur);
    }
}

BlockGuard::BlockGuard(llvm::BasicBlock* block, CodeGenContextBase* ctx)
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
                                       CodeGenContextBase* ctx)
    : ctx_(ctx),
      prev_function_(ctx->GetCurrentFunction()),
      sub_guard_(ctx->GetFunctionScope(function->getName())) {
    ctx_->SetCurrentFunction(function);
}

FunctionScopeGuard::~FunctionScopeGuard() {
    ctx_->SetCurrentFunction(prev_function_);
}

CodeGenContextBase::CodeGenContextBase(::llvm::Module* module)
    : llvm_ctx_(&module->getContext()), llvm_module_(module), llvm_ir_builder_(*llvm_ctx_) {}

CodeGenContext::CodeGenContext(::llvm::Module* module, const vm::SchemasContext* schemas_context,
                               const codec::Schema* parameter_types, node::NodeManager* node_manager)
    : CodeGenContextBase(module),
      schemas_context_(schemas_context),
      parameter_types_(parameter_types),
      node_manager_(node_manager) {
    if (FLAGS_enable_spark_unsaferow_format) {
        parameter_row_format_ = new codec::SingleSliceRowFormat(parameter_types);
    } else {
        parameter_row_format_ = new codec::MultiSlicesRowFormat(parameter_types);
    }
}

CodeGenContext::~CodeGenContext() {
    if (parameter_row_format_) {
        delete parameter_row_format_;
    }
}

::llvm::Function* CodeGenContextBase::GetCurrentFunction() const {
    return current_llvm_function_;
}

void CodeGenContextBase::SetCurrentFunction(::llvm::Function* function) {
    this->current_llvm_function_ = function;
}

CodeScope* CodeGenContextBase::GetCurrentScope() const { return current_scope_; }

void CodeGenContextBase::SetCurrentScope(CodeScope* scope) {
    this->current_scope_ = scope;
}

::llvm::BasicBlock* CodeGenContextBase::GetCurrentBlock() const {
    return current_llvm_block_;
}

void CodeGenContextBase::SetCurrentBlock(::llvm::BasicBlock* block) {
    this->current_llvm_block_ = block;
}

const vm::SchemasContext* CodeGenContext::schemas_context() const {
    return schemas_context_;
}
const codec::Schema* CodeGenContext::parameter_types() const {
    return parameter_types_;
}
const codec::RowFormat* CodeGenContext::parameter_row_format() const {
    return parameter_row_format_;
}
node::NodeManager* CodeGenContext::node_manager() const {
    return node_manager_;
}

::llvm::IRBuilder<>* CodeGenContextBase::GetBuilder() { return &llvm_ir_builder_; }

::llvm::BasicBlock* CodeGenContextBase::AppendNewBlock(const std::string& name) {
    if (GetCurrentFunction() == nullptr) {
        LOG(WARNING) << "Create block out side of any llvm function, "
                        "this may cause ir builder errors";
    }
    return ::llvm::BasicBlock::Create(*llvm_ctx_, name, GetCurrentFunction());
}

Status CodeGenContextBase::CreateBranch(const NativeValue& cond, const std::function<Status()>& left,
                                        const std::function<Status()>& right, absl::string_view name) {
    return CreateBranchImpl(cond.GetValue(this), &left, &right, name);
}

Status CodeGenContextBase::CreateBranch(::llvm::Value* cond, const std::function<Status()>& left,
                                        const std::function<Status()>& right, absl::string_view name) {
    return CreateBranchImpl(cond, &left, &right, name);
}

Status CodeGenContextBase::CreateBranch(const NativeValue& cond, const std::function<Status()>& left,
                                        absl::string_view name) {
    return CreateBranchImpl(cond.GetValue(this), &left, nullptr, name);
}

Status CodeGenContextBase::CreateBranch(::llvm::Value* cond, const std::function<Status()>& left,
                                        absl::string_view name) {
    return CreateBranchImpl(cond, &left, nullptr, name);
}

Status CodeGenContextBase::CreateBranchNot(const NativeValue& cond, const std::function<Status()>& right,
                                           absl::string_view name) {
    return CreateBranchImpl(cond.GetValue(this), nullptr, &right, name);
}

Status CodeGenContextBase::CreateBranchNot(::llvm::Value* cond, const std::function<Status()>& right,
                                           absl::string_view name) {
    return CreateBranchImpl(cond, nullptr, &right, name);
}

Status CodeGenContextBase::CreateBranchImpl(llvm::Value* cond,
                                            const std::function<Status()>* left,
                                            const std::function<Status()>* right,
                                            absl::string_view name) {
    // current scope
    auto cur_scope = this->GetCurrentScope();

    auto builder = this->GetBuilder();
    std::unique_ptr<CodeScope> if_body = nullptr;
    if (left != nullptr) {
        if_body = std::unique_ptr<CodeScope>(
            new CodeScope(this, absl::StrCat(name, "__true_branch__"), cur_scope));
    }

    std::unique_ptr<CodeScope> else_body = nullptr;
    if (right != nullptr) {
        else_body = std::unique_ptr<CodeScope>(
            new CodeScope(this, absl::StrCat(name, "__false_branch__"), cur_scope));
    }

    // exit point
    ::llvm::BasicBlock* exit_block = AppendNewBlock(absl::StrCat(name, "__branch_exit__"));

    // get condition
    auto bool_ty = ::llvm::Type::getInt1Ty(*llvm_ctx_);
    if (cond->getType() != bool_ty) {
        cond = builder->CreateIntCast(cond, bool_ty, true);
    }
    if (left == nullptr) {
        builder->CreateCondBr(cond, exit_block, else_body->blocks()->first());
    } else if (right == nullptr) {
        builder->CreateCondBr(cond, if_body->blocks()->first(), exit_block);
    } else {
        builder->CreateCondBr(cond, if_body->blocks()->first(),
                              else_body->blocks()->first());
    }

    // goto exit point if current branch does not ends with terminal
    auto do_end_block = [this, builder, exit_block]() {
        auto last_inst = GetCurrentScope()->blocks()->last_inst();
        if (last_inst == nullptr || !last_inst->isTerminator()) {
            builder->CreateBr(exit_block);
        }
    };

    if (left != nullptr) {
        CodeScopeGuard guard(if_body.get());
        CHECK_STATUS((*left)());
        do_end_block();
        cur_scope->blocks()->Add(*if_body->blocks());
    }

    if (right != nullptr) {
        CodeScopeGuard guard(else_body.get());
        CHECK_STATUS((*right)());
        do_end_block();
        cur_scope->blocks()->Add(*else_body->blocks());
    }

    cur_scope->blocks()->Add(exit_block);

    GetBuilder()->SetInsertPoint(exit_block);
    SetCurrentBlock(exit_block);

    return Status::OK();
}

Status CodeGenContextBase::CreateWhile(const std::function<Status(::llvm::Value** res)>& build_cond,
                                       const std::function<Status()>& build_body, absl::string_view name) {
    // while entry and body
    auto cur_scope = this->GetCurrentScope();
    CodeScope entry_scope(this, absl::StrCat(name, "__while_entry__"), cur_scope);
    CodeScope body_scope(this, absl::StrCat(name, "__while_body__"), cur_scope);
    GetBuilder()->CreateBr(entry_scope.blocks()->first());

    // exit point
    ::llvm::BasicBlock* exit_block = AppendNewBlock(absl::StrCat(name, "__while_exit__"));

    {
        CodeScopeGuard entry_guard(&entry_scope);
        ::llvm::Value* has_next = nullptr;
        CHECK_STATUS(build_cond(&has_next));

        CHECK_TRUE(has_next != nullptr &&
                       has_next->getType() ==
                           ::llvm::Type::getInt1Ty(has_next->getContext()),
                   common::kCodegenError, "Illegal while condition");

        auto last_inst = entry_scope.blocks()->last_inst();
        CHECK_TRUE(last_inst != nullptr && !last_inst->isTerminator(),
                   common::kCodegenError, "Illegal while condition builder");
        GetBuilder()->CreateCondBr(has_next, body_scope.blocks()->first(),
                                   exit_block);
    }

    {
        CodeScopeGuard body_guard(&body_scope);
        CHECK_STATUS(build_body());

        // body back
        auto last_inst = body_scope.blocks()->last_inst();
        if (last_inst == nullptr || !last_inst->isTerminator()) {
            GetBuilder()->CreateBr(entry_scope.blocks()->first());
        }
    }

    auto cur_blocks = cur_scope->blocks();
    cur_blocks->Add(*entry_scope.blocks());
    cur_blocks->Add(*body_scope.blocks());
    cur_blocks->Add(exit_block);

    // set builder to exit block
    GetBuilder()->SetInsertPoint(exit_block);
    SetCurrentBlock(exit_block);

    return Status::OK();
}

CodeScope* CodeGenContextBase::GetFunctionScope(const std::string& name) {
    ::llvm::Function* function = llvm_module_->getFunction(name);
    if (function == nullptr) {
        LOG(WARNING) << "Can not get function scope for non-created function "
                     << name;
        return nullptr;
    }
    // Create code scope require reside in function
    ::llvm::Function* prev = this->GetCurrentFunction();
    this->SetCurrentFunction(function);
    CodeScope* function_scope = nullptr;
    auto iter = function_scopes_.find(name);
    if (iter == function_scopes_.end()) {
        auto res = function_scopes_.insert(
            iter, {name, CodeScope(this, "__fn_entry__", nullptr)});
        function_scope = &res->second;
    } else {
        function_scope = &iter->second;
    }
    this->SetCurrentFunction(prev);
    return function_scope;
}

}  // namespace codegen
}  // namespace hybridse
