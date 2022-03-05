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

#ifndef HYBRIDSE_SRC_CODEGEN_CONTEXT_H_
#define HYBRIDSE_SRC_CODEGEN_CONTEXT_H_

#include <string>
#include <unordered_map>
#include <vector>
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"

#include "base/fe_status.h"
#include "codegen/native_value.h"
#include "codegen/scope_var.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace codegen {

using ::hybridse::base::Status;

class CodeGenContext;

class BlockGroup {
 public:
    explicit BlockGroup(CodeGenContext* ctx);
    BlockGroup(::llvm::BasicBlock* entry, CodeGenContext* ctx);
    BlockGroup(const std::string& name, CodeGenContext* ctx);

    llvm::BasicBlock* first() const;
    llvm::BasicBlock* last() const;

    const llvm::Instruction* last_inst() const;
    void DropEmptyBlocks();
    void ReInsertTo(::llvm::Function* fn);

    void Add(const BlockGroup& sub);
    void Add(llvm::BasicBlock*);

    const std::vector<llvm::BasicBlock*>& blocks() const {
        return this->blocks_;
    }

    CodeGenContext* ctx() const { return this->ctx_; }

 private:
    CodeGenContext* ctx_;
    std::string name_;
    std::vector<llvm::BasicBlock*> blocks_;
};

class CodeScope {
 public:
    CodeScope(CodeGenContext* ctx, const std::string& name, CodeScope* parent);
    CodeScope(CodeGenContext* ctx, ::llvm::BasicBlock* entry);

    BlockGroup* blocks() { return &blocks_; }
    ScopeVar* sv() { return &sv_; }
    CodeScope* parent() const { return parent_; }
    CodeGenContext* ctx() const { return blocks_.ctx(); }

 private:
    BlockGroup blocks_;
    ScopeVar sv_;
    CodeScope* parent_;
};

class CodeScopeGuard {
 public:
    explicit CodeScopeGuard(CodeScope* scope);
    ~CodeScopeGuard();

 private:
    CodeGenContext* ctx_;
    CodeScope* prev_;
};

class BlockGuard {
 public:
    BlockGuard(llvm::BasicBlock* block, CodeGenContext* ctx);
    ~BlockGuard();

 private:
    CodeGenContext* ctx_;
    llvm::BasicBlock* prev_;
};

class FunctionScopeGuard {
 public:
    FunctionScopeGuard(llvm::Function* function, CodeGenContext* ctx);
    ~FunctionScopeGuard();

 private:
    CodeGenContext* ctx_;
    llvm::Function* prev_function_;
    CodeScopeGuard sub_guard_;
};

class CodeGenContext {
 public:
    CodeGenContext(::llvm::Module*, const vm::SchemasContext* schemas_context,
                   const codec::Schema* parameter_types,
                   node::NodeManager* node_manager);
    ~CodeGenContext();

    ::llvm::Function* GetCurrentFunction() const;
    void SetCurrentFunction(::llvm::Function*);

    CodeScope* GetCurrentScope() const;
    void SetCurrentScope(CodeScope*);

    ::llvm::BasicBlock* GetCurrentBlock() const;
    void SetCurrentBlock(::llvm::BasicBlock*);

    ::llvm::IRBuilder<>* GetBuilder();

    ::llvm::Module* GetModule() { return llvm_module_; }
    ::llvm::LLVMContext& GetLLVMContext() { return *llvm_ctx_; }

    CodeScope* GetFunctionScope(const std::string& name);

    Status CreateBranch(const NativeValue& cond,
                        const std::function<Status()>& left,
                        const std::function<Status()>& right);
    Status CreateBranch(::llvm::Value* cond,
                        const std::function<Status()>& left,
                        const std::function<Status()>& right);
    Status CreateBranch(const NativeValue& cond,
                        const std::function<Status()>& left);
    Status CreateBranch(::llvm::Value* cond,
                        const std::function<Status()>& left);
    Status CreateBranchNot(const NativeValue& cond,
                           const std::function<Status()>& right);
    Status CreateBranchNot(::llvm::Value* cond,
                           const std::function<Status()>& right);

    Status CreateWhile(const std::function<Status(::llvm::Value** res)>& cond,
                       const std::function<Status()>& body);

    ::llvm::BasicBlock* AppendNewBlock(const std::string& name = "");

    const vm::SchemasContext* schemas_context() const;
    const codec::Schema* parameter_types() const;
    const codec::RowFormat* parameter_row_format() const;
    node::NodeManager* node_manager() const;

 private:
    Status CreateBranchImpl(::llvm::Value* cond,
                            const std::function<Status()>* left,
                            const std::function<Status()>* right);

    ::llvm::LLVMContext* llvm_ctx_;
    ::llvm::Module* llvm_module_;
    ::llvm::IRBuilder<> llvm_ir_builder_;

    ::llvm::Function* current_llvm_function_ = nullptr;
    CodeScope* current_scope_ = nullptr;
    ::llvm::BasicBlock* current_llvm_block_ = nullptr;

    const vm::SchemasContext* schemas_context_;
    const codec::Schema* parameter_types_;
    codec::RowFormat* parameter_row_format_ = nullptr;

    std::unordered_map<std::string, CodeScope> function_scopes_;

    node::NodeManager* node_manager_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_CONTEXT_H_
