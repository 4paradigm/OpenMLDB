/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * context.h
 *
 * Author: chenjing
 * Date: 2020/2/12
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_CONTEXT_H_
#define SRC_CODEGEN_CONTEXT_H_

#include <string>
#include <vector>
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"

#include "base/fe_status.h"
#include "codegen/native_value.h"

namespace fesql {
namespace codegen {

using ::fesql::base::Status;

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

class BlockGroupGuard {
 public:
    explicit BlockGroupGuard(BlockGroup* group);
    ~BlockGroupGuard();

 private:
    CodeGenContext* ctx_;
    BlockGroup* prev_;
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
};

class CodeGenContext {
 public:
    explicit CodeGenContext(::llvm::Module*);

    ::llvm::Function* GetCurrentFunction() const;
    void SetCurrentFunction(::llvm::Function*);

    BlockGroup* GetCurrentBlockGroup() const;
    void SetCurrentBlockGroup(BlockGroup*);

    ::llvm::BasicBlock* GetCurrentBlock() const;
    void SetCurrentBlock(::llvm::BasicBlock*);

    ::llvm::IRBuilder<>* GetBuilder();

    ::llvm::Module* GetModule() { return llvm_module_; }

    Status CreateBranch(const NativeValue& cond,
                        const std::function<Status()>& left,
                        const std::function<Status()>& right);

    ::llvm::BasicBlock* AppendNewBlock(const std::string& name = "");

 private:
    ::llvm::LLVMContext* llvm_ctx_;
    ::llvm::Module* llvm_module_;
    ::llvm::IRBuilder<> llvm_ir_builder_;

    ::llvm::Function* current_llvm_function_ = nullptr;
    BlockGroup* current_block_group_ = nullptr;
    ::llvm::BasicBlock* current_llvm_block_ = nullptr;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_CONTEXT_H_
