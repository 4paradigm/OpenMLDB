/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * memery_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/7/22
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_MEMERY_IR_BUILDER_H_
#define SRC_CODEGEN_MEMERY_IR_BUILDER_H_
#include <string>
#include "base/fe_status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
namespace fesql {
namespace codegen {

class MemoryIRBuilder {
 public:
    MemoryIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~MemoryIRBuilder();

    base::Status Alloc(::llvm::Value* request_size,
                       ::llvm::Value** output);  // NOLINT
 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_MEMERY_IR_BUILDER_H_
