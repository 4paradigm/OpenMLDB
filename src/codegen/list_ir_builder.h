/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * list_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/2/14
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_LIST_IR_BUILDER_H_
#define SRC_CODEGEN_LIST_IR_BUILDER_H_

#include <base/status.h>
#include <string>
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/type.pb.h"
namespace fesql {
namespace codegen {


class ListIRBuilder {
 public:
    ListIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~ListIRBuilder();


    bool BuildAt(::llvm::Value* list, ::llvm::Value* pos, ::llvm::Value **output, base::Status &status);

 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_LIST_IR_BUILDER_H_
