/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * date_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/6/1
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_DATE_IR_BUILDER_H_
#define SRC_CODEGEN_DATE_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/struct_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace codegen {

class DateIRBuilder : public StructTypeIRBuilder {
 public:
    explicit DateIRBuilder(::llvm::Module* m);
    ~DateIRBuilder();
    void InitStructType();
    bool NewDate(::llvm::BasicBlock* block,
                      ::llvm::Value** output);
    bool NewDate(::llvm::BasicBlock* block, ::llvm::Value* date,
                      ::llvm::Value** output);
    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                  ::llvm::Value* dist);
    bool GetDays(::llvm::BasicBlock* block, ::llvm::Value* timestamp,
               ::llvm::Value** output);
    bool SetDays(::llvm::BasicBlock* block, ::llvm::Value* timestamp,
               ::llvm::Value* ts);
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_DATE_IR_BUILDER_H_
