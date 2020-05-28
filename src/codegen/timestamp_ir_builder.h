/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * timestamp_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/5/22
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_TIMESTAMP_IR_BUILDER_H_
#define SRC_CODEGEN_TIMESTAMP_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/struct_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace codegen {

class TimestampIRBuilder : public StructTypeIRBuilder {
 public:
    explicit TimestampIRBuilder(::llvm::Module* m);
    ~TimestampIRBuilder();
    void InitStructType();
    bool NewTimestamp(::llvm::BasicBlock* block,
                      ::llvm::Value** output);
    bool NewTimestamp(::llvm::BasicBlock* block, ::llvm::Value* ts,
                      ::llvm::Value** output);
    bool GetTs(::llvm::BasicBlock* block, ::llvm::Value* timestamp,
               ::llvm::Value** output);
    bool SetTs(::llvm::BasicBlock* block, ::llvm::Value* timestamp,
               ::llvm::Value* ts);
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_TIMESTAMP_IR_BUILDER_H_
