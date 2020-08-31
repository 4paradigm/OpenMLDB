/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * cast_expr_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 ***/

#ifndef SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
#define SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"
#include "codegen/cond_select_ir_builder.h"
using fesql::base::Status;
namespace fesql {
namespace codegen {
class CastExprIRBuilder {
 public:
    explicit CastExprIRBuilder(::llvm::BasicBlock* block);
    ~CastExprIRBuilder();

    Status SafeCast(const NativeValue& value, ::llvm::Type* type,
                  NativeValue* output);  // NOLINT

    bool SafeCast(::llvm::Value* value, ::llvm::Type* type,
                  ::llvm::Value** output, base::Status& status);  // NOLINT

    bool UnSafeCast(::llvm::Value* value, ::llvm::Type* type,
                    ::llvm::Value** output, base::Status& status);  // NOLINT
    bool UnSafeCastDouble(::llvm::Value* value, ::llvm::Type* type,
                          ::llvm::Value** output,
                          base::Status& status);  // NOLINT

    bool StringCast(llvm::Value* value, llvm::Value** casted_value,
                    base::Status& status);  // NOLINT

    bool TimestampCast(llvm::Value* value, llvm::Value** cated_value,
                       base::Status& status);  // NOLINT
    bool IsSafeCast(::llvm::Type* src, ::llvm::Type* dist);
    bool IsIntFloat2PointerCast(::llvm::Type* src, ::llvm::Type* dist);
    bool IsStringCast(llvm::Type* type);

    bool BoolCast(llvm::Value* pValue, llvm::Value** pValue1,
                  base::Status& status);  // NOLINT

 private:
    ::llvm::BasicBlock* block_;
    CondSelectIRBuilder cond_select_ir_builder_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
