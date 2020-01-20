/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * cast_expr_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
#define SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
#include "proto/type.pb.h"
#include "base/status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
namespace fesql {
namespace codegen {
class CastExprIRBuilder {
 public:
    CastExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~CastExprIRBuilder();


    bool SafeCast(::llvm::Value* value, ::llvm::Type* type,
                      ::llvm::Value** output, base::Status& status);  // NOLINT

    bool UnSafeCast(::llvm::Value* value, ::llvm::Type* type,
                  ::llvm::Value** output, base::Status& status);  // NOLINT
    bool UnSafeCastDouble(::llvm::Value* value, ::llvm::Type* type,
                    ::llvm::Value** output, base::Status& status);  // NOLINT

    bool StringCast(llvm::Value* value, llvm::Value** casted_value,
                    base::Status& status);

    bool isSafeCast(::llvm::Type* src, ::llvm::Type* dist);
    bool isIFCast(::llvm::Type* src, ::llvm::Type* dist);
    bool isStringCast(llvm::Type* type);


    bool BoolCast(llvm::Value* pValue, llvm::Value** pValue1,
                  base::Status& status);

 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
