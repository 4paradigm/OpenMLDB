/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * arithmetic_expr_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
#define SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
#include "base/status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/type.pb.h"
namespace fesql {
namespace codegen {
class ArithmeticIRBuilder {
 public:
    explicit ArithmeticIRBuilder(::llvm::BasicBlock* block);
    ~ArithmeticIRBuilder();

    bool BuildAddExpr(::llvm::Value* left, ::llvm::Value* right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildSubExpr(::llvm::Value* left, ::llvm::Value* right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildMultiExpr(::llvm::Value* left, ::llvm::Value* right,
                        ::llvm::Value** output,
                        base::Status& status);  // NOLINT
    bool BuildFDivExpr(::llvm::Value* left, ::llvm::Value* right,
                       ::llvm::Value** output, base::Status& status);  // NOLINT

    bool BuildModExpr(llvm::Value* left, llvm::Value* right,
                      llvm::Value** output, base::Status status);

 private:
    bool IsAcceptType(::llvm::Type* type);
    bool InferBaseTypes(::llvm::Value* left, ::llvm::Value* right,
                        ::llvm::Value** casted_left,
                        ::llvm::Value** casted_right,
                        ::fesql::base::Status& status);  // NOLINT
    bool InferBaseDoubleTypes(::llvm::Value* left, ::llvm::Value* right,
                              ::llvm::Value** casted_left,
                              ::llvm::Value** casted_right,
                              ::fesql::base::Status& status);  // NOLINT
    ::llvm::BasicBlock* block_;
    CastExprIRBuilder cast_expr_ir_builder_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
