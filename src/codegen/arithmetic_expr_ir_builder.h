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
#include "proto/type.pb.h"
#include "base/status.h"
#include "codegen/scope_var.h"
#include "codegen/cast_expr_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
namespace fesql {
namespace codegen {
class ArithmeticIRBuilder {
 public:
    ArithmeticIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~ArithmeticIRBuilder();

    bool BuildAddExpr(::llvm::Value* left, ::llvm::Value *right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildSubExpr(::llvm::Value* left, ::llvm::Value *right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildMultiExpr(::llvm::Value* left, ::llvm::Value *right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildFDivExpr(::llvm::Value* left, ::llvm::Value *right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT

    bool BuildModExpr(llvm::Value* left, llvm::Value* right,
                      llvm::Value** output, base::Status status);

 private:
    bool IsAcceptType(::llvm::Type* type);
    bool inferBaseTypes(::llvm::Value* left, ::llvm::Value* right,
                                        ::llvm::Value** casted_left,
                                        ::llvm::Value** casted_right,
                                        ::fesql::base::Status& status);
    bool inferBaseDoubleTypes(::llvm::Value* left, ::llvm::Value* right,
                                                  ::llvm::Value** casted_left,
                                                  ::llvm::Value** casted_right,
                                                  ::fesql::base::Status& status);
    CastExprIRBuilder _cast_expr_ir_builder;
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
