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
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"
using fesql::base::Status;
namespace fesql {
namespace codegen {
class ArithmeticIRBuilder {
 public:
    explicit ArithmeticIRBuilder(::llvm::BasicBlock* block);
    ~ArithmeticIRBuilder();

    static bool BuildAnd(::llvm::BasicBlock* block, ::llvm::Value* left,
                         ::llvm::Value* right, ::llvm::Value** output,
                         base::Status& status);  // NOLINT
    static bool BuildLShiftLeft(::llvm::BasicBlock* block, ::llvm::Value* left,
                                ::llvm::Value* right, ::llvm::Value** output,
                                base::Status& status);  // NOLINT
    static bool BuildLShiftRight(::llvm::BasicBlock* block, ::llvm::Value* left,
                                 ::llvm::Value* right, ::llvm::Value** output,
                                 base::Status& status);  // NOLINT
    Status BuildAnd(const NativeValue& left, const NativeValue& right,
                    NativeValue* output);  // NOLINT
    Status BuildLShiftLeft(const NativeValue& left, const NativeValue& right,
                           NativeValue* output);  // NOLINT
    Status BuildLShiftRight(const NativeValue& left, const NativeValue& right,
                            NativeValue* output);  // NOLINT
    Status BuildAddExpr(const NativeValue& left, const NativeValue& right,
                        NativeValue* output);  // NOLINT
    Status BuildSubExpr(const NativeValue& left, const NativeValue& right,
                        NativeValue* output);  // NOLINT
    Status BuildMultiExpr(const NativeValue& left, const NativeValue& right,
                          NativeValue* output);  // NOLINT
    Status BuildFDivExpr(const NativeValue& left, const NativeValue& right,
                         NativeValue* output);  // NOLINT
    Status BuildSDivExpr(const NativeValue& left, const NativeValue& right,
                         NativeValue* output);  // NOLINT
    Status BuildModExpr(const NativeValue& left, const NativeValue& right,
                        NativeValue* output);  // NOLINT

    static bool BuildAddExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                             ::llvm::Value* right, ::llvm::Value** output,
                             ::fesql::base::Status& status); //NOLINT
    static bool BuildSubExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                             ::llvm::Value* right, ::llvm::Value** output,
                             base::Status& status);  // NOLINT
    static bool BuildMultiExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                               ::llvm::Value* right, ::llvm::Value** output,
                               base::Status& status);  // NOLINT
    static bool BuildFDivExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                              ::llvm::Value* right, ::llvm::Value** output,
                              base::Status& status);  // NOLINT
    static bool BuildSDivExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                              ::llvm::Value* right, ::llvm::Value** output,
                              base::Status& status);  // NOLINT

    static bool BuildModExpr(::llvm::BasicBlock* block, llvm::Value* left,
                             llvm::Value* right, llvm::Value** output,
                             base::Status status);

 private:
    static bool IsAcceptType(::llvm::Type* type);
    static Status FDivTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status SDivTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status ModTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status AddTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status SubTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status MultiTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status AndTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status OrTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static bool InferBaseTypes(::llvm::BasicBlock* block, ::llvm::Value* left,
                               ::llvm::Value* right,
                               ::llvm::Value** casted_left,
                               ::llvm::Value** casted_right,
                               ::fesql::base::Status& status);  // NOLINT

    static bool InferBaseIntegerTypes(::llvm::BasicBlock* block,
                                      ::llvm::Value* left, ::llvm::Value* right,
                                      ::llvm::Value** casted_left,
                                      ::llvm::Value** casted_right,
                                      ::fesql::base::Status& status);  // NOLINT
    static bool InferBaseDoubleTypes(::llvm::BasicBlock* block,
                                     ::llvm::Value* left, ::llvm::Value* right,
                                     ::llvm::Value** casted_left,
                                     ::llvm::Value** casted_right,
                                     ::fesql::base::Status& status);  // NOLINT
    ::llvm::BasicBlock* block_;
    CastExprIRBuilder cast_expr_ir_builder_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
