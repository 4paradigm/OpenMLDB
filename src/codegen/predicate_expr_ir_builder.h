/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * predicate_expr_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/1/9
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_PREDICATE_EXPR_IR_BUILDER_H_
#define SRC_CODEGEN_PREDICATE_EXPR_IR_BUILDER_H_

#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

using fesql::base::Status;

namespace fesql {
namespace codegen {

class PredicateIRBuilder {
 public:
    explicit PredicateIRBuilder(::llvm::BasicBlock* block);
    ~PredicateIRBuilder();

    Status BuildAndExpr(NativeValue left, NativeValue right,
                        NativeValue* output);
    Status BuildOrExpr(NativeValue left, NativeValue right,
                       NativeValue* output);
    Status BuildXorExpr(NativeValue left, NativeValue right,
                        NativeValue* output);
    Status BuildNotExpr(NativeValue left, NativeValue* output);

    Status BuildEqExpr(NativeValue left, NativeValue right,
                       NativeValue* output);
    Status BuildNeqExpr(NativeValue left, NativeValue right,
                        NativeValue* output);
    Status BuildGtExpr(NativeValue left, NativeValue right,
                       NativeValue* output);
    Status BuildGeExpr(NativeValue left, NativeValue right,
                       NativeValue* output);
    Status BuildLtExpr(NativeValue left, NativeValue right,
                       NativeValue* output);
    Status BuildLeExpr(NativeValue left, NativeValue right,
                       NativeValue* output);
    Status BuildIsNullExpr(NativeValue left, NativeValue* output);

    static bool BuildEqExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            base::Status& status);  // NOLINT
    static bool BuildNeqExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                             ::llvm::Value* right, ::llvm::Value** output,
                             base::Status& status);  // NOLINT
    static bool BuildGtExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            base::Status& status);  // NOLINT
    static bool BuildGeExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            base::Status& status);  // NOLINT
    static bool BuildLtExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            base::Status& status);  // NOLINT
    static bool BuildLeExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            base::Status& status);  // NOLINT

    Status BuildIsNullExpr(::llvm::BasicBlock*, NativeValue input,
                           NativeValue* output);

 private:
    static bool IsAcceptType(::llvm::Type* type);
    static bool InferBoolTypes(::llvm::BasicBlock* block, ::llvm::Value* value,
                               ::llvm::Value** casted_value,
                               ::fesql::base::Status& status);  // NOLINT
    static bool InferBaseTypes(::llvm::BasicBlock* block, ::llvm::Value* left,
                               ::llvm::Value* right,
                               ::llvm::Value** casted_left,
                               ::llvm::Value** casted_right,
                               ::fesql::base::Status& status);  // NOLINT

 private:
    ::llvm::BasicBlock* block_;
    CastExprIRBuilder cast_expr_ir_builder_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_PREDICATE_EXPR_IR_BUILDER_H_
