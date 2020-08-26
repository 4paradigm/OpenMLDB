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

    bool BuildEqExpr(::llvm::Value* left, ::llvm::Value* right,
                     ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildNeqExpr(::llvm::Value* left, ::llvm::Value* right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildGtExpr(::llvm::Value* left, ::llvm::Value* right,
                     ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildGeExpr(::llvm::Value* left, ::llvm::Value* right,
                     ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildLtExpr(::llvm::Value* left, ::llvm::Value* right,
                     ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildLeExpr(::llvm::Value* left, ::llvm::Value* right,
                     ::llvm::Value** output, base::Status& status);  // NOLINT

    Status BuildIsNullExpr(NativeValue input, NativeValue* output);

 private:
    bool IsAcceptType(::llvm::Type* type);
    bool InferBoolTypes(::llvm::Value* value, ::llvm::Value** casted_value,
                        ::fesql::base::Status& status);  // NOLINT
    bool InferBaseTypes(::llvm::Value* left, ::llvm::Value* right,
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
