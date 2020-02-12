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

#include "base/status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/type.pb.h"
namespace fesql {
namespace codegen {
class PredicateIRBuilder {
 public:
    explicit PredicateIRBuilder(::llvm::BasicBlock* block);
    ~PredicateIRBuilder();

    bool BuildAndExpr(::llvm::Value* left, ::llvm::Value* right,
                      ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildOrExpr(::llvm::Value* left, ::llvm::Value* right,
                     ::llvm::Value** output, base::Status& status);  // NOLINT
    bool BuildNotExpr(::llvm::Value* left, ::llvm::Value** output,
                      base::Status& status);  // NOLINT

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

 private:
    bool IsAcceptType(::llvm::Type* type);
    bool InferBoolTypes(::llvm::Value* value, ::llvm::Value** casted_value,
                        ::fesql::base::Status& status);  // NOLINT
    bool InferBaseTypes(::llvm::Value* left, ::llvm::Value* right,
                        ::llvm::Value** casted_left,
                        ::llvm::Value** casted_right,
                        ::fesql::base::Status& status);  // NOLINT
    CastExprIRBuilder cast_expr_ir_builder_;
    ::llvm::BasicBlock* block_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_PREDICATE_EXPR_IR_BUILDER_H_
