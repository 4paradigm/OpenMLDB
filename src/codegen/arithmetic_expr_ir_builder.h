/*
 * arithmetic_expr_ir_builder.h
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
                             ::fesql::base::Status& status);  // NOLINT
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
    static Status AndTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status OrTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static bool InferAndCastedNumberTypes(
        ::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
        ::llvm::Value** casted_left, ::llvm::Value** casted_right,
        ::fesql::base::Status& status);  // NOLINT

    static bool InferAndCastIntegerTypes(
        ::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
        ::llvm::Value** casted_left, ::llvm::Value** casted_right,
        ::fesql::base::Status& status);  // NOLINT
    static bool InferAndCastDoubleTypes(
        ::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
        ::llvm::Value** casted_left, ::llvm::Value** casted_right,
        ::fesql::base::Status& status);  // NOLINT
    ::llvm::BasicBlock* block_;
    CastExprIRBuilder cast_expr_ir_builder_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
