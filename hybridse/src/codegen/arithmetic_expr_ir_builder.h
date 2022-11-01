/*
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

#ifndef HYBRIDSE_SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_

#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
namespace codegen {

class ArithmeticIRBuilder {
 public:
    explicit ArithmeticIRBuilder(::llvm::BasicBlock* block);
    ~ArithmeticIRBuilder();

    Status BuildLShiftLeft(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildLShiftRight(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildAddExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildSubExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildMultiExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    // build for float divison
    Status BuildFDivExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    // build for integer divison
    Status BuildSDivExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildModExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildBitwiseAndExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildBitwiseOrExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildBitwiseXorExpr(const NativeValue& left, const NativeValue& right, NativeValue* output);

    Status BuildBitwiseNotExpr(const NativeValue& input, NativeValue* output);

    static bool BuildLShiftLeft(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                                ::llvm::Value** output,
                                base::Status& status);  // NOLINT

    static bool BuildLShiftRight(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                                 ::llvm::Value** output,
                                 base::Status& status);  // NOLINT

    static bool BuildAddExpr(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                             ::llvm::Value** output,
                             ::hybridse::base::Status& status);  // NOLINT

    static bool BuildSubExpr(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                             ::llvm::Value** output,
                             base::Status& status);  // NOLINT

    static bool BuildMultiExpr(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                               ::llvm::Value** output,
                               base::Status& status);  // NOLINT

    static bool BuildFDivExpr(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                              ::llvm::Value** output,
                              base::Status& status);  // NOLINT

    static bool BuildSDivExpr(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                              ::llvm::Value** output,
                              base::Status& status);  // NOLINT

    static bool BuildModExpr(::llvm::BasicBlock* block, llvm::Value* left, llvm::Value* right, llvm::Value** output,
                             base::Status status);

    static bool BuildAnd(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right, ::llvm::Value** output,
                         base::Status& status);  // NOLINT

    static bool BuildOr(::llvm::BasicBlock* block, llvm::Value* left, llvm::Value* right, llvm::Value** output,
                        base::Status* status);

    static bool BuildXor(::llvm::BasicBlock* block, llvm::Value* left, llvm::Value* right, llvm::Value** output,
                         base::Status* status);

 private:
    static bool BuildNot(::llvm::BasicBlock* block, llvm::Value* input, llvm::Value** output, base::Status* status);

    static bool InferAndCastedNumberTypes(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                                          ::llvm::Value** casted_left, ::llvm::Value** casted_right,
                                          ::hybridse::base::Status& status);  // NOLINT

    static bool InferAndCastIntegerTypes(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                                         ::llvm::Value** casted_left, ::llvm::Value** casted_right,
                                         ::hybridse::base::Status& status);  // NOLINT

    static bool InferAndCastDoubleTypes(::llvm::BasicBlock* block, ::llvm::Value* left, ::llvm::Value* right,
                                        ::llvm::Value** casted_left, ::llvm::Value** casted_right,
                                        ::hybridse::base::Status& status);  // NOLINT

    ::llvm::BasicBlock* block_;
    CastExprIRBuilder cast_expr_ir_builder_;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_ARITHMETIC_EXPR_IR_BUILDER_H_
