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

#ifndef SRC_CODEGEN_PREDICATE_EXPR_IR_BUILDER_H_
#define SRC_CODEGEN_PREDICATE_EXPR_IR_BUILDER_H_

#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
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

    Status BuildBetweenExpr(const NativeValue& expr, const NativeValue& left, const NativeValue& right,
                            bool is_not_between, NativeValue* output);

    Status BuildInExpr(const NativeValue& lhs, const NativeValue& in_list, bool is_not, NativeValue* output);

    static bool BuildEqExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            Status& status);  // NOLINT
    static bool BuildNeqExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                             ::llvm::Value* right, ::llvm::Value** output,
                             Status& status);  // NOLINT
    static bool BuildGtExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            Status& status);  // NOLINT
    static bool BuildGeExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            Status& status);  // NOLINT
    static bool BuildLtExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            Status& status);  // NOLINT
    static bool BuildLeExpr(::llvm::BasicBlock* block, ::llvm::Value* left,
                            ::llvm::Value* right, ::llvm::Value** output,
                            Status& status);  // NOLINT

 private:
    static bool IsAcceptType(::llvm::Type* type);
    static Status CompareTypeAccept(::llvm::Type* lhs, ::llvm::Type* rhs);
    static bool InferAndCastBoolTypes(
        ::llvm::BasicBlock* block, ::llvm::Value* value,
        ::llvm::Value** casted_value,
        Status& status);  // NOLINT
    static bool InferAndCastTypes(::llvm::BasicBlock* block,
                                  ::llvm::Value* left, ::llvm::Value* right,
                                  ::llvm::Value** casted_left,
                                  ::llvm::Value** casted_right,
                                  Status& status);  // NOLINT

 private:
    ::llvm::BasicBlock* block_;
    CastExprIRBuilder cast_expr_ir_builder_;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // SRC_CODEGEN_PREDICATE_EXPR_IR_BUILDER_H_
