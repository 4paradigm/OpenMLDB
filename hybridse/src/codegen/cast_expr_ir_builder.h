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

#ifndef HYBRIDSE_SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/cond_select_ir_builder.h"

namespace hybridse {
namespace codegen {

using ::hybridse::base::Status;

class CastExprIRBuilder {
 public:
    explicit CastExprIRBuilder(::llvm::BasicBlock* block);
    ~CastExprIRBuilder();

    Status Cast(const NativeValue& value, ::llvm::Type* cast_type, NativeValue* output);
    Status SafeCast(const NativeValue& value, ::llvm::Type* dst_type, NativeValue* output);
    Status UnSafeCast(const NativeValue& value, ::llvm::Type* dst_type, NativeValue* output);
    static bool IsSafeCast(::llvm::Type* lhs, ::llvm::Type* rhs);
    static Status InferNumberCastTypes(::llvm::Type* left_type, ::llvm::Type* right_type);
    static bool IsIntFloat2PointerCast(::llvm::Type* src, ::llvm::Type* dist);
    bool BoolCast(llvm::Value* pValue, llvm::Value** pValue1,
                  base::Status& status);  // NOLINT
    bool SafeCastNumber(::llvm::Value* value, ::llvm::Type* type, ::llvm::Value** output,
                        base::Status& status);  // NOLINT
    bool UnSafeCastNumber(::llvm::Value* value, ::llvm::Type* type, ::llvm::Value** output,
                          base::Status& status);  // NOLINT
    bool UnSafeCastDouble(::llvm::Value* value, ::llvm::Type* type, ::llvm::Value** output,
                          base::Status& status);  // NOLINT

 private:
    ::llvm::BasicBlock* block_;
    CondSelectIRBuilder cond_select_ir_builder_;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_CAST_EXPR_IR_BUILDER_H_
