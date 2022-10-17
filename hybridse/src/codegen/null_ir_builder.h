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

#ifndef HYBRIDSE_SRC_CODEGEN_NULL_IR_BUILDER_H_

#define HYBRIDSE_SRC_CODEGEN_NULL_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/native_value.h"
namespace hybridse {
namespace codegen {
class NullIRBuilder {
 public:
    NullIRBuilder();
    ~NullIRBuilder();

    base::Status CheckAnyNull(::llvm::BasicBlock* block, const NativeValue& value, ::llvm::Value** should_ret_null);

    base::Status CheckAllNull(::llvm::BasicBlock* block, const NativeValue& value, ::llvm::Value** should_ret_null);

    static base::Status SafeNullBinaryExpr(
        ::llvm::BasicBlock* block, const NativeValue& left, const NativeValue& right,
        const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*, ::llvm::Value*, ::llvm::Value**, base::Status&)>,
        NativeValue* output);

    static base::Status SafeNullUnaryExpr(
        ::llvm::BasicBlock* block, const NativeValue& left,
        const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*, ::llvm::Value**, base::Status&)>,
        NativeValue* output);

    static base::Status SafeNullCastExpr(::llvm::BasicBlock* block, const NativeValue& left, ::llvm::Type* type,
                                         const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*,
                                                                  ::llvm::Type* type, ::llvm::Value**, base::Status&)>,
                                         NativeValue* output);

    // Safe null builder for `A DIV B` expr
    static base::Status SafeNullDivExpr(
        ::llvm::BasicBlock* block, const NativeValue& left, const NativeValue& right,
        const std::function<bool(::llvm::BasicBlock*, ::llvm::Value*, ::llvm::Value*, ::llvm::Value**, base::Status&)>,
        NativeValue* output);
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_NULL_IR_BUILDER_H_
