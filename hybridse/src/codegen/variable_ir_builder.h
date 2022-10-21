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

#ifndef HYBRIDSE_SRC_CODEGEN_VARIABLE_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_VARIABLE_IR_BUILDER_H_

#include <string>
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/native_value.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"
namespace hybridse {
namespace codegen {
class VariableIRBuilder {
 public:
    VariableIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var);
    ~VariableIRBuilder();
    bool StoreRetStruct(const NativeValue& value,
                        base::Status& status);  // NOLINT
    bool LoadRetStruct(NativeValue* output,
                       base::Status& status);  // NOLINT
    bool LoadParameter(NativeValue* output,
                    base::Status& status);             // NOLINT
    bool LoadRowKey(NativeValue* output,
                    base::Status& status);             // NOLINT
    base::Status LoadMemoryPool(NativeValue* output);  // NOLINT
    bool LoadWindow(const std::string& frame_str, NativeValue* output,
                    base::Status& status);  // NOLINT
    bool LoadColumnRef(const std::string& relation_name,
                       const std::string& name, const std::string& frame_str,
                       ::llvm::Value** output,
                       base::Status& status);  // NOLINT (runtime/references)
    bool LoadColumnItem(const std::string& relation_name,
                        const std::string& name, NativeValue* output,
                        base::Status& status);  // NOLINT (runtime/references)
    bool StoreWindow(const std::string& frame_str, ::llvm::Value* value,
                     base::Status& status);  // NOLINT
    bool StoreColumnRef(const std::string& relation_name,
                        const std::string& name, const std::string& frame_str,
                        ::llvm::Value* value,
                        base::Status& status);  // NOLINT (runtime/references)
    bool StoreColumnItem(const std::string& relation_name,
                         const std::string& name, const NativeValue& value,
                         base::Status& status);  // NOLINT (runtime/references)

    bool LoadArrayIndex(std::string array_name, int32_t index,
                        ::llvm::Value** output,
                        base::Status& status);  // NOLINT (runtime/references)
    bool LoadValue(const std::string& name, NativeValue* output,
                   base::Status& status);  // NOLINT (runtime/references)
    bool StoreValue(const std::string& name, const NativeValue& value,
                    base::Status& status);  // NOLINT (runtime/references)
    bool StoreValue(const std::string& name, const NativeValue& value,
                    bool is_register,
                    base::Status& status);  // NOLINT (runtime/references)

    bool StoreStruct(const std::string& name, const NativeValue& value,
                     base::Status& status);  // NOLINT

 private:
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_VARIABLE_IR_BUILDER_H_
