/*
 * Copyright 2022 4Paradigm authors
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

#ifndef HYBRIDSE_SRC_CODEGEN_ARRAY_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_ARRAY_IR_BUILDER_H_

#include <vector>

#include "absl/base/attributes.h"
#include "codegen/struct_ir_builder.h"

namespace hybridse {
namespace codegen {

// Array Struct, consist of following fields
// - Array of raw values: T*
// - Array of nullable values: bool*
// - array size: int64
class ArrayIRBuilder  : public StructTypeIRBuilder {
 public:
    // Array builder with num elements unknown
    ArrayIRBuilder(::llvm::Module* m, llvm::Type* ele_ty);

    // Array builder with num elements known at some point
    ArrayIRBuilder(::llvm::Module* m, llvm::Type* ele_ty, llvm::Value* num_ele);

    ~ArrayIRBuilder() override {}

    // create a new array from `elements` as value
    ABSL_MUST_USE_RESULT
    base::Status NewFixedArray(llvm::BasicBlock* bb, const std::vector<NativeValue>& elements,
                               NativeValue* output) const;

    ABSL_MUST_USE_RESULT
    base::Status NewEmptyArray(llvm::BasicBlock* bb, NativeValue* output) const;

    void InitStructType() override;

    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) override;

    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist) override { return true; }

    base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src, NativeValue* output) override {
        CHECK_TRUE(false, common::kCodegenError, "casting to array un-implemented");
    };

 private:
    ::llvm::Type* element_type_ = nullptr;
    ::llvm::Value* num_elements_ = nullptr;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_ARRAY_IR_BUILDER_H_
