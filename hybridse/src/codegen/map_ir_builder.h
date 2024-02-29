/*
 * Copyright 2022 OpenMLDB authors
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

#ifndef HYBRIDSE_SRC_CODEGEN_MAP_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_MAP_IR_BUILDER_H_

#include <utility>

#include "codegen/struct_ir_builder.h"

namespace hybridse {
namespace codegen {

class MapIRBuilder final : public StructTypeIRBuilder {
 public:
    MapIRBuilder(::llvm::Module* m, ::llvm::Type* key_ty, ::llvm::Type* value_ty);
    ~MapIRBuilder() override {}

    absl::StatusOr<NativeValue> Construct(CodeGenContextBase* ctx, absl::Span<const NativeValue> args) const override;

    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist) override { return true; }
    base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src, NativeValue* output) override {
        return {};
    }

    absl::StatusOr<NativeValue> ExtractElement(CodeGenContextBase* ctx, const NativeValue&,
                                               const NativeValue&) const override;

    absl::StatusOr<NativeValue> MapKeys(CodeGenContextBase*, const NativeValue&) const;

    absl::StatusOr<llvm::Value*> CalEncodeByteSize(CodeGenContextBase* ctx, llvm::Value*) const;

    // Encode the `map_ptr` into `row_ptr`, returns byte size written on success
    // `row_ptr` is ensured to have enough space
    absl::StatusOr<llvm::Value*> Encode(CodeGenContextBase*, llvm::Value* map_ptr, llvm::Value* row_ptr) const;

    // Decode the stored map value at address row_ptr
    absl::StatusOr<llvm::Value*> Decode(CodeGenContextBase*, llvm::Value* row_ptr) const;

    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) override;

 private:
    void InitStructType() override;

    absl::StatusOr<llvm::Value*> CalEncodeSizeForArray(CodeGenContextBase*, llvm::Value* arr_ptr,
                                                       llvm::Value* arr_size) const;
    // get the encode size of llvm type
    absl::StatusOr<llvm::Value*> TypeEncodeByteSize(CodeGenContextBase*, llvm::Type*, llvm::Value*) const;

    absl::StatusOr<llvm::Value*> EncodeArray(CodeGenContextBase*, llvm::Value* row_ptr, llvm::Value* arr_ptr,
                                             llvm::Value* arr_size) const;
    absl::StatusOr<llvm::Value*> EncodeBaseValue(CodeGenContextBase*, llvm::Value* value, llvm::Value* row_ptr,
                                                 llvm::Value* offset) const;

    // decode array values from row ptr, returns (array_ptr, read size) on success
    absl::StatusOr<llvm::Value*> DecodeArrayValue(CodeGenContextBase*, llvm::Value* row_ptr, llvm::Value* sz,
                                                  llvm::Value* arr_alloca, llvm::Type* ele_ty) const;

    // decode base value from row ptr, write results to base_ptr, returns read size on success
    absl::StatusOr<llvm::Value*> DecodeBaseValue(CodeGenContextBase*, llvm::Value* row_ptr, llvm::Value* base_ptr,
                                                 llvm::Type* type) const;

    absl::StatusOr<llvm::Function*> GetOrBuildEncodeArrFunction(CodeGenContextBase* ctx, llvm::Type* ele_type) const;
    absl::StatusOr<llvm::Function*> GetOrBuildDecodeArrFunction(CodeGenContextBase* ctx) const;

 private:
    llvm::Type* key_type_ = nullptr;
    llvm::Type* value_type_ = nullptr;
};

}  // namespace codegen
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_CODEGEN_MAP_IR_BUILDER_H_
