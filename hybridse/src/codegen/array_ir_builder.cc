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

#include "codegen/array_ir_builder.h"

#include <string>

#include "codegen/context.h"
#include "codegen/ir_base_builder.h"

namespace hybridse {
namespace codegen {

#define SZ_IDX 2
#define RAW_IDX 0
#define NULL_IDX 1

ArrayIRBuilder::ArrayIRBuilder(::llvm::Module* m, llvm::Type* ele_ty)
    : StructTypeIRBuilder(m), element_type_(ele_ty) {
    InitStructType();
}

void ArrayIRBuilder::InitStructType() {
    // name must unique between different array type
    std::string name = absl::StrCat("fe.array_", GetLlvmObjectString(element_type_));
    ::llvm::StructType* stype = m_->getTypeByName(name);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);

    ::llvm::Type* arr_type = element_type_->getPointerTo();
    ::llvm::Type* nullable_type = ::llvm::IntegerType::getInt1Ty(m_->getContext())->getPointerTo();
    ::llvm::Type* size_type = ::llvm::IntegerType::getInt64Ty(m_->getContext());
    stype->setBody({arr_type, nullable_type, size_type});
    struct_type_ = stype;
}

absl::StatusOr<NativeValue> ArrayIRBuilder::Construct(CodeGenContextBase* ctx,
                                                      absl::Span<NativeValue const> elements) const {
    auto bb = ctx->GetCurrentBlock();
    // alloc array struct
    llvm::Value* array_alloca = nullptr;
    if (!Allocate(bb, &array_alloca)) {
        return absl::InternalError("can't create struct type for array");
    }

    // ============================
    // Init array elements
    // ============================
    llvm::IRBuilder<> builder(bb);
    auto num_elements = ctx->GetBuilder()->getInt64(elements.size());
    if (!Set(bb, array_alloca, SZ_IDX, num_elements)) {
        return absl::InternalError("fail to set array size");
    }

    if (elements.empty()) {
        // empty array
        return NativeValue::Create(array_alloca);
    }

    // init raw array and nullable array
    auto* raw_array_ptr = builder.CreateAlloca(element_type_, num_elements);
    auto* nullables_ptr = builder.CreateAlloca(builder.getInt1Ty(), num_elements);

    // fullfill the array struct
    auto* idx_val_ptr = builder.CreateAlloca(builder.getInt64Ty());
    builder.CreateStore(builder.getInt64(0), idx_val_ptr);

    for (auto& val : elements) {
        auto* idx_val = builder.CreateLoad(idx_val_ptr);
        auto* raw_ele_ptr = builder.CreateGEP(raw_array_ptr, idx_val);
        auto* nullable_ele = builder.CreateGEP(nullables_ptr, idx_val);

        builder.CreateStore(val.GetValue(&builder), raw_ele_ptr);
        builder.CreateStore(val.GetIsNull(&builder), nullable_ele);

        // update index
        auto* new_idx = builder.CreateAdd(idx_val, builder.getInt64(1));
        builder.CreateStore(new_idx, idx_val_ptr);
    }

    // Set raw array
    if (!Set(bb, array_alloca, RAW_IDX, raw_array_ptr)) {
        return absl::InternalError("fail to set array values");
    }
    // Set nullable list
    if (!Set(bb, array_alloca, NULL_IDX, nullables_ptr)) {
        return absl::InternalError("fail to set array nulls");
    }

    return NativeValue::Create(array_alloca);
}

bool ArrayIRBuilder::CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) {
    llvm::Value* array_alloca = nullptr;
    if (!Allocate(block, &array_alloca)) {
        return false;
    }

    llvm::IRBuilder<> builder(block);
    ::llvm::Value* array_sz = builder.getInt64(0);
    if (!Set(block, array_alloca, SZ_IDX, array_sz)) {
        return false;
    }

    *output = array_alloca;
    return true;
}

}  // namespace codegen
}  // namespace hybridse
