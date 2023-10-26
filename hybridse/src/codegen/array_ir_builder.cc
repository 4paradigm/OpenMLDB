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
#include "codegen/ir_base_builder.h"

namespace hybridse {
namespace codegen {

ArrayIRBuilder::ArrayIRBuilder(::llvm::Module* m, llvm::Type* ele_ty)
    : StructTypeIRBuilder(m), element_type_(ele_ty) {
    InitStructType();
}

ArrayIRBuilder::ArrayIRBuilder(::llvm::Module* m, llvm::Type* ele_ty, llvm::Value* num_ele)
    : StructTypeIRBuilder(m), element_type_(ele_ty), num_elements_(num_ele) {
    InitStructType();
}

void ArrayIRBuilder::InitStructType() {
    // name must unique between different array type
    std::string name = absl::StrCat("fe.array_", GetLlvmObjectString(element_type_));
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);

    ::llvm::Type* arr_type = element_type_->getPointerTo();
    ::llvm::Type* nullable_type = ::llvm::IntegerType::getInt1Ty(m_->getContext())->getPointerTo();
    ::llvm::Type* size_type = ::llvm::IntegerType::getInt64Ty(m_->getContext());
    std::vector<::llvm::Type*> elements = {arr_type, nullable_type, size_type};
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    struct_type_ = stype;
}

base::Status ArrayIRBuilder::NewFixedArray(llvm::BasicBlock* bb, const std::vector<NativeValue>& elements,
                                           NativeValue* output) const {
    // TODO(ace): reduce IR size with loop block

    CHECK_TRUE(num_elements_ != nullptr, common::kCodegenError, "num elements unknown");

    // alloc array struct
    llvm::Value* array_alloca = nullptr;
    CHECK_TRUE(Create(bb, &array_alloca), common::kCodegenError, "can't create struct type for array");

    // ============================
    // Init array elements
    // ============================
    llvm::IRBuilder<> builder(bb);

    // init raw array and nullable array
    auto* raw_array_ptr = builder.CreateAlloca(element_type_, num_elements_);
    auto* nullables_ptr = builder.CreateAlloca(builder.getInt1Ty(), num_elements_);

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
    CHECK_TRUE(Set(bb, array_alloca, 0, raw_array_ptr), common::kCodegenError);
    // Set nullable list
    CHECK_TRUE(Set(bb, array_alloca, 1, nullables_ptr), common::kCodegenError);

    ::llvm::Value* array_sz = builder.CreateLoad(idx_val_ptr);
    CHECK_TRUE(Set(bb, array_alloca, 2, array_sz), common::kCodegenError);

    *output = NativeValue::Create(array_alloca);
    return base::Status::OK();
}


base::Status ArrayIRBuilder::NewEmptyArray(llvm::BasicBlock* bb, NativeValue* output) const {
    llvm::Value* array_alloca = nullptr;
    CHECK_TRUE(Create(bb, &array_alloca), common::kCodegenError, "can't create struct type for array");

    llvm::IRBuilder<> builder(bb);

    ::llvm::Value* array_sz = builder.getInt64(0);
    CHECK_TRUE(Set(bb, array_alloca, 2, array_sz), common::kCodegenError);

    *output = NativeValue::Create(array_alloca);

    return base::Status::OK();
}

bool ArrayIRBuilder::CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) {
    llvm::Value* array_alloca = nullptr;
    if (!Create(block, &array_alloca)) {
        return false;
    }

    llvm::IRBuilder<> builder(block);
    ::llvm::Value* array_sz = builder.getInt64(0);
    if (!Set(block, array_alloca, 2, array_sz)) {
        return false;
    }

    *output = array_alloca;
    return true;
}

}  // namespace codegen
}  // namespace hybridse
