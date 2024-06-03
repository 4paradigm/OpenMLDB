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

#include "codegen/cast_expr_ir_builder.h"
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

absl::StatusOr<NativeValue> ArrayIRBuilder::ExtractElement(CodeGenContextBase* ctx, const NativeValue& arr,
                                                           const NativeValue& key) const {
    return absl::UnimplementedError("array extract element");
}

absl::StatusOr<llvm::Value*> ArrayIRBuilder::NumElements(CodeGenContextBase* ctx, llvm::Value* arr) const {
    llvm::Value* out = nullptr;
    if (!Load(ctx->GetCurrentBlock(), arr, SZ_IDX, &out)) {
        return absl::InternalError("codegen: fail to extract array size");
    }

    return out;
}

absl::StatusOr<llvm::Value*> ArrayIRBuilder::CastFrom(CodeGenContextBase* ctx, llvm::Value* src) {
    auto sb = StructTypeIRBuilder::CreateStructTypeIRBuilder(ctx->GetModule(), src->getType());
    CHECK_ABSL_STATUSOR(sb);

    ArrayIRBuilder* src_builder = dynamic_cast<ArrayIRBuilder*>(sb.value().get());
    if (!src_builder) {
        return absl::InvalidArgumentError("input value not a array");
    }

    llvm::Type* src_ele_type = src_builder->element_type_;
    auto fields = src_builder->Load(ctx, src);
    CHECK_ABSL_STATUSOR(fields);
    llvm::Value* src_raws = fields.value().at(RAW_IDX);
    llvm::Value* num_elements = fields.value().at(SZ_IDX);


    llvm::Value* casted = nullptr;
    if (!CreateDefault(ctx->GetCurrentBlock(), &casted)) {
        return absl::InternalError("codegen error: fail to construct default array");
    }

    auto builder = ctx->GetBuilder();
    auto* raw_array_ptr = builder->CreateAlloca(element_type_, num_elements);
    auto* nullables_ptr = builder->CreateAlloca(builder->getInt1Ty(), num_elements);

    llvm::Type* idx_type = builder->getInt64Ty();
    llvm::Value* idx = builder->CreateAlloca(idx_type);
    builder->CreateStore(builder->getInt64(0), idx);
    CHECK_STATUS_TO_ABSL(ctx->CreateWhile(
        [&](llvm::Value** cond) -> base::Status {
            *cond = builder->CreateICmpSLT(builder->CreateLoad(idx_type, idx), num_elements);
            return {};
        },
        [&]() -> base::Status {
            llvm::Value* idx_val = builder->CreateLoad(idx_type, idx);
            codegen::CastExprIRBuilder cast_builder(ctx->GetCurrentBlock());

            llvm::Value* src_ele_value =
                builder->CreateLoad(src_ele_type, builder->CreateGEP(src_ele_type, src_raws, idx_val));

            NativeValue out;
            CHECK_STATUS(cast_builder.Cast(NativeValue::Create(src_ele_value), element_type_, &out));

            builder->CreateStore(out.GetRaw(), builder->CreateGEP(element_type_, raw_array_ptr, idx_val));
            builder->CreateStore(out.GetIsNull(builder),
                                 builder->CreateGEP(builder->getInt1Ty(), nullables_ptr, idx_val));

            builder->CreateStore(builder->CreateAdd(idx_val, builder->getInt64(1)), idx);
            return {};
        }));

    CHECK_ABSL_STATUS(Set(ctx, casted, {raw_array_ptr, nullables_ptr, num_elements}));
    return casted;
}

}  // namespace codegen
}  // namespace hybridse
