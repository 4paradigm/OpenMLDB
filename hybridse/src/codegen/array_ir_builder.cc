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

#include "absl/strings/substitute.h"
#include "base/fe_status.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/context.h"
#include "codegen/ir_base_builder.h"
#include "codegen/string_ir_builder.h"

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

absl::StatusOr<llvm::Value*> ArrayIRBuilder::CastToArrayString(CodeGenContextBase* ctx, llvm::Value* src) {
    auto sb = StructTypeIRBuilder::CreateStructTypeIRBuilder(ctx->GetModule(), src->getType());
    CHECK_ABSL_STATUSOR(sb);

    ArrayIRBuilder* src_builder = dynamic_cast<ArrayIRBuilder*>(sb.value().get());
    if (!src_builder) {
        return absl::InvalidArgumentError("input value not a array");
    }

    llvm::Type* src_ele_type = src_builder->element_type_;
    if (IsStringPtr(src_ele_type)) {
        // already array<string>
        return src;
    }

    auto fields = src_builder->Load(ctx, src);
    CHECK_ABSL_STATUSOR(fields);
    llvm::Value* src_raws = fields.value().at(RAW_IDX);
    llvm::Value* src_nulls = fields.value().at(NULL_IDX);
    llvm::Value* num_elements = fields.value().at(SZ_IDX);

    llvm::Value* casted = nullptr;
    if (!CreateDefault(ctx->GetCurrentBlock(), &casted)) {
        return absl::InternalError("codegen error: fail to construct default array");
    }
    // initialize each element
    CHECK_ABSL_STATUS(Initialize(ctx, casted, {num_elements}));

    auto builder = ctx->GetBuilder();
    auto dst_fields = Load(ctx, casted);
    CHECK_ABSL_STATUSOR(fields);
    auto* raw_array_ptr = dst_fields.value().at(RAW_IDX);
    auto* nullables_ptr = dst_fields.value().at(NULL_IDX);

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
            llvm::Value* dst_ele =
                builder->CreateLoad(element_type_, builder->CreateGEP(element_type_, raw_array_ptr, idx_val));

            codegen::StringIRBuilder str_builder(ctx->GetModule());
            auto s = str_builder.CastFrom(ctx->GetCurrentBlock(), src_ele_value, dst_ele);
            CHECK_TRUE(s.ok(), common::kCodegenError, s.ToString());

            builder->CreateStore(
                builder->CreateLoad(builder->getInt1Ty(), builder->CreateGEP(builder->getInt1Ty(), src_nulls, idx_val)),
                builder->CreateGEP(builder->getInt1Ty(), nullables_ptr, idx_val));

            builder->CreateStore(builder->CreateAdd(idx_val, builder->getInt64(1)), idx);
            return {};
        }));

    CHECK_ABSL_STATUS(Set(ctx, casted, {raw_array_ptr, nullables_ptr, num_elements}));
    return casted;
}

absl::Status ArrayIRBuilder::Initialize(CodeGenContextBase* ctx, ::llvm::Value* alloca,
                                        absl::Span<llvm::Value* const> args) const {
    auto* builder = ctx->GetBuilder();
    StringIRBuilder str_builder(ctx->GetModule());
    auto ele_type = str_builder.GetType();
    if (!alloca->getType()->isPointerTy() || alloca->getType()->getPointerElementType() != struct_type_ ||
        ele_type->getPointerTo() != element_type_) {
        return absl::UnimplementedError(absl::Substitute(
            "not able to Initialize array except array<string>, got type $0", GetLlvmObjectString(alloca->getType())));
    }
    if (args.size() != 1) {
        // require one argument that is array size
        return absl::InvalidArgumentError("initialize array requries one argument which is array size");
    }
    if (!args[0]->getType()->isIntegerTy()) {
        return absl::InvalidArgumentError("array size argument should be integer");
    }
    auto sz = args[0];
    if (sz->getType() != builder->getInt64Ty()) {
        CastExprIRBuilder cast_builder(ctx->GetCurrentBlock());
        base::Status s;
        cast_builder.SafeCastNumber(sz, builder->getInt64Ty(), &sz, s);
        CHECK_STATUS_TO_ABSL(s);
    }
    auto fn = ctx->GetModule()->getOrInsertFunction("hybridse_alloc_array_string", builder->getVoidTy(),
                                                    struct_type_->getPointerTo(), builder->getInt64Ty());

    builder->CreateCall(fn, {alloca, sz});
    return absl::OkStatus();
}
}  // namespace codegen
}  // namespace hybridse
