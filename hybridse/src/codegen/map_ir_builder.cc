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

#include "codegen/map_ir_builder.h"

#include <string>

#include "absl/status/status.h"
#include "codegen/array_ir_builder.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/context.h"
#include "codegen/ir_base_builder.h"
#include "codegen/cond_select_ir_builder.h"
#include "codegen/predicate_expr_ir_builder.h"

namespace hybridse {
namespace codegen {

static const char* PREFIX = "fe.map";
#define SZ_IDX 0
#define KEY_VEC_IDX 1
#define VALUE_VEC_IDX 2
#define VALUE_NULL_VEC_IDX 3

MapIRBuilder::MapIRBuilder(::llvm::Module* m, ::llvm::Type* key_ty, ::llvm::Type* value_ty)
    : StructTypeIRBuilder(m), key_type_(key_ty), value_type_(value_ty) {
    InitStructType();
}

void MapIRBuilder::InitStructType() {
    std::string name =
        absl::StrCat(PREFIX, "__", GetLlvmObjectString(key_type_), "_", GetLlvmObjectString(value_type_), "__");
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);

    // %map__{key}_{value}__ = { size, vec<key>, vec<value>, vec<bool> }
    ::llvm::Type* size_type = ::llvm::IntegerType::getInt64Ty(m_->getContext());
    // ::llvm::Type* key_vec = ::llvm::VectorType::get(key_type_, {MIN_VEC_SIZE, true});
    LOG(INFO) << "key vec is " << GetLlvmObjectString(key_type_);
    ::llvm::Type* key_vec = key_type_->getPointerTo();
    ::llvm::Type* value_vec = value_type_->getPointerTo();
    ::llvm::Type* value_null_type = ::llvm::IntegerType::getInt1Ty(m_->getContext())->getPointerTo();
    stype->setBody({size_type, key_vec, value_vec, value_null_type});
    struct_type_ = stype;
}

absl::StatusOr<NativeValue> MapIRBuilder::Construct(CodeGenContext* ctx, absl::Span<const NativeValue> args) const {
    EnsureOK();

    ::llvm::Value* map_alloca = nullptr;
    if (!Allocate(ctx->GetCurrentBlock(), &map_alloca)) {
        return absl::FailedPreconditionError(absl::StrCat("unable to allocate ", GetLlvmObjectString(struct_type_)));
    }

    auto builder = ctx->GetBuilder();
    auto* original_size = builder->getInt64(args.size() / 2);
    auto* key_vec = builder->CreateAlloca(key_type_, original_size, "key_vec");
    auto* value_vec = builder->CreateAlloca(value_type_, original_size, "value_vec");
    auto* value_nulls_vec = builder->CreateAlloca(builder->getInt1Ty(), original_size, "value_nulls_vec");

    // creating raw values for map

    CastExprIRBuilder cast_builder(ctx->GetCurrentBlock());

    // original vector, may contains duplicate keys
    auto* original_keys = builder->CreateAlloca(key_type_, original_size, "original_keys");
    auto* original_keys_is_null = builder->CreateAlloca(builder->getInt1Ty(), original_size, "original_keys_is_null");
    auto* original_values = builder->CreateAlloca(value_type_, original_size, "original_values");
    auto* original_values_is_null =
        builder->CreateAlloca(builder->getInt1Ty(), original_size, "original_values_is_null");
    for (size_t i = 0; i < args.size(); i += 2) {
        auto* update_idx = builder->getInt64(i / 2);
        NativeValue key = args[i];
        if (key.GetValue(builder)->getType() != key_type_) {
            auto s = cast_builder.Cast(key, key_type_, &key);
            if (!s.isOK()) {
                return absl::InternalError(absl::StrCat("fail to case map key: ", s.str()));
            }
        }
        NativeValue value = args[i + 1];
        if (value.GetValue(builder)->getType() != value_type_) {
            auto s = cast_builder.Cast(value, value_type_, &value);
            if (!s.isOK()) {
                return absl::InternalError(absl::StrCat("fail to case map value: ", s.str()));
            }
        }
        builder->CreateStore(key.GetIsNull(ctx), builder->CreateGEP(original_keys_is_null, update_idx));
        builder->CreateStore(key.GetValue(ctx), builder->CreateGEP(original_keys, update_idx));
        builder->CreateStore(value.GetIsNull(ctx), builder->CreateGEP(original_values_is_null, update_idx));
        builder->CreateStore(value.GetValue(ctx), builder->CreateGEP(original_values, update_idx));
    }

    ::llvm::Value* update_idx_ptr = builder->CreateAlloca(builder->getInt64Ty(), nullptr, "update_idx");
    builder->CreateStore(builder->getInt64(0), update_idx_ptr);
    ::llvm::Value* true_idx_ptr = builder->CreateAlloca(builder->getInt64Ty(), nullptr, "true_idx");
    builder->CreateStore(builder->getInt64(0), true_idx_ptr);

    auto s = ctx->CreateWhile(
        [&](llvm::Value** cond) -> base::Status {
            *cond = builder->CreateAnd(
                builder->CreateICmpSLT(builder->CreateLoad(update_idx_ptr), original_size, "if_while_true"),
                builder->CreateICmpSLT(builder->CreateLoad(true_idx_ptr), original_size));
            return {};
        },
        [&]() -> base::Status {
            auto idx = builder->CreateLoad(update_idx_ptr, "update_idx_value");
            auto true_idx = builder->CreateLoad(true_idx_ptr, "true_idx_value");
            CHECK_STATUS(ctx->CreateBranchNot(
                builder->CreateLoad(builder->CreateGEP(original_keys_is_null, idx)), [&]() -> base::Status {
                    // write to map if key is not null
                    builder->CreateStore(builder->CreateLoad(builder->CreateGEP(original_keys, idx)),
                                         builder->CreateGEP(key_vec, true_idx));
                    builder->CreateStore(builder->CreateLoad(builder->CreateGEP(original_values, idx)),
                                         builder->CreateGEP(value_vec, true_idx));
                    builder->CreateStore(builder->CreateLoad(builder->CreateGEP(original_values_is_null, idx)),
                                         builder->CreateGEP(value_nulls_vec, true_idx));

                    builder->CreateStore(builder->CreateAdd(builder->getInt64(1), true_idx), true_idx_ptr);
                    return {};
                }));

            builder->CreateStore(builder->CreateAdd(builder->getInt64(1), idx), update_idx_ptr);
            return {};
        });
    if (!s.isOK()) {
        return absl::InternalError(s.str());
    }

    auto* final_size = builder->CreateLoad(true_idx_ptr, "true_size");
    auto as = Set(ctx, map_alloca, {final_size, key_vec, value_vec, value_nulls_vec});

    if (!as.ok()) {
        return as;
    }

    return NativeValue::Create(map_alloca);
}

bool MapIRBuilder::CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) {
    llvm::Value* map_alloca = nullptr;
    if (!Allocate(block, &map_alloca)) {
        return false;
    }

    llvm::IRBuilder<> builder(block);
    ::llvm::Value* size = builder.getInt64(0);
    if (!Set(block, map_alloca, SZ_IDX, size)) {
        return false;
    }

    *output = map_alloca;
    return true;
}

absl::StatusOr<NativeValue> MapIRBuilder::ExtractElement(CodeGenContext* ctx, const NativeValue& arr,
                                                         const NativeValue& key) const {
    EnsureOK();

    auto builder = ctx->GetBuilder();
    auto* arr_is_null = arr.GetIsNull(ctx);
    auto* key_is_null = key.GetIsNull(ctx);

    auto* out_val_alloca = builder->CreateAlloca(value_type_);
    builder->CreateStore(::llvm::UndefValue::get(value_type_), out_val_alloca);
    auto* out_null_alloca = builder->CreateAlloca(builder->getInt1Ty());
    builder->CreateStore(builder->getInt1(true), out_null_alloca);

    auto s = ctx->CreateBranch(
        builder->CreateOr(arr_is_null, key_is_null),
        [&]() -> base::Status {
            return {};
        },
        [&]() -> base::Status {
            NativeValue casted_key = key;
            if (key.GetType() != key_type_) {
                CastExprIRBuilder cast_builder(ctx->GetCurrentBlock());
                CHECK_STATUS(cast_builder.Cast(key, key_type_, &casted_key));
            }
            auto* key_val = casted_key.GetValue(ctx);

            auto* map_ptr = arr.GetValue(ctx);
            ::llvm::Value* sz = nullptr;
            CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr, SZ_IDX, &sz), common::kCodegenError);

            ::llvm::Value* keys = nullptr;
            CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr, KEY_VEC_IDX, &keys), common::kCodegenError);

            ::llvm::Value* idx_alloc = builder->CreateAlloca(builder->getInt64Ty());
            builder->CreateStore(builder->getInt64(0), idx_alloc);
            ::llvm::Value* found_idx_alloc = builder->CreateAlloca(builder->getInt64Ty());
            builder->CreateStore(builder->getInt64(-1), found_idx_alloc);

            CHECK_STATUS(ctx->CreateWhile(
                [&](::llvm::Value** cond) -> base::Status {
                    ::llvm::Value* idx = builder->CreateLoad(idx_alloc);
                    ::llvm::Value* found = builder->CreateLoad(found_idx_alloc);
                    *cond = builder->CreateAnd(builder->CreateICmpSLT(idx, sz),
                                               builder->CreateICmpSLT(found, builder->getInt64(0)));
                    return {};
                },
                [&]() -> base::Status {
                    ::llvm::Value* idx = builder->CreateLoad(idx_alloc);
                    // key never null
                    auto* ele = builder->CreateLoad(builder->CreateGEP(keys, idx));
                    ::llvm::Value* eq = nullptr;
                    base::Status s;
                    PredicateIRBuilder::BuildEqExpr(ctx->GetCurrentBlock(), ele, key_val, &eq, s);
                    CHECK_STATUS(s);

                    ::llvm::Value* update_found_idx = builder->CreateSelect(eq, idx, builder->getInt64(-1));

                    builder->CreateStore(update_found_idx, found_idx_alloc);
                    builder->CreateStore(builder->CreateAdd(idx, builder->getInt64(1)), idx_alloc);
                    return {};
                }));

            auto* found_idx = builder->CreateLoad(found_idx_alloc);

            CHECK_STATUS(ctx->CreateBranch(
                builder->CreateAnd(builder->CreateICmpSLT(found_idx, sz),
                                   builder->CreateICmpSGE(found_idx, builder->getInt64(0))),
                [&]() -> base::Status {
                    ::llvm::Value* values = nullptr;
                    CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr, VALUE_VEC_IDX, &values), common::kCodegenError);

                    ::llvm::Value* value_nulls = nullptr;
                    CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr, VALUE_NULL_VEC_IDX, &value_nulls),
                               common::kCodegenError);

                    auto* val = builder->CreateLoad(builder->CreateGEP(values, found_idx));
                    auto* val_nullable = builder->CreateLoad(builder->CreateGEP(value_nulls, found_idx));

                    builder->CreateStore(val, out_val_alloca);
                    builder->CreateStore(val_nullable, out_null_alloca);
                    return {};
                },
                [&]() -> base::Status { return {}; }));

            return {};
        });

    if (!s.isOK()) {
        return absl::InvalidArgumentError(s.str());
    }

    auto* out_val = builder->CreateLoad(out_val_alloca);
    auto* out_null_val = builder->CreateLoad(out_null_alloca);

    return NativeValue::CreateWithFlag(out_val, out_null_val);
}

absl::StatusOr<NativeValue> MapIRBuilder::MapKeys(CodeGenContext* ctx, const NativeValue& in) const {
    EnsureOK();

    auto map_is_null = in.GetIsNull(ctx);
    auto map_ptr = in.GetValue(ctx);

    auto builder = ctx->GetBuilder();
    ::llvm::Value* keys_ptr = nullptr;
    if (!Load(ctx->GetCurrentBlock(), map_ptr, KEY_VEC_IDX, &keys_ptr)) {
        return absl::FailedPreconditionError("failed to extract map keys");
    }
    if (!keys_ptr->getType()->isPointerTy()) {
        return absl::FailedPreconditionError("map keys entry is not pointer");
    }
    ::llvm::Value* size = nullptr;
    if (!Load(ctx->GetCurrentBlock(), map_ptr, SZ_IDX, &size)) {
        return absl::FailedPreconditionError("failed to extract map size");
    }

    // construct nulls as [false ...]
    auto nulls = builder->CreateAlloca(builder->getInt1Ty(), size);
    auto idx_ptr = builder->CreateAlloca(builder->getInt64Ty());
    builder->CreateStore(builder->getInt64(0), idx_ptr);
    ctx->CreateWhile(
        [&](::llvm::Value** cond) -> base::Status {
            *cond = builder->CreateICmpSLT(builder->CreateLoad(idx_ptr), size);
            return {};
        },
        [&]() -> base::Status {
            auto idx = builder->CreateLoad(idx_ptr);

            builder->CreateStore(builder->getInt1(false), builder->CreateGEP(nulls, idx));

            builder->CreateStore(builder->CreateAdd(idx, builder->getInt64(1)), idx_ptr);
            return {};
        });

    ArrayIRBuilder array_builder(ctx->GetModule(), keys_ptr->getType()->getPointerElementType());
    auto rs = array_builder.ConstructFromRaw(ctx, {keys_ptr, nulls, size});

    if (!rs.ok()) {
        return rs.status();
    }

    NativeValue out;
    CondSelectIRBuilder cond_builder;
    auto s = cond_builder.Select(ctx->GetCurrentBlock(), NativeValue::Create(map_is_null),
                        NativeValue::CreateNull(array_builder.GetType()), NativeValue::Create(rs.value()), &out);

    if (!s.isOK()) {
        return absl::FailedPreconditionError(s.str());
    }

    return out;
}
}  // namespace codegen
}  // namespace hybridse
