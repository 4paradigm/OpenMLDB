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
#include "absl/strings/substitute.h"
#include "base/fe_status.h"
#include "codegen/array_ir_builder.h"
#include "codegen/cast_expr_ir_builder.h"
#include "codegen/cond_select_ir_builder.h"
#include "codegen/context.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/predicate_expr_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"

namespace hybridse {
namespace codegen {

static const char* PREFIX = "fe.map";
#define SZ_IDX 0
#define KEY_VEC_IDX 1
#define VALUE_VEC_IDX 2
#define VALUE_NULL_VEC_IDX 3
#define FIELDS_CNT 4


MapIRBuilder::MapIRBuilder(::llvm::Module* m, ::llvm::Type* key_ty, ::llvm::Type* value_ty)
    : StructTypeIRBuilder(m), key_type_(key_ty), value_type_(value_ty) {
    InitStructType();
}

void MapIRBuilder::InitStructType() {
    std::string name =
        absl::StrCat(PREFIX, "__", GetIRTypeName(key_type_), "_", GetIRTypeName(value_type_), "__");
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);

    // %map__{key}_{value}__ = { size, vec<key>, vec<value>, vec<bool> }
    ::llvm::Type* size_type = ::llvm::IntegerType::getInt32Ty(m_->getContext());
    // ::llvm::Type* key_vec = ::llvm::VectorType::get(key_type_, {MIN_VEC_SIZE, true});
    ::llvm::Type* key_vec = key_type_->getPointerTo();
    ::llvm::Type* value_vec = value_type_->getPointerTo();
    ::llvm::Type* value_null_type = ::llvm::IntegerType::getInt1Ty(m_->getContext())->getPointerTo();
    stype->setBody({size_type, key_vec, value_vec, value_null_type});
    struct_type_ = stype;
}

// FIXME(ace): the map value is returned and possibly used in other function context, so create a global variable,
// instead of alloca instruction, alloca is local to current function only.
//
// It does not become a problem because we codegen directly to the top-level function.
absl::StatusOr<NativeValue> MapIRBuilder::Construct(CodeGenContextBase* ctx, absl::Span<const NativeValue> args) const {
    EnsureOK();

    ::llvm::Value* map_alloca = nullptr;
    if (!Allocate(ctx->GetCurrentBlock(), &map_alloca)) {
        return absl::FailedPreconditionError(absl::StrCat("unable to allocate ", GetLlvmObjectString(struct_type_)));
    }

    auto builder = ctx->GetBuilder();
    if (args.empty()) {
        if (!Set(ctx->GetCurrentBlock(), map_alloca, SZ_IDX, builder->getInt32(0))) {
            return absl::InternalError("setting map size=0 for map failed");
        }
        return NativeValue::Create(map_alloca);
    }

    auto* original_size = builder->getInt32(args.size() / 2);
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
        auto* update_idx = builder->getInt32(i / 2);
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

    ::llvm::Value* update_idx_ptr = builder->CreateAlloca(builder->getInt32Ty(), nullptr, "update_idx");
    builder->CreateStore(builder->getInt32(0), update_idx_ptr);
    ::llvm::Value* true_idx_ptr = builder->CreateAlloca(builder->getInt32Ty(), nullptr, "true_idx");
    builder->CreateStore(builder->getInt32(0), true_idx_ptr);

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

                    builder->CreateStore(builder->CreateAdd(builder->getInt32(1), true_idx), true_idx_ptr);
                    return {};
                }), "pick_and_write_non_null_key");

            builder->CreateStore(builder->CreateAdd(builder->getInt32(1), idx), update_idx_ptr);
            return {};
        },
        "pick_non_null_kyes_for_map");
    CHECK_STATUS_TO_ABSL(s);

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
    ::llvm::Value* size = builder.getInt32(0);
    if (!Set(block, map_alloca, SZ_IDX, size)) {
        return false;
    }

    *output = map_alloca;
    return true;
}

absl::StatusOr<NativeValue> MapIRBuilder::ExtractElement(CodeGenContextBase* ctx, const NativeValue& arr,
                                                         const NativeValue& key) const {
    EnsureOK();

    std::string fn_name = absl::StrCat("extract_map_value_", GetIRTypeName(struct_type_));
    llvm::Function* fn = ctx->GetModule()->getFunction(fn_name);
    if (fn == nullptr) {
        llvm::FunctionType* fnt =
            llvm::FunctionType::get(ctx->GetBuilder()->getVoidTy(),
                                    {
                                        struct_type_->getPointerTo(),                   // arr ptr
                                        ctx->GetBuilder()->getInt1Ty(),                 // arr is null
                                        key_type_,                                      // key value
                                        ctx->GetBuilder()->getInt1Ty(),                 // key is null
                                        value_type_->getPointerTo(),                    // output value ptr
                                        ctx->GetBuilder()->getInt1Ty()->getPointerTo()  // output is null ptr
                                    },
                                    false);
        fn = llvm::Function::Create(fnt, llvm::GlobalValue::ExternalLinkage, fn_name, ctx->GetModule());

        FunctionScopeGuard fg(fn, ctx);

        llvm::Value* map_ptr_param = fn->arg_begin();
        llvm::Value* arr_is_null_param = fn->arg_begin() + 1;
        llvm::Value* key_val_param = fn->arg_begin() + 2;
        llvm::Value* key_is_null_param = fn->arg_begin() + 3;
        llvm::Value* out_val_alloca_param = fn->arg_begin() + 4;
        llvm::Value* out_null_alloca_param = fn->arg_begin() + 5;

        auto builder = ctx->GetBuilder();

        builder->CreateStore(builder->getInt1(true), out_null_alloca_param);
        ::llvm::Value* idx_alloc = builder->CreateAlloca(builder->getInt32Ty());
        builder->CreateStore(builder->getInt32(0), idx_alloc);
        ::llvm::Value* found_idx_alloc = builder->CreateAlloca(builder->getInt32Ty());
        builder->CreateStore(builder->getInt32(-1), found_idx_alloc);

        llvm::Value* sz_alloca = builder->CreateAlloca(builder->getInt32Ty());
        llvm::Value* keys_alloca = builder->CreateAlloca(key_type_->getPointerTo());

        auto s = ctx->CreateBranchNot(
            builder->CreateOr(arr_is_null_param, key_is_null_param),
            [&]() -> base::Status {
                ::llvm::Value* sz = nullptr;
                CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr_param, SZ_IDX, &sz), common::kCodegenError);
                ctx->GetBuilder()->CreateStore(sz, sz_alloca);

                CHECK_STATUS(ctx->CreateBranch(builder->CreateICmpSLE(sz, builder->getInt32(0)), [&]() -> base::Status {
                    builder->CreateRetVoid();
                    return {};
                }));

                ::llvm::Value* keys = nullptr;
                CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr_param, KEY_VEC_IDX, &keys), common::kCodegenError);
                ctx->GetBuilder()->CreateStore(keys, keys_alloca);

                CHECK_STATUS(
                    ctx->CreateWhile(
                        [&](::llvm::Value** cond) -> base::Status {
                            ::llvm::Value* idx = builder->CreateLoad(idx_alloc);
                            ::llvm::Value* found = builder->CreateLoad(found_idx_alloc);
                            *cond = builder->CreateAnd(builder->CreateICmpSLT(idx, builder->CreateLoad(sz_alloca)),
                                                       builder->CreateICmpSLT(found, builder->getInt32(0)));
                            return {};
                        },
                        [&]() -> base::Status {
                            ::llvm::Value* idx = builder->CreateLoad(idx_alloc);
                            // key never null
                            auto* ele = builder->CreateLoad(builder->CreateGEP(builder->CreateLoad(keys_alloca), idx));
                            ::llvm::Value* eq = nullptr;
                            base::Status s;
                            PredicateIRBuilder::BuildEqExpr(ctx->GetCurrentBlock(), ele, key_val_param, &eq, s);
                            CHECK_STATUS(s);

                            ::llvm::Value* update_found_idx = builder->CreateSelect(eq, idx, builder->getInt32(-1));

                            builder->CreateStore(update_found_idx, found_idx_alloc);
                            builder->CreateStore(builder->CreateAdd(idx, builder->getInt32(1)), idx_alloc);
                            return {};
                        }),
                    "iter_map");

                auto* found_idx = builder->CreateLoad(found_idx_alloc);

                CHECK_STATUS(ctx->CreateBranch(
                    builder->CreateAnd(builder->CreateICmpSLT(found_idx, builder->CreateLoad(sz_alloca)),
                                       builder->CreateICmpSGE(found_idx, builder->getInt32(0))),
                    [&]() -> base::Status {
                        ::llvm::Value* values = nullptr;
                        CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr_param, VALUE_VEC_IDX, &values),
                                   common::kCodegenError);

                        ::llvm::Value* value_nulls = nullptr;
                        CHECK_TRUE(Load(ctx->GetCurrentBlock(), map_ptr_param, VALUE_NULL_VEC_IDX, &value_nulls),
                                   common::kCodegenError);

                        auto* val = builder->CreateLoad(builder->CreateGEP(values, found_idx));
                        auto* val_nullable = builder->CreateLoad(builder->CreateGEP(value_nulls, found_idx));

                        builder->CreateStore(val, out_val_alloca_param);
                        builder->CreateStore(val_nullable, out_null_alloca_param);
                        return {};
                    },
                    [&]() -> base::Status { return {}; }, "load_map_value_if_found"));

                return {};
            },
            "iter_map_if_not_null");

        if (!s.isOK()) {
            return absl::InvalidArgumentError(s.str());
        }

        builder->CreateRetVoid();
    }

    auto builder = ctx->GetBuilder();
    auto s = CreateSafeNull(ctx->GetCurrentBlock(), value_type_);
    CHECK_ABSL_STATUSOR(s);
    auto* out_val_alloca = builder->CreateAlloca(value_type_);
    builder->CreateStore(s->GetValue(ctx), out_val_alloca);
    auto* out_null_alloca = builder->CreateAlloca(builder->getInt1Ty());
    builder->CreateStore(s->GetIsNull(ctx), out_null_alloca);

    builder->CreateCall(fn, {arr.GetValue(builder), arr.GetIsNull(builder), key.GetValue(builder),
                             key.GetIsNull(builder), out_val_alloca, out_null_alloca});

    return NativeValue::CreateWithFlag(builder->CreateLoad(out_val_alloca), builder->CreateLoad(out_null_alloca));
}

absl::StatusOr<NativeValue> MapIRBuilder::MapKeys(CodeGenContextBase* ctx, const NativeValue& in) const {
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
    auto idx_ptr = builder->CreateAlloca(builder->getInt32Ty());
    builder->CreateStore(builder->getInt32(0), idx_ptr);
    {
        auto s = ctx->CreateWhile(
            [&](::llvm::Value** cond) -> base::Status {
                *cond = builder->CreateICmpSLT(builder->CreateLoad(idx_ptr), size);
                return {};
            },
            [&]() -> base::Status {
                auto idx = builder->CreateLoad(idx_ptr);

                builder->CreateStore(builder->getInt1(false), builder->CreateGEP(nulls, idx));

                builder->CreateStore(builder->CreateAdd(idx, builder->getInt32(1)), idx_ptr);
                return {};
            });
        if (!s.isOK()) {
            return absl::InternalError(absl::StrCat("codegen map_keys error: ", s.GetMsg()));
        }
    }

    ArrayIRBuilder array_builder(ctx->GetModule(), keys_ptr->getType()->getPointerElementType());
    auto* casted_sz = builder->CreateIntCast(size, builder->getInt64Ty(), true);
    auto rs = array_builder.ConstructFromRaw(ctx, {keys_ptr, nulls, casted_sz});

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

absl::StatusOr<llvm::Function*> MapIRBuilder::BuildEncodeByteSizeFn(CodeGenContextBase* ctx) const {
    std::string fn_name = absl::StrCat("calc_encode_map_sz_", GetIRTypeName(struct_type_));
    llvm::Function* fn = ctx->GetModule()->getFunction(fn_name);
    auto builder = ctx->GetBuilder();
    if (fn == nullptr) {
        llvm::FunctionType* fnt = llvm::FunctionType::get(builder->getInt32Ty(),  // return size
                                                          {
                                                              struct_type_->getPointerTo(),
                                                          },
                                                          false);
        fn = llvm::Function::Create(fnt, llvm::GlobalValue::ExternalLinkage, fn_name, ctx->GetModule());
        FunctionScopeGuard fg(fn, ctx);

        llvm::Value* raw = fn->arg_begin();
        // map_size + [key_ele_sz * map_size] + [val_ele_sz * map_sz] + [sizeof(bool) * map_size]
        llvm::Value* final_size = CodecSizeForPrimitive(builder, builder->getInt32Ty());

        auto elements = Load(ctx, raw);
        if (!elements.ok()) {
            return elements.status();
        }

        if (elements->size() != FIELDS_CNT) {
            return absl::FailedPreconditionError(
                absl::Substitute("element count error, expect $0, got $1", FIELDS_CNT, elements->size()));
        }
        auto& elements_vec = elements.value();
        auto& map_size = elements_vec[0];
        auto& key_vec = elements_vec[1];
        auto& value_vec = elements_vec[2];
        auto& value_null_vec = elements_vec[3];

        CHECK_STATUS_TO_ABSL(
            ctx->CreateBranch(builder->CreateICmpSLE(map_size, builder->getInt32(0)), [&]() -> base::Status {
                builder->CreateRet(CodecSizeForPrimitive(builder, builder->getInt32Ty()));
                return {};
            }));

        auto keys_sz = CalEncodeSizeForArray(ctx, key_vec, map_size);
        CHECK_ABSL_STATUSOR(keys_sz);
        auto values_sz = CalEncodeSizeForArray(ctx, value_vec, map_size);
        CHECK_ABSL_STATUSOR(values_sz);
        auto values_null_sz = CalEncodeSizeForArray(ctx, value_null_vec, map_size);
        CHECK_ABSL_STATUSOR(values_null_sz);

        builder->CreateRet(builder->CreateAdd(
            final_size,
            builder->CreateAdd(keys_sz.value(), builder->CreateAdd(values_sz.value(), values_null_sz.value()))));
    }

    return fn;
}

absl::StatusOr<llvm::Value*> MapIRBuilder::CalEncodeByteSize(CodeGenContextBase* ctx, llvm::Value* raw) const {
    auto builder = ctx->GetBuilder();
    if (!raw->getType()->isPointerTy() || raw->getType()->getPointerElementType() != struct_type_) {
        return absl::FailedPreconditionError(absl::Substitute("struct type not match, expect $0, got $1",
                                                              GetLlvmObjectString(struct_type_),
                                                              GetLlvmObjectString(raw->getType())));
    }

    auto fns = BuildEncodeByteSizeFn(ctx);

    CHECK_ABSL_STATUSOR(fns);

    return builder->CreateCall(fns.value(), {raw});
}

absl::StatusOr<llvm::Value*> MapIRBuilder::CalEncodeSizeForArray(CodeGenContextBase* ctx, llvm::Value* arr_ptr,
                                                                 llvm::Value* arr_size) const {
    auto* arr_type = arr_ptr->getType();

    if (!arr_type->isPointerTy()) {
        // we only implemental a list of element with a size and pointer
        return absl::FailedPreconditionError(
            absl::StrCat("arr_ptr not a pointer, got ", GetLlvmObjectString(arr_type)));
    }

    if (!arr_size->getType()->isIntegerTy()) {
        return absl::FailedPreconditionError(
            absl::StrCat("arr_size is not integer, got ", GetLlvmObjectString(arr_size->getType())));
    }

    auto builder = ctx->GetBuilder();

    std::string fn_name =
        absl::StrCat("calc_encode_arr_sz_", GetIRTypeName(arr_ptr->getType()->getPointerElementType()));

    llvm::Function* fn = ctx->GetModule()->getFunction(fn_name);
    if (fn == nullptr) {
        // declaration

        llvm::FunctionType* fnt = llvm::FunctionType::get(builder->getInt32Ty(),
                                                          {
                                                              arr_ptr->getType(),    // arr ptr
                                                              builder->getInt32Ty()  // arr size
                                                          },
                                                          false);
        fn = llvm::Function::Create(fnt, llvm::GlobalValue::ExternalLinkage, fn_name, ctx->GetModule());

        FunctionScopeGuard fg(fn, ctx);
        auto sub_builder = ctx->GetBuilder();

        llvm::Value* arr_param = fn->arg_begin();
        llvm::Value* sz_param = fn->arg_begin() + 1;

        llvm::Value* sz_ptr = sub_builder->CreateAlloca(sub_builder->getInt32Ty());
        sub_builder->CreateStore(sub_builder->getInt32(0), sz_ptr);
        llvm::Value* idx_ptr = sub_builder->CreateAlloca(sub_builder->getInt32Ty());
        sub_builder->CreateStore(sub_builder->getInt32(0), idx_ptr);

        auto s = ctx->CreateWhile(
            [&](llvm::Value** cond) -> base::Status {
                auto* idx = sub_builder->CreateLoad(idx_ptr);
                *cond = sub_builder->CreateICmpSLT(idx, sz_param);
                return {};
            },
            [&]() -> base::Status {
                auto* sz = sub_builder->CreateLoad(sz_ptr);
                auto* idx = sub_builder->CreateLoad(idx_ptr);
                auto ele_ptr =
                    sub_builder->CreateLoad(sub_builder->CreateGEP(arr_type->getPointerElementType(), arr_param, idx));
                auto ele_sz_res = TypeEncodeByteSize(ctx, arr_type->getPointerElementType(), ele_ptr);
                if (!ele_sz_res.ok()) {
                    return {common::kCodegenError, ele_sz_res.status().ToString()};
                }
                sub_builder->CreateStore(sub_builder->CreateAdd(ele_sz_res.value(), sz), sz_ptr);
                sub_builder->CreateStore(sub_builder->CreateAdd(sub_builder->getInt32(1), idx), idx_ptr);

                return {};
            });
        if (!s.isOK()) {
            return absl::InternalError(absl::StrCat("codegen row size error: ", s.GetMsg()));
        }

        sub_builder->CreateRet(sub_builder->CreateLoad(sz_ptr));
    }

    return builder->CreateCall(fn, {arr_ptr, arr_size});
}

absl::StatusOr<llvm::Value*> MapIRBuilder::TypeEncodeByteSize(CodeGenContextBase* ctx, llvm::Type* ele_type,
                                                              llvm::Value* val_ptr) const {
    auto builder = ctx->GetBuilder();
    if (ele_type->isIntegerTy() || ele_type->isDoubleTy() || ele_type->isFloatTy()) {
        // bool/intXXX/float/double
        return CodecSizeForPrimitive(builder, ele_type);
    }

    // struct pointer
    if (ele_type->isPointerTy() && ele_type->getPointerElementType()->isStructTy() &&
        !ele_type->getPointerElementType()->getStructName().empty()) {
        auto identified_name = ele_type->getPointerElementType()->getStructName();

        if (identified_name == "fe.timestamp") {
            // timestamp
            return CodecSizeForPrimitive(builder, builder->getInt64Ty());
        }

        if (identified_name == "fe.date") {
            // date
            return CodecSizeForPrimitive(builder, builder->getInt32Ty());
        }

        if (identified_name == "fe.string_ref") {
            StringIRBuilder string_builder(ctx->GetModule());
            auto str_type = string_builder.GetType();
            auto str_size = builder->CreateLoad(builder->getInt32Ty(), builder->CreateStructGEP(str_type, val_ptr, 0));
            return builder->CreateAdd(CodecSizeForPrimitive(builder, str_size->getType()), str_size);
        }

        // TODO(someone): array & map type
    }

    return absl::UnimplementedError(absl::StrCat("encode type ", GetLlvmObjectString(ele_type)));
}

absl::StatusOr<llvm::Function*> MapIRBuilder::BuildEncodeFn(CodeGenContextBase* ctx) const {
    std::string fn_name = absl::StrCat("encode_map_", GetIRTypeName(struct_type_));
    llvm::Function* fn = ctx->GetModule()->getFunction(fn_name);

    auto builder = ctx->GetBuilder();
    if (fn == nullptr) {
        llvm::FunctionType* fnt = llvm::FunctionType::get(builder->getInt32Ty(),  // encoded byte size
                                                          {
                                                              builder->getInt8PtrTy(),       // row ptr
                                                              struct_type_->getPointerTo(),  // map ptr
                                                          },
                                                          false);
        fn = llvm::Function::Create(fnt, llvm::GlobalValue::ExternalLinkage, fn_name, ctx->GetModule());

        FunctionScopeGuard fg(fn, ctx);

        llvm::Value* row_ptr = fn->arg_begin();
        llvm::Value* map_ptr = fn->arg_begin() + 1;
        llvm::Value* written = builder->getInt32(0);

        auto elements = Load(ctx, map_ptr);
        if (!elements.ok()) {
            return elements.status();
        }

        if (elements->size() != FIELDS_CNT) {
            return absl::FailedPreconditionError(
                absl::Substitute("element count error, expect $0, got $1", FIELDS_CNT, elements->size()));
        }

        auto& elements_vec = elements.value();
        auto& map_size = elements_vec[0];
        auto& key_vec = elements_vec[1];
        auto& value_vec = elements_vec[2];
        auto& value_null_vec = elements_vec[3];

        // *(int32*) row_ptr = map_size
        {
            CHECK_ABSL_STATUS(BuildStoreOffset(builder, row_ptr, builder->getInt32(0), map_size));

            written = builder->CreateAdd(written, builder->getInt32(4));
        }
        {
            // *(key_type[map_size]) (row_ptr + 4) = key_vec
            auto row_ptr_with_offset = BuildGetPtrOffset(builder, row_ptr, written);
            CHECK_ABSL_STATUSOR(row_ptr_with_offset);
            auto s = EncodeArray(ctx, row_ptr_with_offset.value(), key_vec, map_size);
            CHECK_ABSL_STATUSOR(s);
            written = builder->CreateAdd(written, s.value());
        }
        {
            // *(value_type[map_size]) (row_ptr + ?) = value_vec
            auto row_ptr_with_offset = BuildGetPtrOffset(builder, row_ptr, written);
            CHECK_ABSL_STATUSOR(row_ptr_with_offset);
            auto s = EncodeArray(ctx, row_ptr_with_offset.value(), value_vec, map_size);
            CHECK_ABSL_STATUSOR(s);
            written = builder->CreateAdd(written, s.value());
        }
        {
            // *(bool[map_size]) (row_ptr + ?) = value_null_vec
            auto row_ptr_with_offset = BuildGetPtrOffset(builder, row_ptr, written);
            CHECK_ABSL_STATUSOR(row_ptr_with_offset);
            // TODO(someone): alignment issue, bitwise operation for better performance ?
            auto s = EncodeArray(ctx, row_ptr_with_offset.value(), value_null_vec, map_size);
            CHECK_ABSL_STATUSOR(s);
            written = builder->CreateAdd(written, s.value());
        }

        builder->CreateRet(written);
    }
    return fn;
}

absl::StatusOr<llvm::Value*> MapIRBuilder::Encode(CodeGenContextBase* ctx, llvm::Value* row_ptr,
                                                  llvm::Value* map_ptr) const {
    auto builder = ctx->GetBuilder();

    if (row_ptr->getType() != builder->getInt8Ty()->getPointerTo()) {
        return absl::FailedPreconditionError(
            absl::StrCat("expect int8* as row ptr, got ", GetLlvmObjectString(row_ptr->getType())));
    }

    // type check
    if (!map_ptr->getType()->isPointerTy() || map_ptr->getType()->getPointerElementType() != struct_type_) {
        return absl::FailedPreconditionError(
            absl::Substitute("type not match, expect $0, got $1", GetLlvmObjectString(struct_type_),
                             GetLlvmObjectString(map_ptr->getType()->getPointerElementType())));
    }

    auto fns = BuildEncodeFn(ctx);
    CHECK_ABSL_STATUSOR(fns);

    return builder->CreateCall(fns.value(), {row_ptr, map_ptr});
}

absl::StatusOr<llvm::Value*> MapIRBuilder::EncodeArray(CodeGenContextBase* ctx_, llvm::Value* row_ptr,
                                                       llvm::Value* arr_ptr, llvm::Value* arr_size) const {
    if (!arr_ptr->getType()->isPointerTy()) {
        return absl::InvalidArgumentError(
            absl::Substitute("expect arr_ptr as poiner, but got $0", GetLlvmObjectString(arr_ptr->getType())));
    }

    auto encode_key_vec_fn = GetOrBuildEncodeArrFunction(ctx_, arr_ptr->getType()->getPointerElementType());
    CHECK_ABSL_STATUSOR(encode_key_vec_fn);

    auto* fn = encode_key_vec_fn.value();

    return ctx_->GetBuilder()->CreateCall(fn, {row_ptr, arr_ptr, arr_size}, "map_encode_arr");
}

absl::StatusOr<llvm::Value*> MapIRBuilder::EncodeBaseValue(CodeGenContextBase* ctx, llvm::Value* value,
                                                           llvm::Value* row_ptr, llvm::Value* offset) const {
    auto builder = ctx->GetBuilder();
    auto type = value->getType();
    if (type->isIntegerTy() || type->isDoubleTy() || type->isFloatTy()) {
        // bool/intXXX/float/double
        auto s = BuildGetPtrOffset(builder, row_ptr, offset, type->getPointerTo());
        CHECK_ABSL_STATUSOR(s);
        builder->CreateStore(value, s.value());
        return CodecSizeForPrimitive(builder, type);
    }

    // struct pointer
    if (type->isPointerTy()) {
        auto* tyee = type->getPointerElementType();
        if (tyee->isStructTy() && !tyee->getStructName().empty()) {
            auto identified_name = tyee->getStructName();

            if (identified_name == "fe.timestamp") {
                // timestamp
                ::llvm::Value* ts;
                TimestampIRBuilder timestamp_builder(ctx->GetModule());
                if (!timestamp_builder.GetTs(ctx->GetCurrentBlock(), value, &ts)) {
                    return absl::InternalError("extract ts from timestamp");
                }
                return EncodeBaseValue(ctx, ts, row_ptr, offset);
            }

            if (identified_name == "fe.date") {
                // date
                ::llvm::Value* days;
                DateIRBuilder date_builder(m_);
                if (!date_builder.GetDate(ctx->GetCurrentBlock(), value, &days)) {
                    return absl::InternalError("extract day from date");
                }

                return EncodeBaseValue(ctx, days, row_ptr, offset);
            }

            if (identified_name == "fe.string_ref") {
                StringIRBuilder string_builder(ctx->GetModule());
                llvm::Value* sz = nullptr;
                if (!string_builder.GetSize(ctx->GetCurrentBlock(), value, &sz)) {
                    return absl::InternalError("extract size from string");
                }
                auto s = EncodeBaseValue(ctx, sz, row_ptr, offset);
                llvm::Value* str = nullptr;
                if (!string_builder.GetData(ctx->GetCurrentBlock(), value, &str)) {
                    return absl::InternalError("extract data from string");
                }

                auto s2 = BuildGetPtrOffset(builder, row_ptr, builder->CreateAdd(builder->getInt32(4), offset),
                                            builder->getInt8Ty()->getPointerTo());
                CHECK_ABSL_STATUSOR(s2);

                builder->CreateMemCpy(s2.value(), 0, str, 0, sz);

                return builder->CreateAdd(builder->getInt32(4), sz);
            }
        }
    }

    // TODO(someone): array & map type

    return absl::UnimplementedError(absl::StrCat("encode type inside map: ", GetLlvmObjectString(type)));
}

absl::StatusOr<llvm::Value*> MapIRBuilder::Decode(CodeGenContextBase* ctx, llvm::Value* row_ptr) const {
    auto* builder = ctx->GetBuilder();
    auto* map_sz = builder->CreateLoad(builder->getInt32Ty(),
                                       builder->CreatePointerCast(row_ptr, builder->getInt32Ty()->getPointerTo()));

    llvm::Value* map_alloca = nullptr;
    if (!Allocate(ctx->GetCurrentBlock(), &map_alloca)) {
        return absl::InternalError("fail to allocate map");
    }
    if (!Set(ctx->GetCurrentBlock(), map_alloca, SZ_IDX, builder->getInt32(0))) {
        return absl::InternalError("fail to set default size for map");
    }

    CHECK_STATUS_TO_ABSL(ctx->CreateBranch(builder->CreateICmpSGT(map_sz, builder->getInt32(0)), [&]() -> base::Status {
        // only work if allocation happens in the top function, otherwise vectors will be cleared
        llvm::Value* key_vec = builder->CreateAlloca(key_type_, map_sz, "map_key_vec");
        llvm::Value* value_vec = builder->CreateAlloca(value_type_, map_sz, "map_value_vec");
        llvm::Value* value_nulls_vec = builder->CreateAlloca(builder->getInt1Ty(), map_sz, "map_nulls_vec");

        llvm::Value* idx0_alloca = builder->CreateAlloca(builder->getInt32Ty());
        builder->CreateStore(builder->getInt32(0), idx0_alloca);
        // also allocate space for pointer type that points to
        CHECK_STATUS(ctx->CreateWhile(
            [&](llvm::Value** cond) -> base::Status {
                *cond = ctx->GetBuilder()->CreateICmpSLT(builder->CreateLoad(idx0_alloca), map_sz);
                return {};
            },
            [&]() -> base::Status {
                llvm::Value* idx = builder->CreateLoad(idx0_alloca);
                if (key_type_->isPointerTy()) {
                    auto* ele_val = builder->CreateAlloca(key_type_->getPointerElementType());
                    builder->CreateStore(ele_val, builder->CreateGEP(key_type_, key_vec, idx));
                }
                if (value_type_->isPointerTy()) {
                    auto* ele_val = builder->CreateAlloca(value_type_->getPointerElementType());
                    builder->CreateStore(ele_val, builder->CreateGEP(value_type_, value_vec, idx));
                }

                builder->CreateStore(builder->CreateAdd(builder->getInt32(1), idx), idx0_alloca);
                return {};
            }));

        auto s0 = BuildGetPtrOffset(builder, row_ptr, builder->getInt32(4), builder->getInt8Ty()->getPointerTo());
        CHECK_TRUE(s0.ok(), common::kCodegenError, s0.status());
        auto key_vec_res = DecodeArrayValue(ctx, s0.value(), map_sz, key_vec, key_type_);
        CHECK_TRUE(key_vec_res.ok(), common::kCodegenError, key_vec_res.status());

        auto s1 = BuildGetPtrOffset(builder, row_ptr, builder->CreateAdd(builder->getInt32(4), key_vec_res.value()),
                                    builder->getInt8Ty()->getPointerTo());
        CHECK_TRUE(s1.ok(), common::kCodegenError, s1.status());
        auto value_vec_res = DecodeArrayValue(ctx, s1.value(), map_sz, value_vec, value_type_);
        CHECK_TRUE(value_vec_res.ok(), common::kCodegenError, value_vec_res.status());

        auto s2 = BuildGetPtrOffset(
            builder, row_ptr,
            builder->CreateAdd(builder->getInt32(4), builder->CreateAdd(key_vec_res.value(), value_vec_res.value())),
            builder->getInt8Ty()->getPointerTo());
        CHECK_TRUE(s2.ok(), common::kCodegenError, s2.status());
        auto value_null_vec_res = DecodeArrayValue(ctx, s2.value(), map_sz, value_nulls_vec, builder->getInt1Ty());
        CHECK_TRUE(value_null_vec_res.ok(), common::kCodegenError, value_null_vec_res.status());

        {
            auto s = Set(ctx, map_alloca, {map_sz, key_vec, value_vec, value_nulls_vec});
            CHECK_TRUE(s.ok(), common::kCodegenError, s);
        }
        return {};
    }));

    return map_alloca;
}

absl::StatusOr<llvm::Value*> MapIRBuilder::DecodeArrayValue(CodeGenContextBase* ctx, llvm::Value* row_ptr,
                                                            llvm::Value* sz, llvm::Value* arr_alloca,
                                                            llvm::Type* ele_ty) const {
    auto* builder = ctx->GetBuilder();

    std::string fn_name = absl::StrCat("decode_map_arr_", GetIRTypeName(ele_ty));
    llvm::Function* fn = ctx->GetModule()->getFunction(fn_name);
    if (fn == nullptr) {
        // declaration
        llvm::FunctionType* fnt =
            llvm::FunctionType::get(builder->getInt32Ty(),  // return type, read size
                                    {
                                        builder->getInt8Ty()->getPointerTo(),  // row ptr
                                        ele_ty->getPointerTo(),  // parsed pointer, space should allocate externally
                                        builder->getInt32Ty(),   // arr size
                                    },
                                    false);

        fn = llvm::Function::Create(fnt, llvm::GlobalValue::ExternalLinkage, fn_name, ctx->GetModule());

        FunctionScopeGuard fg(fn, ctx);
        auto* sub_builder = ctx->GetBuilder();

        llvm::Value* row_ptr_param = fn->arg_begin();
        llvm::Value* arr_ptr_param = fn->arg_begin() + 1;
        llvm::Value* sz_param = fn->arg_begin() + 2;

        llvm::Value* idx_ptr = sub_builder->CreateAlloca(sub_builder->getInt32Ty());
        sub_builder->CreateStore(sub_builder->getInt32(0), idx_ptr);
        llvm::Value* offset_ptr = sub_builder->CreateAlloca(sub_builder->getInt32Ty());
        sub_builder->CreateStore(sub_builder->getInt32(0), offset_ptr);

        auto s = ctx->CreateWhile(
            [&](llvm::Value** cond) -> base::Status {
                *cond = sub_builder->CreateICmpSLT(sub_builder->CreateLoad(idx_ptr), sz_param);
                return {};
            },
            [&]() -> base::Status {
                auto offset = sub_builder->CreateLoad(offset_ptr);
                auto idx = sub_builder->CreateLoad(idx_ptr);
                auto row_with_offset =
                    BuildGetPtrOffset(sub_builder, row_ptr_param, offset, sub_builder->getInt8PtrTy());
                CHECK_TRUE(row_with_offset.ok(), common::kCodegenError, row_with_offset.status().ToString());

                auto ele_ptr = sub_builder->CreateGEP(ele_ty, arr_ptr_param, idx);
                auto s = DecodeBaseValue(ctx, row_with_offset.value(), ele_ptr, ele_ty);
                CHECK_TRUE(s.ok(), common::kCodegenError, s.status().ToString());

                sub_builder->CreateStore(sub_builder->CreateAdd(s.value(), offset), offset_ptr);
                sub_builder->CreateStore(sub_builder->CreateAdd(sub_builder->getInt32(1), idx), idx_ptr);
                return {};
            }, "decode_map_arr");
        if (!s.isOK()) {
            return absl::InternalError(s.GetMsg());
        }

        sub_builder->CreateRet(sub_builder->CreateLoad(offset_ptr));
    }

    return builder->CreateCall(fn, {row_ptr, arr_alloca, sz});
}

absl::StatusOr<llvm::Value*> MapIRBuilder::DecodeBaseValue(CodeGenContextBase* ctx, llvm::Value* ptr,
                                                           llvm::Value* base_ptr, llvm::Type* type) const {
    auto builder = ctx->GetBuilder();
    if (type->isIntegerTy() || type->isFloatTy() || type->isDoubleTy()) {
        builder->CreateStore(builder->CreateLoad(type, builder->CreatePointerCast(ptr, type->getPointerTo())),
                             base_ptr);
        return CodecSizeForPrimitive(builder, type);
    }
    // struct pointer
    if (type->isPointerTy()) {
        llvm::Type* tyee = type->getPointerElementType();
        if (tyee->isStructTy() && !tyee->getStructName().empty()) {
            auto identified_name = tyee->getStructName();

            llvm::Value* struct_ptr = builder->CreateLoad(type, base_ptr);

            if (identified_name == "fe.timestamp") {
                // timestamp
                TimestampIRBuilder timestamp_builder(ctx->GetModule());
                llvm::Value* ts_ptr =
                    builder->CreateStructGEP(timestamp_builder.GetType(), struct_ptr, 0, "load_timestamp_ts_ptr");
                auto read = DecodeBaseValue(ctx, ptr, ts_ptr, builder->getInt64Ty());

                CHECK_ABSL_STATUSOR(read);
                return read.value();
            }

            if (identified_name == "fe.date") {
                // date
                DateIRBuilder date_builder(m_);
                llvm::Value* val_ptr =
                    builder->CreateStructGEP(date_builder.GetType(), struct_ptr, 0, "load_data_val_ptr");
                auto val = DecodeBaseValue(ctx, ptr, val_ptr, builder->getInt32Ty());
                CHECK_ABSL_STATUSOR(val);
                return val.value();
            }

            if (identified_name == "fe.string_ref") {
                StringIRBuilder string_builder(ctx->GetModule());

                llvm::Value* str_sz_ptr =
                    builder->CreateStructGEP(string_builder.GetType(), struct_ptr, 0, "load_str_sz_ptr");
                llvm::Value* str_data_ptr =
                    builder->CreateStructGEP(string_builder.GetType(), struct_ptr, 1, "load_str_data_ptr");

                auto sz_res = DecodeBaseValue(ctx, ptr, str_sz_ptr, builder->getInt32Ty());
                CHECK_ABSL_STATUSOR(sz_res);

                auto data_ptr_res =
                    BuildGetPtrOffset(builder, ptr, builder->getInt32(4), builder->getInt8Ty()->getPointerTo());
                CHECK_ABSL_STATUSOR(data_ptr_res);
                builder->CreateStore(data_ptr_res.value(), str_data_ptr);

                return builder->CreateAdd(builder->CreateLoad(str_sz_ptr), sz_res.value());
            }
        }
    }

    return absl::UnimplementedError(absl::StrCat("decode for type inside map: ", GetLlvmObjectString(type)));
}

absl::StatusOr<llvm::Function*> MapIRBuilder::GetOrBuildEncodeArrFunction(CodeGenContextBase* ctx,
                                                                          llvm::Type* ele_type) const {
    // declare
    std::string fn_name = absl::StrCat("encode_map_arr_", GetIRTypeName(ele_type));
    auto* fn = ctx->GetModule()->getFunction(fn_name);
    if (fn != nullptr) {
        return fn;
    }

    auto* builder = ctx->GetBuilder();
    llvm::FunctionType* fnt = llvm::FunctionType::get(
        builder->getInt32Ty(), {builder->getInt8Ty()->getPointerTo(), ele_type->getPointerTo(), builder->getInt32Ty()},
        false);

    fn = llvm::Function::Create(fnt, llvm::GlobalValue::ExternalLinkage, fn_name, ctx->GetModule());

    // enter function
    FunctionScopeGuard fg(fn, ctx);
    auto* sub_builder = ctx->GetBuilder();

    llvm::Value* row_ptr = fn->arg_begin();
    llvm::Value* arr_ptr = fn->arg_begin() + 1;
    llvm::Value* arr_size = fn->arg_begin() + 2;

    llvm::Value* idx_ptr = sub_builder->CreateAlloca(sub_builder->getInt32Ty(), nullptr, "idx_ptr");
    llvm::Value* written_ptr = sub_builder->CreateAlloca(sub_builder->getInt32Ty(), nullptr, "written_ptr");
    sub_builder->CreateStore(sub_builder->getInt32(0), idx_ptr);
    sub_builder->CreateStore(sub_builder->getInt32(0), written_ptr);

    // definition
    auto s = ctx->CreateWhile(
        [&](llvm::Value** cond) -> base::Status {
            llvm::Value* idx = sub_builder->CreateLoad(idx_ptr);
            *cond = sub_builder->CreateICmpSLT(idx, arr_size);
            return {};
        },
        [&]() -> base::Status {
            llvm::Value* idx = sub_builder->CreateLoad(idx_ptr);
            llvm::Value* ele = sub_builder->CreateLoad(ele_type, sub_builder->CreateGEP(arr_ptr, idx));
            // write ele
            llvm::Value* offset = sub_builder->CreateLoad(written_ptr);
            auto s = EncodeBaseValue(ctx, ele, row_ptr, offset);
            CHECK_TRUE(s.ok(), common::kCodegenError, s.status().ToString());
            // update states
            sub_builder->CreateStore(sub_builder->CreateAdd(idx, sub_builder->getInt32(1)), idx_ptr);
            sub_builder->CreateStore(sub_builder->CreateAdd(offset, s.value()), written_ptr);
            return {};
        });
    if (!s.isOK()) {
        return absl::InternalError(s.GetMsg());
    }

    sub_builder->CreateRet(sub_builder->CreateLoad(sub_builder->getInt32Ty(), written_ptr));

    return fn;
}


}  // namespace codegen
}  // namespace hybridse
