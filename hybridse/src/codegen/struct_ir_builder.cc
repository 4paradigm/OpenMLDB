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

#include "codegen/struct_ir_builder.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "codegen/array_ir_builder.h"
#include "codegen/context.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/map_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "node/node_manager.h"

namespace hybridse {
namespace codegen {
StructTypeIRBuilder::StructTypeIRBuilder(::llvm::Module* m)
    : TypeIRBuilder(), m_(m), struct_type_(nullptr) {}
StructTypeIRBuilder::~StructTypeIRBuilder() {}

bool StructTypeIRBuilder::StructCopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist) {
    auto struct_builder = CreateStructTypeIRBuilder(block->getModule(), src->getType());
    if (struct_builder.ok()) {
        return struct_builder.value()->CopyFrom(block, src, dist);
    }
    return false;
}

absl::StatusOr<std::unique_ptr<StructTypeIRBuilder>> StructTypeIRBuilder::CreateStructTypeIRBuilder(
    ::llvm::Module* m, ::llvm::Type* type) {
    node::NodeManager nm;
    const node::TypeNode* ctype = nullptr;
    if (!GetFullType(&nm, type, &ctype)) {
        return absl::InvalidArgumentError(absl::StrCat("can't get full type for: ", GetLlvmObjectString(type)));
    }

    switch (ctype->base()) {
        case node::kTimestamp:
            return std::make_unique<TimestampIRBuilder>(m);
        case node::kDate:
            return std::make_unique<DateIRBuilder>(m);
        case node::kVarchar:
            return std::make_unique<StringIRBuilder>(m);
        case node::DataType::kMap: {
            assert(ctype->IsMap() && "logic error: not a map type");
            auto map_type = ctype->GetAsOrNull<node::MapType>();
            assert(map_type != nullptr && "logic error: map type empty");
            ::llvm::Type* key_type = nullptr;
            ::llvm::Type* value_type = nullptr;
            if (codegen::GetLlvmType(m, map_type->key_type(), &key_type) &&
                codegen::GetLlvmType(m, map_type->value_type(), &value_type)) {
                return std::make_unique<MapIRBuilder>(m, key_type, value_type);
            } else {
                return absl::InvalidArgumentError(
                    absl::Substitute("not able to casting map type: $0", GetLlvmObjectString(type)));
            }
            break;
        }
        case node::DataType::kArray: {
            assert(ctype->IsArray() && "logic error: not a array type");
            assert(ctype->GetGenericSize() == 1 && "logic error: not a array type");
            ::llvm::Type* ele_type = nullptr;
            if (!codegen::GetLlvmType(m, ctype->GetGenericType(0), &ele_type)) {
                return absl::InvalidArgumentError(
                    absl::Substitute("not able to casting array type: $0", GetLlvmObjectString(type)));
            }
            return std::make_unique<ArrayIRBuilder>(m, ele_type);
        }
        default: {
            break;
        }
    }
    return absl::UnimplementedError(
        absl::StrCat("fail to create struct type ir builder for ", GetLlvmObjectString(type)));
}

absl::StatusOr<NativeValue> StructTypeIRBuilder::CreateNull(::llvm::BasicBlock* block) {
    EnsureOK();

    ::llvm::Value* value = nullptr;
    if (!CreateDefault(block, &value)) {
        return absl::InternalError(absl::StrCat("fail to construct ", GetLlvmObjectString(GetType())));
    }
    ::llvm::IRBuilder<> builder(block);
    return NativeValue::CreateWithFlag(value, builder.getInt1(true));
}

::llvm::Type* StructTypeIRBuilder::GetType() const { return struct_type_; }

bool StructTypeIRBuilder::Allocate(::llvm::BasicBlock* block,
                                 ::llvm::Value** output) const {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    // value is a pointer to struct type
    ::llvm::Value* value = CreateAllocaAtHead(&builder, struct_type_, GetIRTypeName(struct_type_));
    *output = value;
    return true;
}
bool StructTypeIRBuilder::Load(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx,
                               ::llvm::Value** output) const {
    ::llvm::Value* value_ptr = nullptr;
    if (false == Get(block, struct_value, idx, &value_ptr)) {
        return false;
    }

    ::llvm::IRBuilder<> builder(block);
    *output = builder.CreateLoad(value_ptr);
    return true;
}
bool StructTypeIRBuilder::Set(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx,
                              ::llvm::Value* value) const {
    if (block == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    if (!IsStructPtr(struct_value->getType())) {
        LOG(WARNING) << "Fail set Struct value: struct pointer is required";
        return false;
    }

    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* value_ptr = builder.CreateStructGEP(struct_type_, struct_value, idx);
    builder.CreateStore(value, value_ptr);
    return true;
}

bool StructTypeIRBuilder::Get(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx,
                               ::llvm::Value** output) const {
    if (block == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    if (!IsStructPtr(struct_value->getType())) {
        LOG(WARNING) << "Fail get Struct value: struct pointer is required";
        return false;
    }
    if (struct_value->getType()->getPointerElementType() != struct_type_) {
        LOG(WARNING) << "Fail get Struct value: struct value type invalid "
                     << struct_value->getType()
                            ->getPointerElementType()
                            ->getStructName()
                            .str();
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    *output = builder.CreateStructGEP(struct_type_, struct_value, idx);
    return true;
}
absl::StatusOr<NativeValue> StructTypeIRBuilder::Construct(CodeGenContextBase* ctx,
                                                           absl::Span<const NativeValue> args) const {
    return absl::UnimplementedError(absl::StrCat("Construct for type ", GetLlvmObjectString(struct_type_)));
}

absl::StatusOr<::llvm::Value*> StructTypeIRBuilder::ConstructFromRaw(CodeGenContextBase* ctx,
                                                                     absl::Span<::llvm::Value* const> args) const {
    EnsureOK();

    llvm::Value* alloca = nullptr;
    if (!Allocate(ctx->GetCurrentBlock(), &alloca)) {
        return absl::FailedPreconditionError("failed to allocate array");
    }

    auto s = Set(ctx, alloca, args);
    if (!s.ok()) {
        return s;
    }

    return alloca;
}

absl::StatusOr<NativeValue> StructTypeIRBuilder::ExtractElement(CodeGenContextBase* ctx, const NativeValue& arr,
                                                                const NativeValue& key) const {
    return absl::UnimplementedError(
        absl::StrCat("extract element unimplemented for ", GetLlvmObjectString(struct_type_)));
}

absl::StatusOr<llvm::Value*> StructTypeIRBuilder::NumElements(CodeGenContextBase* ctx, llvm::Value* arr) const {
    return absl::UnimplementedError(
        absl::StrCat("element size unimplemented for ", GetLlvmObjectString(struct_type_)));
}

void StructTypeIRBuilder::EnsureOK() const {
    assert(struct_type_ != nullptr && "filed struct_type_ uninitialized");
    // it's a identified type
    assert(!struct_type_->getName().empty() && "struct_type not a identified type");
}
std::string StructTypeIRBuilder::GetTypeDebugString() const { return GetLlvmObjectString(struct_type_); }

absl::Status StructTypeIRBuilder::Set(CodeGenContextBase* ctx, ::llvm::Value* struct_value,
                                      absl::Span<::llvm::Value* const> members) const {
    if (ctx == nullptr || struct_value == nullptr) {
        return absl::InvalidArgumentError("ctx or struct pointer is null");
    }

    if (!IsStructPtr(struct_value->getType())) {
        return absl::InvalidArgumentError(
            absl::StrCat("value not a struct pointer: ", GetLlvmObjectString(struct_value->getType())));
    }

    if (struct_value->getType()->getPointerElementType() != struct_type_) {
        return absl::InvalidArgumentError(
            absl::Substitute("input value has different type, expect $0 but got $1", GetLlvmObjectString(struct_type_),
                             GetLlvmObjectString(struct_value->getType()->getPointerElementType())));
    }

    if (members.size() != struct_type_->getNumElements()) {
        return absl::InvalidArgumentError(absl::Substitute("struct $0 requires exact $1 member, but got $2",
                                                           GetLlvmObjectString(struct_type_),
                                                           struct_type_->getNumElements(), members.size()));
    }

    for (unsigned idx = 0; idx < struct_type_->getNumElements(); ++idx) {
        auto ele_type = struct_type_->getElementType(idx);
        if (ele_type != members[idx]->getType()) {
            return absl::InvalidArgumentError(absl::Substitute("$0th member: expect $1 but got $2", idx,
                                                               GetLlvmObjectString(ele_type),
                                                               GetLlvmObjectString(members[idx]->getType())));
        }
        ::llvm::Value* value_ptr = ctx->GetBuilder()->CreateStructGEP(struct_type_, struct_value, idx);
        ctx->GetBuilder()->CreateStore(members[idx], value_ptr);
    }

    return absl::OkStatus();
}

absl::StatusOr<std::vector<llvm::Value*>> StructTypeIRBuilder::Load(CodeGenContextBase* ctx,
                                                                    llvm::Value* struct_ptr) const {
    assert(ctx != nullptr && struct_ptr != nullptr);

    if (!IsStructPtr(struct_ptr->getType())) {
        return absl::InvalidArgumentError(
            absl::StrCat("value not a struct pointer: ", GetLlvmObjectString(struct_ptr->getType())));
    }
    if (struct_ptr->getType()->getPointerElementType() != struct_type_) {
        return absl::InvalidArgumentError(
            absl::Substitute("input value has different type, expect $0 but got $1", GetLlvmObjectString(struct_type_),
                             GetLlvmObjectString(struct_ptr->getType()->getPointerElementType())));
    }

    std::vector<llvm::Value*> res;
    res.reserve(struct_type_->getNumElements());

    auto builder = ctx->GetBuilder();
    for (unsigned idx = 0; idx < struct_type_->getNumElements(); ++idx) {
        auto* ele = builder->CreateLoad(struct_type_->getElementType(idx),
                                        builder->CreateStructGEP(struct_type_, struct_ptr, idx));
        res.push_back(ele);
    }

    return res;
}

absl::StatusOr<NativeValue> CreateSafeNull(::llvm::BasicBlock* block, ::llvm::Type* type) {
    if (TypeIRBuilder::IsStructPtr(type)) {
        auto s = StructTypeIRBuilder::CreateStructTypeIRBuilder(block->getModule(), type);
        CHECK_ABSL_STATUSOR(s);
        return s.value()->CreateNull(block);
    }

    return NativeValue(nullptr, nullptr, type);
}

// args should be array of string
absl::StatusOr<NativeValue> Combine(CodeGenContextBase* ctx, const NativeValue delimiter,
                                    absl::Span<const NativeValue> args) {
    auto builder = ctx->GetBuilder();

    StringIRBuilder str_builder(ctx->GetModule());
    ArrayIRBuilder arr_builder(ctx->GetModule(), str_builder.GetType()->getPointerTo());

    llvm::Value* empty_str = nullptr;
    if (!str_builder.CreateDefault(ctx->GetCurrentBlock(), &empty_str)) {
        return absl::InternalError("codegen error: fail to construct empty string");
    }
    llvm::Value* del = builder->CreateSelect(delimiter.GetIsNull(builder), empty_str, delimiter.GetValue(builder));

    llvm::Type* input_arr_type = arr_builder.GetType()->getPointerTo();
    llvm::Value* empty_arr = nullptr;
    if (!arr_builder.CreateDefault(ctx->GetCurrentBlock(), &empty_arr)) {
        return absl::InternalError("codegen error: fail to construct empty string of array");
    }
    llvm::Value* input_arrays = builder->CreateAlloca(input_arr_type, builder->getInt32(args.size()), "array_data");
    node::NodeManager nm;
    std::vector<NativeValue> casted_args(args.size());
    for (int i = 0; i < args.size(); ++i) {
        const node::TypeNode* tp = nullptr;
        if (!GetFullType(&nm, args.at(i).GetType(), &tp)) {
            return absl::InternalError("codegen error: fail to get valid type from llvm value");
        }
        if (!tp->IsArray() || tp->GetGenericSize() != 1) {
            return absl::InternalError("codegen error: arguments to array_combine is not ARRAY");
        }
        if (!tp->GetGenericType(0)->IsString()) {
            auto s = arr_builder.CastToArrayString(ctx, args.at(i).GetRaw());
            CHECK_ABSL_STATUSOR(s);
            casted_args.at(i) = NativeValue::Create(s.value());
        } else {
            casted_args.at(i) = args.at(i);
        }

        auto safe_str_arr =
            builder->CreateSelect(casted_args.at(i).GetIsNull(builder), empty_arr, casted_args.at(i).GetRaw());
        builder->CreateStore(safe_str_arr, builder->CreateGEP(input_arr_type, input_arrays, builder->getInt32(i)));
    }

    ::llvm::FunctionCallee array_combine_fn = ctx->GetModule()->getOrInsertFunction(
        "hybridse_array_combine", builder->getVoidTy(), str_builder.GetType()->getPointerTo(), builder->getInt32Ty(),
        input_arr_type->getPointerTo(), input_arr_type);
    assert(array_combine_fn);

    llvm::Value* out = builder->CreateAlloca(arr_builder.GetType());
    builder->CreateCall(array_combine_fn, {
                                              del,                             // delimiter should ensure non-null
                                              builder->getInt32(args.size()),  // num of arrays
                                              input_arrays,                    // ArrayRef<StringRef>**
                                              out                              // output string
                                          });

    return NativeValue::Create(out);
}

absl::Status StructTypeIRBuilder::Initialize(CodeGenContextBase* ctx, ::llvm::Value* alloca,
                                             absl::Span<llvm::Value* const> args) const {
    return absl::UnimplementedError(absl::StrCat("Initialize for type ", GetLlvmObjectString(struct_type_)));
}
}  // namespace codegen
}  // namespace hybridse
