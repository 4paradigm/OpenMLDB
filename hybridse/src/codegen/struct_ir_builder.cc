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

#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "codegen/context.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"

namespace hybridse {
namespace codegen {
StructTypeIRBuilder::StructTypeIRBuilder(::llvm::Module* m)
    : TypeIRBuilder(), m_(m), struct_type_(nullptr) {}
StructTypeIRBuilder::~StructTypeIRBuilder() {}

bool StructTypeIRBuilder::StructCopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist) {
    StructTypeIRBuilder* struct_builder = CreateStructTypeIRBuilder(block->getModule(), src->getType());
    bool ok = struct_builder->CopyFrom(block, src, dist);
    delete struct_builder;
    return ok;
}

StructTypeIRBuilder* StructTypeIRBuilder::CreateStructTypeIRBuilder(::llvm::Module* m, ::llvm::Type* type) {
    node::DataType base_type;
    if (!GetBaseType(type, &base_type)) {
        return nullptr;
    }

    switch (base_type) {
        case node::kTimestamp:
            return new TimestampIRBuilder(m);
        case node::kDate:
            return new DateIRBuilder(m);
        case node::kVarchar:
            return new StringIRBuilder(m);
        default: {
            LOG(WARNING) << "fail to create struct type ir builder for " << DataTypeName(base_type);
            return nullptr;
        }
    }
    return nullptr;
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
    ::llvm::Value* value = CreateAllocaAtHead(&builder, struct_type_, GetLlvmObjectString(struct_type_));
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
absl::StatusOr<NativeValue> StructTypeIRBuilder::Construct(CodeGenContext* ctx,
                                                           absl::Span<const NativeValue> args) const {
    return absl::UnimplementedError(absl::StrCat("Construct for type ", GetLlvmObjectString(struct_type_)));
}

absl::StatusOr<::llvm::Value*> StructTypeIRBuilder::ConstructFromRaw(CodeGenContext* ctx,
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

absl::StatusOr<NativeValue> StructTypeIRBuilder::ExtractElement(CodeGenContext* ctx, const NativeValue& arr,
                                                                const NativeValue& key) const {
    return absl::UnimplementedError(
        absl::StrCat("extract element unimplemented for ", GetLlvmObjectString(struct_type_)));
}

void StructTypeIRBuilder::EnsureOK() const {
    assert(struct_type_ != nullptr);
    // it's a identified type
    assert(!struct_type_->getName().empty());
}
std::string StructTypeIRBuilder::GetTypeDebugString() const { return GetLlvmObjectString(struct_type_); }

absl::Status StructTypeIRBuilder::Set(CodeGenContext* ctx, ::llvm::Value* struct_value,
                                      absl::Span<::llvm::Value* const> members) const {
    if (ctx == nullptr || struct_value == nullptr) {
        return absl::InvalidArgumentError("ctx or struct pointer is null");
    }

    if (!IsStructPtr(struct_value->getType())) {
        return absl::InvalidArgumentError(
            absl::StrCat("value not a struct pointer: ", GetLlvmObjectString(struct_value->getType())));
    }

    if (struct_value->getType()->getPointerElementType() != struct_type_) {
        return absl::InvalidArgumentError(absl::Substitute("input value has different type, expect $0 but got $1",
                                                           GetLlvmObjectString(struct_type_),
                                                           GetLlvmObjectString(struct_value->getType())));
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

}  // namespace codegen
}  // namespace hybridse
