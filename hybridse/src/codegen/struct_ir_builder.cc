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
    ::llvm::Value* value = nullptr;
    if (!CreateDefault(block, &value)) {
        return absl::InternalError(absl::StrCat("fail to construct ", GetLlvmObjectString(GetType())));
    }
    ::llvm::IRBuilder<> builder(block);
    return NativeValue::CreateWithFlag(value, builder.getInt1(true));
}

::llvm::Type* StructTypeIRBuilder::GetType() { return struct_type_; }

bool StructTypeIRBuilder::Create(::llvm::BasicBlock* block,
                                 ::llvm::Value** output) const {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* value = CreateAllocaAtHead(&builder, struct_type_, "struct_alloca");
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
    if (struct_value->getType()->getPointerElementType() != struct_type_) {
        LOG(WARNING) << "Fail set Struct value: struct value type invalid "
                     << struct_value->getType()
                            ->getPointerElementType()
                            ->getStructName()
                            .str();
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.getInt64(1);
    ::llvm::Value* value_ptr =
        builder.CreateStructGEP(struct_type_, struct_value, idx);
    if (nullptr == builder.CreateStore(value, value_ptr)) {
        LOG(WARNING) << "Fail Set Struct Value idx = " << idx;
        return false;
    }
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
}  // namespace codegen
}  // namespace hybridse
