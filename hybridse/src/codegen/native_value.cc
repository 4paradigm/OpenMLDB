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

#include "codegen/native_value.h"
#include <dlfcn.h>
#include <execinfo.h>
#include "codegen/context.h"
#include "codegen/ir_base_builder.h"

namespace hybridse {
namespace codegen {

::llvm::Value* NativeValue::GetIsNull(::llvm::IRBuilder<>* builder) const {
    auto bool_ty = ::llvm::Type::getInt1Ty(builder->getContext());
    ::llvm::Value* is_null;
    if (IsConstNull()) {
        is_null = ::llvm::ConstantInt::getTrue(builder->getContext());
    } else if (IsMemFlag()) {
        is_null = builder->CreateLoad(flag_);
    } else if (IsRegFlag()) {
        is_null = flag_;
    } else {
        is_null = ::llvm::ConstantInt::getFalse(builder->getContext());
    }
    if (is_null->getType() != bool_ty) {
        is_null = builder->CreateIntCast(is_null, bool_ty, true);
    }
    return is_null;
}

::llvm::Value* NativeValue::GetIsNull(CodeGenContextBase* ctx) const {
    return GetIsNull(ctx->GetBuilder());
}

::llvm::Value* NativeValue::GetValue(::llvm::IRBuilder<>* builder) const {
    if (IsConstNull()) {
        return ::llvm::UndefValue::get(GetType());
    } else if (IsMem()) {
        return builder->CreateLoad(raw_);
    } else {
        return raw_;
    }
}

::llvm::Value* NativeValue::GetValue(CodeGenContextBase* ctx) const {
    return GetValue(ctx->GetBuilder());
}

::llvm::Value* NativeValue::GetAddr(::llvm::IRBuilder<>* builder) const {
    if (IsConstNull()) {
        LOG(WARNING) << "Get addr from const null";
        return nullptr;
    } else if (IsReg()) {
        ::llvm::Value* alloca =
            CreateAllocaAtHead(builder, type_, "addr_alloca");
        builder->CreateStore(raw_, alloca);
        return alloca;
    } else {
        return raw_;
    }
}

void NativeValue::SetType(::llvm::Type* type) { type_ = type; }
::llvm::Type* NativeValue::GetType() const { return type_; }

::llvm::Value* NativeValue::GetRaw() const { return raw_; }

bool NativeValue::IsMem() const {
    return raw_ != nullptr && raw_->getType() == type_->getPointerTo();
}

bool NativeValue::IsReg() const {
    return raw_ != nullptr && raw_->getType() == type_;
}

bool NativeValue::HasFlag() const { return flag_ != nullptr; }

bool NativeValue::IsMemFlag() const {
    return HasFlag() && flag_->getType()->isPointerTy();
}

bool NativeValue::IsRegFlag() const {
    return HasFlag() && !flag_->getType()->isPointerTy();
}

bool NativeValue::IsNullable() const { return IsConstNull() || HasFlag(); }

// NativeValue is null if:
// - raw_ is null
// - type_ is of void type.
bool NativeValue::IsConstNull() const { return raw_ == nullptr || (type_ != nullptr && type_->isVoidTy()); }

void NativeValue::SetName(const std::string& name) {
    if (raw_ == nullptr) {
        LOG(WARNING) << "Can not set name to null";
        return;
    }
    raw_->setName(name);
    if (flag_ != nullptr) {
        flag_->setName(name + "__NullFlag");
    }
}

NativeValue NativeValue::Create(::llvm::Value* raw) {
    return NativeValue(raw, nullptr, raw->getType());
}

NativeValue NativeValue::CreateMem(::llvm::Value* raw) {
    return NativeValue(raw, nullptr,
                       reinterpret_cast<::llvm::PointerType*>(raw->getType())
                           ->getElementType());
}

NativeValue NativeValue::CreateNull(::llvm::Type* ty) {
    return NativeValue(nullptr, nullptr, ty);
}

NativeValue NativeValue::CreateWithFlag(::llvm::Value* raw,
                                        ::llvm::Value* flag) {
    return NativeValue(raw, flag, raw->getType());
}

NativeValue NativeValue::CreateMemWithFlag(::llvm::Value* raw,
                                           ::llvm::Value* flag) {
    return NativeValue(raw, flag,
                       reinterpret_cast<::llvm::PointerType*>(raw->getType())
                           ->getElementType());
}

NativeValue NativeValue::Replace(::llvm::Value* val) const {
    if (HasFlag()) {
        return CreateWithFlag(val, flag_);
    } else {
        return Create(val);
    }
}

NativeValue NativeValue::WithFlag(::llvm::Value* flag) const {
    if (IsTuple()) {
        NativeValue res = *this;
        for (size_t i = 0; i < GetFieldNum(); ++i) {
            res.args_[i] = res.args_[i].WithFlag(flag);
        }
        return res;
    } else {
        return NativeValue(raw_, flag, type_);
    }
}

NativeValue::NativeValue(::llvm::Value* raw, ::llvm::Value* flag,
                         ::llvm::Type* type)
    : raw_(raw), flag_(flag), type_(type) {}

}  // namespace codegen
}  // namespace hybridse
