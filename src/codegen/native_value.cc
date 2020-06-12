/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * native_value.cc
 *
 * Author: chenjing
 * Date: 2020/2/12
 *--------------------------------------------------------------------------
 **/
#include "codegen/native_value.h"

namespace fesql {
namespace codegen {

::llvm::Value* NativeValue::GetFlag(::llvm::IRBuilder<>* builder) const {
    auto bool_ty = ::llvm::Type::getInt1Ty(builder->getContext());
    ::llvm::Value* is_null;
    if (IsConstNull()) {
        is_null = ::llvm::ConstantInt::getTrue(builder->getContext());
    } else if (IsMemFlag()) {
        is_null = builder->CreateLoad(flag_);
    } else if (IsRegFlag()) {
        is_null = flag_;
    } else {
        LOG(WARNING) << "Get null flag from value without flag";
        is_null = ::llvm::ConstantInt::getFalse(builder->getContext());
    }
    if (is_null->getType() != bool_ty) {
        is_null = builder->CreateIntCast(is_null, bool_ty, true);
    }
    return is_null;
}

::llvm::Value* NativeValue::GetValue(::llvm::IRBuilder<>* builder) const {
    if (IsConstNull()) {
        LOG(WARNING) << "Get value from const null";
        return nullptr;
    } else if (IsMem()) {
        return builder->CreateLoad(raw_);
    } else {
        return raw_;
    }
}

::llvm::Value* NativeValue::GetAddr(::llvm::IRBuilder<>* builder) const {
    if (IsConstNull()) {
        LOG(WARNING) << "Get addr from const null";
        return nullptr;
    } else if (IsReg()) {
        ::llvm::Value* alloca = builder->CreateAlloca(type_);
        builder->CreateStore(raw_, alloca);
        return alloca;
    } else {
        return raw_;
    }
}

::llvm::Type* NativeValue::GetType() const {
    return type_;
}

::llvm::Value* NativeValue::GetRaw() const {
    return raw_;
}

bool NativeValue::IsMem() const {
    return raw_ != nullptr &&
        raw_->getType() == type_->getPointerTo();
}

bool NativeValue::IsReg() const {
    return raw_ != nullptr &&
        raw_->getType() == type_;
}

bool NativeValue::HasFlag() const {
    return flag_ != nullptr;
}

bool NativeValue::IsMemFlag() const {
    return HasFlag() &&
        flag_->getType()->isPointerTy();
}

bool NativeValue::IsRegFlag() const {
    return HasFlag() &&
        !flag_->getType()->isPointerTy();
}

bool NativeValue::IsConstNull() const {
    return raw_ == nullptr;
}

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

NativeValue NativeValue::CreateNull(::llvm::Type* ty) {
    return NativeValue(nullptr, nullptr, ty);
}

NativeValue NativeValue::CreateWithFlag(
    ::llvm::Value* raw, ::llvm::Value* flag) {
    return NativeValue(raw, flag, raw->getType());
}

NativeValue NativeValue::CreateMemWithFlag(
    ::llvm::Value* raw, ::llvm::Value* flag) {
    return NativeValue(raw, flag,
        reinterpret_cast<::llvm::PointerType*>(
            raw->getType())->getElementType());
}

NativeValue NativeValue::Replace(::llvm::Value* val) const {
    if (HasFlag()) {
        return CreateWithFlag(val, flag_);
    } else {
        return Create(val);
    }
}

NativeValue::NativeValue(::llvm::Value* raw,
                         ::llvm::Value* flag,
                         ::llvm::Type* type):
    raw_(raw), flag_(flag), type_(type) {}


}  // namespace codegen
}  // namespace fesql
