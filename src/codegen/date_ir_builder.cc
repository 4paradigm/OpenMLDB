/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * date_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/6/1
 *--------------------------------------------------------------------------
 **/

#include "codegen/date_ir_builder.h"
#include <string>
#include <vector>
namespace fesql {
namespace codegen {
DateIRBuilder::DateIRBuilder(::llvm::Module* m) : StructTypeIRBuilder(m) {
    InitStructType();
}
DateIRBuilder::~DateIRBuilder() {}
void DateIRBuilder::InitStructType() {
    std::string name = "fe.date";
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);
    ::llvm::Type* ts_ty = (::llvm::Type::getInt32Ty(m_->getContext()));
    std::vector<::llvm::Type*> elements;
    elements.push_back(ts_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    struct_type_ = stype;
    return;
}
bool DateIRBuilder::NewDate(::llvm::BasicBlock* block, ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* date;
    if (!Create(block, &date)) {
        return false;
    }
    if (!SetDays(block, date,
                 ::llvm::ConstantInt::get(
                     ::llvm::Type::getInt32Ty(m_->getContext()), 0, false))) {
        return false;
    }
    *output = date;
    return true;
}
bool DateIRBuilder::NewDate(::llvm::BasicBlock* block, ::llvm::Value* days,
                            ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* date;
    if (!Create(block, &date)) {
        return false;
    }
    if (!SetDays(block, date, days)) {
        return false;
    }
    *output = date;
    return true;
}
bool DateIRBuilder::CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
              ::llvm::Value* dist) {
    if (nullptr == src || nullptr == dist) {
        LOG(WARNING) << "Fail to copy string: src or dist is null";
        return false;
    }
    if (!IsDatePtr(src->getType()) || !IsDatePtr(dist->getType())) {
        LOG(WARNING) << "Fail to copy string: src or dist isn't Date Ptr";
        return false;
    }
    ::llvm::Value* days;
    if (!GetDays(block, src, &days)) {
        return false;
    }
    if (!SetDays(block, dist, days)) {
        return false;
    }
    return true;
}
bool DateIRBuilder::GetDays(::llvm::BasicBlock* block, ::llvm::Value* date,
                            ::llvm::Value** output) {
    return Get(block, date, 0, output);
}
bool DateIRBuilder::SetDays(::llvm::BasicBlock* block, ::llvm::Value* date,
                            ::llvm::Value* days) {
    return Set(block, date, 0, days);
}
}  // namespace codegen
}  // namespace fesql
