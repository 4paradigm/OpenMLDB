/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * string_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/5/26
 *--------------------------------------------------------------------------
 **/
#include "codegen/string_ir_builder.h"
#include <string>
#include <vector>
namespace fesql {
namespace codegen {
StringIRBuilder::StringIRBuilder(::llvm::Module* m) : StructTypeIRBuilder(m) {
    InitStructType();
}
StringIRBuilder::~StringIRBuilder() {}
void StringIRBuilder::InitStructType() {
    std::string name = "fe.string_ref";
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);
    ::llvm::Type* size_ty = (::llvm::Type::getInt32Ty(m_->getContext()));
    ::llvm::Type* data_ptr_ty = (::llvm::Type::getInt8PtrTy(m_->getContext()));
    std::vector<::llvm::Type*> elements;
    elements.push_back(size_ty);
    elements.push_back(data_ptr_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    struct_type_ = stype;
}
bool StringIRBuilder::NewString(::llvm::BasicBlock* block,
                                const std::string& val,
                                ::llvm::Value** output) {
    ::llvm::IRBuilder<> builder(block);
    ::llvm::StringRef val_ref(val);
    ::llvm::Value* str_val = builder.CreateGlobalStringPtr(val_ref);
    ::llvm::Value* size = builder.getInt32(val.size());
    return NewString(block, size, str_val, output);
}
bool StringIRBuilder::NewString(::llvm::BasicBlock* block,
                                ::llvm::Value** output) {
    if (!Create(block, output)) {
        LOG(WARNING) << "Fail to Create Default String";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* str_val = builder.CreateGlobalStringPtr("");
    if (!SetData(block, *output, str_val)) {
        LOG(WARNING) << "Fail to Init String Data";
        return false;
    }

    if (!SetSize(block, *output, builder.getInt32(0))) {
        LOG(WARNING) << "Fail to Init String Size";
        return false;
    }
    return true;
}
bool StringIRBuilder::NewString(::llvm::BasicBlock* block, ::llvm::Value* size,
                                ::llvm::Value* data, ::llvm::Value** output) {
    if (!Create(block, output)) {
        LOG(WARNING) << "Fail to Create Default String";
        return false;
    }
    if (!SetData(block, *output, data)) {
        LOG(WARNING) << "Fail to Init String Data";
        return false;
    }
    if (!SetSize(block, *output, size)) {
        LOG(WARNING) << "Fail to Init String Size";
        return false;
    }
    return true;
}
bool StringIRBuilder::GetSize(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value** output) {
    return Get(block, str, 0, output);
}
bool StringIRBuilder::CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                               ::llvm::Value* dist) {
    if (nullptr == src || nullptr == dist) {
        LOG(WARNING) << "Fail to copy string: src or dist is null";
        return false;
    }
    if (!IsStringPtr(src->getType()) || !IsStringPtr(dist->getType())) {
        LOG(WARNING) << "Fail to copy string: src or dist isn't String Ptr";
        return false;
    }
    ::llvm::Value* size;
    ::llvm::Value* data;
    if (!GetSize(block, src, &size)) {
        return false;
    }

    if (!GetData(block, src, &data)) {
        return false;
    }

    if (!SetSize(block, dist, size)) {
        return false;
    }

    if (!SetData(block, dist, data)) {
        return false;
    }

    return true;
}
bool StringIRBuilder::SetSize(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value* size) {
    return Set(block, str, 0, size);
}
bool StringIRBuilder::GetData(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value** output) {
    return Get(block, str, 1, output);
}
bool StringIRBuilder::SetData(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value* data) {
    return Set(block, str, 1, data);
}
}  // namespace codegen
}  // namespace fesql
