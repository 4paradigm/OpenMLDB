/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * struct_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/5/26
 *--------------------------------------------------------------------------
 **/
#include "codegen/struct_ir_builder.h"
#include "codegen/ir_base_builder.h"
namespace fesql {
namespace codegen {
StructTypeIRBuilder::StructTypeIRBuilder(::llvm::Module* m)
    : TypeIRBuilder(), m_(m), struct_type_(nullptr) {
}
StructTypeIRBuilder::~StructTypeIRBuilder() {}

::llvm::Type* StructTypeIRBuilder::GetType() {
    return struct_type_;
}
bool StructTypeIRBuilder::Create(::llvm::BasicBlock* block,
                                 ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* value = builder.CreateAlloca(struct_type_);
    *output = value;
    return true;
}
bool StructTypeIRBuilder::Get(::llvm::BasicBlock* block,
                              ::llvm::Value* struct_value, unsigned int idx,
                              ::llvm::Value** output) {
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* value_ptr =
        builder.CreateStructGEP(struct_type_, struct_value, idx);
    *output = builder.CreateLoad(value_ptr);
    return true;
}
bool StructTypeIRBuilder::Set(::llvm::BasicBlock* block,
                              ::llvm::Value* struct_value, unsigned int idx,
                              ::llvm::Value* value) {
    if (block == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
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
}  // namespace codegen
}  // namespace fesql
