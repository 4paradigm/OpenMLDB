/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mutable_value_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/2/11
 *--------------------------------------------------------------------------
 **/
#include "codegen/variable_ir_builder.h"
#include <glog/logging.h>

fesql::codegen::VariableIRBuilder::VariableIRBuilder(::llvm::BasicBlock* block,
                                                     ScopeVar* scope_var)
    : block_(block), sv_(scope_var) {}
fesql::codegen::VariableIRBuilder::~VariableIRBuilder() {}

bool fesql::codegen::VariableIRBuilder::StoreValue(
    const std::string& name, ::llvm::Value* value, bool is_register,
    fesql::base::Status& status) {
    if (nullptr == value) {
        status.msg = "value is null";
        status.code = common::kCodegenError;
        return false;
    }

    if (is_register) {
        // store value into register
        bool is_reg;
        if (sv_->FindVar(name, &value, &is_reg)) {
            status.code = common::kCodegenError;
            status.msg =
                "fail to store register value: register value already exist";
            return false;
        } else {
            return sv_->AddVar(name, value, is_register);
        }
    } else {
        // store value into memory address
        ::llvm::IRBuilder<> builder(block_);
        // get value addr
        ::llvm::Value* addr = nullptr;
        bool is_reg = false;
        if (!sv_->FindVar(name, &addr, &is_reg)) {
            addr = builder.CreateAlloca(value->getType());
            sv_->AddVar(name, addr, false);
        }

        if (nullptr == addr) {
            status.msg = "fail to store value: addr is null";
            status.code = common::kCodegenError;
            return false;
        }

        if (is_reg) {
            status.msg =
                "fail to store mutable value: register value exists in scope";
            status.code = common::kCodegenError;
            LOG(WARNING) << status.msg;
            return false;
        }
        // store value on address
        if (nullptr == builder.CreateStore(value, addr)) {
            status.msg = "fail to store value";
            status.code = common::kCodegenError;
            return false;
        }
        return true;
    }
}

bool fesql::codegen::VariableIRBuilder::LoadValue(std::string name,
                                                  ::llvm::Value** output,
                                                  fesql::base::Status& status) {
    ::llvm::Value* value = nullptr;
    bool is_register;
    if (!sv_->FindVar(name, &value, &is_register)) {
        return false;
    }

    if (nullptr == value) {
        status.msg = "fail to get value: value is null";
        status.code = common::kCodegenError;
        return false;
    }

    if (is_register) {
        // load value directly from register
        *output = value;
        return true;
    } else {
        ::llvm::IRBuilder<> builder(block_);
        // load value from address
        *output = builder.CreateLoad(value);
        if (nullptr == *output) {
            status.msg = "fail to load mutable value";
            status.code = common::kCodegenError;
            return false;
        }
        return true;
    }
}
bool fesql::codegen::VariableIRBuilder::StoreValue(
    const std::string& name, ::llvm::Value* value,
    fesql::base::Status& status) {
    return StoreValue(name, value, true, status);
}
bool fesql::codegen::VariableIRBuilder::LoadColumnRef(
    const std::string& relation_name, const std::string& name,
    ::llvm::Value** output, fesql::base::Status& status) {
    return LoadValue("col." + relation_name + "." + name, output, status);
}
bool fesql::codegen::VariableIRBuilder::LoadColumnItem(
    const std::string& relation_name, const std::string& name,
    ::llvm::Value** output, fesql::base::Status& status) {
    return LoadValue("item." + relation_name + "." + name, output, status);
}
bool fesql::codegen::VariableIRBuilder::StoreColumnRef(
    const std::string& relation_name, const std::string& name,
    ::llvm::Value* value, fesql::base::Status& status) {
    return StoreValue("col." + relation_name + "." + name, value, status);
}
bool fesql::codegen::VariableIRBuilder::StoreColumnItem(
    const std::string& relation_name, const std::string& name,
    ::llvm::Value* value, fesql::base::Status& status) {
    return StoreValue("item." + relation_name + "." + name, value, status);
}
bool fesql::codegen::VariableIRBuilder::LoadArrayIndex(
    std::string array_ptr_name, int32_t index, ::llvm::Value** output,
    base::Status& status) {
    std::string array_index_name = array_ptr_name;
    array_index_name.append("[").append(std::to_string(index)).append("]");
    if (LoadValue(array_index_name, output, status)) {
        return true;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* array_ptr;
    if (!LoadValue(array_ptr_name, &array_ptr, status)) {
        status.msg = "fail load array ptr" + array_ptr_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::Value* ptr = builder.CreateInBoundsGEP(
        array_ptr, ::llvm::ArrayRef<::llvm::Value*>(builder.getInt64(index)));

    ::llvm::Value* value = builder.CreateLoad(ptr);
    if (nullptr == value) {
        status.msg =
            "fail load " + array_ptr_name + "[" + std::to_string(index) + "]";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (!StoreValue(array_index_name, value, status)) {
        LOG(WARNING) << "fail to cache " << array_index_name;
    }
    *output = value;
    return true;
}
