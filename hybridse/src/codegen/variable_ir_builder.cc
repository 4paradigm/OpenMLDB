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

#include "codegen/variable_ir_builder.h"

#include "codegen/ir_base_builder.h"
#include "codegen/struct_ir_builder.h"
#include "glog/logging.h"

using ::hybridse::common::kCodegenError;

namespace hybridse {
namespace codegen {

hybridse::codegen::VariableIRBuilder::VariableIRBuilder(
    ::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block), sv_(scope_var) {}
hybridse::codegen::VariableIRBuilder::~VariableIRBuilder() {}
bool VariableIRBuilder::StoreStruct(const std::string& name,
                                    const NativeValue& value,
                                    base::Status& status) {
    // store value into memory address
    ::llvm::IRBuilder<> builder(block_);
    // get value addr
    NativeValue addr;
    if (!sv_->FindVar(name, &addr)) {
        addr = NativeValue::Create(CreateAllocaAtHead(
            &builder, value.GetType()->getPointerElementType(),
            "struct_alloca_of_var_" + name));
        sv_->AddVar(name, addr);
    }

    if (addr.GetType() != value.GetType()) {
        status.msg =
            "fail to store value: src and dist value type aren't match";
        status.code = common::kCodegenError;
        return false;
    }

    if (nullptr == addr.GetRaw()) {
        status.msg = "fail to store value: addr is null";
        status.code = common::kCodegenError;
        return false;
    }

    if (!StructTypeIRBuilder::StructCopyFrom(block_, value.GetValue(&builder),
                                             addr.GetValue(&builder))) {
        status.msg = "fail to store struct: copy from struct fail";
        status.code = common::kCodegenError;
        return false;
    }
    return true;
}
bool hybridse::codegen::VariableIRBuilder::StoreValue(
    const std::string& name, const NativeValue& value, bool is_register,
    hybridse::base::Status& status) {
    if (is_register) {
        // store value into register
        NativeValue exist;
        if (sv_->FindVar(name, &exist)) {
            status.code = common::kCodegenError;
            status.msg =
                "fail to store register value: register value already exist";
            return false;
        } else {
            return sv_->AddVar(name, value);
        }
    } else {
        if (TypeIRBuilder::IsStructPtr(value.GetType())) {
            return StoreStruct(name, value, status);
        }
        // store value into memory address
        ::llvm::IRBuilder<> builder(block_);
        // get value addr
        NativeValue addr;
        if (!sv_->FindVar(name, &addr)) {
            addr = NativeValue::CreateMem(CreateAllocaAtHead(
                &builder, value.GetType(), "alloca_of_var_" + name));
            sv_->AddVar(name, addr);
        }

        if (addr.GetType() != value.GetType()) {
            status.msg =
                "fail to store value: src and dist value type aren't match";
            status.code = common::kCodegenError;
            return false;
        }

        if (nullptr == addr.GetRaw()) {
            status.msg = "fail to store value: addr is null";
            status.code = common::kCodegenError;
            return false;
        }

        if (!addr.IsMem()) {
            status.msg =
                "fail to store mutable value: register value exists in scope";
            status.code = common::kCodegenError;
            LOG(WARNING) << status;
            return false;
        }
        // store value on address
        if (nullptr == builder.CreateStore(value.GetValue(&builder),
                                           addr.GetAddr(&builder))) {
            status.msg = "fail to store value";
            status.code = common::kCodegenError;
            return false;
        }
        return true;
    }
}

bool hybridse::codegen::VariableIRBuilder::LoadValue(
    const std::string& name, NativeValue* output, hybridse::base::Status& status) {
    NativeValue value;
    if (!sv_->FindVar(name, &value)) {
        status.msg = "fail to get value " + name + ": value is null";
        status.code = common::kCodegenError;
        return false;
    }
    *output = value;
    return true;
}
bool hybridse::codegen::VariableIRBuilder::StoreValue(
    const std::string& name, const NativeValue& value,
    hybridse::base::Status& status) {
    return StoreValue(name, value, true, status);
}

bool VariableIRBuilder::StoreRetStruct(const NativeValue& value,
                                       base::Status& status) {
    return StoreValue("@ret_struct", value, status);
}
bool VariableIRBuilder::LoadRetStruct(NativeValue* output,
                                      base::Status& status) {
    return LoadValue("@ret_struct", output, status);
}
bool VariableIRBuilder::LoadRowKey(NativeValue* output, base::Status& status) {
    return LoadValue("@row_key", output, status);
}
bool VariableIRBuilder::LoadParameter(NativeValue* output, base::Status& status) {
    return LoadValue("@parameter", output, status);
}
base::Status VariableIRBuilder::LoadMemoryPool(NativeValue* output) {
    base::Status status;
    LoadValue("@mem_pool", output, status);
    CHECK_STATUS(status, "fail to load memory pool")
    return status;
}
bool hybridse::codegen::VariableIRBuilder::LoadWindow(
    const std::string& frame_str, NativeValue* output,
    hybridse::base::Status& status) {
    bool ok =
        LoadValue("@window" + (frame_str.empty() ? "" : ("." + frame_str)),
                  output, status);
    return ok;
}
bool hybridse::codegen::VariableIRBuilder::LoadColumnRef(
    const std::string& relation_name, const std::string& name,
    const std::string& frame_str, ::llvm::Value** output,
    hybridse::base::Status& status) {
    NativeValue col_ref;
    bool ok = LoadValue(absl::StrCat("@col.", relation_name, ".", name, (frame_str.empty() ? "" : ("." + frame_str))),
                        &col_ref, status);
    *output = col_ref.GetRaw();
    return ok;
}
bool hybridse::codegen::VariableIRBuilder::LoadColumnItem(
    const std::string& relation_name, const std::string& name,
    NativeValue* output, hybridse::base::Status& status) {
    return LoadValue("@item." + relation_name + "." + name, output, status);
}
bool hybridse::codegen::VariableIRBuilder::StoreWindow(
    const std::string& frame_str, ::llvm::Value* value,
    hybridse::base::Status& status) {
    return StoreValue("@window" + (frame_str.empty() ? "" : ("." + frame_str)),
                      NativeValue::Create(value), status);
}
bool hybridse::codegen::VariableIRBuilder::StoreColumnRef(
    const std::string& relation_name, const std::string& name,
    const std::string& frame_str, ::llvm::Value* value,
    hybridse::base::Status& status) {
    return StoreValue("@col." + relation_name + "." + name +
                          (frame_str.empty() ? "" : ("." + frame_str)),
                      NativeValue::Create(value), status);
}
bool hybridse::codegen::VariableIRBuilder::StoreColumnItem(
    const std::string& relation_name, const std::string& name,
    const NativeValue& value, hybridse::base::Status& status) {
    ::llvm::IRBuilder<> builder(block_);
    return StoreValue("@item." + relation_name + "." + name, value, status);
}
bool hybridse::codegen::VariableIRBuilder::LoadArrayIndex(
    std::string array_ptr_name, int32_t index, ::llvm::Value** output,
    base::Status& status) {
    std::string array_index_name = array_ptr_name;
    array_index_name.append("[").append(std::to_string(index)).append("]");
    ::llvm::IRBuilder<> builder(block_);

    NativeValue output_wrapper;
    if (LoadValue(array_index_name, &output_wrapper, status)) {
        *output = output_wrapper.GetValue(&builder);
        return true;
    }

    NativeValue array_ptr_wrapper;
    if (!LoadValue(array_ptr_name, &array_ptr_wrapper, status)) {
        status.msg = "fail load array ptr" + array_ptr_name;
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }
    ::llvm::Value* array_ptr = array_ptr_wrapper.GetValue(&builder);
    ::llvm::Value* ptr = builder.CreateInBoundsGEP(
        array_ptr, ::llvm::ArrayRef<::llvm::Value*>(builder.getInt64(index)));

    ::llvm::Value* value = builder.CreateLoad(ptr);
    if (nullptr == value) {
        status.msg =
            "fail load " + array_ptr_name + "[" + std::to_string(index) + "]";
        status.code = common::kCodegenError;
        LOG(WARNING) << status;
        return false;
    }

    if (!StoreValue(array_index_name, NativeValue::Create(value), status)) {
        LOG(WARNING) << "fail to cache " << array_index_name;
    }
    *output = value;
    status.msg = "ok";
    status.code = common::kOk;
    return true;
}
}  // namespace codegen
}  // namespace hybridse
