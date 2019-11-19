/*
 * buf_ir_builder.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/buf_ir_builder.h"

#include <string>
#include <utility>
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

BufIRBuilder::BufIRBuilder(::fesql::type::TableDef* table,
        ::llvm::BasicBlock* block,
        ScopeVar* scope_var):table_(table), block_(block), sv_(scope_var),
    types_() {
    // two byte header
    int32_t offset = 2;
    for (int32_t i = 0; i < table_->columns_size(); i++) {
        const ::fesql::type::ColumnDef& column = table_->columns(i);
        types_.insert(std::make_pair(column.name(),
                    std::make_pair(column.type(), offset)));
        switch (column.type()) {
            case ::fesql::type::kInt16:
            case ::fesql::type::kVarchar:
                {
                    offset += 2;
                    break;
                }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat:
                {
                    offset += 4;
                    break;
                }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
                {
                    offset += 8;
                    break;
                }
            default:
                {
                    LOG(WARNING) << "not support type "
                        << ::fesql::type::Type_Name(column.type());
                }
        }
    }
}

BufIRBuilder::~BufIRBuilder() {}

bool BufIRBuilder::BuildGetString(const std::string& name,
                                 ::llvm::Value* row_ptr, 
                                 ::llvm::Value* row_size,
                                 ::llvm::Value** output) {

    ::llvm::Value* field_offset = NULL;
    bool ok = GetFieldOffset(name, row_ptr, row_size, &field_offset);

    if (!ok) {
        LOG(WARNING) << "fail to get field " << name << " offset from table " << table_->name();
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* int16_ty = builder.getInt16Ty();
    ::llvm::Value* start_offset = NULL;

    ok = BuildLoadOffset(builder, row_ptr, field_offset, 
                         int16_ty, &start_offset);

    if (!ok) {
        LOG(WARNING) << "fail to get string filed " << name 
            << " start offset from table "  << table_->name();
        return false;
    }

    ::llvm::Value* next_str_field_offset = NULL;
    ok = GetNextOffset(name, row_ptr, row_size, &next_str_field_offset);
    ::llvm::StructType* type = ::llvm::StructType::create(block_->getContext(), "fe.string_ref");
    ::llvm::Type* size_ty = builder.getInt32Ty();
    ::llvm::Type* data_ptr_ty = builder.getInt8PtrTy();
    std::vector<::llvm::Type*> elements;
    elements.push_back(size_ty);
    elements.push_back(data_ptr_ty);
    type->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    ::llvm::Value* string_ref = builder.CreateAlloca(type);
    ::llvm::Value* data_ptr = NULL;
    ::llvm::Value* size = NULL;
    // no next str field
    if (!ok) {
        size = builder.CreateSub(row_size, start_offset, "str_sub");
    }else {
        ::llvm::Value* end_offset = NULL;
        ok = BuildLoadOffset(builder, row_ptr, next_str_field_offset, 
                         int16_ty, &end_offset);
        if (!ok) {
            LOG(WARNING) << "fail to load end offset for field " << name;
            return false;
        }
        size = builder.CreateSub(end_offset, start_offset, "str_sub");
    }

    ok = BuildGetPtrOffset(builder, row_ptr, start_offset, data_ptr_ty, data_ptr);
    if (!ok) {
        LOG(WARNING) << "fail to get string data ptr for field " << name;
        return false;
    }

    ::llvm::Value* data_ptr_ptr = builder.CreateGEP(string_ref, builder.getInt32(1));
    builder.CreateStore(data_ptr_ptr, data_ptr, false);
    ::llvm::Value* size_ptr = builder.CreateGEP(string_ref, builder.getInt32(0));
    builder.CreateStore(size_ptr, size, false);
    *output = string_ref;
}

bool BufIRBuilder::GetNextOffset(const std::string& name,
        ::llvm::Value* row_ptr,
        ::llvm::Value* row_size,
        ::llvm::Value** output) {

    std::string last;
    for (int32_t i = 0; i < table_->columns_size(); i++) { 
        const ::fesql::type::ColumnDef& column = table_->columns(i);
        if (i == 0) {
            last = column.name();
            continue;
        }

        if (last.compare(name) == 0) {
            return GetFieldOffset(column.name(), row_ptr, row_size);
        }
        last = column.name();
    }
    return false;
}

bool BufIRBuilder::GetFieldOffset(const std::string& name,
        ::llvm::Value* row_ptr,
        ::llvm::Value* row_size,
        ::llvm::Value** output) {

    Types::iterator it = types_.find(name);
    if (it == types_.end()) {
        LOG(WARNING) << "no column " << name << " in table " << table_->name();
        return false;
    }

    DLOG(INFO) << "find column " << name << " with field offset "  << it->second.second 
        << " in table " << table_->name();
    ::llvm::ConstantInt* llvm_offset = builder.getInt32(it->second.second);
    *output = llvm_offset;
    return true;
}

bool BufIRBuilder::BuildGetField(const std::string& name,
     ::llvm::Value* row_ptr,
     ::llvm::Value** output) {

    if (output == NULL) {
        LOG(WARNING) << "output is null";
        return false;
    }

    Types::iterator it = types_.find(name);
    if (it == types_.end()) {
        LOG(WARNING) << "no column " << name << " in table " << table_->name();
        return false;
    }

    ::fesql::type::Type& fe_type =  it->second.first;
    int32_t offset = it->second.second;
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* llvm_type = NULL;

    bool ok = GetLLVMType(builder, fe_type, &llvm_type);
    if (!ok) {
        LOG(WARNING) << "fail to convert fe type to llvm type ";
        return false;
    }

    ::llvm::ConstantInt* llvm_offse = builder.getInt32(offset);
    return BuildLoadOffset(builder, row_ptr, llvm_offse, llvm_type, output);
}


}  // namespace codegen
}  // namespace fesql



