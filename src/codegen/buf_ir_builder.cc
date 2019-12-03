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
#include <vector>
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

#define BitMapSize(size) (((size) >> 3) + !!((size)&0x07))

static const uint8_t VERSION_LENGTH = 2;
static const uint8_t SIZE_LENGTH = 4;
static const uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
static const std::map<::fesql::type::Type, uint8_t> TYPE_SIZE_MAP = {
    {::fesql::type::kBool, sizeof(bool)},
    {::fesql::type::kInt16, sizeof(int16_t)},
    {::fesql::type::kInt32, sizeof(int32_t)},
    {::fesql::type::kFloat, sizeof(float)},
    {::fesql::type::kInt64, sizeof(int64_t)},
    {::fesql::type::kDouble, sizeof(double)}};

static uint8_t GetAddrLength(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= 1 << 24) {
        return 3;
    } else {
        return 4;
    }
}

BufIRBuilder::BufIRBuilder(::fesql::type::TableDef* table,
                           ::llvm::BasicBlock* block, ScopeVar* scope_var)
    : table_(table), block_(block), sv_(scope_var), types_() {
    // two byte header
    int32_t offset = 2;
    for (int32_t i = 0; i < table_->columns_size(); i++) {
        const ::fesql::type::ColumnDef& column = table_->columns(i);
        types_.insert(std::make_pair(column.name(),
                                     std::make_pair(column.type(), offset)));
        DLOG(INFO) << "add column " << column.name() << " with type "
                   << ::fesql::type::Type_Name(column.type()) << " offset "
                   << offset;
        switch (column.type()) {
            case ::fesql::type::kInt16:
            case ::fesql::type::kVarchar: {
                offset += 2;
                break;
            }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat: {
                offset += 4;
                break;
            }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble: {
                offset += 8;
                break;
            }
            default: {
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
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* field_offset = NULL;
    bool ok = GetFieldOffset(name, row_ptr, row_size, &field_offset);

    if (!ok) {
        LOG(WARNING) << "fail to get field " << name << " offset from table "
                     << table_->name();
        return false;
    }

    ::llvm::Type* int16_ty = builder.getInt16Ty();
    ::llvm::Value* start_offset = NULL;

    ok = BuildLoadOffset(builder, row_ptr, field_offset, int16_ty,
                         &start_offset);
    if (!ok) {
        LOG(WARNING) << "fail to get string filed " << name
                     << " start offset from table " << table_->name();
        return false;
    }
    ::llvm::Value* next_str_field_offset = NULL;
    ok = GetNextOffset(name, row_ptr, row_size, &next_str_field_offset);

    ::llvm::StructType* type =
        ::llvm::StructType::create(block_->getContext(), "fe.string_ref");
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
        ::llvm::Value* start_offset_i32 =
            builder.CreateIntCast(start_offset, size_ty, true, "cast_16_to_32");
        size = builder.CreateSub(row_size, start_offset_i32, "str_sub");
    } else {
        ::llvm::Value* end_offset = NULL;
        ok = BuildLoadOffset(builder, row_ptr, next_str_field_offset, int16_ty,
                             &end_offset);

        if (!ok) {
            LOG(WARNING) << "fail to load end offset for field " << name;
            return false;
        }
        size = builder.CreateSub(end_offset, start_offset, "str_sub");
    }

    ok = BuildGetPtrOffset(builder, row_ptr, start_offset, data_ptr_ty,
                           &data_ptr);

    if (!ok) {
        LOG(WARNING) << "fail to get string data ptr for field " << name;
        return false;
    }

    ::llvm::Value* data_ptr_ptr = builder.CreateStructGEP(type, string_ref, 1);
    ::llvm::Value* cast_data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, data_ptr->getType()->getPointerTo());
    builder.CreateStore(data_ptr, cast_data_ptr_ptr, false);

    ::llvm::Value* size_ptr = builder.CreateStructGEP(type, string_ref, 0);
    ::llvm::Value* cast_type_size_ptr =
        builder.CreatePointerCast(size_ptr, size->getType()->getPointerTo());
    builder.CreateStore(size, cast_type_size_ptr, false);
    *output = string_ref;
    return true;
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
            return GetFieldOffset(column.name(), row_ptr, row_size, output);
        }
        last = column.name();
    }
    DLOG(INFO) << "no next string field offset for " << name;
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

    DLOG(INFO) << "find column " << name << " with field offset "
               << it->second.second << " in table " << table_->name();
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::ConstantInt* llvm_offset = builder.getInt32(it->second.second);
    *output = llvm_offset;
    return true;
}

bool BufIRBuilder::BuildGetField(const std::string& name,
                                 ::llvm::Value* row_ptr,
                                 ::llvm::Value* row_size,
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

    ::fesql::type::Type& fe_type = it->second.first;
    if (fe_type == ::fesql::type::kVarchar) {
        return BuildGetString(name, row_ptr, row_size, output);
    }

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

BufNativeIRBuilder::BufNativeIRBuilder(::fesql::type::TableDef* table,
                                       ::llvm::BasicBlock* block,
                                       ScopeVar* scope_var)
    : table_(table), block_(block), sv_(scope_var), types_() {
    uint32_t offset = HEADER_LENGTH + BitMapSize(table_->columns_size());
    uint32_t string_field_cnt = 0;
    for (int32_t i = 0; i < table_->columns_size(); i++) {
        const ::fesql::type::ColumnDef& column = table_->columns(i);
        if (column.type() == ::fesql::type::kVarchar) {
            types_.insert(std::make_pair(
                column.name(),
                std::make_pair(column.type(), string_field_cnt)));
            next_str_pos_.insert(
                std::make_pair(string_field_cnt, string_field_cnt));
            string_field_cnt += 1;
        } else {
            auto it = TYPE_SIZE_MAP.find(column.type());
            if (it == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << "fail to find column type "
                             << ::fesql::type::Type_Name(column.type());
            } else {
                types_.insert(std::make_pair(
                    column.name(), std::make_pair(column.type(), offset)));
                offset += it->second;
            }
        }
    }
    uint32_t next_pos = 0;
    for (auto iter = next_str_pos_.rbegin(); iter != next_str_pos_.rend();
         iter++) {
        uint32_t tmp = iter->second;
        iter->second = next_pos;
        next_pos = tmp;
    }
    str_field_start_offset_ = offset;
}

BufNativeIRBuilder::~BufNativeIRBuilder() {}

bool BufNativeIRBuilder::BuildGetField(const std::string& name,
                                       ::llvm::Value* row_ptr,
                                       ::llvm::Value* row_size,
                                       ::llvm::Value** output) {

    if (row_ptr == NULL || row_size == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }

    Types::iterator it = types_.find(name);
    if (it == types_.end()) {
        LOG(WARNING) << "no column " << name << " in table " << table_->name();
        return false;
    }
    // TODO(wangtaize) support null check
    ::fesql::type::Type& fe_type = it->second.first;
    ::llvm::IRBuilder<> builder(block_);
    uint32_t offset = it->second.second;
    switch (fe_type) {
        case ::fesql::type::kInt16: {
            llvm::Type* i16_ty = builder.getInt16Ty();
            return BuildGetPrimaryField("fesql_storage_get_int16_field", row,
                                        offset, i16_ty, output);
        }
        case ::fesql::type::kInt32: {
            llvm::Type* i32_ty = builder.getInt32Ty();
            return BuildGetPrimaryField("fesql_storage_get_int32_field", row,
                                        offset, i32_ty, output);
        }
        case ::fesql::type::kInt64: {
            llvm::Type* i64_ty = builder.getInt64Ty();
            return BuildGetPrimaryField("fesql_storage_get_int64_field", row,
                                        offset, i64_ty, output);
        }
        case ::fesql::type::kFloat: {
            llvm::Type* float_ty = builder.getFloatTy();
            return BuildGetPrimaryField("fesql_storage_get_float_field", row,
                                        offset, float_ty, output);
        }
        case ::fesql::type::kDouble: {
            llvm::Type* double_ty = builder.getDoubleTy();
            return BuildGetPrimaryField("fesql_storage_get_double_field", row,
                                        offset, double_ty, output);
        }
        default: {
            return false;
        }
    }
}

bool BufNativeIRBuilder::BuildGetPrimaryField(const std::string& fn_name,
                                              ::llvm::Value* row_ptr,
                                              uint32_t offset,
                                              ::llvm::Type* type,
                                              ::llvm::Value** output) {
    if (row_ptr == NULL || type == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Value* offset = builder.getInt32(offset);
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        fn_name, type, i8_ptr_ty, i32_ty);
    std::vector<Value*> call_args;
    call_args.push_back(row_ptr);
    call_args.push_back(offset);
    ::llvm::ArrayRef<Value*> call_args_ref(call_args);
    *output = builder.CreateCall(callee, call_args_ref);
    return true;
}

}  // namespace codegen
}  // namespace fesql
