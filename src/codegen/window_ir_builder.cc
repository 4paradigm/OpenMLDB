/*
 * window_ir_builder.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#include "codegen/window_ir_builder.h"
#include <string>
#include <utility>
#include <vector>
#include "codec/fe_row_codec.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {
MemoryWindowDecodeIRBuilder::MemoryWindowDecodeIRBuilder(
    const vm::Schema& schema, ::llvm::BasicBlock* block)
    : block_(block), decoder_list_({codec::RowDecoder(schema)}) {}

MemoryWindowDecodeIRBuilder::MemoryWindowDecodeIRBuilder(
    const std::vector<vm::RowSchemaInfo>& schema_list,
    ::llvm::BasicBlock* block)
    : block_(block), decoder_list_() {
    for (auto info : schema_list) {
        decoder_list_.push_back(codec::RowDecoder(*info.schema_));
    }
}

MemoryWindowDecodeIRBuilder::~MemoryWindowDecodeIRBuilder() {}
bool MemoryWindowDecodeIRBuilder::BuildGetCol(const std::string& name,
                                              ::llvm::Value* window_ptr,
                                              ::llvm::Value** output) {
    return BuildGetCol(name, window_ptr, 0, output);
}
bool MemoryWindowDecodeIRBuilder::BuildGetCol(const std::string& name,
                                              ::llvm::Value* window_ptr,
                                              uint32_t row_idx,
                                              ::llvm::Value** output) {
    if (window_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }
    ::fesql::node::DataType data_type;
    uint32_t offset;
    if (!GetColOffsetType(name, row_idx, &offset, &data_type)) {
        LOG(WARNING) << "fail to get filed offset " << name;
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    switch (data_type) {
        case ::fesql::node::kInt16:
        case ::fesql::node::kInt32:
        case ::fesql::node::kInt64:
        case ::fesql::node::kFloat:
        case ::fesql::node::kDouble:
        case ::fesql::node::kTimestamp: {
            return BuildGetPrimaryCol("fesql_storage_get_col", window_ptr,
                                      row_idx, offset, data_type,
                                      output);
        }
        case ::fesql::node::kVarchar: {
            uint32_t next_offset = 0;
            uint32_t str_start_offset = 0;
            if (!decoder_list_[row_idx].GetStringFieldOffset(
                    name, &offset, &next_offset, &str_start_offset)) {
                LOG(WARNING)
                    << "fail to get string filed offset and next offset"
                    << name;
            }
            DLOG(INFO) << "get string with offset " << offset << " next offset "
                       << next_offset << " for col " << name;
            return BuildGetStringCol(row_idx, offset, next_offset,
                                     str_start_offset, data_type, window_ptr,
                                     output);
        }
        default: {
            return false;
        }
    }
}  // namespace codegen

bool MemoryWindowDecodeIRBuilder::BuildGetPrimaryCol(
    const std::string& fn_name, ::llvm::Value* row_ptr, uint32_t row_idx,
    uint32_t offset, const fesql::node::DataType& type,
    ::llvm::Value** output) {
    if (row_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i32_ty = builder.getInt32Ty();

    ::llvm::Type* list_ref_type = NULL;
    bool ok = GetLLVMListType(block_->getModule(), type, &list_ref_type);
    if (!ok) {
        LOG(WARNING) << "fail to get list type";
        return false;
    }
    uint32_t col_iterator_size = 0;
    ok = GetLLVMColumnSize(type, &col_iterator_size);
    if (!ok || col_iterator_size == 0) {
        LOG(WARNING) << "fail to get col iterator size";
    }
    // alloca memory on stack for col iterator
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, col_iterator_size);
    ::llvm::Value* col_iter = builder.CreateAlloca(array_type);
    // alloca memory on stack
    ::llvm::Value* list_ref = builder.CreateAlloca(list_ref_type);
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(list_ref_type, list_ref, 0);
    data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, col_iter->getType()->getPointerTo());
    builder.CreateStore(col_iter, data_ptr_ptr, false);
    col_iter = builder.CreatePointerCast(col_iter, i8_ptr_ty);

    ::llvm::Value* val_row_idx = builder.getInt32(row_idx);
    ::llvm::Value* val_offset = builder.getInt32(offset);
    ::fesql::type::Type schema_type;
    if (!DataType2SchemaType(type, &schema_type)) {
        LOG(WARNING) << "fail to convert data type to schema type: "
                     << node::DataTypeName(type);
        return false;
    }

    ::llvm::Value* val_type_id =
        builder.getInt32(static_cast<int32_t>(schema_type));
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        fn_name, i32_ty, i8_ptr_ty, i32_ty, i32_ty, i32_ty, i8_ptr_ty);
    builder.CreateCall(
        callee, ::llvm::ArrayRef<::llvm::Value*>{
                    row_ptr, val_row_idx, val_offset, val_type_id, col_iter});
    *output = list_ref;
    return true;
}

bool MemoryWindowDecodeIRBuilder::BuildGetStringCol(
    uint32_t row_idx, uint32_t offset, uint32_t next_str_field_offset,
    uint32_t str_start_offset, const fesql::node::DataType& type,
    ::llvm::Value* window_ptr, ::llvm::Value** output) {
    if (window_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i32_ty = builder.getInt32Ty();

    ::llvm::Type* list_ref_type = NULL;
    bool ok = GetLLVMListType(block_->getModule(), type, &list_ref_type);
    if (!ok) {
        LOG(WARNING) << "fail to get list type";
        return false;
    }
    uint32_t col_iterator_size;
    ok = GetLLVMColumnSize(type, &col_iterator_size);
    if (!ok) {
        LOG(WARNING) << "fail to get col iterator size";
    }
    // alloca memory on stack for col iterator
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, col_iterator_size);
    ::llvm::Value* col_iter = builder.CreateAlloca(array_type);

    // alloca memory on stack
    ::llvm::Value* list_ref = builder.CreateAlloca(list_ref_type);
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(list_ref_type, list_ref, 0);
    data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, col_iter->getType()->getPointerTo());
    builder.CreateStore(col_iter, data_ptr_ptr, false);
    col_iter = builder.CreatePointerCast(col_iter, i8_ptr_ty);

    // get str field declear
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "fesql_storage_get_str_col", i32_ty, i8_ptr_ty, i32_ty, i32_ty, i32_ty,
        i32_ty, i32_ty, i8_ptr_ty);

    ::llvm::Value* val_row_idx = builder.getInt32(row_idx);
    ::llvm::Value* str_offset = builder.getInt32(offset);
    ::llvm::Value* next_str_offset = builder.getInt32(next_str_field_offset);
    ::fesql::type::Type schema_type;
    if (!DataType2SchemaType(type, &schema_type)) {
        LOG(WARNING) << "fail to convert data type to schema type: "
                     << node::DataTypeName(type);
        return false;
    }
    ::llvm::Value* val_type_id =
        builder.getInt32(static_cast<int32_t>(schema_type));
    builder.CreateCall(
        callee, ::llvm::ArrayRef<::llvm::Value*>{
                    window_ptr, val_row_idx, str_offset, next_str_offset,
                    builder.getInt32(str_start_offset), val_type_id, col_iter});
    *output = list_ref;
    return true;
}
bool MemoryWindowDecodeIRBuilder::GetColOffsetType(
    const std::string& name, uint32_t row_idx, uint32_t* offset_ptr,
    node::DataType* data_type_ptr) {
    fesql::type::Type type;
    if (!decoder_list_[row_idx].GetPrimayFieldOffsetType(name, offset_ptr,
                                                         &type)) {
        return false;
    }

    if (!SchemaType2DataType(type, data_type_ptr)) {
        LOG(WARNING) << "unrecognized data type " +
                            fesql::type::Type_Name(type);
        return false;
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
