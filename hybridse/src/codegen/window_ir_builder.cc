/*
 * Copyright 2021 4Paradigm
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

namespace hybridse {
namespace codegen {

MemoryWindowDecodeIRBuilder::MemoryWindowDecodeIRBuilder(
    const vm::SchemasContext* schemas_context, ::llvm::BasicBlock* block)
    : block_(block), schemas_context_(schemas_context) {}

MemoryWindowDecodeIRBuilder::~MemoryWindowDecodeIRBuilder() {}

bool MemoryWindowDecodeIRBuilder::BuildInnerRowsList(::llvm::Value* list_ptr,
                                                     int64_t start_offset,
                                                     int64_t end_offset,
                                                     ::llvm::Value** output) {
    if (list_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }
    if (list_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Type* i64_ty = builder.getInt64Ty();
    uint32_t inner_list_size =
        sizeof(::hybridse::codec::InnerRowsList<hybridse::codec::Row>);
    // alloca memory on stack for col iterator
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, inner_list_size);
    ::llvm::Value* inner_list_ptr =
        CreateAllocaAtHead(&builder, array_type, "sub_window_alloca");
    inner_list_ptr = builder.CreatePointerCast(inner_list_ptr, i8_ptr_ty);

    ::llvm::Value* val_start_offset = builder.getInt64(start_offset);
    ::llvm::Value* val_end_offset = builder.getInt64(end_offset);
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "hybridse_storage_get_inner_rows_list", i32_ty, i8_ptr_ty, i64_ty,
        i64_ty, i8_ptr_ty);
    builder.CreateCall(callee, ::llvm::ArrayRef<::llvm::Value*>{
                                   list_ptr, val_start_offset, val_end_offset,
                                   inner_list_ptr});
    *output = inner_list_ptr;
    return true;
}
bool MemoryWindowDecodeIRBuilder::BuildInnerRangeList(::llvm::Value* list_ptr,
                                                      ::llvm::Value* row_key,
                                                      int64_t start_offset,
                                                      int64_t end_offset,
                                                      ::llvm::Value** output) {
    if (list_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }
    if (list_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Type* i64_ty = builder.getInt64Ty();
    uint32_t inner_list_size =
        sizeof(::hybridse::codec::InnerRangeList<hybridse::codec::Row>);
    // alloca memory on stack for col iterator
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, inner_list_size);
    ::llvm::Value* inner_list_ptr =
        CreateAllocaAtHead(&builder, array_type, "sub_window_alloca");
    inner_list_ptr = builder.CreatePointerCast(inner_list_ptr, i8_ptr_ty);

    ::llvm::Value* val_start_offset = builder.getInt64(start_offset);
    ::llvm::Value* val_end_offset = builder.getInt64(end_offset);
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "hybridse_storage_get_inner_range_list", i32_ty, i8_ptr_ty, i64_ty,
        i64_ty, i64_ty, i8_ptr_ty);
    builder.CreateCall(callee, ::llvm::ArrayRef<::llvm::Value*>{
                                   list_ptr, row_key, val_start_offset,
                                   val_end_offset, inner_list_ptr});
    *output = inner_list_ptr;
    return true;
}

bool MemoryWindowDecodeIRBuilder::BuildGetCol(size_t schema_idx, size_t col_idx,
                                              ::llvm::Value* window_ptr,
                                              ::llvm::Value** output) {
    if (window_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }
    ::hybridse::node::TypeNode data_type;
    auto row_format = schemas_context_->GetRowFormat(schema_idx);
    if (row_format == nullptr) {
        LOG(WARNING) << "fail to get row format at " << schema_idx;
        return false;
    }
    const codec::ColInfo* col_info = row_format->GetColumnInfo(col_idx);
    if (col_info == nullptr) {
        LOG(WARNING) << "fail to get column info at " << schema_idx << ":"
                     << col_idx;
        return false;
    }
    if (!SchemaType2DataType(col_info->type, &data_type)) {
        LOG(WARNING) << "unrecognized data type " +
                            hybridse::type::Type_Name(col_info->type);
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    switch (data_type.base_) {
        case ::hybridse::node::kBool:
        case ::hybridse::node::kInt16:
        case ::hybridse::node::kInt32:
        case ::hybridse::node::kInt64:
        case ::hybridse::node::kFloat:
        case ::hybridse::node::kDouble:
        case ::hybridse::node::kTimestamp:
        case ::hybridse::node::kDate: {
            return BuildGetPrimaryCol("hybridse_storage_get_col", window_ptr,
                                      schema_idx, col_idx, col_info->offset,
                                      &data_type, output);
        }
        case ::hybridse::node::kVarchar: {
            codec::StringColInfo str_col_info;
            if (!schemas_context_->GetRowFormat(schema_idx)
                     ->GetStringColumnInfo(col_idx, &str_col_info)) {
                LOG(WARNING)
                    << "fail to get string filed offset and next offset"
                    << " at " << col_idx;
            }
            DLOG(INFO) << "get string with offset " << str_col_info.offset
                       << " next offset " << str_col_info.str_next_offset
                       << " for col at " << str_col_info.name;
            return BuildGetStringCol(
                schema_idx, str_col_info.idx, str_col_info.offset,
                str_col_info.str_next_offset, str_col_info.str_start_offset,
                &data_type, window_ptr, output);
        }
        default: {
            LOG(WARNING) << "Fail get col, invalid data type "
                         << data_type.GetName();
            return false;
        }
    }
}  // namespace codegen

bool MemoryWindowDecodeIRBuilder::BuildGetPrimaryCol(
    const std::string& fn_name, ::llvm::Value* row_ptr, size_t schema_idx,
    size_t col_idx, uint32_t offset, hybridse::node::TypeNode* type,
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
    bool ok = GetLlvmListType(block_->getModule(), type, &list_ref_type);
    if (!ok) {
        LOG(WARNING) << "fail to get list type";
        return false;
    }
    uint32_t col_iterator_size = 0;
    ok = GetLlvmColumnSize(type, &col_iterator_size);
    if (!ok || col_iterator_size == 0) {
        LOG(WARNING) << "fail to get col iterator size";
    }
    // alloca memory on stack for col iterator
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, col_iterator_size);
    ::llvm::Value* col_iter =
        CreateAllocaAtHead(&builder, array_type, "col_iter_alloca");
    // alloca memory on stack
    ::llvm::Value* list_ref =
        CreateAllocaAtHead(&builder, list_ref_type, "list_ref_alloca");
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(list_ref_type, list_ref, 0);
    data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, col_iter->getType()->getPointerTo());
    builder.CreateStore(col_iter, data_ptr_ptr, false);
    col_iter = builder.CreatePointerCast(col_iter, i8_ptr_ty);

    ::llvm::Value* val_schema_idx = builder.getInt32(schema_idx);
    ::llvm::Value* val_col_idx = builder.getInt32(col_idx);
    ::llvm::Value* val_offset = builder.getInt32(offset);
    ::hybridse::type::Type schema_type;
    if (!DataType2SchemaType(*type, &schema_type)) {
        LOG(WARNING) << "fail to convert data type to schema type: "
                     << type->GetName();
        return false;
    }

    ::llvm::Value* val_type_id =
        builder.getInt32(static_cast<int32_t>(schema_type));
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        fn_name, i32_ty, i8_ptr_ty, i32_ty, i32_ty, i32_ty, i32_ty, i8_ptr_ty);
    builder.CreateCall(callee, {row_ptr, val_schema_idx, val_col_idx,
                                val_offset, val_type_id, col_iter});
    *output = list_ref;
    return true;
}

bool MemoryWindowDecodeIRBuilder::BuildGetStringCol(
    size_t schema_idx, size_t col_idx, uint32_t offset,
    uint32_t next_str_field_offset, uint32_t str_start_offset,
    hybridse::node::TypeNode* type, ::llvm::Value* window_ptr,
    ::llvm::Value** output) {
    if (window_ptr == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i32_ty = builder.getInt32Ty();

    ::llvm::Type* list_ref_type = NULL;
    bool ok = GetLlvmListType(block_->getModule(), type, &list_ref_type);
    if (!ok) {
        LOG(WARNING) << "fail to get list type";
        return false;
    }
    uint32_t col_iterator_size;
    ok = GetLlvmColumnSize(type, &col_iterator_size);
    if (!ok) {
        LOG(WARNING) << "fail to get col iterator size";
    }
    // alloca memory on stack for col iterator
    ::llvm::ArrayType* array_type =
        ::llvm::ArrayType::get(i8_ty, col_iterator_size);
    ::llvm::Value* col_iter =
        CreateAllocaAtHead(&builder, array_type, "col_iter_alloca");

    // alloca memory on stack
    ::llvm::Value* list_ref =
        CreateAllocaAtHead(&builder, list_ref_type, "list_ref_alloca");
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(list_ref_type, list_ref, 0);
    data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, col_iter->getType()->getPointerTo());
    builder.CreateStore(col_iter, data_ptr_ptr, false);
    col_iter = builder.CreatePointerCast(col_iter, i8_ptr_ty);

    // get str field declear
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "hybridse_storage_get_str_col", i32_ty, i8_ptr_ty, i32_ty, i32_ty,
        i32_ty, i32_ty, i32_ty, i32_ty, i8_ptr_ty);

    ::llvm::Value* val_schema_idx = builder.getInt32(schema_idx);
    ::llvm::Value* val_col_idx = builder.getInt32(col_idx);
    ::llvm::Value* str_offset = builder.getInt32(offset);
    ::llvm::Value* next_str_offset = builder.getInt32(next_str_field_offset);
    ::hybridse::type::Type schema_type;
    if (!DataType2SchemaType(*type, &schema_type)) {
        LOG(WARNING) << "fail to convert data type to schema type: "
                     << type->GetName();
        return false;
    }
    ::llvm::Value* val_type_id =
        builder.getInt32(static_cast<int32_t>(schema_type));
    builder.CreateCall(
        callee,
        {window_ptr, val_schema_idx, val_col_idx, str_offset, next_str_offset,
         builder.getInt32(str_start_offset), val_type_id, col_iter});
    *output = list_ref;
    return true;
}

}  // namespace codegen
}  // namespace hybridse
