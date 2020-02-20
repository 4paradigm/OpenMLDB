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

BufNativeIRBuilder::BufNativeIRBuilder(::fesql::type::TableDef* table,
                                       ::llvm::BasicBlock* block,
                                       ScopeVar* scope_var)
    : table_(table),
      block_(block),
      sv_(scope_var),
      variable_ir_builder_(block, scope_var),
      types_() {
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

bool BufNativeIRBuilder::BuildGetFiledOffset(const std::string& name,
                                             uint32_t* offset,
                                             ::fesql::type::Type* fe_type) {
    if (nullptr == offset || nullptr == fe_type) {
        LOG(WARNING) << "input args have null";
        return false;
    }
    Types::iterator it = types_.find(name);
    if (it == types_.end()) {
        LOG(WARNING) << "no column " << name << " in table " << table_->name();
        return false;
    }
    // TODO(wangtaize) support null check
    *fe_type = it->second.first;
    *offset = it->second.second;
    return true;
}
bool BufNativeIRBuilder::BuildGetField(const std::string& name,
                                       ::llvm::Value* row_ptr,
                                       ::llvm::Value* row_size,
                                       ::llvm::Value** output) {
    if (row_ptr == NULL || row_size == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }

    ::fesql::type::Type fe_type;
    uint32_t offset;
    BuildGetFiledOffset(name, &offset, &fe_type);

    ::llvm::IRBuilder<> builder(block_);
    switch (fe_type) {
        case ::fesql::type::kInt16: {
            llvm::Type* i16_ty = builder.getInt16Ty();
            return BuildGetPrimaryField("fesql_storage_get_int16_field",
                                        row_ptr, offset, i16_ty, output);
        }
        case ::fesql::type::kInt32: {
            llvm::Type* i32_ty = builder.getInt32Ty();
            return BuildGetPrimaryField("fesql_storage_get_int32_field",
                                        row_ptr, offset, i32_ty, output);
        }
        case ::fesql::type::kInt64: {
            llvm::Type* i64_ty = builder.getInt64Ty();
            return BuildGetPrimaryField("fesql_storage_get_int64_field",
                                        row_ptr, offset, i64_ty, output);
        }
        case ::fesql::type::kFloat: {
            llvm::Type* float_ty = builder.getFloatTy();
            return BuildGetPrimaryField("fesql_storage_get_float_field",
                                        row_ptr, offset, float_ty, output);
        }
        case ::fesql::type::kDouble: {
            llvm::Type* double_ty = builder.getDoubleTy();
            return BuildGetPrimaryField("fesql_storage_get_double_field",
                                        row_ptr, offset, double_ty, output);
        }
        case ::fesql::type::kVarchar: {
            uint32_t next_offset = 0;
            auto nit = next_str_pos_.find(offset);
            if (nit != next_str_pos_.end()) {
                next_offset = nit->second;
            }
            DLOG(INFO) << "get string with offset " << offset << " next offset "
                       << next_offset << " for col " << name;
            return BuildGetStringField(offset, next_offset, row_ptr, row_size,
                                       output);
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
    ::llvm::Value* val_offset = builder.getInt32(offset);
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        fn_name, type, i8_ptr_ty, i32_ty);
    *output = builder.CreateCall(
        callee, ::llvm::ArrayRef<::llvm::Value*>{row_ptr, val_offset});
    return true;
}

bool BufNativeIRBuilder::BuildGetStringField(uint32_t offset,
                                             uint32_t next_str_field_offset,
                                             ::llvm::Value* row_ptr,
                                             ::llvm::Value* size,
                                             ::llvm::Value** output) {
    base::Status status;
    if (row_ptr == NULL || size == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }

    ::llvm::Value* str_addr_space = NULL;
    bool ok = variable_ir_builder_.LoadValue("str_addr_space", &str_addr_space,
                                             status);
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    if (!ok || str_addr_space == NULL) {
        ::llvm::FunctionCallee callee =
            block_->getModule()->getOrInsertFunction(
                "fesql_storage_get_str_addr_space", i8_ty, i32_ty);
        str_addr_space =
            builder.CreateCall(callee, ::llvm::ArrayRef<::llvm::Value*>{size});
        str_addr_space = builder.CreateIntCast(str_addr_space, i32_ty, true,
                                               "cast_i8_to_i32");
        ok = variable_ir_builder_.StoreValue("str_addr_space", str_addr_space,
                                             status);
        if (!ok) {
            LOG(WARNING) << "fail to add str add space var";
            return false;
        }
    }

    ::llvm::Type* str_type = NULL;
    ok = GetLLVMType(block_, ::fesql::type::kVarchar, &str_type);
    if (!ok) {
        LOG(WARNING) << "fail to get string type";
        return false;
    }

    // alloca memory on stack
    ::llvm::Value* string_ref = builder.CreateAlloca(str_type);
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(str_type, string_ref, 1);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    // get str field declear
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "fesql_storage_get_str_field", i32_ty, i8_ptr_ty, i32_ty, i32_ty,
        i32_ty, i32_ty, i8_ptr_ty->getPointerTo(), i32_ty->getPointerTo());

    ::llvm::Value* str_offset = builder.getInt32(offset);
    ::llvm::Value* next_str_offset = builder.getInt32(next_str_field_offset);
    // get the data ptr
    data_ptr_ptr =
        builder.CreatePointerCast(data_ptr_ptr, i8_ptr_ty->getPointerTo());
    ::llvm::Value* size_ptr = builder.CreateStructGEP(str_type, string_ref, 0);
    // get the size ptr
    size_ptr = builder.CreatePointerCast(size_ptr, i32_ty->getPointerTo());
    // TODO(wangtaize) add status check
    builder.CreateCall(callee, ::llvm::ArrayRef<::llvm::Value*>{
                                   row_ptr, str_offset, next_str_offset,
                                   builder.getInt32(str_field_start_offset_),
                                   str_addr_space, data_ptr_ptr, size_ptr});
    *output = string_ref;
    return true;
}

bool BufNativeIRBuilder::BuildGetCol(const std::string& name,
                                     ::llvm::Value* window_ptr,
                                     ::llvm::Value** output) {
    if (window_ptr == NULL || output == NULL) {
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
        case ::fesql::type::kInt16:
        case ::fesql::type::kInt32:
        case ::fesql::type::kInt64:
        case ::fesql::type::kFloat:
        case ::fesql::type::kDouble: {
            return BuildGetPrimaryCol("fesql_storage_get_col", window_ptr,
                                      offset, fe_type, output);
        }
        case ::fesql::type::kVarchar: {
            uint32_t next_offset = 0;
            auto nit = next_str_pos_.find(offset);
            if (nit != next_str_pos_.end()) {
                next_offset = nit->second;
            }
            DLOG(INFO) << "get string with offset " << offset << " next offset "
                       << next_offset << " for col " << name;
            return BuildGetStringCol(offset, next_offset, fe_type, window_ptr,
                                     output);
        }
        default: {
            return false;
        }
    }
}
bool BufNativeIRBuilder::BuildGetPrimaryCol(const std::string& fn_name,
                                            ::llvm::Value* row_ptr,
                                            uint32_t offset,
                                            fesql::type::Type type,
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
    uint32_t col_iterator_size;
    ok = GetLLVMColumnSize(type, &col_iterator_size);
    if (!ok) {
        LOG(WARNING) << "fail to get col list size";
    }
    // alloca memory on stack for col list
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

    ::llvm::Value* val_offset = builder.getInt32(offset);
    ::llvm::Value* val_type_id = builder.getInt32(static_cast<int32_t>(type));

    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        fn_name, i32_ty, i8_ptr_ty, i32_ty, i32_ty, i8_ptr_ty);
    builder.CreateCall(callee, ::llvm::ArrayRef<::llvm::Value*>{
                                   row_ptr, val_offset, val_type_id, col_iter});
    *output = list_ref;
    return true;
}
bool BufNativeIRBuilder::BuildGetStringCol(uint32_t offset,
                                           uint32_t next_str_field_offset,
                                           fesql::type::Type type,
                                           ::llvm::Value* window_ptr,
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
    bool ok = GetLLVMListType(block_->getModule(), type, &list_ref_type);
    if (!ok) {
        LOG(WARNING) << "fail to get list type";
        return false;
    }
    uint32_t col_iterator_size;
    ok = GetLLVMColumnSize(type, &col_iterator_size);
    if (!ok) {
        LOG(WARNING) << "fail to get col list size";
    }
    // alloca memory on stack for col list
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
        i32_ty, i8_ptr_ty);

    ::llvm::Value* str_offset = builder.getInt32(offset);
    ::llvm::Value* next_str_offset = builder.getInt32(next_str_field_offset);
    ::llvm::Value* val_type_id = builder.getInt32(static_cast<int32_t>(type));
    // get the data ptr
    // TODO(wangtaize) add status check
    builder.CreateCall(callee, ::llvm::ArrayRef<::llvm::Value*>{
                                   window_ptr, str_offset, next_str_offset,
                                   builder.getInt32(str_field_start_offset_),
                                   val_type_id, col_iter});
    *output = list_ref;
    return true;
}

BufNativeEncoderIRBuilder::BufNativeEncoderIRBuilder(
    const std::map<uint32_t, ::llvm::Value*>* outputs,
    const std::vector<::fesql::type::ColumnDef>* schema,
    ::llvm::BasicBlock* block)
    : outputs_(outputs),
      schema_(schema),
      str_field_start_offset_(0),
      offset_vec_(),
      str_field_cnt_(0),
      block_(block) {
    str_field_start_offset_ = HEADER_LENGTH + BitMapSize(schema_->size());
    for (uint32_t idx = 0; idx < schema_->size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema_->at(idx);
        if (column.type() == ::fesql::type::kVarchar) {
            offset_vec_.push_back(str_field_cnt_);
            str_field_cnt_++;
        } else {
            auto it = TYPE_SIZE_MAP.find(column.type());
            if (it == TYPE_SIZE_MAP.end()) {
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
            } else {
                offset_vec_.push_back(str_field_start_offset_);
                DLOG(INFO) << "idx " << idx << " offset "
                           << str_field_start_offset_;
                str_field_start_offset_ += it->second;
            }
        }
    }
}

BufNativeEncoderIRBuilder::~BufNativeEncoderIRBuilder() {}

bool BufNativeEncoderIRBuilder::BuildEncode(::llvm::Value* output_ptr) {
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Value* str_addr_space_ptr = builder.CreateAlloca(i32_ty);
    ::llvm::Value* row_size = NULL;
    bool ok = CalcTotalSize(&row_size, str_addr_space_ptr);

    if (!ok) {
        return false;
    }

    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* i8_ptr =
        ::llvm::CallInst::CreateMalloc(block_, row_size->getType(), i8_ptr_ty,
                                       row_size, nullptr, nullptr, "malloc");
    block_->getInstList().push_back(::llvm::cast<::llvm::Instruction>(i8_ptr));
    i8_ptr = builder.CreatePointerCast(i8_ptr, i8_ptr_ty);
    DLOG(INFO) << "i8_ptr type " << i8_ptr->getType()->getTypeID()
               << " output ptr type " << output_ptr->getType()->getTypeID();
    // make sure free it in c++ always
    builder.CreateStore(i8_ptr, output_ptr, false);
    // encode all field to buf
    // append header
    ok = AppendHeader(i8_ptr, row_size, builder.getInt32(schema_->size()));

    if (!ok) {
        return false;
    }

    ::llvm::Value* str_body_offset = NULL;
    ::llvm::Value* str_addr_space_val = NULL;
    for (uint32_t idx = 0; idx < schema_->size(); idx++) {
        const ::fesql::type::ColumnDef& column = schema_->at(idx);
        // TODO(wangtaize) null check
        ::llvm::Value* val = outputs_->at(idx);
        switch (column.type()) {
            case ::fesql::type::kInt16:
            case ::fesql::type::kInt32:
            case ::fesql::type::kInt64:
            case ::fesql::type::kFloat:
            case ::fesql::type::kDouble: {
                uint32_t offset = offset_vec_.at(idx);
                if (val->getType()->isFloatTy() ||
                    val->getType()->isDoubleTy() ||
                    val->getType()->isIntegerTy()) {
                    ok = AppendPrimary(i8_ptr, val, offset);
                    if (!ok) {
                        LOG(WARNING) << "fail to append number for output col "
                                     << column.name();
                        return false;
                    }
                    break;
                } else {
                    LOG(WARNING) << "number type is required but "
                                 << val->getType()->getTypeID();
                    return false;
                }
            }
            case ::fesql::type::kVarchar: {
                if (str_body_offset == NULL) {
                    str_addr_space_val = builder.CreateLoad(
                        i32_ty, str_addr_space_ptr, "load_str_space");
                    ok = CalcStrBodyStart(&str_body_offset, str_addr_space_val);
                    if (!ok || str_addr_space_val == NULL) {
                        return false;
                    }
                }
                uint32_t field_cnt = offset_vec_.at(idx);
                ::llvm::Value* temp_body_size = NULL;
                ok = AppendString(i8_ptr, row_size, val, str_addr_space_val,
                                  str_body_offset, field_cnt, &temp_body_size);
                if (!ok) {
                    LOG(WARNING) << "fail to append string for output col "
                                 << column.name();
                    return false;
                }
                str_body_offset = temp_body_size;
                break;
            }
            default: {
                LOG(WARNING) << "unsuported type, append val for output col "
                             << column.name();
                return false;
            }
        }
    }
    return true;
}
bool BufNativeEncoderIRBuilder::AppendString(
    ::llvm::Value* i8_ptr, ::llvm::Value* buf_size, ::llvm::Value* str_val,
    ::llvm::Value* str_addr_space, ::llvm::Value* str_body_offset,
    uint32_t str_field_idx, ::llvm::Value** output) {
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* str_ty = NULL;
    bool ok = GetLLVMType(block_, ::fesql::type::kVarchar, &str_ty);
    if (!ok || str_ty == NULL) {
        LOG(WARNING) << "fail to get str llvm type";
        return false;
    }

    ::llvm::Type* size_ty = builder.getInt32Ty();
    // get fe.string size
    ::llvm::Value* size_ptr = builder.CreateStructGEP(str_ty, str_val, 0);
    ::llvm::Value* size_i32_ptr =
        builder.CreatePointerCast(size_ptr, size_ty->getPointerTo());
    ::llvm::Value* fe_str_size =
        builder.CreateLoad(size_ty, size_i32_ptr, "load_str_length");

    // get fe.string char*
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* data_ptr_ptr = builder.CreateStructGEP(str_ty, str_val, 1);
    data_ptr_ptr =
        builder.CreatePointerCast(data_ptr_ptr, i8_ptr_ty->getPointerTo());
    ::llvm::Value* data_ptr =
        builder.CreateLoad(i8_ptr_ty, data_ptr_ptr, "load_str_data_ptr");
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "fesql_storage_encode_string_field",
        size_ty,    // return type
        i8_ptr_ty,  // buf ptr
        size_ty,    // buf size
        i8_ptr_ty,  // str val ptr
        size_ty,    // str val size
        size_ty,    // str_start_offset
        size_ty,    // str_field_offset
        size_ty,    // str_addr_space
        size_ty);
    *output = builder.CreateCall(
        callee,
        ::llvm::ArrayRef<::llvm::Value*>{
            i8_ptr, buf_size, data_ptr, fe_str_size,
            builder.getInt32(str_field_start_offset_),
            builder.getInt32(str_field_idx), str_addr_space, str_body_offset});
    return true;
}

bool BufNativeEncoderIRBuilder::CalcStrBodyStart(
    ::llvm::Value** output, ::llvm::Value* str_addr_space) {
    if (output == NULL || str_addr_space == NULL) {
        LOG(WARNING) << "input ptr is null";
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* str_field_start = builder.getInt32(str_field_start_offset_);
    ::llvm::Value* str_field_cnt = builder.getInt32(str_field_cnt_);
    ::llvm::Value* temp = builder.CreateMul(str_field_cnt, str_addr_space);
    *output = builder.CreateAdd(str_field_start, temp);
    return true;
}

bool BufNativeEncoderIRBuilder::AppendPrimary(::llvm::Value* i8_ptr,
                                              ::llvm::Value* val,
                                              uint32_t field_offset) {
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* offset = builder.getInt32(field_offset);
    return BuildStoreOffset(builder, i8_ptr, offset, val);
}

bool BufNativeEncoderIRBuilder::AppendHeader(::llvm::Value* i8_ptr,
                                             ::llvm::Value* size,
                                             ::llvm::Value* bitmap_size) {
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* fversion = builder.getInt8(1);
    ::llvm::Value* sversion = builder.getInt8(1);
    ::llvm::Value* fverion_offset = builder.getInt32(0);
    ::llvm::Value* sverion_offset = builder.getInt32(1);
    bool ok = BuildStoreOffset(builder, i8_ptr, fverion_offset, fversion);
    if (!ok) {
        LOG(WARNING) << "fail to add fversion to row";
        return false;
    }

    ok = BuildStoreOffset(builder, i8_ptr, sverion_offset, sversion);
    if (!ok) {
        LOG(WARNING) << "fail to add sversion to row";
        return false;
    }

    ::llvm::Value* size_offset = builder.getInt32(2);
    ok = BuildStoreOffset(builder, i8_ptr, size_offset, size);

    if (!ok) {
        LOG(WARNING) << "fail to add size to row";
        return false;
    }

    ::llvm::Value* output = NULL;
    ok = BuildGetPtrOffset(builder, i8_ptr, builder.getInt32(6),
                           builder.getInt8PtrTy(), &output);
    if (!ok) {
        LOG(WARNING) << "fail to get ptr with offset ";
        return false;
    }
    builder.CreateMemSet(output, builder.getInt8(0), bitmap_size, 1u);
    return true;
}

bool BufNativeEncoderIRBuilder::CalcTotalSize(::llvm::Value** output_ptr,
                                              ::llvm::Value* str_addr_space) {
    if (output_ptr == NULL) {
        LOG(WARNING) << "input ptr is null";
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    if (str_field_cnt_ <= 0 || schema_->size() == 0) {
        *output_ptr = builder.getInt32(str_field_start_offset_);
        return true;
    }

    ::llvm::Value* total_size = NULL;
    ::llvm::Type* str_ty = NULL;
    bool ok = GetLLVMType(block_, ::fesql::type::kVarchar, &str_ty);
    if (!ok || str_ty == NULL) {
        LOG(WARNING) << "fail to get str llvm type";
        return false;
    }
    // build get string length and call native functon
    ::llvm::Type* size_ty = builder.getInt32Ty();
    for (uint32_t idx = 0; idx < schema_->size(); ++idx) {
        const ::fesql::type::ColumnDef& column = schema_->at(idx);
        DLOG(INFO) << "output column " << column.name() << " " << idx;
        if (column.type() == ::fesql::type::kVarchar) {
            ::llvm::Value* fe_str = outputs_->at(idx);
            if (fe_str == NULL) {
                LOG(WARNING) << "str output is null for " << column.name();
                return false;
            }
            ::llvm::Value* fe_str_ptr =
                builder.CreatePointerCast(fe_str, str_ty->getPointerTo());
            ::llvm::Value* size_ptr =
                builder.CreateStructGEP(str_ty, fe_str_ptr, 0);
            ::llvm::Value* size_i32_ptr =
                builder.CreatePointerCast(size_ptr, size_ty->getPointerTo());
            ::llvm::Value* fe_str_size =
                builder.CreateLoad(size_ty, size_i32_ptr, "load_str_length");
            if (total_size == NULL) {
                total_size = fe_str_size;
            } else {
                total_size = builder.CreateAdd(fe_str_size, total_size,
                                               "add_str_length");
            }
        }
    }

    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "fesql_storage_encode_calc_size", size_ty, size_ty, size_ty, size_ty,
        size_ty->getPointerTo());
    *output_ptr = builder.CreateCall(
        callee,
        ::llvm::ArrayRef<::llvm::Value*>{
            builder.getInt32(str_field_start_offset_),
            builder.getInt32(str_field_cnt_), total_size, str_addr_space});
    return true;
}

}  // namespace codegen
}  // namespace fesql
