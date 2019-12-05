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
    ::llvm::Type* size_ty = builder.getInt32Ty();
    ::llvm::Type* data_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* start_offset = NULL;

    ok = BuildLoadOffset(builder, row_ptr, field_offset, int16_ty,
                         &start_offset);
    if (!ok) {
        LOG(WARNING) << "fail to get string filed " << name
                     << " start offset from table " << table_->name();
        return false;
    }
    ::llvm::Type* str_type = NULL;
    ok = GetLLVMType(block_, ::fesql::type::kVarchar, &str_type);
    if (!ok) {
        LOG(WARNING) << "fail to get string type";
        return false;
    }
    ::llvm::Value* next_str_field_offset = NULL;
    ok = GetNextOffset(name, row_ptr, row_size, &next_str_field_offset);
    ::llvm::Value* string_ref = builder.CreateAlloca(str_type);
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

    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(str_type, string_ref, 1);
    ::llvm::Value* cast_data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, data_ptr->getType()->getPointerTo());
    builder.CreateStore(data_ptr, cast_data_ptr_ptr, false);

    ::llvm::Value* size_ptr = builder.CreateStructGEP(str_type, string_ref, 0);
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

    bool ok = GetLLVMType(block_, fe_type, &llvm_type);
    if (!ok) {
        LOG(WARNING) << "fail to convert fe type to llvm type ";
        return false;
    }

    ::llvm::ConstantInt* llvm_offse = builder.getInt32(offset);
    return BuildLoadOffset(builder, row_ptr, llvm_offse, llvm_type, output);
}
bool BufIRBuilder::GetFieldOffset(const std::string& name, uint32_t& offset,
                                  ::fesql::type::Type& fe_type) {
    Types::iterator it = types_.find(name);
    if (it == types_.end()) {
        LOG(WARNING) << "no column " << name << " in table " << table_->name();
        return false;
    }
    fe_type = it->second.first;
    offset = it->second.second;
    return true;
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
    std::vector<::llvm::Value*> call_args;
    call_args.push_back(row_ptr);
    call_args.push_back(val_offset);
    ::llvm::ArrayRef<::llvm::Value*> call_args_ref(call_args);
    *output = builder.CreateCall(callee, call_args_ref);
    return true;
}

bool BufNativeIRBuilder::BuildGetStringField(uint32_t offset,
                                             uint32_t next_str_field_offset,
                                             ::llvm::Value* row_ptr,
                                             ::llvm::Value* size,
                                             ::llvm::Value** output) {
    if (row_ptr == NULL || size == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }

    ::llvm::Value* str_addr_space = NULL;
    bool ok = sv_->FindVar("str_addr_space", &str_addr_space);
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    if (!ok) {
        ::llvm::FunctionCallee callee =
            block_->getModule()->getOrInsertFunction(
                "fesql_storage_get_str_addr_space", i8_ty, i32_ty);
        std::vector<::llvm::Value*> call_args;
        call_args.push_back(size);
        ::llvm::ArrayRef<::llvm::Value*> call_args_ref(call_args);
        str_addr_space = builder.CreateCall(callee, call_args_ref);
        ok = sv_->AddVar("str_addr_space", str_addr_space);
        if (!ok) {
            LOG(WARNING) << "fail to add str add space var";
            return false;
        }
        str_addr_space = builder.CreateIntCast(str_addr_space, i32_ty, true,
                                               "cast_i8_to_i32");
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
    std::vector<::llvm::Value*> call_args;
    call_args.push_back(row_ptr);
    call_args.push_back(str_offset);
    call_args.push_back(next_str_offset);
    call_args.push_back(builder.getInt32(str_field_start_offset_));
    call_args.push_back(str_addr_space);
    call_args.push_back(data_ptr_ptr);
    call_args.push_back(size_ptr);
    ::llvm::ArrayRef<::llvm::Value*> call_args_ref(call_args);
    // TODO(wangtaize) add status check
    builder.CreateCall(callee, call_args_ref);
    *output = string_ref;
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
    std::vector<::llvm::Value*> call_args;
    // add buf args
    call_args.push_back(i8_ptr);
    call_args.push_back(buf_size);

    // add str args
    call_args.push_back(data_ptr);
    call_args.push_back(fe_str_size);

    call_args.push_back(builder.getInt32(str_field_start_offset_));
    call_args.push_back(builder.getInt32(str_field_idx));
    call_args.push_back(str_addr_space);
    call_args.push_back(str_body_offset);
    ::llvm::ArrayRef<::llvm::Value*> call_args_ref(call_args);
    *output = builder.CreateCall(callee, call_args_ref);
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
    std::vector<::llvm::Value*> call_args;
    call_args.push_back(builder.getInt32(str_field_start_offset_));
    call_args.push_back(builder.getInt32(str_field_cnt_));
    call_args.push_back(total_size);
    call_args.push_back(str_addr_space);
    ::llvm::ArrayRef<::llvm::Value*> call_args_ref(call_args);
    *output_ptr = builder.CreateCall(callee, call_args_ref);
    return true;
}

}  // namespace codegen
}  // namespace fesql
