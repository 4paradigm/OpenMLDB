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

#include "codegen/buf_ir_builder.h"
#include <string>
#include <utility>
#include <vector>
#include "codec/fe_row_codec.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "glog/logging.h"

DECLARE_bool(enable_spark_unsaferow_format);

namespace hybridse {
namespace codegen {

BufNativeIRBuilder::BufNativeIRBuilder(const size_t schema_idx, const codec::RowFormat* format,
                                       ::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block), sv_(scope_var), schema_idx_(schema_idx), format_(format), variable_ir_builder_(block, scope_var) {}

BufNativeIRBuilder::~BufNativeIRBuilder() {}

bool BufNativeIRBuilder::BuildGetField(size_t col_idx, ::llvm::Value* row_ptr, ::llvm::Value* row_size,
                                       NativeValue* output) {
    if (row_ptr == NULL || row_size == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }

    node::TypeNode data_type;
    const codec::ColInfo* col_info = format_->GetColumnInfo(col_idx);
    if (col_info == nullptr) {
        LOG(WARNING) << "fail to resolve field info at " << col_idx;
        return false;
    }
    if (!SchemaType2DataType(col_info->type, &data_type)) {
        LOG(WARNING) << "unrecognized data type " + hybridse::type::Type_Name(col_info->type);
        return false;
    }
    uint32_t offset = col_info->offset;

    ::llvm::IRBuilder<> builder(block_);
    switch (data_type.base_) {
        case ::hybridse::node::kBool: {
            llvm::Type* bool_ty = builder.getInt1Ty();
            return BuildGetPrimaryField("hybridse_storage_get_bool_field", row_ptr, col_idx, offset, bool_ty, output);
        }
        case ::hybridse::node::kInt16: {
            llvm::Type* i16_ty = builder.getInt16Ty();
            return BuildGetPrimaryField("hybridse_storage_get_int16_field", row_ptr, col_idx, offset, i16_ty, output);
        }
        case ::hybridse::node::kInt32: {
            llvm::Type* i32_ty = builder.getInt32Ty();
            return BuildGetPrimaryField("hybridse_storage_get_int32_field", row_ptr, col_idx, offset, i32_ty, output);
        }
        case ::hybridse::node::kInt64: {
            llvm::Type* i64_ty = builder.getInt64Ty();
            return BuildGetPrimaryField("hybridse_storage_get_int64_field", row_ptr, col_idx, offset, i64_ty, output);
        }
        case ::hybridse::node::kFloat: {
            llvm::Type* float_ty = builder.getFloatTy();
            return BuildGetPrimaryField("hybridse_storage_get_float_field", row_ptr, col_idx, offset, float_ty, output);
        }
        case ::hybridse::node::kDouble: {
            llvm::Type* double_ty = builder.getDoubleTy();
            return BuildGetPrimaryField("hybridse_storage_get_double_field", row_ptr, col_idx, offset, double_ty,
                                        output);
        }
        case ::hybridse::node::kTimestamp: {
            NativeValue int64_val;
            if (!BuildGetPrimaryField("hybridse_storage_get_int64_field", row_ptr, col_idx, offset,
                                      builder.getInt64Ty(), &int64_val)) {
                return false;
            }
            codegen::TimestampIRBuilder timestamp_builder(block_->getModule());
            llvm::Value* ts_st = nullptr;
            if (!timestamp_builder.NewTimestamp(block_, int64_val.GetValue(&builder), &ts_st)) {
                return false;
            }
            *output = int64_val.Replace(ts_st);
            return true;
        }
        case ::hybridse::node::kDate: {
            NativeValue int32_val;
            if (!BuildGetPrimaryField("hybridse_storage_get_int32_field", row_ptr, col_idx, offset,
                                      builder.getInt32Ty(), &int32_val)) {
                return false;
            }
            codegen::DateIRBuilder date_ir_builder(block_->getModule());
            llvm::Value* ts_st = nullptr;
            if (!date_ir_builder.NewDate(block_, int32_val.GetValue(&builder), &ts_st)) {
                return false;
            }
            *output = int32_val.Replace(ts_st);
            return true;
        }

        case ::hybridse::node::kVarchar: {
            codec::StringColInfo str_info;
            if (!format_->GetStringColumnInfo(col_idx, &str_info)) {
                LOG(WARNING) << "fail to get string filed offset and next offset" << col_info->name;
            }
            DLOG(INFO) << "get string with offset " << offset << " next offset " << str_info.str_next_offset
                       << " for col " << col_idx;
            return BuildGetStringField(col_idx, offset, str_info.str_next_offset, str_info.str_start_offset, row_ptr,
                                       row_size, output);
        }
        default: {
            LOG(WARNING) << "fail to get col for type: " << data_type.GetName();
            return false;
        }
    }
}

bool BufNativeIRBuilder::BuildGetPrimaryField(const std::string& fn_name, ::llvm::Value* row_ptr, uint32_t col_idx,
                                              uint32_t offset, ::llvm::Type* type, NativeValue* output) {
    if (row_ptr == NULL || type == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Value* val_col_idx = builder.getInt32(col_idx);
    ::llvm::Value* val_offset = builder.getInt32(offset);
    ::llvm::Value* is_null_alloca = CreateAllocaAtHead(&builder, builder.getInt8Ty(), "is_null_addr");
    ::llvm::FunctionCallee callee =
        block_->getModule()->getOrInsertFunction(fn_name, type, i8_ptr_ty, i32_ty, i32_ty, i8_ptr_ty);

    ::llvm::Value* raw = builder.CreateCall(callee, {row_ptr, val_col_idx, val_offset, is_null_alloca});
    ::llvm::Value* is_null = builder.CreateLoad(is_null_alloca);
    *output = NativeValue::CreateWithFlag(raw, is_null);
    return true;
}

bool BufNativeIRBuilder::BuildGetStringField(uint32_t col_idx, uint32_t offset, uint32_t next_str_field_offset,
                                             uint32_t str_start_offset, ::llvm::Value* row_ptr, ::llvm::Value* size,
                                             NativeValue* output) {
    base::Status status;
    if (row_ptr == NULL || size == NULL || output == NULL) {
        LOG(WARNING) << "input args have null ptr";
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::FunctionCallee addr_space_callee =
        block_->getModule()->getOrInsertFunction("hybridse_storage_get_str_addr_space", i8_ty, i32_ty);
    ::llvm::Value* str_addr_space = builder.CreateCall(addr_space_callee, {size});
    str_addr_space = builder.CreateIntCast(str_addr_space, i32_ty, true, "cast_i8_to_i32");
    codegen::StringIRBuilder string_ir_builder(block_->getModule());

    // alloca memory on stack
    ::llvm::Value* string_ref;
    if (!string_ir_builder.NewString(block_, &string_ref)) {
        LOG(WARNING) << "fail to initialize string ref";
        return false;
    }
    ::llvm::Value* data_ptr_ptr = builder.CreateStructGEP(string_ir_builder.GetType(), string_ref, 1);

    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Type* bool_ptr_ty = builder.getInt1Ty()->getPointerTo();

    // get str field declear
    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "hybridse_storage_get_str_field", i32_ty, i8_ptr_ty, i32_ty, i32_ty, i32_ty, i32_ty, i32_ty,
        i8_ptr_ty->getPointerTo(), i32_ty->getPointerTo(), bool_ptr_ty);

    ::llvm::Value* str_offset = builder.getInt32(offset);
    ::llvm::Value* val_col_idx = builder.getInt32(col_idx);
    ::llvm::Value* next_str_offset = builder.getInt32(next_str_field_offset);
    // get the data ptr
    data_ptr_ptr = builder.CreatePointerCast(data_ptr_ptr, i8_ptr_ty->getPointerTo());
    ::llvm::Value* size_ptr = builder.CreateStructGEP(string_ir_builder.GetType(), string_ref, 0);
    // get the size ptr
    size_ptr = builder.CreatePointerCast(size_ptr, i32_ty->getPointerTo());

    // null flag
    ::llvm::Type* bool_ty = builder.getInt1Ty();
    ::llvm::Value* is_null_alloca = CreateAllocaAtHead(&builder, bool_ty, "string_is_null");

    // TODO(wangtaize) add status check
    builder.CreateCall(callee, {row_ptr, val_col_idx, str_offset, next_str_offset, builder.getInt32(str_start_offset),
                                str_addr_space, data_ptr_ptr, size_ptr, is_null_alloca});

    ::llvm::Value* is_null = builder.CreateLoad(is_null_alloca);
    *output = NativeValue::CreateWithFlag(string_ref, is_null);
    return true;
}

BufNativeEncoderIRBuilder::BufNativeEncoderIRBuilder(const std::map<uint32_t, NativeValue>* outputs,
                                                     const vm::Schema* schema, ::llvm::BasicBlock* block)
    : outputs_(outputs), schema_(schema), str_field_start_offset_(0), offset_vec_(), str_field_cnt_(0), block_(block) {
    str_field_start_offset_ = codec::GetStartOffset(schema_->size());
    for (int32_t idx = 0; idx < schema_->size(); idx++) {
        // Support Spark UnsafeRow format where all fields will take up 8 bytes
        if (FLAGS_enable_spark_unsaferow_format) {
            offset_vec_.push_back(str_field_start_offset_);
            str_field_start_offset_ += 8;
            const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
            if (column.type() == ::hybridse::type::kVarchar) {
                str_field_cnt_++;
            }
        } else {
            const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
            if (column.type() == ::hybridse::type::kVarchar) {
                offset_vec_.push_back(str_field_cnt_);
                str_field_cnt_++;
            } else {
                auto TYPE_SIZE_MAP = codec::GetTypeSizeMap();
                auto it = TYPE_SIZE_MAP.find(column.type());
                if (it == TYPE_SIZE_MAP.end()) {
                    LOG(WARNING) << ::hybridse::type::Type_Name(column.type()) << " is not supported";
                } else {
                    offset_vec_.push_back(str_field_start_offset_);
                    DLOG(INFO) << "idx " << idx << " offset " << str_field_start_offset_;
                    str_field_start_offset_ += it->second;
                }
            }
        }
    }
}

BufNativeEncoderIRBuilder::~BufNativeEncoderIRBuilder() {}

bool BufNativeEncoderIRBuilder::BuildEncodePrimaryField(::llvm::Value* i8_ptr, size_t idx, const NativeValue& val) {
    uint32_t offset = offset_vec_.at(idx);
    return AppendPrimary(i8_ptr, val, idx, offset);
}

bool BufNativeEncoderIRBuilder::BuildEncode(::llvm::Value* output_ptr) {
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* i32_ty = builder.getInt32Ty();
    ::llvm::Value* str_addr_space_ptr = CreateAllocaAtHead(&builder, i32_ty, "str_addr_space_alloca");
    ::llvm::Value* row_size = NULL;
    bool ok = CalcTotalSize(&row_size, str_addr_space_ptr);

    if (!ok) {
        return false;
    }

    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* i8_ptr =
        ::llvm::CallInst::CreateMalloc(block_, row_size->getType(), i8_ptr_ty, row_size, nullptr, nullptr, "malloc");
    block_->getInstList().push_back(::llvm::cast<::llvm::Instruction>(i8_ptr));
    i8_ptr = builder.CreatePointerCast(i8_ptr, i8_ptr_ty);
    DLOG(INFO) << "i8_ptr type " << i8_ptr->getType()->getTypeID() << " output ptr type "
               << output_ptr->getType()->getTypeID();
    // make sure free it in c++ always
    builder.CreateStore(i8_ptr, output_ptr, false);
    // encode all field to buf
    // append header
    ok = AppendHeader(i8_ptr, row_size, builder.getInt32(::hybridse::codec::GetBitmapSize(schema_->size())));
    if (!ok) {
        return false;
    }

    ::llvm::Value* str_body_offset = NULL;
    ::llvm::Value* str_addr_space_val = NULL;
    TimestampIRBuilder timestamp_builder(block_->getModule());
    for (int32_t idx = 0; idx < schema_->size(); idx++) {
        const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
        // TODO(wangtaize) null check
        auto output_iter = outputs_->find(idx);
        if (output_iter == outputs_->end()) {
            continue;
        }

        const NativeValue& val = output_iter->second;
        switch (column.type()) {
            case ::hybridse::type::kBool:
            case ::hybridse::type::kInt16:
            case ::hybridse::type::kInt32:
            case ::hybridse::type::kInt64:
            case ::hybridse::type::kTimestamp:
            case ::hybridse::type::kDate:
            case ::hybridse::type::kFloat:
            case ::hybridse::type::kDouble: {
                uint32_t offset = offset_vec_.at(idx);
                if (val.GetType()->isFloatTy() || val.GetType()->isDoubleTy() || val.GetType()->isIntegerTy()) {
                    ok = AppendPrimary(i8_ptr, val, idx, offset);
                    if (!ok) {
                        LOG(WARNING) << "fail to append number for output col " << column.name();
                        return false;
                    }
                    break;
                } else if (codegen::TypeIRBuilder::IsTimestampPtr(val.GetType())) {
                    if (val.IsConstNull()) {
                        ok = AppendPrimary(i8_ptr,
                                           NativeValue::CreateWithFlag(builder.getInt64(0), builder.getInt1(true)), idx,
                                           offset);
                    } else {
                        ::llvm::Value* ts;
                        timestamp_builder.GetTs(block_, val.GetValue(&builder), &ts);
                        ok = AppendPrimary(i8_ptr, val.Replace(ts), idx, offset);
                    }
                    if (!ok) {
                        LOG(WARNING) << "fail to append timestamp for output col " << column.name();
                        return false;
                    }
                    break;
                } else if (codegen::TypeIRBuilder::IsDatePtr(val.GetType())) {
                    if (val.IsConstNull()) {
                        ok = AppendPrimary(i8_ptr,
                                           NativeValue::CreateWithFlag(builder.getInt32(0), builder.getInt1(true)), idx,
                                           offset);
                    } else {
                        ::llvm::Value* days;
                        DateIRBuilder date_builder(block_->getModule());
                        date_builder.GetDate(block_, val.GetValue(&builder), &days);
                        ok = AppendPrimary(i8_ptr, val.Replace(days), idx, offset);
                    }
                    if (!ok) {
                        LOG(WARNING) << "fail to append timestamp for output col " << column.name();
                        return false;
                    }
                    break;
                } else if (TypeIRBuilder::IsNull(val.GetType())) {
                    ok = AppendPrimary(
                        i8_ptr, NativeValue::CreateWithFlag(builder.getInt1(true), builder.getInt1(true)), idx, offset);
                    break;
                } else {
                    LOG(WARNING) << "number/timestamp/date type is required but " << val.GetType()->getTypeID();
                    return false;
                }
                break;
            }
            case ::hybridse::type::kVarchar: {
                if (str_body_offset == NULL) {
                    str_addr_space_val = builder.CreateLoad(i32_ty, str_addr_space_ptr, "load_str_space");
                    ok = CalcStrBodyStart(&str_body_offset, str_addr_space_val);
                    if (!ok || str_addr_space_val == NULL) {
                        return false;
                    }
                }
                uint32_t field_cnt = offset_vec_.at(idx);

                ::llvm::Value* temp_body_size = NULL;
                if (val.IsConstNull()) {
                    StringIRBuilder string_ir_builder(block_->getModule());
                    ::llvm::Value* empty_str = nullptr;
                    string_ir_builder.NewString(block_, &empty_str);
                    ok = AppendString(i8_ptr, row_size, idx,
                                      NativeValue::CreateWithFlag(empty_str, builder.getInt1(true)), str_addr_space_val,
                                      str_body_offset, field_cnt, &temp_body_size);
                } else {
                    ok = AppendString(i8_ptr, row_size, idx, val, str_addr_space_val, str_body_offset, field_cnt,
                                      &temp_body_size);
                }
                if (!ok) {
                    LOG(WARNING) << "fail to append string for output col " << column.name();
                    return false;
                }
                str_body_offset = temp_body_size;
                break;
            }
            default: {
                LOG(WARNING) << "unsuported type, append val for output col " << Type_Name(column.type());
                return false;
            }
        }
    }
    return true;
}

bool BufNativeEncoderIRBuilder::AppendString(::llvm::Value* i8_ptr, ::llvm::Value* buf_size, uint32_t field_idx,
                                             const NativeValue& str_val, ::llvm::Value* str_addr_space,
                                             ::llvm::Value* str_body_offset, uint32_t str_field_idx,
                                             ::llvm::Value** output) {
    ::llvm::IRBuilder<> builder(block_);
    StringIRBuilder string_ir_builder(block_->getModule());
    ::llvm::Type* str_ty = string_ir_builder.GetType();
    if (str_ty == NULL) {
        LOG(WARNING) << "fail to get str llvm type";
        return false;
    }

    ::llvm::Type* i8_ty = builder.getInt8Ty();
    ::llvm::Type* size_ty = builder.getInt32Ty();
    // get fe.string size
    ::llvm::Value* val_field_idx = builder.getInt32(field_idx);
    ::llvm::Value* raw_str_st = str_val.GetValue(&builder);
    ::llvm::Value* size_ptr = builder.CreateStructGEP(str_ty, raw_str_st, 0);
    ::llvm::Value* size_i32_ptr = builder.CreatePointerCast(size_ptr, size_ty->getPointerTo());
    ::llvm::Value* fe_str_size = builder.CreateLoad(size_ty, size_i32_ptr, "load_str_length");

    // get fe.string char*
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* data_ptr_ptr = builder.CreateStructGEP(str_ty, raw_str_st, 1);
    data_ptr_ptr = builder.CreatePointerCast(data_ptr_ptr, i8_ptr_ty->getPointerTo());
    ::llvm::Value* data_ptr = builder.CreateLoad(i8_ptr_ty, data_ptr_ptr, "load_str_data_ptr");
    ::llvm::Value* is_null = builder.CreateIntCast(str_val.GetIsNull(&builder), i8_ty, true);

    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction("hybridse_storage_encode_string_field",
                                                                             size_ty,    // return type
                                                                             i8_ptr_ty,  // buf ptr
                                                                             size_ty,    // buf size
                                                                             size_ty,    // col idx
                                                                             i8_ptr_ty,  // str val ptr
                                                                             size_ty,    // str val size
                                                                             i8_ty,      // is null
                                                                             size_ty,    // str_start_offset
                                                                             size_ty,    // str_field_offset
                                                                             size_ty,    // str_addr_space
                                                                             size_ty);   // str_body_offset

    if (FLAGS_enable_spark_unsaferow_format) {
        *output = builder.CreateCall(
            callee, ::llvm::ArrayRef<::llvm::Value*>{i8_ptr, buf_size, val_field_idx, data_ptr, fe_str_size, is_null,
                                                     // Notice that we pass nullbitmap size as str_field_start_offset
                                                     builder.getInt32(codec::BitMapSize(schema_->size())),
                                                     builder.getInt32(str_field_idx), str_addr_space, str_body_offset});
    } else {
        *output = builder.CreateCall(
            callee, ::llvm::ArrayRef<::llvm::Value*>{i8_ptr, buf_size, val_field_idx, data_ptr, fe_str_size, is_null,
                                                     builder.getInt32(str_field_start_offset_),
                                                     builder.getInt32(str_field_idx), str_addr_space, str_body_offset});
    }
    return true;
}

bool BufNativeEncoderIRBuilder::CalcStrBodyStart(::llvm::Value** output, ::llvm::Value* str_addr_space) {
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

bool BufNativeEncoderIRBuilder::AppendPrimary(::llvm::Value* i8_ptr, const NativeValue& val, size_t field_idx,
                                              uint32_t field_offset) {
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* offset = builder.getInt32(field_offset);
    if (val.IsNullable()) {
        ::llvm::Type* size_ty = builder.getInt32Ty();
        ::llvm::Type* i8_ty = builder.getInt8Ty();
        ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
        ::llvm::Type* void_ty = builder.getVoidTy();
        auto callee = block_->getModule()->getOrInsertFunction("hybridse_storage_encode_nullbit", void_ty, i8_ptr_ty,
                                                               size_ty, i8_ty);
        builder.CreateCall(
            callee, {i8_ptr, builder.getInt32(field_idx), builder.CreateIntCast(val.GetIsNull(&builder), i8_ty, true)});
    }
    return BuildStoreOffset(builder, i8_ptr, offset, val.GetValue(&builder));
}

bool BufNativeEncoderIRBuilder::AppendHeader(::llvm::Value* i8_ptr, ::llvm::Value* size, ::llvm::Value* bitmap_size) {
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
    ok = BuildGetPtrOffset(builder, i8_ptr, builder.getInt32(6), builder.getInt8PtrTy(), &output);
    if (!ok) {
        LOG(WARNING) << "fail to get ptr with offset ";
        return false;
    }
    builder.CreateMemSet(output, builder.getInt8(0), bitmap_size, 1u);
    return true;
}

bool BufNativeEncoderIRBuilder::CalcTotalSize(::llvm::Value** output_ptr, ::llvm::Value* str_addr_space) {
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
    StringIRBuilder string_ir_builder(block_->getModule());
    ::llvm::Type* str_ty = string_ir_builder.GetType();
    if (str_ty == NULL) {
        LOG(WARNING) << "fail to get str llvm type";
        return false;
    }
    // build get string length and call native functon
    ::llvm::Type* size_ty = builder.getInt32Ty();
    for (int32_t idx = 0; idx < schema_->size(); ++idx) {
        const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
        DLOG(INFO) << "output column " << column.name() << " " << idx;
        if (column.type() == ::hybridse::type::kVarchar) {
            const NativeValue& fe_str = outputs_->at(idx);
            ::llvm::Value* fe_str_st = fe_str.GetValue(&builder);
            if (fe_str_st == NULL) {
                LOG(WARNING) << "str output is null for " << column.name();
                return false;
            }
            ::llvm::Value* fe_str_ptr = builder.CreatePointerCast(fe_str_st, str_ty->getPointerTo());
            ::llvm::Value* size_ptr = builder.CreateStructGEP(str_ty, fe_str_ptr, 0);
            ::llvm::Value* size_i32_ptr = builder.CreatePointerCast(size_ptr, size_ty->getPointerTo());

            ::llvm::Value* fe_str_size = builder.CreateLoad(size_ty, size_i32_ptr, "load_str_length");
            fe_str_size = builder.CreateSelect(fe_str.GetIsNull(&builder), builder.getInt32(0), fe_str_size);

            if (total_size == NULL) {
                total_size = fe_str_size;
            } else {
                total_size = builder.CreateAdd(fe_str_size, total_size, "add_str_length");
            }
        }
    }

    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "hybridse_storage_encode_calc_size", size_ty, size_ty, size_ty, size_ty, size_ty->getPointerTo());
    *output_ptr = builder.CreateCall(
        callee, ::llvm::ArrayRef<::llvm::Value*>{builder.getInt32(str_field_start_offset_),
                                                 builder.getInt32(str_field_cnt_), total_size, str_addr_space});
    return true;
}

}  // namespace codegen
}  // namespace hybridse
