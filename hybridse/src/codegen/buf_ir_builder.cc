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
#include "codegen/context.h"
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/map_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "node/node_manager.h"

DECLARE_bool(enable_spark_unsaferow_format);

namespace hybridse {
namespace codegen {

BufNativeIRBuilder::BufNativeIRBuilder(CodeGenContextBase* ctx, const size_t schema_idx, const codec::RowFormat* format)
    : ctx_(ctx),
      sv_(ctx->GetCurrentScope()->sv()),
      schema_idx_(schema_idx),
      format_(format),
      variable_ir_builder_(ctx->GetCurrentBlock(), sv_) {}

BufNativeIRBuilder::BufNativeIRBuilder(CodeGenContextBase* ctx, const size_t schema_idx, const codec::RowFormat* format,
                                       ScopeVar* sv)
    : ctx_(ctx),
      sv_(sv),
      schema_idx_(schema_idx),
      format_(format),
      variable_ir_builder_(ctx->GetCurrentBlock(), sv_) {}

BufNativeIRBuilder::~BufNativeIRBuilder() {}

bool BufNativeIRBuilder::BuildGetField(size_t col_idx, ::llvm::Value* slice_ptr, ::llvm::Value* slice_size,
                                       NativeValue* output) {
    auto row_ptr = slice_ptr;
    auto row_size = slice_size;

    if (row_ptr == NULL || row_size == NULL || output == NULL) {
        LOG(WARNING) << "input args have null";
        return false;
    }

    const codec::ColInfo* col_info = format_->GetColumnInfo(schema_idx_, col_idx);
    if (col_info == nullptr) {
        LOG(WARNING) << "fail to resolve field info at " << col_idx;
        return false;
    }

    // Get the corrected column index from RowFormat
    auto row_format_corrected_col_idx = col_info->idx;

    node::NodeManager tmp_nm;
    auto s = ColumnSchema2Type(col_info->schema, &tmp_nm);
    if (!s.ok()) {
        LOG(WARNING) << s.value();
        return false;
    }
    uint32_t offset = col_info->offset;

    auto& builder = *ctx_->GetBuilder();
    switch (s.value()->base()) {
        case ::hybridse::node::kBool: {
            llvm::Type* bool_ty = builder.getInt1Ty();
            return BuildGetPrimaryField("hybridse_storage_get_bool_field", row_ptr, row_format_corrected_col_idx,
                                        offset, bool_ty, output);
        }
        case ::hybridse::node::kInt16: {
            llvm::Type* i16_ty = builder.getInt16Ty();
            return BuildGetPrimaryField("hybridse_storage_get_int16_field", row_ptr, row_format_corrected_col_idx,
                                        offset, i16_ty, output);
        }
        case ::hybridse::node::kInt32: {
            llvm::Type* i32_ty = builder.getInt32Ty();
            return BuildGetPrimaryField("hybridse_storage_get_int32_field", row_ptr, row_format_corrected_col_idx,
                                        offset, i32_ty, output);
        }
        case ::hybridse::node::kInt64: {
            llvm::Type* i64_ty = builder.getInt64Ty();
            return BuildGetPrimaryField("hybridse_storage_get_int64_field", row_ptr, row_format_corrected_col_idx,
                                        offset, i64_ty, output);
        }
        case ::hybridse::node::kFloat: {
            llvm::Type* float_ty = builder.getFloatTy();
            return BuildGetPrimaryField("hybridse_storage_get_float_field", row_ptr, row_format_corrected_col_idx,
                                        offset, float_ty, output);
        }
        case ::hybridse::node::kDouble: {
            llvm::Type* double_ty = builder.getDoubleTy();
            return BuildGetPrimaryField("hybridse_storage_get_double_field", row_ptr, row_format_corrected_col_idx,
                                        offset, double_ty,
                                        output);
        }
        case ::hybridse::node::kTimestamp: {
            NativeValue int64_val;
            if (!BuildGetPrimaryField("hybridse_storage_get_int64_field", row_ptr, row_format_corrected_col_idx, offset,
                                      builder.getInt64Ty(), &int64_val)) {
                return false;
            }
            codegen::TimestampIRBuilder timestamp_builder(ctx_->GetModule());
            llvm::Value* ts_st = nullptr;
            if (!timestamp_builder.NewTimestamp(ctx_->GetCurrentBlock(), int64_val.GetValue(&builder), &ts_st)) {
                return false;
            }
            *output = int64_val.Replace(ts_st);
            return true;
        }
        case ::hybridse::node::kDate: {
            NativeValue int32_val;
            if (!BuildGetPrimaryField("hybridse_storage_get_int32_field", row_ptr, row_format_corrected_col_idx, offset,
                                      builder.getInt32Ty(), &int32_val)) {
                return false;
            }
            codegen::DateIRBuilder date_ir_builder(ctx_->GetModule());
            llvm::Value* ts_st = nullptr;
            if (!date_ir_builder.NewDate(ctx_->GetCurrentBlock(), int32_val.GetValue(&builder), &ts_st)) {
                return false;
            }
            *output = int32_val.Replace(ts_st);
            return true;
        }

        case ::hybridse::node::kVarchar: {
            auto s = format_->GetStringColumnInfo(schema_idx_, col_idx);
            if (!s.ok()) {
                LOG(WARNING) << "fail to get string filed offset and next offset " << s.status();
                return false;
            }
            auto& str_info = s.value();
            DLOG(INFO) << "get string with offset " << offset << " next offset " << str_info.str_next_offset
                       << " for col " << col_idx;
            return BuildGetStringField(str_info.idx, offset, str_info.str_next_offset, str_info.str_start_offset,
                                       row_ptr, row_size, output);
        }
        case ::hybridse::node::kMap: {
            auto si = format_->GetStringColumnInfo(schema_idx_, col_idx);
            if (!si.ok()) {
                LOG(WARNING) << "fail to get string filed offset and next offset " << s.status();
                return false;
            }
            auto& str_info = si.value();
            NativeValue str_val;
            if (!BuildGetStringField(str_info.idx, col_info->offset, str_info.str_next_offset,
                                     str_info.str_start_offset, row_ptr, row_size, &str_val)) {
                LOG(WARNING) << "get str data for map failed";
                return false;
            }

            auto map_type = s.value()->GetAsOrNull<node::MapType>();
            llvm::Type* key_type = nullptr;
            if (!GetLlvmType(ctx_->GetCurrentBlock(), map_type->key_type(), &key_type)) {
                return false;
            }
            llvm::Type* value_type = nullptr;
            if (!GetLlvmType(ctx_->GetCurrentBlock(), map_type->value_type(), &value_type)) {
                return false;
            }

            MapIRBuilder map_builder(ctx_->GetModule(), key_type, value_type);

            auto raw = str_val.GetValue(&builder);
            llvm::Value* map_encoded_str = builder.CreateLoad(builder.CreateStructGEP(raw, 1, "load_map_encoded_str"));
            auto res = map_builder.Decode(ctx_, map_encoded_str);
            if (!res.ok()) {
                return false;
            }

            *output = NativeValue::CreateWithFlag(res.value(), str_val.GetIsNull(&builder));
            return true;
        }
        default: {
            LOG(WARNING) << "fail to get col for type: " << s.value()->DebugString();
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
    auto* builder = ctx_->GetBuilder();
    ::llvm::Type* i8_ptr_ty = builder->getInt8PtrTy();
    ::llvm::Type* i32_ty = builder->getInt32Ty();
    ::llvm::Value* val_col_idx = builder->getInt32(col_idx);
    ::llvm::Value* val_offset = builder->getInt32(offset);
    ::llvm::Value* is_null_alloca = CreateAllocaAtHead(builder, builder->getInt8Ty(), "is_null_addr");
    ::llvm::FunctionCallee callee =
        ctx_->GetModule()->getOrInsertFunction(fn_name, type, i8_ptr_ty, i32_ty, i32_ty, i8_ptr_ty);

    ::llvm::Value* raw = builder->CreateCall(callee, {row_ptr, val_col_idx, val_offset, is_null_alloca});
    ::llvm::Value* is_null = builder->CreateLoad(is_null_alloca);
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

    auto builder = ctx_->GetBuilder();
    ::llvm::Type* i32_ty = builder->getInt32Ty();
    ::llvm::Type* i8_ty = builder->getInt8Ty();
    ::llvm::FunctionCallee addr_space_callee =
        ctx_->GetModule()->getOrInsertFunction("hybridse_storage_get_str_addr_space", i8_ty, i32_ty);
    ::llvm::Value* str_addr_space = builder->CreateCall(addr_space_callee, {size});
    str_addr_space = builder->CreateIntCast(str_addr_space, i32_ty, true, "cast_i8_to_i32");
    codegen::StringIRBuilder string_ir_builder(ctx_->GetModule());

    // alloca memory on stack
    ::llvm::Value* string_ref;
    if (!string_ir_builder.NewString(ctx_->GetCurrentBlock(), &string_ref)) {
        LOG(WARNING) << "fail to initialize string ref";
        return false;
    }
    ::llvm::Value* data_ptr_ptr = builder->CreateStructGEP(string_ir_builder.GetType(), string_ref, 1);

    ::llvm::Type* i8_ptr_ty = builder->getInt8PtrTy();
    ::llvm::Type* bool_ptr_ty = builder->getInt1Ty()->getPointerTo();

    // get str field declear
    ::llvm::FunctionCallee callee = ctx_->GetModule()->getOrInsertFunction(
        "hybridse_storage_get_str_field", i32_ty, i8_ptr_ty, i32_ty, i32_ty, i32_ty, i32_ty, i32_ty,
        i8_ptr_ty->getPointerTo(), i32_ty->getPointerTo(), bool_ptr_ty);

    ::llvm::Value* str_offset = builder->getInt32(offset);
    ::llvm::Value* val_col_idx = builder->getInt32(col_idx);
    ::llvm::Value* next_str_offset = builder->getInt32(next_str_field_offset);
    // get the data ptr
    data_ptr_ptr = builder->CreatePointerCast(data_ptr_ptr, i8_ptr_ty->getPointerTo());
    ::llvm::Value* size_ptr = builder->CreateStructGEP(string_ir_builder.GetType(), string_ref, 0);
    // get the size ptr
    size_ptr = builder->CreatePointerCast(size_ptr, i32_ty->getPointerTo());

    // null flag
    ::llvm::Type* bool_ty = builder->getInt1Ty();
    ::llvm::Value* is_null_alloca = CreateAllocaAtHead(builder, bool_ty, "string_is_null");

    // TODO(wangtaize) add status check
    builder->CreateCall(callee, {row_ptr, val_col_idx, str_offset, next_str_offset, builder->getInt32(str_start_offset),
                                str_addr_space, data_ptr_ptr, size_ptr, is_null_alloca});

    ::llvm::Value* is_null = builder->CreateLoad(is_null_alloca);
    *output = NativeValue::CreateWithFlag(string_ref, is_null);
    return true;
}

BufNativeEncoderIRBuilder::BufNativeEncoderIRBuilder(CodeGenContextBase* ctx,
                                                     const std::map<uint32_t, NativeValue>* outputs,
                                                     const vm::Schema* schema)
    : ctx_(ctx),
      outputs_(outputs),
      schema_(schema),
      str_field_start_offset_(0),
      offset_vec_(),
      str_field_cnt_(0) {
    offset_vec_.resize(schema_->size(), 0);
}

BufNativeEncoderIRBuilder::~BufNativeEncoderIRBuilder() {}

base::Status BufNativeEncoderIRBuilder::BuildEncodePrimaryField(::llvm::Value* i8_ptr, size_t idx,
                                                                const NativeValue& val) {
    EnsureInitialized();

    uint32_t offset = offset_vec_.at(idx);
    CHECK_STATUS(AppendPrimary(i8_ptr, val, idx, offset), "Fail to encode primary field")
    return base::Status::OK();
}

base::Status BufNativeEncoderIRBuilder::BuildEncode(::llvm::Value* output_ptr) {
    EnsureInitialized();

    // things geting wired when putting CalcTotalSize in Init():
    // AggregateIRBuilder has non-standard usage of BufNativeEncoderIRBuilder
    CHECK_STATUS(CalcTotalSize(), "Fail to calculate row's size")

    auto& builder_ = *ctx_->GetBuilder();
    auto* block_ = ctx_->GetCurrentBlock();

    ::llvm::Type* i8_ptr_ty = builder_.getInt8PtrTy();
    ::llvm::Value* i8_ptr =
        ::llvm::CallInst::CreateMalloc(block_, row_size_->getType(), i8_ptr_ty, row_size_, nullptr, nullptr, "malloc");
    block_->getInstList().push_back(::llvm::cast<::llvm::Instruction>(i8_ptr));
    i8_ptr = builder_.CreatePointerCast(i8_ptr, i8_ptr_ty);
    DLOG(INFO) << "i8_ptr type " << i8_ptr->getType() << " output ptr type " << output_ptr->getType();
    // make sure free it in c++ always
    builder_.CreateStore(i8_ptr, output_ptr, false);
    // encode all field to buf
    // append header
    CHECK_STATUS(AppendHeader(i8_ptr, row_size_, builder_.getInt32(::hybridse::codec::GetBitmapSize(schema_->size()))),
                 "Fail to encode row header");


    ::llvm::Value* str_body_start_offset = nullptr;
    llvm::Value* str_addr_space_val = builder_.CreateLoad(builder_.getInt32Ty(), str_addr_space_ptr_, "load_str_space");
    CHECK_TRUE(CalcStrBodyStart(&str_body_start_offset, str_addr_space_val) && str_addr_space_val != NULL,
               common::kCodegenEncodeError, "Fail to calculate the start offset of string body");

    TimestampIRBuilder timestamp_builder(block_->getModule());
    node::NodeManager tmp_nm;
    for (int32_t idx = 0; idx < schema_->size(); idx++) {
        const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
        auto& col_schema = column.schema();
        // TODO(wangtaize) null check
        auto output_iter = outputs_->find(idx);
        if (output_iter == outputs_->end()) {
            continue;
        }

        const NativeValue& val = output_iter->second;
        if (codec::IsCodecBaseType(col_schema)) {
            uint32_t offset = offset_vec_.at(idx);
            if (val.GetType()->isFloatTy() || val.GetType()->isDoubleTy() || val.GetType()->isIntegerTy()) {
                CHECK_STATUS(AppendPrimary(i8_ptr, val, idx, offset), "Fail to encode number column", column.name());
            } else if (codegen::TypeIRBuilder::IsTimestampPtr(val.GetType())) {
                if (val.IsConstNull()) {
                    CHECK_STATUS(
                        AppendPrimary(i8_ptr, NativeValue::CreateWithFlag(builder_.getInt64(0), builder_.getInt1(true)),
                                      idx, offset),
                        "Fail to encode timestamp column ", column.name())
                } else {
                    ::llvm::Value* ts;
                    timestamp_builder.GetTs(block_, val.GetValue(&builder_), &ts);
                    CHECK_STATUS(AppendPrimary(i8_ptr, val.Replace(ts), idx, offset),
                                 "Fail to encode timestamp column ", column.name())
                }
            } else if (codegen::TypeIRBuilder::IsDatePtr(val.GetType())) {
                if (val.IsConstNull()) {
                    CHECK_STATUS(
                        AppendPrimary(i8_ptr, NativeValue::CreateWithFlag(builder_.getInt32(0), builder_.getInt1(true)),
                                      idx, offset),
                        "Fail to encode date column ", column.name())
                } else {
                    ::llvm::Value* days;
                    DateIRBuilder date_builder(block_->getModule());
                    date_builder.GetDate(block_, val.GetValue(&builder_), &days);
                    CHECK_STATUS(AppendPrimary(i8_ptr, val.Replace(days), idx, offset), "Fail to encode date column ",
                                 column.name())
                }
            } else if (TypeIRBuilder::IsNull(val.GetType())) {
                CHECK_STATUS(
                    AppendPrimary(i8_ptr, NativeValue::CreateWithFlag(builder_.getInt1(true), builder_.getInt1(true)),
                                  idx, offset),
                    "Fail to encode NULL column ", column.name())
            } else {
                FAIL_STATUS(common::kCodegenEncodeError,
                            "Invalid column type, number/timestamp/date type is required but got ", val.GetType());
            }
        } else if (col_schema.has_base_type() && ::hybridse::type::kVarchar == col_schema.base_type()) {
            uint32_t field_cnt = offset_vec_.at(idx);

            ::llvm::Value* str_body_end_offset = nullptr;
            if (val.IsConstNull()) {
                StringIRBuilder string_ir_builder(block_->getModule());
                ::llvm::Value* empty_str = nullptr;
                string_ir_builder.NewString(block_, &empty_str);
                CHECK_STATUS(AppendString(i8_ptr, idx, NativeValue::CreateWithFlag(empty_str, builder_.getInt1(true)),
                                          str_addr_space_val, str_body_start_offset, field_cnt, &str_body_end_offset),
                             "Fail to encode string column ", column.name())
            } else {
                CHECK_STATUS(AppendString(i8_ptr, idx, val, str_addr_space_val, str_body_start_offset, field_cnt,
                                          &str_body_end_offset),
                             "Fail to encode string column ", column.name())
            }
            str_body_start_offset = str_body_end_offset;
        } else if (col_schema.has_map_type()) {
            llvm::Value* is_null = val.GetIsNull(&builder_);

            auto fn_res = GetOrBuildAppendMapFn(col_schema);
            CHECK_TRUE(fn_res.ok(), common::kCodegenError, fn_res.status());

            uint32_t field_cnt = offset_vec_.at(idx);
            str_body_start_offset = builder_.CreateCall(
                fn_res.value(), {i8_ptr, val.GetValue(&builder_), is_null, builder_.getInt32(idx), str_addr_space_val,
                                 str_body_start_offset, builder_.getInt32(field_cnt)});

        } else {
            FAIL_STATUS(common::kCodegenEncodeError, "UnSupport encode column for base type ", col_schema.DebugString())
        }
    }

    return base::Status::OK();
}

base::Status BufNativeEncoderIRBuilder::AppendString(::llvm::Value* i8_ptr, uint32_t field_idx,
                                                     const NativeValue& str_val, ::llvm::Value* str_addr_space,
                                                     ::llvm::Value* str_body_offset, uint32_t str_field_idx,
                                                     ::llvm::Value** output) {
    auto& builder = *ctx_->GetBuilder();
    StringIRBuilder string_ir_builder(ctx_->GetModule());
    ::llvm::Type* str_ty = string_ir_builder.GetType();
    CHECK_TRUE(str_ty != NULL, common::kCodegenEncodeError, "Fail to get str llvm type")

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

    ::llvm::FunctionCallee callee = ctx_->GetModule()->getOrInsertFunction("hybridse_storage_encode_string_field",
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
            callee, ::llvm::ArrayRef<::llvm::Value*>{i8_ptr, row_size_, val_field_idx, data_ptr, fe_str_size, is_null,
                                                     // Notice that we pass nullbitmap size as str_field_start_offset
                                                     builder.getInt32(codec::BitMapSize(schema_->size())),
                                                     builder.getInt32(str_field_idx), str_addr_space, str_body_offset});
    } else {
        *output = builder.CreateCall(
            callee, ::llvm::ArrayRef<::llvm::Value*>{i8_ptr, row_size_, val_field_idx, data_ptr, fe_str_size, is_null,
                                                     builder.getInt32(str_field_start_offset_),
                                                     builder.getInt32(str_field_idx), str_addr_space, str_body_offset});
    }
    return base::Status::OK();
}

absl::StatusOr<llvm::Function*> BufNativeEncoderIRBuilder::GetOrBuildAppendMapFn(const type::ColumnSchema& sc) const {
    auto* builder = ctx_->GetBuilder();
    auto* block_ = ctx_->GetCurrentBlock();
    node::NodeManager tmp_nm;
    auto res = ColumnSchema2Type(sc, &tmp_nm);
    CHECK_ABSL_STATUSOR(res);
    auto* map_type = res.value()->GetAsOrNull<node::MapType>();
    if (map_type == nullptr) {
        return absl::InternalError("can not casting to MapType");
    }
    llvm::Type* key_type = nullptr;
    if (!GetLlvmType(block_, map_type->key_type(), &key_type)) {
        return absl::InternalError("can not get llvm type for map key");
    }
    llvm::Type* value_type = nullptr;
    if (!GetLlvmType(block_, map_type->value_type(), &value_type)) {
        return absl::InternalError("can not get llvm type for map value");
    }

    MapIRBuilder map_builder(block_->getModule(), key_type, value_type);
    std::string fn_name = absl::StrCat("encode_map_val_", GetIRTypeName(map_builder.GetType()));

    llvm::Function* fn = ctx_->GetModule()->getFunction(fn_name);
    if (fn != nullptr) {
        return fn;
    }

    // define function
    auto* fnt = llvm::FunctionType::get(builder->getInt32Ty(),                   // return next str body offset
                                        {builder->getInt8Ty()->getPointerTo(),   // row ptr
                                         map_builder.GetType()->getPointerTo(),  // map ptr
                                         builder->getInt1Ty(),                   // is null
                                         builder->getInt32Ty(),                  // field idx
                                         builder->getInt32Ty(),                  // str addr space
                                         builder->getInt32Ty(),                  // str body offset for this column
                                         builder->getInt32Ty()},                 // str field idx
                                        false);
    fn = llvm::Function::Create(fnt, llvm::Function::ExternalLinkage, fn_name, ctx_->GetModule());

    {
        // enter function definition
        FunctionScopeGuard fg(fn, ctx_);
        auto* sub_builder = ctx_->GetBuilder();

        llvm::Value* i8_ptr = fn->arg_begin();                  // original allocated row ptr
        llvm::Value* map_ptr = fn->arg_begin() + 1;             //  map pointer
        llvm::Value* is_null = fn->arg_begin() + 2;             // map is null
        llvm::Value* idx = fn->arg_begin() + 3;                 // column index (zero-based)
        llvm::Value* str_addr_space_val = fn->arg_begin() + 4;  // str address space value
        llvm::Value* str_body_offset =
            fn->arg_begin() + 5;                           // offset in bytes from row ptr, true encoded content for
                                                           // string-like columns will written starting from this offset
        llvm::Value* str_field_idx = fn->arg_begin() + 6;  // index for string-like columns (zero-based)

        CHECK_STATUS_TO_ABSL(EncodeNullbit(i8_ptr, is_null, idx));
        // write str offset
        CHECK_STATUS_TO_ABSL(EncodeStrOffset(i8_ptr, str_body_offset, str_field_idx, str_addr_space_val))

        llvm::Value* encode_sz_alloca =
            sub_builder->CreateAlloca(sub_builder->getInt32Ty(), nullptr, "map_encode_sz_alloca");
        sub_builder->CreateStore(sub_builder->getInt32(0), encode_sz_alloca);

        auto bs = ctx_->CreateBranchNot(is_null, [&]() -> base::Status {
            auto row_ptr = BuildGetPtrOffset(sub_builder, i8_ptr, str_body_offset);
            CHECK_TRUE(row_ptr.ok(), common::kCodegenError, row_ptr.status().ToString());
            auto sz = map_builder.Encode(ctx_, map_ptr, row_ptr.value());
            CHECK_TRUE(sz.ok(), common::kCodegenError, sz.status().ToString());
            sub_builder->CreateStore(sz.value(), encode_sz_alloca);
            return {};
        }, "encode_map_if_not_null");
        CHECK_STATUS_TO_ABSL(bs);

        sub_builder->CreateRet(sub_builder->CreateAdd(
            str_body_offset, sub_builder->CreateLoad(sub_builder->getInt32Ty(), encode_sz_alloca)));
    }

    return fn;
}

bool BufNativeEncoderIRBuilder::CalcStrBodyStart(::llvm::Value** output, ::llvm::Value* str_addr_space) {
    if (output == NULL || str_addr_space == NULL) {
        LOG(WARNING) << "CalcStrBodyStart#output is null";
        return false;
    }
    auto& builder_ = *ctx_->GetBuilder();
    ::llvm::Value* str_field_start = builder_.getInt32(str_field_start_offset_);
    ::llvm::Value* str_field_cnt = builder_.getInt32(str_field_cnt_);
    ::llvm::Value* temp = builder_.CreateMul(str_field_cnt, str_addr_space);
    *output = builder_.CreateAdd(str_field_start, temp);
    return true;
}

base::Status BufNativeEncoderIRBuilder::AppendPrimary(::llvm::Value* i8_ptr, const NativeValue& val, size_t field_idx,
                                              uint32_t field_offset) {
    auto& builder_ = *ctx_->GetBuilder();
    ::llvm::Value* offset = builder_.getInt32(field_offset);
    if (val.IsNullable()) {
        CHECK_STATUS(EncodeNullbit(i8_ptr, val.GetIsNull(&builder_), builder_.getInt32(field_idx)));
    }
    auto s = BuildStoreOffset(&builder_, i8_ptr, offset, val.GetValue(&builder_));
    CHECK_TRUE(s.ok(), common::kCodegenEncodeError, s.ToString());
    return base::Status::OK();
}

base::Status BufNativeEncoderIRBuilder::AppendHeader(::llvm::Value* i8_ptr, ::llvm::Value* size,
                                                     ::llvm::Value* bitmap_size) {
    auto& builder_ = *ctx_->GetBuilder();
    ::llvm::Value* fversion = builder_.getInt8(1);
    ::llvm::Value* sversion = builder_.getInt8(1);
    ::llvm::Value* fverion_offset = builder_.getInt32(0);
    ::llvm::Value* sverion_offset = builder_.getInt32(1);
    {
        auto s = BuildStoreOffset(&builder_, i8_ptr, fverion_offset, fversion);
        CHECK_TRUE(s.ok(), common::kCodegenEncodeError, "fail to encode fversion to row: ", s.ToString());
    }

    {
        auto s = BuildStoreOffset(&builder_, i8_ptr, sverion_offset, sversion);
        CHECK_TRUE(s.ok(), common::kCodegenEncodeError, "fail to encode sversion to row: ", s.ToString());
    }

    {
        ::llvm::Value* size_offset = builder_.getInt32(2);
        auto s = BuildStoreOffset(&builder_, i8_ptr, size_offset, size);
        CHECK_TRUE(s.ok(), common::kCodegenEncodeError, "fail to encode size to row: ", s.ToString());
    }

    auto s = BuildGetPtrOffset(&builder_, i8_ptr, builder_.getInt32(6));
    CHECK_TRUE(s.ok(), common::kCodegenEncodeError, s.status().ToString());
    builder_.CreateMemSet(s.value(), builder_.getInt8(0), bitmap_size, 1u);
    return base::Status::OK();
}

base::Status BufNativeEncoderIRBuilder::CalcTotalSize() {
    auto& builder_ = *ctx_->GetBuilder();
    auto* block_ = ctx_->GetCurrentBlock();
    if (str_field_cnt_ <= 0 || schema_->size() == 0) {
        row_size_ = builder_.getInt32(str_field_start_offset_);
        return base::Status::OK();
    }

    StringIRBuilder string_ir_builder(block_->getModule());
    ::llvm::Type* str_ty = string_ir_builder.GetType();

    CHECK_TRUE(str_ty != NULL, common::kCodegenError, "Fail to get str llvm type")
    // initialize total string length as int32 zero
    ::llvm::Value* total_size = builder_.getInt32(0);
    ::llvm::Type* size_ty = builder_.getInt32Ty();
    node::NodeManager tmp_nm;

    // go through columns value, accumulate string length if there is a string type column
    for (int32_t idx = 0; idx < schema_->size(); ++idx) {
        const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
        DLOG(INFO) << "output column " << column.name() << " " << idx;
        if (column.schema().has_base_type()) {
            if (column.schema().base_type() == ::hybridse::type::kVarchar) {
                const NativeValue& fe_str = outputs_->at(idx);
                // skip accumulate string length once the column is const NULL string
                if (fe_str.IsConstNull()) {
                    continue;
                }
                // build get string length and call native function
                ::llvm::Value* fe_str_st = fe_str.GetValue(&builder_);
                CHECK_TRUE(fe_str_st != NULL, common::kCodegenEncodeError, "String output is null for ", column.name())
                ::llvm::Value* fe_str_ptr = builder_.CreatePointerCast(fe_str_st, str_ty->getPointerTo());
                ::llvm::Value* size_ptr = builder_.CreateStructGEP(str_ty, fe_str_ptr, 0);
                ::llvm::Value* size_i32_ptr = builder_.CreatePointerCast(size_ptr, size_ty->getPointerTo());

                ::llvm::Value* fe_str_size = builder_.CreateLoad(size_ty, size_i32_ptr, "load_str_length");
                fe_str_size = builder_.CreateSelect(fe_str.GetIsNull(&builder_), builder_.getInt32(0), fe_str_size);

                total_size = builder_.CreateAdd(fe_str_size, total_size, "add_str_length");
            }
        } else if (column.schema().has_map_type()) {
            auto res = ColumnSchema2Type(column.schema(), &tmp_nm);
            CHECK_TRUE(res.ok(), common::kTypeError, res.status().ToString());
            auto map_type = res.value()->GetAsOrNull<node::MapType>();
            CHECK_TRUE(map_type != nullptr, common::kCodegenError);
            llvm::Type* key_type = nullptr;
            CHECK_TRUE(GetLlvmType(block_, map_type->key_type(), &key_type), common::kTypeError);
            llvm::Type* value_type = nullptr;
            CHECK_TRUE(GetLlvmType(block_, map_type->value_type(), &value_type), common::kTypeError);

            MapIRBuilder map_builder(block_->getModule(), key_type, value_type);
            const auto& val = outputs_->at(idx);
            auto map_sz_res = map_builder.CalEncodeByteSize(ctx_, val.GetValue(&builder_));
            CHECK_TRUE(map_sz_res.ok(), common::kCodegenError, map_sz_res.status().ToString());

            llvm::Value* sz = builder_.CreateSelect(val.GetIsNull(&builder_), builder_.getInt32(0), map_sz_res.value());
            total_size = builder_.CreateAdd(total_size, sz, "add_map_sz_2_total_sz");
        } else {
            FAIL_STATUS(common::kTypeError, "unimplemented encoding for type ", column.schema().DebugString());
        }
    }

    ::llvm::FunctionCallee callee = block_->getModule()->getOrInsertFunction(
        "hybridse_storage_encode_calc_size", size_ty, size_ty, size_ty, size_ty, size_ty->getPointerTo());
    row_size_ = builder_.CreateCall(callee, {builder_.getInt32(str_field_start_offset_),
                                             builder_.getInt32(str_field_cnt_), total_size, str_addr_space_ptr_});

    return base::Status::OK();
}

base::Status BufNativeEncoderIRBuilder::Init() noexcept {
    auto row_size_without_string_cols = codec::GetStartOffset(schema_->size());
    uint32_t string_cols_cnt = 0;
    for (int32_t idx = 0; idx < schema_->size(); idx++) {
        // Support Spark UnsafeRow format where all fields will take up 8 bytes
        if (FLAGS_enable_spark_unsaferow_format) {
            offset_vec_.at(idx) = row_size_without_string_cols;
            row_size_without_string_cols += 8;
            const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
            if (column.type() == ::hybridse::type::kVarchar) {
                string_cols_cnt++;
            }
        } else {
            const ::hybridse::type::ColumnDef& column = schema_->Get(idx);
            if (!codec::IsCodecBaseType(column.schema())) {
                // string-like column
                offset_vec_.at(idx) = string_cols_cnt++;
            } else {
                auto& TYPE_SIZE_MAP = codec::GetTypeSizeMap();
                auto it = TYPE_SIZE_MAP.find(column.type());
                if (it == TYPE_SIZE_MAP.end()) {
                    FAIL_STATUS(common::kTypeError, "unimplemented encoding for ", column.schema().DebugString());
                }

                offset_vec_.at(idx) = row_size_without_string_cols;
                DLOG(INFO) << "idx " << idx << " offset " << row_size_without_string_cols;
                row_size_without_string_cols += it->second;
            }
        }
    }

    str_field_start_offset_ = row_size_without_string_cols;
    str_field_cnt_ = string_cols_cnt;
    auto& builder_ = *ctx_->GetBuilder();
    str_addr_space_ptr_ = CreateAllocaAtHead(&builder_, builder_.getInt32Ty(), "str_addr_space_alloca");

    initialized_ = true;
    return {};
}

base::Status BufNativeEncoderIRBuilder::EncodeNullbit(::llvm::Value* i8_ptr, llvm::Value* is_null,
                                                      llvm::Value* field_idx) const {
    auto* builder = ctx_->GetBuilder();
    ::llvm::Type* size_ty = builder->getInt32Ty();
    ::llvm::Type* i8_ty = builder->getInt8Ty();
    ::llvm::Type* i8_ptr_ty = builder->getInt8PtrTy();
    ::llvm::Type* void_ty = builder->getVoidTy();
    auto callee =
        ctx_->GetModule()->getOrInsertFunction("hybridse_storage_encode_nullbit", void_ty, i8_ptr_ty, size_ty, i8_ty);
    builder->CreateCall(callee, {i8_ptr, field_idx, builder->CreateIntCast(is_null, i8_ty, true)});
    return {};
}

base::Status BufNativeEncoderIRBuilder::EncodeStrOffset(::llvm::Value* ptr, llvm::Value* str_offset,
                                                        llvm::Value* str_col_idx,
                                                        llvm::Value* str_addr_space_val) const {
    auto* builder = ctx_->GetBuilder();
    auto* block_ = ctx_->GetCurrentBlock();
    llvm::Value* str_sz_arena_offset = builder->CreateAdd(builder->getInt32(str_field_start_offset_),
                                                          builder->CreateMul(str_addr_space_val, str_col_idx));
    auto ptr_offset = BuildGetPtrOffset(builder, ptr, str_sz_arena_offset, builder->getInt8Ty()->getPointerTo());
    CHECK_TRUE(ptr_offset.ok(), common::kCodegenError, ptr_offset.status().ToString());

    // write
    ::llvm::FunctionCallee callee =
        block_->getModule()->getOrInsertFunction("hybridse_storage_encode_string_offset",
                                                 builder->getVoidTy(),                  // return type
                                                 builder->getInt8Ty()->getPointerTo(),  // str_offset_ptr
                                                 builder->getInt32Ty(),                 // str offset
                                                 builder->getInt32Ty());                // str addr space

    builder->CreateCall(callee, {ptr_offset.value(), str_offset, str_addr_space_val});

    return {};
}
}  // namespace codegen
}  // namespace hybridse
