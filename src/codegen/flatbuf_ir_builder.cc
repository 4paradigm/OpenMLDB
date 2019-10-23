/*
 * flatbuf_ir_builder.cc
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

#include "codegen/flatbuf_ir_builder.h"
#include "codegen/ir_base_builder.h"

#include "glog/logging.h"


namespace fesql {
namespace codegen {

FlatBufDecodeIRBuilder::FlatBufDecodeIRBuilder(const ::fesql::type::TableDef* table):table_(table) {}

FlatBufDecodeIRBuilder::~FlatBufDecodeIRBuilder() {}

bool FlatBufDecodeIRBuilder::BuildGetTableOffset(::llvm::IRBuilder<>& builder,
        ::llvm::Value* row,
        ::llvm::LLVMContext& ctx,
        ::llvm::Value** output) {
    if (output == NULL) {
        LOG(WARNING) << "output ptr is null";
        return false;
    }
    ::llvm::IntegerType* int32_type = ::llvm::Type::getInt32Ty(ctx);
    ::llvm::PointerType* int32_type_ptr = ::llvm::Type::getInt32PtrTy(ctx);
    ::llvm::Value* row_int32_ptr = builder.CreatePointerCast(row, int32_type_ptr, "int8_ptr_to_int32_ptr");
    *output = builder.CreateLoad(int32_type, row_int32_ptr, "load_table_start_offset");
    return true;
}

bool FlatBufDecodeIRBuilder::BuildGetVTable(::llvm::IRBuilder<>& builder, 
        ::llvm::Value* row,
        ::llvm::Value* table_start_offset,
        ::llvm::LLVMContext& ctx,
        ::llvm::Value** output) {

    if (output == NULL) {
        LOG(WARNING) << "output ptr is null";
        return false;
    }
    ::llvm::IntegerType* int32_type = ::llvm::Type::getInt32Ty(ctx);
    ::llvm::Value* vtable_offset_relative = NULL;
    bool ok = BuildLoadRelative(builder, ctx, row, table_start_offset, int32_type, &vtable_offset_relative);
    if (!ok) {
        LOG(WARNING) << "fail to build get vtable offset ins";
        return false;
    }
    *output = builder.CreateSub(table_start_offset, vtable_offset_relative, "vtable_start_offset");
    return true;
}

bool FlatBufDecodeIRBuilder::BuildGetOptionalFieldOffset(::llvm::IRBuilder<>& builder,
        ::llvm::Value* row, ::llvm::LLVMContext& ctx,
        ::llvm::Value* vtable_offset, const std::string& column_name,
        ::llvm::Value** output) {

    if (output == NULL) {
        LOG(WARNING) << "output ptr is NULL";
        return false;
    }

    uint32_t offset = 4;
    bool found = false;
    uint32_t column_size = table_->columns_size();

    for (uint32_t i = 0; i < column_size; i++) {
        const ::fesql::type::ColumnDef& column = table_->columns(i);
        if (column_name.compare(column.name()) == 0) {
            offset += i * 2;
            found = true;
        }
    }

    if (!found) {
        LOG(WARNING) << "fail to find column " << column_name << " from table "<< table_->name();
        return false;
    }
    ::llvm::IntegerType* int16_type = ::llvm::Type::getInt16Ty(ctx);
    ::llvm::Value *offset_value = builder.getInt32(offset);
    ::llvm::Value* add_field_offset = builder.CreateAdd(vtable_offset, offset_value, "vtable_start_add_field_offset");
    return BuildLoadRelative(builder, ctx, row, add_field_offset, int16_type, output);
}

bool FlatBufDecodeIRBuilder::BuildGetField(::llvm::IRBuilder<>& builder, 
            ::llvm::Value* row,
            ::llvm::Value* table_start_offset,
            ::llvm::LLVMContext& ctx,
            ::llvm::Value* field_voffset,
            const std::string& column_name,
            ::llvm::Value** output) {

    if (output == NULL) {
        LOG(WARNING) << "output ptr is NULL";
        return false;
    }

    llvm::Type* field_type = NULL;
    uint32_t column_size = table_->columns_size();
    for (uint32_t i = 0; i < column_size; i++) {
        const ::fesql::type::ColumnDef& column = table_->columns(i);
        if (column_name.compare(column.name()) == 0) {
            switch (column.type()) {
                case ::fesql::type::kBool:
                    {
                        field_type = ::llvm::Type::getInt1Ty(ctx);
                        break;
                    }
                case ::fesql::type::kInt16:
                    {
                        field_type = ::llvm::Type::getInt16Ty(ctx);
                        break;
                    }
                case ::fesql::type::kInt32:
                    {
                        field_type = ::llvm::Type::getInt32Ty(ctx);
                        break;
                    }
                case ::fesql::type::kInt64:
                    {
                        field_type = ::llvm::Type::getInt64Ty(ctx);
                        break;
                    }
                case ::fesql::type::kFloat:
                    {
                        field_type = ::llvm::Type::getFloatTy(ctx);
                        break;
                    }
                case ::fesql::type::kDouble:
                    {
                        field_type = ::llvm::Type::getDoubleTy(ctx);
                        break;
                    }
                default:
                    {
                        LOG(WARNING) << ::fesql::type::Type_Name(column.type()) << " is not supported";
                        return false;
                    }
            }
        }
    }
    ::llvm::Value* field_offset_relative = builder.CreateAdd(field_voffset, table_start_offset, "add_float_offset");
    return BuildLoadRelative(builder, ctx, row, field_offset_relative, field_type, output);
}

} // namespace of codegen
} // namespace of fesql
