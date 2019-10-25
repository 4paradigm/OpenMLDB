/*
 * flatbuf_ir_builder.h
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

#ifndef CODEGEN_FLATBUF_IR_BUILDER_H_
#define CODEGEN_FLATBUF_IR_BUILDER_H_

#include "llvm/IR/IRBuilder.h"
#include "proto/type.pb.h"

namespace fesql {
namespace codegen {

typedef ::google::protobuf::RepeatedPtrField<::fesql::type::ColumnDef> Columns;

/// the common ins builder in basic block
class FlatBufDecodeIRBuilder {
public:
    // it_builder, the current block 
    // buf ,the row pointer
    // table, the schema of row 
    FlatBufDecodeIRBuilder(const ::fesql::type::TableDef* table);

    ~FlatBufDecodeIRBuilder();

    bool BuildGetTableOffset(::llvm::IRBuilder<>& builder,
            ::llvm::Value* row,
            ::llvm::LLVMContext& ctx,
            ::llvm::Value** output);

    // the pointer must not be null
    bool BuildGetVTable(::llvm::IRBuilder<>& builder, 
                          ::llvm::Value* row,
                          ::llvm::Value* table_start_offset,
                          ::llvm::LLVMContext& ctx,
                          ::llvm::Value** output);

    bool BuildGetOptionalFieldOffset(::llvm::IRBuilder<>& builder,
            ::llvm::Value* row,
            ::llvm::LLVMContext& ctx,
            ::llvm::Value* vtable_offset,
            const std::string& column,
            ::llvm::Value** output);

    bool BuildGetField(::llvm::IRBuilder<>& builder, 
            ::llvm::Value* row,
            ::llvm::Value* table_start_offset,
            ::llvm::LLVMContext& ctx,
            ::llvm::Value* field_offset,
            const std::string& column,
            ::llvm::Value** output);

    bool BuildGetStrPtr();
private:
    const ::fesql::type::TableDef* table_;
};

} // namespace of codegen
} // namespace of fesql

#endif /* !CODEGEN_FLATBUF_IR_BUILDER_H_ */
