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

#ifndef HYBRIDSE_SRC_CODEGEN_WINDOW_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_WINDOW_IR_BUILDER_H_

#include <map>
#include <string>
#include <utility>
#include <vector>
#include "codec/fe_row_codec.h"
#include "codegen/ir_base_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/fe_type.pb.h"
#include "vm/catalog.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace codegen {

class WindowDecodeIRBuilder {
 public:
    WindowDecodeIRBuilder() {}

    virtual ~WindowDecodeIRBuilder() {}

    virtual bool BuildInnerRangeList(::llvm::Value* window_ptr,
                                     ::llvm::Value* row_key, int64_t start,
                                     int64_t end, ::llvm::Value** output) = 0;
    virtual bool BuildInnerRowsList(::llvm::Value* window_ptr, int64_t start,
                                    int64_t end, ::llvm::Value** output) = 0;
    virtual bool BuildGetCol(size_t schema_idx, size_t col_idx,
                             ::llvm::Value* window_ptr,
                             ::llvm::Value** output) = 0;
};

class MemoryWindowDecodeIRBuilder : public WindowDecodeIRBuilder {
 public:
    MemoryWindowDecodeIRBuilder(const vm::SchemasContext* schemas_context,
                                ::llvm::BasicBlock* block);

    ~MemoryWindowDecodeIRBuilder();
    virtual bool BuildInnerRangeList(::llvm::Value* window_ptr,
                                     ::llvm::Value* row_key, int64_t start,
                                     int64_t end, ::llvm::Value** output);
    virtual bool BuildInnerRowsList(::llvm::Value* window_ptr, int64_t start,
                                    int64_t end, ::llvm::Value** output);

    bool BuildInnerRowsRangeList(::llvm::Value* window_ptr, ::llvm::Value* row_key, int64_t start_rows,
                                 int64_t end_range, ::llvm::Value** output);
    virtual bool BuildGetCol(size_t schema_idx, size_t col_idx,
                             ::llvm::Value* window_ptr, ::llvm::Value** output);

 private:
    bool BuildGetPrimaryCol(const std::string& fn_name, ::llvm::Value* row_ptr,
                            size_t schema_idx, size_t col_idx, uint32_t offset,
                            hybridse::node::TypeNode* type,
                            ::llvm::Value** output);

    bool BuildGetStringCol(size_t schema_idx, size_t col_idx, uint32_t offset,
                           uint32_t next_str_field_offset,
                           uint32_t str_start_offset,
                           hybridse::node::TypeNode* type,
                           ::llvm::Value* window_ptr, ::llvm::Value** output);

 private:
    ::llvm::BasicBlock* block_;
    const vm::SchemasContext* schemas_context_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_WINDOW_IR_BUILDER_H_
