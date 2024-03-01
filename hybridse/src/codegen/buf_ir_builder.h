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

#ifndef HYBRIDSE_SRC_CODEGEN_BUF_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_BUF_IR_BUILDER_H_

#include <map>
#include <string>
#include <vector>

#include "codec/fe_row_codec.h"
#include "codegen/row_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/variable_ir_builder.h"
#include "vm/catalog.h"

namespace hybridse {
namespace codegen {

class BufNativeEncoderIRBuilder : public RowEncodeIRBuilder {
 public:
    BufNativeEncoderIRBuilder(CodeGenContextBase* ctx, const std::map<uint32_t, NativeValue>* outputs,
                              const vm::Schema* schema);

    ~BufNativeEncoderIRBuilder() override;

    base::Status Init() noexcept;

    // the output_ptr like int8_t**
    base::Status BuildEncode(::llvm::Value* output_ptr) override;

    base::Status BuildEncodePrimaryField(::llvm::Value* buf, size_t idx, const NativeValue& val);

 private:
    void EnsureInitialized() const { assert(initialized_ && "BufNativeEncoderIRBuilder not explicitly initialized"); }

    base::Status CalcTotalSize();
    bool CalcStrBodyStart(::llvm::Value** output, ::llvm::Value* str_add_space);
    base::Status AppendPrimary(::llvm::Value* i8_ptr, const NativeValue& val, size_t field_idx, uint32_t field_offset);

    base::Status AppendString(::llvm::Value* i8_ptr, uint32_t field_idx, const NativeValue& str_val,
                              ::llvm::Value* str_addr_space, ::llvm::Value* str_body_offset, uint32_t str_field_idx,
                              ::llvm::Value** output);

    // encode SQL map data type into row
    base::Status AppendMapVal(const type::ColumnSchema& sc, llvm::Value* i8_ptr, uint32_t field_idx,
                              const NativeValue& val, llvm::Value* str_addr_space, llvm::Value* str_body_offset,
                              uint32_t str_field_idx, llvm::Value** next_str_body_offset);
    absl::StatusOr<llvm::Function*> GetOrBuildAppendMapFn(const type::ColumnSchema& sc) const;

    base::Status AppendHeader(::llvm::Value* i8_ptr, ::llvm::Value* size,
                      ::llvm::Value* bitmap_size);

    base::Status EncodeNullbit(::llvm::Value* i8_ptr, llvm::Value* is_null, llvm::Value* field_idx) const;

    /// write the offset bits (distance to the start of row_ptr) of string to ptr.
    /// how many bytes written is determined by str_addr_space value
    base::Status EncodeStrOffset(::llvm::Value* ptr, llvm::Value* str_offset, llvm::Value* str_col_idx,
                                 llvm::Value* str_add_space) const;

 private:
    CodeGenContextBase* ctx_;
    const std::map<uint32_t, NativeValue>* outputs_;
    const vm::Schema* schema_;
    uint32_t str_field_start_offset_;
    // n = offset_vec_[i] is
    //   schema_[i] is base type (except string): col encode offset in row
    //   otherwise: n th string-like column
    std::vector<uint32_t> offset_vec_;
    uint32_t str_field_cnt_;

    llvm::Value* row_size_ = nullptr;
    ::llvm::Value* str_addr_space_ptr_ = nullptr;

    bool initialized_ = false;
};

class BufNativeIRBuilder : public RowDecodeIRBuilder {
 public:
    BufNativeIRBuilder(CodeGenContextBase* ctx, size_t schema_idx, const codec::RowFormat* format);
    BufNativeIRBuilder(CodeGenContextBase* ctx, size_t schema_idx, const codec::RowFormat* format, ScopeVar* sv);
    ~BufNativeIRBuilder();

    bool BuildGetField(size_t col_idx, ::llvm::Value* row_ptr,
                       ::llvm::Value* row_size, NativeValue* output);

 private:
    bool BuildGetPrimaryField(const std::string& fn_name,
                              ::llvm::Value* row_ptr, uint32_t col_idx,
                              uint32_t offset, ::llvm::Type* type,
                              NativeValue* output);
    bool BuildGetStringField(uint32_t col_idx, uint32_t offset,
                             uint32_t next_str_field_offset,
                             uint32_t str_start_offset, ::llvm::Value* row_ptr,
                             ::llvm::Value* size, NativeValue* output);

 private:
    CodeGenContextBase* ctx_;
    ScopeVar* sv_;
    size_t schema_idx_;
    const codec::RowFormat* format_;
    VariableIRBuilder variable_ir_builder_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_BUF_IR_BUILDER_H_
