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

#ifndef SRC_CODEGEN_ROW_IR_BUILDER_H_
#define SRC_CODEGEN_ROW_IR_BUILDER_H_

#include <string>
#include "codegen/native_value.h"
#include "llvm/IR/IRBuilder.h"
#include "node/node_enum.h"

namespace hybridse {
namespace codegen {

class Decoder {
 public:
    Decoder() {}
    virtual ~Decoder() {}
    virtual bool GetColOffsetType(const std::string& name, uint32_t* col_idx,
                                  uint32_t* offset_ptr,
                                  node::DataType* type_ptr) = 0;
};

class RowDecodeIRBuilder {
 public:
    RowDecodeIRBuilder() {}

    virtual ~RowDecodeIRBuilder() {}

    virtual bool BuildGetField(size_t col_idx, ::llvm::Value* row_ptr,
                               ::llvm::Value* row_size,
                               NativeValue* output) = 0;
};

class RowEncodeIRBuilder {
 public:
    RowEncodeIRBuilder() {}

    virtual ~RowEncodeIRBuilder() {}

    // build the encode ir, output  the row data to output ptr
    virtual bool BuildEncode(::llvm::Value* output_ptr) = 0;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // SRC_CODEGEN_ROW_IR_BUILDER_H_
