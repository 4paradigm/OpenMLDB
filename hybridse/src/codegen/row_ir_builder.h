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

#ifndef HYBRIDSE_SRC_CODEGEN_ROW_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_ROW_IR_BUILDER_H_

#include "base/fe_status.h"
#include "codegen/native_value.h"

namespace hybridse {
namespace codegen {

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
    virtual base::Status BuildEncode(::llvm::Value* output_ptr) = 0;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_ROW_IR_BUILDER_H_
