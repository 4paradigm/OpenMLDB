/*
 * row_ir_builder.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#include "llvm/IR/IRBuilder.h"


namespace fesql {
namespace codegen {

class RowDecodeIRBuilder {

 public:

    RowDecodeIRBuilder() {}

    virtual ~RowDecodeIRBuilder() {}

    // get the one field of row
    // name, the column name
    // row_ptr, the row object ptr
    // row_size, the row ptr size
    // output, the output value
    virtual bool BuildGetField(const std::string& name, 
            ::llvm::Value* row_ptr,
            ::llvm::Value* row_size, ::llvm::Value** output) = 0;

};

class RowEncodeIRBuilder {

 public:

    RowEncodeIRBuilder() {}

    virtual ~RowEncodeIRBuilder() {}

    // build the encode ir, output  the row data to output ptr
    virtual bool BuildEncode(::llvm::Value* output_ptr) = 0;

};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_ROW_IR_BUILDER_H_ 
