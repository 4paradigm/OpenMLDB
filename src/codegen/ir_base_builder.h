/*
 * ir_base_builder.h
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

#ifndef CODEGEN_IR_BASE_BUILDER_H_
#define CODEGEN_IR_BASE_BUILDER_H_

#include "llvm/IR/IRBuilder.h"
#include "type/type_def.h"

namespace fesql {
namespace codegen {

/// the common ins builder in basic block
class FlatBufDecodeIRBuilder {
public:
    // it_builder, the current block 
    // buf ,the row pointer
    // table, the schema of row 
    FlatBufDecodeIRBuilder(::llvm::IRBuilder* ir_builder,
                           PointerType* buf,
                           std::share_ptr<::fesql::type::Table>& table);
    ~FlatBufDecodeIRBuilder();

};

} // namespace of codegen
} // namespace of fesql
#endif /* !CODEGEN_IR_BASE_BUILDER_H */
