/*
 * buf_ir_builder.h
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

#ifndef SRC_CODEGEN_BUF_IR_BUILDER_H_
#define SRC_CODEGEN_BUF_IR_BUILDER_H_

#include <map>
#include <utility>
#include <string>
#include "proto/type.pb.h"
#include "llvm/IR/IRBuilder.h"
#include "codegen/scope_var.h"


namespace fesql {
namespace codegen {

// the table row access builder refer to fesql-docs/schema.md
class BufIRBuilder {
 public:

    BufIRBuilder(::fesql::type::TableDef* table, 
                 ::llvm::BasicBlock* block,
                 ScopeVar* scope_var);

    ~BufIRBuilder();

    bool BuildGetField(const std::string& name, 
                       ::llvm::Value* row_ptr,
                       ::llvm::Value* row_size,
                       ::llvm::Value** output);

 private:

    bool BuildGetString(const std::string& name, ::llvm::Value* row_ptr,
            ::llvm::Value** output);

    bool GetFieldOffset(const std::string& name,
            ::llvm::Value* row_ptr,
            ::llvm::Value* row_size,
            ::llvm::Value** output);

    bool GetNextOffset(const std::string& name,
            ::llvm::Value* row_ptr,
            ::llvm::Value* row_size,
            ::llvm::Value** output);

 private:
    ::fesql::type::TableDef* const table_;
    ::llvm::BasicBlock* block_;
    ScopeVar* sv_;
    typedef std::map<std::string, std::pair<::fesql::type::Type, int32_t>>
        Types;
    Types types_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_BUF_IR_BUILDER_H_
