/*
 * window_ir_builder.h
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

#ifndef SRC_CODEGEN_WINDOW_IR_BUILDER_H_
#define SRC_CODEGEN_WINDOW_IR_BUILDER_H_

#include <map>
#include "proto/type.pb.h"
#include "catalog/catalog.h"
#include "llvm/IR/IRBuilder.h"

namespace fesql {
namespace codegen {

class WindowDecodeIRBuilder {

 public:
    WindowDecodeIRBuilder() {}

    virtual ~WindowDecodeIRBuilder() {}

    virtual bool BuildGetCol(const std::string& name,
            ::llvm::Value* window_ptr,
            ::llvm::Value** output) = 0;

};

class MemoryWindowDecodeIRBuilder : public WindowDecodeIRBuilder {

 public:
    MemoryWindowDecodeIRBuilder(const catalog::Schema& schema,
            ::llvm::BasicBlock* block);

    ~MemoryWindowDecodeIRBuilder();


    bool BuildGetCol(const std::string& name, 
            ::llvm::Value* window_ptr,
            ::llvm::Value** output);

 private:

    bool BuildGetPrimaryCol(const std::string& fn_name,
                            ::llvm::Value* row_ptr,
                             uint32_t offset,
                             fesql::type::Type type,
                             ::llvm::Value** output);

    bool BuildGetStringCol(uint32_t offset,
                           uint32_t next_str_field_offset,
                           fesql::type::Type type,
                           ::llvm::Value* window_ptr,
                           ::llvm::Value** output);

 private:
    catalog::Schema schema_;
    ::llvm::BasicBlock* block_;
    typedef std::map<std::string, std::pair<::fesql::type::Type, int32_t>>
        Types;
    Types types_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_WINDOW_IR_BUILDER_H_
