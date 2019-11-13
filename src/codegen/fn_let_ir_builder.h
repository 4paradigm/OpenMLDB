/*
 * fn_let_ir_builder.h
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

#ifndef SRC_CODEGEN_FN_LET_IR_BUILDER_H_
#define SRC_CODEGEN_FN_LET_IR_BUILDER_H_

#include <map>
#include <utility>
#include "node/plan_node.h"
#include "llvm/IR/IRBuilder.h"
#include "proto/type.pb.h"
#include "codegen/scope_var.h"


namespace fesql {
namespace codegen {

typedef std::map<std::string, std::pair<::fesql::type::ColumnDef, int32_t> > Schema;

class RowFnLetIRBuilder {

 public:
    RowFnLetIRBuilder(::fesql::type::TableDef* table,
            ::llvm::Module* module);

    ~RowFnLetIRBuilder();

    bool Build(const std::string& name, 
               const ::fesql::node::ProjectListPlanNode* node,
               std::vector<::fesql::type::ColumnDef>& schema);
 private:

    bool BuildFnHeader(const std::string& name, 
            ::llvm::Function **fn);

    bool FillArgs(const std::string& row_ptr_name,
            const std::string& output_ptr_name,
            ::llvm::Function *fn,
            ScopeVar& sv);

    bool StoreColumn(int64_t offset, ::llvm::Value* value, 
            ScopeVar& sv, const std::string& output_ptr_name,
            ::llvm::BasicBlock* block);

 private:
    // input schema
    ::fesql::type::TableDef* table_;
    ::llvm::Module* module_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODGEN_FN_LET_IR_BUILDER_H 
