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
#include <string>
#include <utility>
#include <vector>
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
#include "node/plan_node.h"
#include "proto/type.pb.h"
#include "vm/catalog.h"

namespace fesql {
namespace codegen {

class RowFnLetIRBuilder {
 public:
    RowFnLetIRBuilder(const vm::Schema& schema, ::llvm::Module* module,
                      bool is_window_agg);

    ~RowFnLetIRBuilder();

    bool Build(const std::string& name,
               const ::fesql::node::ProjectListPlanNode* node,
               vm::Schema& schema);  // NOLINT (runtime/references)

 private:
    bool BuildFnHeader(const std::string& name, ::llvm::Function** fn);

    bool BuildFnHeader(const std::string& name,
                       const std::vector<::llvm::Type*>& args_type,
                       ::llvm::Type* ret_type, ::llvm::Function** fn);

    bool FillArgs(const std::string& row_ptr_name,
                  const std::string& row_size_name,
                  const std::string& output_ptr_name, ::llvm::Function* fn,
                  ScopeVar& sv);  // NOLINT

    bool EncodeBuf(const std::map<uint32_t, ::llvm::Value*>* values,
                   const vm::Schema& schema,
                   ScopeVar& sv,  // NOLINT (runtime/references)
                   ::llvm::BasicBlock* block,
                   const std::string& output_ptr_name);

 private:
    // input schema
    vm::Schema schema_;
    ::llvm::Module* module_;
    bool is_window_agg_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_FN_LET_IR_BUILDER_H_
