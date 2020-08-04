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
#include "codegen/expr_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "proto/fe_type.pb.h"
#include "vm/catalog.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace codegen {
using fesql::vm::RowSchemaInfo;
class RowFnLetIRBuilder {
 public:
    RowFnLetIRBuilder(const vm::SchemaSourceList& schema_sources,
                      const node::FrameNode* frame, ::llvm::Module* module);

    ~RowFnLetIRBuilder();

    bool Build(const std::string& name, node::LambdaNode* project_func,
               vm::Schema* output_schema,
               vm::ColumnSourceList*
                   output_column_sources);  // NOLINT (runtime/references)

    bool Build(const std::string& name, const node::PlanNodeList& projects,
               vm::Schema* output_schema,
               vm::ColumnSourceList*
                   output_column_sources);  // NOLINT (runtime/references)

 private:
    bool BuildFnHeader(const std::string& name,
                       const std::vector<::llvm::Type*>& args_type,
                       ::llvm::Type* ret_type, ::llvm::Function** fn);

    bool FillArgs(const std::vector<std::string>& args, ::llvm::Function* fn,
                  ScopeVar& sv);  // NOLINT

    bool EncodeBuf(
        const std::map<uint32_t, NativeValue>* values, const vm::Schema& schema,
        VariableIRBuilder& variable_ir_builder,  // NOLINT (runtime/references)
        ::llvm::BasicBlock* block, const std::string& output_ptr_name);

    bool BuildProject(
        const uint32_t index, const node::ExprNode* expr,
        const std::string& col_name, std::map<uint32_t, NativeValue>* output,
        ExprIRBuilder& expr_ir_builder,  // NOLINT (runtime/references)
        vm::Schema* output_schema, vm::ColumnSourceList* output_column_sources,
        base::Status& status);  // NOLINT (runtime/references)

    bool AddOutputColumnInfo(const std::string& col_name,
                             ::fesql::type::Type ctype,
                             const node::ExprNode* expr,
                             vm::Schema* output_schema,
                             vm::ColumnSourceList* output_column_sources);

 private:
    // input schema
    const vm::SchemasContext schema_context_;
    const node::FrameNode* frame_;
    ::llvm::Module* module_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_FN_LET_IR_BUILDER_H_
