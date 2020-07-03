/*
 * fn_let_ir_builder_test.h
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

#ifndef SRC_CODEGEN_FN_LET_IR_BUILDER_TEST_H_
#define SRC_CODEGEN_FN_LET_IR_BUILDER_TEST_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codegen/codegen_base_test.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/jit.h"
#include "vm/sql_compiler.h"
#include "vm/transform.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

namespace fesql {
namespace codegen {
using fesql::codec::ArrayListV;
using fesql::codec::Row;

/// Check E. If it's in a success state then return the contained value. If
/// it's in a failure state log the error(s) and exit.
template <typename T>
T FeCheck(::llvm::Expected<T>&& E) {
    if (E.takeError()) {
        // NOLINT
    }
    return std::move(*E);
}
node::ProjectListNode* GetPlanNodeList(node::PlanNodeList trees) {
    ::fesql::node::ProjectPlanNode* plan_node = nullptr;
    auto node = trees[0]->GetChildren();
    while (!node.empty()) {
        if (node[0]->GetType() == node::kPlanTypeProject) {
            plan_node = dynamic_cast<fesql::node::ProjectPlanNode*>(node[0]);
            break;
        }
        node = node[0]->GetChildren();
    }

    ::fesql::node::ProjectListNode* pp_node_ptr =
        dynamic_cast<fesql::node::ProjectListNode*>(
            plan_node->project_list_vec_[0]);
    return pp_node_ptr;
}

void AddFunc(const std::string& fn, ::fesql::node::NodeManager* manager,
             ::llvm::Module* m) {
    if (fn.empty()) {
        return;
    }
    ::fesql::node::NodePointVector trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::base::Status status;
    int ret = parser.parse(fn, trees, manager, status);
    ASSERT_EQ(0, ret);
    FnIRBuilder fn_ir_builder(m);
    for (node::SQLNode* node : trees) {
        LOG(INFO) << "Add Func: " << *node;
        ::llvm::Function* func = nullptr;
        bool ok = fn_ir_builder.Build(dynamic_cast<node::FnNodeFnDef*>(node),
                                      &func, status);
        ASSERT_TRUE(ok);
    }
}

void CheckFnLetBuilder(::fesql::node::NodeManager* manager,
                       const vm::SchemaSourceList& name_schemas,
                       std::string udf_str, std::string sql, int8_t** row_ptrs,
                       int8_t* window_ptr, int32_t* row_sizes,
                       vm::Schema* output_schema,
                       vm::ColumnSourceList* column_sources, int8_t** output) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::base::Status status;
    ::fesql::udf::RegisterUDFToModule(m.get());
    udf::DefaultUDFLibrary lib;
    AddFunc(udf_str, manager, m.get());
    m->print(::llvm::errs(), NULL);
    int ret = parser.parse(sql, list, manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(manager);
    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    fesql::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    bool is_multi_row = pp_node_ptr->GetW() != nullptr;
    ASSERT_TRUE(vm::ResolveProjects(name_schemas, pp_node_ptr->GetProjects(),
                                    !is_multi_row, manager, &lib, status));

    RowFnLetIRBuilder ir_builder(name_schemas,
                                 nullptr == pp_node_ptr->GetW()
                                     ? nullptr
                                     : pp_node_ptr->GetW()->frame_node(),
                                 m.get());
    bool ok = ir_builder.Build("test_at_fn", pp_node_ptr->GetProjects(),
                               output_schema, column_sources);
    ASSERT_TRUE(ok);
    LOG(INFO) << "fn let ir build ok";
    m->print(::llvm::errs(), NULL);
    ExitOnError ExitOnErr;
    auto J = ExitOnErr(LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    lib.InitJITSymbols(J.get());
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_at_fn"));

    int32_t (*decode)(int8_t**, int8_t*, int32_t*, int8_t**) = (int32_t(*)(
        int8_t**, int8_t*, int32_t*, int8_t**))load_fn_jit.getAddress();
    int32_t ret2 = decode(row_ptrs, window_ptr, row_sizes, output);
    ASSERT_EQ(0, ret2);
}

void CheckFnLetBuilder(::fesql::node::NodeManager* manager,
                       type::TableDef& table, std::string udf_str,  // NOLINT
                       std::string sql, int8_t** row_ptrs, int8_t* window_ptr,
                       int32_t* row_sizes, vm::Schema* output_schema,
                       int8_t** output) {
    vm::SchemaSourceList name_schema_list;
    name_schema_list.AddSchemaSource(table.name(), &table.columns());
    vm::ColumnSourceList column_sources;
    CheckFnLetBuilder(manager, name_schema_list, udf_str, sql, row_ptrs,
                      window_ptr, row_sizes, output_schema, &column_sources,
                      output);
}
void CheckFnLetBuilder(::fesql::node::NodeManager* manager,
                       type::TableDef& table, std::string udf_str,  // NOLINT
                       std::string sql, int8_t** row_ptrs, int8_t* window_ptr,
                       int32_t* row_sizes, vm::Schema* output_schema,
                       vm::ColumnSourceList* column_sources, int8_t** output) {
    vm::SchemaSourceList name_schema_list;
    name_schema_list.AddSchemaSource(table.name(), &table.columns());
    CheckFnLetBuilder(manager, name_schema_list, udf_str, sql, row_ptrs,
                      window_ptr, row_sizes, output_schema, column_sources,
                      output);
}

}  // namespace codegen
}  // namespace fesql

#endif  // SRC_CODEGEN_FN_LET_IR_BUILDER_TEST_H_
