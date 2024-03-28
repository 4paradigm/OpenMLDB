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

#ifndef HYBRIDSE_SRC_CODEGEN_FN_LET_IR_BUILDER_TEST_H_
#define HYBRIDSE_SRC_CODEGEN_FN_LET_IR_BUILDER_TEST_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codegen/codegen_base_test.h"
#include "codegen/context.h"
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
#include "plan/plan_api.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/jit.h"
#include "vm/simple_catalog.h"
#include "vm/sql_compiler.h"
#include "vm/transform.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

namespace hybridse {
namespace codegen {
using hybridse::codec::ArrayListV;
using hybridse::codec::Row;

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
    ::hybridse::node::ProjectPlanNode* plan_node = nullptr;
    auto node = trees[0]->GetChildren();
    while (!node.empty()) {
        if (node[0]->GetType() == node::kPlanTypeProject) {
            plan_node = dynamic_cast<hybridse::node::ProjectPlanNode*>(node[0]);
            break;
        }
        node = node[0]->GetChildren();
    }

    ::hybridse::node::ProjectListNode* pp_node_ptr =
        dynamic_cast<hybridse::node::ProjectListNode*>(
            plan_node->project_list_vec_[0]);
    return pp_node_ptr;
}

void CheckFnLetBuilderWithParameterRow(::hybridse::node::NodeManager* manager, vm::SchemasContext* schemas_ctx,
                                       const codec::Schema* parameter_types, std::string udf_str, std::string sql,
                                       int8_t* row_ptr, int8_t* window_ptr, int8_t* parameter_row_ptr,
                                       vm::Schema* output_schema, int8_t** output) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);

    // Parse SQL
    ::hybridse::base::Status status;
    auto lib = udf::DefaultUdfLibrary::get();
    ASSERT_TRUE(udf_str.empty()) << "Un-support user define function";
    m->print(::llvm::errs(), NULL);
    ::hybridse::node::PlanNodeList plan;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(sql, plan, manager, status)) << status;
    hybridse::node::ProjectListNode* pp_node_ptr = GetPlanNodeList(plan);

    // Create physical function def
    schemas_ctx->Build();
    vm::ColumnProjects column_projects;
    const node::FrameNode* frame_node = nullptr;
    if (pp_node_ptr->GetW() != nullptr) {
        frame_node = pp_node_ptr->GetW()->frame_node();
    }
    status = vm::ExtractProjectInfos(pp_node_ptr->GetProjects(), frame_node, &column_projects);
    ASSERT_TRUE(status.isOK()) << status.str();

    bool is_agg = window_ptr != nullptr;
    vm::PhysicalPlanContext plan_ctx(
        manager, lib, "db", std::make_shared<vm::SimpleCatalog>(), parameter_types, false);
    status = plan_ctx.InitFnDef(column_projects, schemas_ctx, !is_agg,
                                &column_projects);
    ASSERT_TRUE(status.isOK()) << status.str();

    // Instantiate llvm function
    const auto& fn_info = column_projects.fn_info();
    codegen::CodeGenContext codegen_ctx(m.get(), fn_info.schemas_ctx(), parameter_types,
                                        manager);
    codegen::RowFnLetIRBuilder builder(&codegen_ctx);
    status =
        builder.Build("test_at_fn", fn_info.fn_def(), fn_info.GetPrimaryFrame(),
                      fn_info.GetFrames(), *fn_info.fn_schema());
    LOG(INFO) << "fn let ir build status: " << status;
    ASSERT_TRUE(status.isOK());
    *output_schema = *fn_info.fn_schema();

    m->print(::llvm::errs(), NULL);
    auto jit = std::unique_ptr<vm::HybridSeJitWrapper>(
        vm::HybridSeJitWrapper::Create());
    jit->Init();

    ASSERT_TRUE(jit->AddModule(std::move(m), std::move(ctx)));
    auto address = jit->FindFunction("test_at_fn");

    int32_t (*decode)(int64_t, int8_t*, int8_t*, int8_t*, int8_t**) =
        (int32_t(*)(int64_t, int8_t*, int8_t*, int8_t*, int8_t**))address;
    int32_t ret2 = decode(0, row_ptr, window_ptr, parameter_row_ptr, output);
    ASSERT_EQ(0, ret2);
}
void CheckFnLetBuilderWithParameterRow(::hybridse::node::NodeManager* manager,
                                       type::TableDef& table, // NOLINT
                                       const type::TableDef& parameter_schema,
                                       std::string udf_str,
                                       std::string sql, int8_t* row_ptr, int8_t* window_ptr,
                                       int8_t * parameter_row_ptr,
                                       vm::Schema* output_schema, int8_t** output) {
    vm::SchemasContext schemas_ctx;
    auto source = schemas_ctx.AddSource();
    source->SetSourceDBAndTableName(table.catalog(), table.name());
    source->SetSchema(&table.columns());
    for (int i = 0; i < table.columns().size(); ++i) {
        source->SetColumnID(i, i);
    }
    schemas_ctx.Build();
    CheckFnLetBuilderWithParameterRow(manager, &schemas_ctx, &parameter_schema.columns(), udf_str, sql, row_ptr,
                                      window_ptr, parameter_row_ptr, output_schema, output);
}

void CheckFnLetBuilder(::hybridse::node::NodeManager* manager, vm::SchemasContext* schemas_ctx, std::string udf_str,
                       std::string sql, int8_t* row_ptr, int8_t* window_ptr, vm::Schema* output_schema,
                       int8_t** output) {
    CheckFnLetBuilderWithParameterRow(manager, schemas_ctx, nullptr, udf_str, sql, row_ptr, window_ptr, nullptr,
                                      output_schema, output);
}
void CheckFnLetBuilder(::hybridse::node::NodeManager* manager, type::TableDef& table, std::string udf_str,  // NOLINT
                       std::string sql, int8_t* row_ptr, int8_t* window_ptr, vm::Schema* output_schema,
                       int8_t** output) {
    type::TableDef empty_schema;
    CheckFnLetBuilderWithParameterRow(manager, table, empty_schema, udf_str, sql, row_ptr, window_ptr, nullptr,
                                      output_schema, output);
}

}  // namespace codegen
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_CODEGEN_FN_LET_IR_BUILDER_TEST_H_
