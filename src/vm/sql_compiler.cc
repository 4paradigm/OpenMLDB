/*
 * sql_compiler.cc
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

#include "vm/sql_compiler.h"
#include <memory>
#include <utility>
#include <vector>
#include "boost/filesystem.hpp"
#include "boost/filesystem/string_file.hpp"
#include "codec/fe_schema_codec.h"
#include "codec/type_codec.h"
#include "codegen/block_ir_builder.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/raw_ostream.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "udf/default_udf_library.h"
#include "vm/runner.h"
#include "vm/transform.h"

using ::fesql::base::Status;
using fesql::common::kPlanError;

namespace fesql {
namespace vm {

SQLCompiler::SQLCompiler(const std::shared_ptr<Catalog>& cl, bool keep_ir,
                         bool dump_plan, bool plan_only)
    : cl_(cl),
      keep_ir_(keep_ir),
      dump_plan_(dump_plan),
      plan_only_(plan_only) {}

SQLCompiler::~SQLCompiler() {}

bool GetLibsFiles(const std::string& dir_path,
                  std::vector<std::string>& filenames,  // NOLINT
                  Status& status) {                     // NOLINT
    boost::filesystem::path path(dir_path);
    if (!boost::filesystem::exists(path)) {
        status.msg = "Libs path " + dir_path + " not exist";
        status.code = common::kSQLError;
        return false;
    }
    boost::filesystem::directory_iterator end_iter;
    for (boost::filesystem::directory_iterator iter(path); iter != end_iter;
         ++iter) {
        if (boost::filesystem::is_regular_file(iter->status())) {
            filenames.push_back(iter->path().string());
        }

        if (boost::filesystem::is_directory(iter->status())) {
            if (!GetLibsFiles(iter->path().string(), filenames, status)) {
                return false;
            }
        }
    }
    return true;
}

const std::string FindFesqlDirPath() {
    boost::filesystem::path current_path(boost::filesystem::current_path());
    boost::filesystem::path fesql_path;

    while (current_path.has_parent_path()) {
        current_path = current_path.parent_path();
        if (current_path.filename().string() == "fesql") {
            break;
        }
    }

    if (current_path.filename().string() == "fesql") {
        LOG(INFO) << "Fesql Dir Path is : " << current_path.string()
                  << std::endl;
        return current_path.string();
    }
    return std::string();
}
bool RegisterFeLibs(udf::UDFLibrary* library, Status& status,  // NOLINT
                    const std::string& libs_home,
                    const std::string& libs_name) {
    if (libs_name.empty()) {
        LOG(WARNING) << "fail register fe libs: No fesql libs config exist";
        return true;
    }
    std::vector<std::string> filepaths;
    std::string fesql_libs_path = "";
    if (libs_home.empty()) {
        fesql_libs_path = FindFesqlDirPath();
    } else {
        fesql_libs_path = libs_home;
    }
    fesql_libs_path.append("/").append(libs_name);
    if (!GetLibsFiles(fesql_libs_path, filepaths, status)) {
        status.msg = "fail to get libs file " + fesql_libs_path;
        LOG(WARNING) << status;
        return false;
    }

    std::ostringstream runner_oss;
    runner_oss << "Libs:\n";
    for_each(filepaths.cbegin(), filepaths.cend(),
             [&](const std::string item) { runner_oss << item << "\n"; });
    DLOG(INFO) << runner_oss.str();

    for (auto path_str : filepaths) {
        status = library->RegisterFromFile(path_str);
        if (!status.isOK()) {
            LOG(WARNING) << status;
            return false;
        }
    }
    return true;
}
void SQLCompiler::KeepIR(SQLContext& ctx, llvm::Module* m) {
    if (m == NULL) {
        LOG(WARNING) << "module is null";
        return;
    }
    ctx.ir.clear();
    llvm::raw_string_ostream ss(ctx.ir);
    ss << *m;
    ss.flush();
    LOG(INFO) << "keep ir length: " << ctx.ir.size();
}

bool SQLCompiler::Compile(SQLContext& ctx, Status& status) {  // NOLINT
    bool ok = Parse(ctx, status);
    if (!ok) {
        return false;
    }
    if (ctx.logical_plan.empty() || nullptr == ctx.logical_plan[0]) {
        status.msg = "error: generate empty/null logical plan";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }
    if (dump_plan_) {
        std::stringstream logical_plan_ss;
        ctx.logical_plan[0]->Print(logical_plan_ss, "\t");
        ctx.logical_plan_str = logical_plan_ss.str();
    }
    auto llvm_ctx = ::llvm::make_unique<::llvm::LLVMContext>();
    auto m = ::llvm::make_unique<::llvm::Module>("sql", *llvm_ctx);
    ctx.udf_library = udf::DefaultUDFLibrary::get();

    status =
        BuildPhysicalPlan(&ctx, ctx.logical_plan, m.get(), &ctx.physical_plan);
    if (!status.isOK()) {
        return false;
    }

    if (nullptr == ctx.physical_plan) {
        status.msg = "error: generate null physical plan";
        status.code = common::kPlanError;
        LOG(WARNING) << status;
        return false;
    }

    if (dump_plan_) {
        std::stringstream physical_plan_ss;
        ctx.physical_plan->Print(physical_plan_ss, "\t");
        ctx.physical_plan_str = physical_plan_ss.str();
    }
    ok = codec::SchemaCodec::Encode(ctx.schema, &ctx.encoded_schema);
    if (!ok) {
        LOG(WARNING) << "fail to encode output schema";
        return false;
    }
    if (plan_only_) {
        return true;
    }
    if (llvm::verifyModule(*(m.get()), &llvm::errs(), nullptr)) {
        LOG(WARNING) << "fail to verify codegen module";
        m->print(::llvm::errs(), NULL, true, true);
        return false;
    }
    // ::llvm::errs() << *(m.get());
    auto jit = std::unique_ptr<FeSQLJITWrapper>(
        FeSQLJITWrapper::Create(ctx.jit_options));
    if (jit == nullptr || !jit->Init()) {
        status.msg = "fail to init jit let";
        status.code = common::kJitError;
        LOG(WARNING) << status;
        return false;
    }
    InitBuiltinJITSymbols(jit.get());
    ctx.udf_library->InitJITSymbols(jit.get());
    if (!jit->OptModule(m.get())) {
        LOG(WARNING) << "fail to opt ir module for sql " << ctx.sql;
        return false;
    }
    if (keep_ir_) {
        KeepIR(ctx, m.get());
    }
    if (!jit->AddModule(std::move(m), std::move(llvm_ctx))) {
        LOG(WARNING) << "fail to add ir module  for sql " << ctx.sql;
        return false;
    }
    if (!ResolvePlanFnAddress(ctx.physical_plan, jit, status)) {
        return false;
    }
    ctx.jit = std::move(jit);
    DLOG(INFO) << "compile sql " << ctx.sql << " done";
    return true;
}

std::string EngineModeName(EngineMode mode) {
    switch (mode) {
        case kBatchMode:
            return "kBatchMode";
        case kRequestMode:
            return "kRequestMode";
        case kBatchRequestMode:
            return "kBatchRequestMode";
        default:
            return "unknown";
    }
}

Status SQLCompiler::BuildBatchModePhysicalPlan(
    SQLContext* ctx, const ::fesql::node::PlanNodeList& plan_list,
    ::llvm::Module* llvm_module, udf::UDFLibrary* library,
    PhysicalOpNode** output) {
    vm::BatchModeTransformer transformer(&ctx->nm, ctx->db, cl_, llvm_module,
                                         library, ctx->is_performance_sensitive,
                                         ctx->is_cluster_optimized);
    transformer.AddDefaultPasses();
    CHECK_STATUS(transformer.TransformPhysicalPlan(plan_list, output),
                 "Fail to generate physical plan (batch mode)");
    ctx->schema = *(*output)->GetOutputSchema();
    return Status::OK();
}

Status SQLCompiler::BuildRequestModePhysicalPlan(
    SQLContext* ctx, const ::fesql::node::PlanNodeList& plan_list,
    ::llvm::Module* llvm_module, udf::UDFLibrary* library,
    PhysicalOpNode** output) {
    vm::RequestModeTransformer transformer(
        &ctx->nm, ctx->db, cl_, llvm_module, library, {},
        ctx->is_performance_sensitive, ctx->is_cluster_optimized, false);
    transformer.AddDefaultPasses();
    CHECK_STATUS(transformer.TransformPhysicalPlan(plan_list, output),
                 "Fail to generate physical plan (request mode)");
    ctx->request_schema = transformer.request_schema();
    CHECK_TRUE(codec::SchemaCodec::Encode(transformer.request_schema(),
                                          &ctx->encoded_request_schema),
               kPlanError, "Fail to encode request schema");
    ctx->request_name = transformer.request_name();
    ctx->schema = *(*output)->GetOutputSchema();
    return Status::OK();
}

Status SQLCompiler::BuildBatchRequestModePhysicalPlan(
    SQLContext* ctx, const ::fesql::node::PlanNodeList& plan_list,
    ::llvm::Module* llvm_module, udf::UDFLibrary* library,
    PhysicalOpNode** output) {
    vm::RequestModeTransformer transformer(
        &ctx->nm, ctx->db, cl_, llvm_module, library,
        ctx->batch_request_info.common_column_indices,
        ctx->is_performance_sensitive, ctx->is_cluster_optimized,
        ctx->is_batch_request_optimized);
    transformer.AddDefaultPasses();
    PhysicalOpNode* output_plan = nullptr;
    CHECK_STATUS(transformer.TransformPhysicalPlan(plan_list, &output_plan),
                 "Fail to generate physical plan (batch request mode)");
    *output = output_plan;

    ctx->request_schema = transformer.request_schema();
    CHECK_TRUE(codec::SchemaCodec::Encode(transformer.request_schema(),
                                          &ctx->encoded_request_schema),
               kPlanError, "Fail to encode request schema");
    ctx->request_name = transformer.request_name();

    // set batch request output schema
    const auto& output_common_indices =
        transformer.batch_request_info().output_common_column_indices;
    ctx->batch_request_info.output_common_column_indices =
        output_common_indices;
    ctx->batch_request_info.common_node_set =
        transformer.batch_request_info().common_node_set;
    if (!output_common_indices.empty() &&
        output_common_indices.size() < output_plan->GetOutputSchemaSize()) {
        CHECK_TRUE(output_plan->GetOutputSchemaSourceSize() == 2, kPlanError,
                   "Output plan should take 2 schema sources for "
                   "non-trival common columns");
        CHECK_TRUE(output_plan->GetOutputSchemaSource(0)->size() ==
                       output_common_indices.size(),
                   kPlanError, "Illegal common column schema size");
        ctx->schema.Clear();
        size_t common_col_idx = 0;
        size_t non_common_col_idx = 0;
        for (size_t i = 0; i < (*output)->GetOutputSchemaSize(); ++i) {
            if (output_common_indices.find(i) != output_common_indices.end()) {
                *(ctx->schema.Add()) =
                    output_plan->GetOutputSchemaSource(0)->GetSchema()->Get(
                        common_col_idx);
                common_col_idx += 1;
            } else {
                *(ctx->schema.Add()) =
                    output_plan->GetOutputSchemaSource(1)->GetSchema()->Get(
                        non_common_col_idx);
                non_common_col_idx += 1;
            }
        }
    } else {
        CHECK_TRUE(output_plan->GetOutputSchemaSourceSize() == 1, kPlanError,
                   "Output plan should take 1 schema source for trival "
                   "common columns");
        ctx->schema = *(*output)->GetOutputSchema();
    }
    return Status::OK();
}

Status SQLCompiler::BuildPhysicalPlan(
    SQLContext* ctx, const ::fesql::node::PlanNodeList& plan_list,
    ::llvm::Module* llvm_module, PhysicalOpNode** output) {
    Status status;
    CHECK_TRUE(ctx != nullptr, kPlanError, "Null sql context");

    udf::UDFLibrary* library = ctx->udf_library;
    CHECK_TRUE(library != nullptr, kPlanError, "Null udf library");

    switch (ctx->engine_mode) {
        case kBatchMode: {
            return BuildBatchModePhysicalPlan(ctx, plan_list, llvm_module,
                                              library, output);
        }
        case kRequestMode: {
            return BuildRequestModePhysicalPlan(ctx, plan_list, llvm_module,
                                                library, output);
        }
        case kBatchRequestMode: {
            return BuildBatchRequestModePhysicalPlan(
                ctx, plan_list, llvm_module, library, output);
        }
        default:
            return Status(kPlanError, "Unknown engine mode: " +
                                          EngineModeName(ctx->engine_mode));
    }
}

bool SQLCompiler::BuildClusterJob(SQLContext& ctx, Status& status) {  // NOLINT
    if (nullptr == ctx.physical_plan) {
        status.msg = "fail to build cluster job: physical plan is empty";
        status.code = common::kOpGenError;
        return false;
    }
    bool is_request_mode = vm::kRequestMode == ctx.engine_mode ||
                           vm::kBatchRequestMode == ctx.engine_mode;
    RunnerBuilder runner_builder(&ctx.nm, ctx.sql,
                                 ctx.is_cluster_optimized && is_request_mode,
                                 ctx.batch_request_info.common_column_indices,
                                 ctx.batch_request_info.common_node_set);
    ctx.cluster_job = runner_builder.BuildClusterJob(ctx.physical_plan, status);
    return status.isOK();
}

/**
 * Parse SQL string and transform into logical plan
 * @param ctx [out]
 * @param status
 * @return true if success to transform logical plan, store logical
 *         plan into SQLContext
 */
bool SQLCompiler::Parse(SQLContext& ctx,
                        ::fesql::base::Status& status) {  // NOLINT
    ::fesql::node::NodePointVector parser_trees;
    ::fesql::parser::FeSQLParser parser;

    bool is_batch_mode = ctx.engine_mode == kBatchMode;
    ::fesql::plan::SimplePlanner planer(&ctx.nm, is_batch_mode,
                                        ctx.is_cluster_optimized);

    int ret = parser.parse(ctx.sql, parser_trees, &ctx.nm, status);
    if (ret != 0) {
        LOG(WARNING) << "fail to parse sql " << ctx.sql << " with error "
                     << status;
        return false;
    }
    ret = planer.CreatePlanTree(parser_trees, ctx.logical_plan, status);
    if (ret != 0) {
        LOG(WARNING) << "Fail create sql plan: " << status;
        return false;
    }
    return true;
}
bool SQLCompiler::ResolvePlanFnAddress(vm::PhysicalOpNode* node,
                                       std::unique_ptr<FeSQLJITWrapper>& jit,
                                       Status& status) {
    if (nullptr == node) {
        status.msg = "fail to resolve project fn address: node is null";
    }

    if (!node->producers().empty()) {
        for (auto iter = node->producers().cbegin();
             iter != node->producers().cend(); iter++) {
            if (!ResolvePlanFnAddress(*iter, jit, status)) {
                return false;
            }
        }
    }

    // TODO(chenjing):
    // 待优化，内嵌子查询的Resolved不适合特别处理，需要把子查询的function信息注册到主查询中，
    // 函数指针的注册需要整体优化一下设计
    switch (node->GetOpType()) {
        case kPhysicalOpRequestUnion: {
            auto request_union_op =
                dynamic_cast<PhysicalRequestUnionNode*>(node);
            if (!request_union_op->window_unions_.Empty()) {
                for (auto window_union :
                     request_union_op->window_unions_.window_unions_) {
                    if (!ResolvePlanFnAddress(window_union.first, jit,
                                              status)) {
                        return false;
                    }
                }
            }
            break;
        }
        case kPhysicalOpProject: {
            auto project_op = dynamic_cast<PhysicalProjectNode*>(node);
            if (kWindowAggregation == project_op->project_type_) {
                auto window_agg_op =
                    dynamic_cast<PhysicalWindowAggrerationNode*>(node);
                if (!window_agg_op->window_joins_.Empty()) {
                    for (auto window_join :
                         window_agg_op->window_joins_.window_joins_) {
                        if (!ResolvePlanFnAddress(window_join.first, jit,
                                                  status)) {
                            return false;
                        }
                    }
                }
                if (!window_agg_op->window_unions_.Empty()) {
                    for (auto window_union :
                         window_agg_op->window_unions_.window_unions_) {
                        if (!ResolvePlanFnAddress(window_union.first, jit,
                                                  status)) {
                            return false;
                        }
                    }
                }
            }
            break;
        }
        default: {
        }
    }
    if (!node->GetFnInfos().empty()) {
        for (auto info_ptr : node->GetFnInfos()) {
            if (!info_ptr->fn_name().empty()) {
                DLOG(INFO) << "Start to resolve fn address "
                           << info_ptr->fn_name();
                auto addr = jit->FindFunction(info_ptr->fn_name());
                if (addr == nullptr) {
                    LOG(WARNING) << "Fail to find jit function "
                                 << info_ptr->fn_name() << " for node\n"
                                 << *node;
                }
                const_cast<FnInfo*>(info_ptr)->SetFnPtr(addr);
            }
        }
    }
    return true;
}

}  // namespace vm
}  // namespace fesql
