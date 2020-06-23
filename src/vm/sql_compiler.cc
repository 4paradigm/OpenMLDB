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
#include "codec/fe_schema_codec.h"
#include "codec/type_codec.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/Bitcode/BitcodeReader.h"
#include "llvm/Support/raw_ostream.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "udf/udf.h"
#include "vm/runner.h"
#include "vm/transform.h"

namespace fesql {
namespace vm {

void InitCodecSymbol(::llvm::orc::JITDylib& jd,             // NOLINT
                     ::llvm::orc::MangleAndInterner& mi) {  // NOLINT
    fesql::vm::FeSQLJIT::AddSymbol(jd, mi, "malloc",
                                   (reinterpret_cast<void*>(&malloc)));
    fesql::vm::FeSQLJIT::AddSymbol(jd, mi, "memset",
                                   (reinterpret_cast<void*>(&memset)));
    fesql::vm::FeSQLJIT::AddSymbol(jd, mi, "__bzero",
                                   (reinterpret_cast<void*>(&bzero)));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_int16_field",
        reinterpret_cast<void*>(
            static_cast<int16_t(*)(
                const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetInt16Field)));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_int32_field",
        reinterpret_cast<void*>(
            static_cast<int32_t(*)(
                const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetInt32Field)));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_int64_field",
        reinterpret_cast<void*>(
            static_cast<int64_t(*)(
                const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetInt64Field)));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_float_field",
        reinterpret_cast<void*>(static_cast<float(*)(
            const int8_t*, uint32_t, uint32_t, int8_t*)>(
            &codec::v1::GetFloatField)));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_double_field",
        reinterpret_cast<void*>(
            static_cast<double(*)(
                const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetDoubleField)));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_timestamp_field",
        reinterpret_cast<void*>(
            static_cast<codec::Timestamp(*)(
                const int8_t*, uint32_t, uint32_t, int8_t*)>(
                &codec::v1::GetTimestampField)));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_str_addr_space",
        reinterpret_cast<void*>(&codec::v1::GetAddrSpace));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_str_field",
        reinterpret_cast<void*>(
            static_cast<int32_t(*)(
                const int8_t*, uint32_t, uint32_t, uint32_t, uint32_t,
                      uint32_t, int8_t**, uint32_t*, int8_t*)>(
                &codec::v1::GetStrField)));
    fesql::vm::FeSQLJIT::AddSymbol(jd, mi, "fesql_storage_get_col",
                                   reinterpret_cast<void*>(&codec::v1::GetCol));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_str_col",
        reinterpret_cast<void*>(&codec::v1::GetStrCol));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_inner_range_list",
        reinterpret_cast<void*>(&codec::v1::GetInnerRangeList));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_inner_rows_list",
        reinterpret_cast<void*>(&codec::v1::GetInnerRowsList));

    // encode
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_int16_field",
        reinterpret_cast<void*>(&codec::v1::AppendInt16));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_int32_field",
        reinterpret_cast<void*>(&codec::v1::AppendInt32));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_int64_field",
        reinterpret_cast<void*>(&codec::v1::AppendInt64));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_float_field",
        reinterpret_cast<void*>(&codec::v1::AppendFloat));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_double_field",
        reinterpret_cast<void*>(&codec::v1::AppendDouble));

    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_string_field",
        reinterpret_cast<void*>(&codec::v1::AppendString));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_calc_size",
        reinterpret_cast<void*>(&codec::v1::CalcTotalLength));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_encode_nullbit",
        reinterpret_cast<void*>(&codec::v1::AppendNullBit));

    // row iteration
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_get_row_iter",
        reinterpret_cast<void*>(&fesql::vm::GetRowIter));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_row_iter_has_next",
        reinterpret_cast<void*>(&fesql::vm::RowIterHasNext));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_row_iter_next",
        reinterpret_cast<void*>(&fesql::vm::RowIterNext));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_row_iter_get_cur_slice",
        reinterpret_cast<void*>(&fesql::vm::RowIterGetCurSlice));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_row_iter_get_cur_slice_size",
        reinterpret_cast<void*>(&fesql::vm::RowIterGetCurSliceSize));
    fesql::vm::FeSQLJIT::AddSymbol(
        jd, mi, "fesql_storage_row_iter_delete",
        reinterpret_cast<void*>(&fesql::vm::RowIterDelete));
}

void InitCodecSymbol(vm::FeSQLJIT* jit_ptr) {
    ::llvm::orc::MangleAndInterner mi(jit_ptr->getExecutionSession(),
                                      jit_ptr->getDataLayout());
    InitCodecSymbol(jit_ptr->getMainJITDylib(), mi);
}

using ::fesql::base::Status;

SQLCompiler::SQLCompiler(const std::shared_ptr<Catalog>& cl, bool keep_ir,
                         bool dump_plan, bool plan_only)
    : cl_(cl),
      keep_ir_(keep_ir),
      dump_plan_(dump_plan),
      plan_only_(plan_only) {}

SQLCompiler::~SQLCompiler() {}

void SQLCompiler::KeepIR(SQLContext& ctx, llvm::Module* m) {
    if (m == NULL) {
        LOG(WARNING) << "module is null";
        return;
    }
    ctx.ir.reserve(1024);
    llvm::raw_string_ostream buf(ctx.ir);
    llvm::WriteBitcodeToFile(*m, buf);
    buf.flush();
    LOG(INFO) << "keep ir length: " << ctx.ir.size();
}

bool SQLCompiler::Compile(SQLContext& ctx, Status& status) {  // NOLINT
    ::fesql::node::PlanNodeList trees;
    bool ok = Parse(ctx, ctx.nm, trees, status);
    if (!ok) {
        return false;
    }
    if (dump_plan_ && trees.size() > 0) {
        std::stringstream logical_plan_ss;
        trees[0]->Print(logical_plan_ss, "\t");
        ctx.logical_plan = logical_plan_ss.str();
    }
    auto llvm_ctx = ::llvm::make_unique<::llvm::LLVMContext>();
    auto m = ::llvm::make_unique<::llvm::Module>("sql", *llvm_ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    if (ctx.is_batch_mode) {
        vm::BatchModeTransformer transformer(&(ctx.nm), ctx.db, cl_, m.get());
        transformer.AddDefaultPasses();
        if (!transformer.TransformPhysicalPlan(trees, &ctx.plan, status)) {
            LOG(WARNING) << "fail to generate physical plan (batch mode): "
                         << status.msg << " for sql: \n"
                         << ctx.sql;
            return false;
        }
    } else {
        vm::RequestModeransformer transformer(&(ctx.nm), ctx.db, cl_, m.get());
        transformer.AddDefaultPasses();
        if (!transformer.TransformPhysicalPlan(trees, &ctx.plan, status)) {
            LOG(WARNING) << "fail to generate physical plan (request mode) "
                            "for sql: \n"
                         << ctx.sql;
            return false;
        }
        ctx.request_schema = transformer.request_schema();
        ok = codec::SchemaCodec::Encode(transformer.request_schema(),
                                        &ctx.encoded_request_schema);
        if (!ok) {
            LOG(WARNING) << "fail to encode request schema";
            return false;
        }
        ctx.request_name = transformer.request_name();
    }

    if (nullptr == ctx.plan) {
        status.msg = "error: generate null physical plan";
        status.code = common::kPlanError;
        LOG(WARNING) << status.msg;
        return false;
    }
    if (dump_plan_) {
        std::stringstream physical_plan_ss;
        ctx.plan->Print(physical_plan_ss, "\t");
        ctx.physical_plan = physical_plan_ss.str();
    }
    if (plan_only_) {
        return true;
    }
    if (llvm::verifyModule(*(m.get()), &llvm::errs(), nullptr)) {
        LOG(WARNING) << "fail to verify codegen module";
        return false;
    }

    ::llvm::Expected<std::unique_ptr<FeSQLJIT>> jit_expected(
        FeSQLJITBuilder().create());
    {
        ::llvm::Error e = jit_expected.takeError();
        if (e) {
            status.msg = "fail to init jit let";
            status.code = common::kJitError;
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    ctx.jit = std::move(*jit_expected);
    ctx.jit->Init();
    if (false == ctx.jit->OptModule(m.get())) {
        LOG(WARNING) << "fail to opt ir module for sql " << ctx.sql;
        return false;
    }


    if (keep_ir_) {
        KeepIR(ctx, m.get());
    }

    ::llvm::Error e = ctx.jit->addIRModule(
        ::llvm::orc::ThreadSafeModule(std::move(m), std::move(llvm_ctx)));
    if (e) {
        LOG(WARNING) << "fail to add ir module  for sql " << ctx.sql;
        return false;
    }

    InitCodecSymbol(ctx.jit.get());
    udf::InitUDFSymbol(ctx.jit.get());

    if (!ResolvePlanFnAddress(ctx.plan, ctx.jit, status)) {
        return false;
    }
    ctx.schema = ctx.plan->output_schema_;
    ok = codec::SchemaCodec::Encode(ctx.schema, &ctx.encoded_schema);
    if (!ok) {
        LOG(WARNING) << "fail to encode output schema";
        return false;
    }
    DLOG(INFO) << "compile sql " << ctx.sql << " done";
    return true;
}
bool SQLCompiler::BuildRunner(SQLContext& ctx, Status& status) {  // NOLINT
    RunnerBuilder runner_builder;
    Runner* runner = runner_builder.Build(ctx.plan, status);
    if (nullptr == runner) {
        status.msg = "fail to build runner: " + status.msg;
        status.code = common::kOpGenError;
        return false;
    }
    ctx.runner = runner;
    return true;
}

bool SQLCompiler::Parse(SQLContext& ctx, ::fesql::node::NodeManager& node_mgr,
                        ::fesql::node::PlanNodeList& plan_trees,  // NOLINT
                        ::fesql::base::Status& status) {          // NOLINT
    ::fesql::node::NodePointVector parser_trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::plan::SimplePlanner planer(&node_mgr, ctx.is_batch_mode);

    int ret = parser.parse(ctx.sql, parser_trees, &node_mgr, status);
    if (ret != 0) {
        LOG(WARNING) << "fail to parse sql " << ctx.sql << " with error "
                     << status.msg;
        return false;
    }

    ret = planer.CreatePlanTree(parser_trees, plan_trees, status);
    if (ret != 0) {
        LOG(WARNING) << "Fail create sql plan: " << status.msg;
        return false;
    }

    return true;
}
bool SQLCompiler::ResolvePlanFnAddress(PhysicalOpNode* node,
                                       std::unique_ptr<FeSQLJIT>& jit,
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
    switch (node->type_) {
        case kPhysicalOpRequestUnoin: {
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
            if (!info_ptr->fn_name_.empty()) {
                DLOG(INFO) << "start to resolve fn address "
                           << info_ptr->fn_name_;
                ::llvm::Expected<::llvm::JITEvaluatedSymbol> symbol(
                    jit->lookup(info_ptr->fn_name_));
                ::llvm::Error e = symbol.takeError();
                if (e) {
                    LOG(WARNING) << "fail to resolve fn address "
                                 << info_ptr->fn_name_ << " not found in jit";
                    return false;
                }
                info_ptr->fn_ =
                    (reinterpret_cast<int8_t*>(symbol->getAddress()));
            }
        }
    }
    return true;
}

}  // namespace vm
}  // namespace fesql
