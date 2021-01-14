/*
 * engine.cc
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

#include "vm/engine.h"
#include <string>
#include <utility>
#include <vector>
#include "base/fe_strings.h"
#include "boost/none.hpp"
#include "boost/optional.hpp"
#include "codec/fe_row_codec.h"
#include "codec/fe_schema_codec.h"
#include "codec/list_iterator_codec.h"
#include "codegen/buf_ir_builder.h"
#include "gflags/gflags.h"
#include "llvm-c/Target.h"
#include "vm/mem_catalog.h"

DECLARE_bool(logtostderr);
DECLARE_string(log_dir);

namespace fesql {
namespace vm {

static bool LLVM_IS_INITIALIZED = false;
Engine::Engine(const std::shared_ptr<Catalog>& catalog)
    : cl_(catalog), options_(), mu_(), lru_cache_() {}
Engine::Engine(const std::shared_ptr<Catalog>& catalog,
               const EngineOptions& options)
    : cl_(catalog), options_(options), mu_(), lru_cache_() {}
Engine::~Engine() {}
void Engine::InitializeGlobalLLVM() {
    if (LLVM_IS_INITIALIZED) return;
    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
    LLVM_IS_INITIALIZED = true;
}

bool Engine::GetDependentTables(const std::string& sql, const std::string& db,
                                EngineMode engine_mode,
                                std::set<std::string>* tables,
                                base::Status& status) {
    std::shared_ptr<CompileInfo> info(new CompileInfo());
    info->get_sql_context().sql = sql;
    info->get_sql_context().db = db;
    info->get_sql_context().engine_mode = engine_mode;
    SQLCompiler compiler(
        std::atomic_load_explicit(&cl_, std::memory_order_acquire),
        options_.is_keep_ir(), false, options_.is_plan_only());
    bool ok = compiler.Parse(info->get_sql_context(), status);
    if (!ok || 0 != status.code) {
        // TODO(chenjing): do clean
        status.msg = "fail to get depend tables:" + status.str();
        return false;
    }

    auto& logical_plan = info->get_sql_context().logical_plan;

    if (logical_plan.empty()) {
        status.msg = "fail to get depend tables: logical plan is empty";
        return false;
    }

    for (auto iter = logical_plan.cbegin(); iter != logical_plan.cend();
         iter++) {
        if (!GetDependentTables(*iter, tables, status)) {
            return false;
        }
    }
    return true;
}

/**
 * Get Dependent tables for given logical node.
 *
 * @param node
 * @param tables
 * @param status
 * @return
 */
bool Engine::GetDependentTables(node::PlanNode* node,
                                std::set<std::string>* tables,
                                base::Status& status) {  // NOLINT
    if (nullptr == tables) {
        status.code = common::kNullPointer;
        status.msg =
            "fail to get sql depend tables, output tables vector is null";
        return false;
    }

    if (nullptr != node) {
        switch (node->GetType()) {
            case node::kPlanTypeTable: {
                const node::TablePlanNode* table_node =
                    dynamic_cast<const node::TablePlanNode*>(node);
                tables->insert(table_node->table_);
                return true;
            }
            default: {
                if (node->GetChildrenSize() > 0) {
                    for (auto child : node->GetChildren()) {
                        if (!GetDependentTables(child, tables, status)) {
                            return false;
                        }
                    }
                }
            }
        }
    }
    return true;
}

bool Engine::IsCompatibleCache(RunSession& session,  // NOLINT
                               std::shared_ptr<CompileInfo> info,
                               base::Status& status) {  // NOLINT
    auto& cache_ctx = info->get_sql_context();
    if (cache_ctx.engine_mode != session.engine_mode()) {
        status =
            Status(common::kSQLError,
                   "Inconsistent cache, mode expect " +
                       EngineModeName(session.engine_mode()) + " but get " +
                       EngineModeName(cache_ctx.engine_mode));
        return false;
    }
    if (session.engine_mode() == kBatchRequestMode) {
        auto batch_req_sess = dynamic_cast<BatchRequestRunSession*>(&session);
        if (batch_req_sess == nullptr) {
            return false;
        }
        auto& cache_indices =
            cache_ctx.batch_request_info.common_column_indices;
        auto& sess_indices = batch_req_sess->common_column_indices();
        if (cache_indices != sess_indices) {
            status =
                Status(common::kSQLError, "Inconsistent common column config");
            return false;
        }
    }
    return true;
}

bool Engine::Get(const std::string& sql, const std::string& db,
                 RunSession& session,
                 base::Status& status) {  // NOLINT (runtime/references)
    std::shared_ptr<CompileInfo> info =
        GetCacheLocked(db, sql, session.engine_mode());
    if (info && IsCompatibleCache(session, info, status)) {
        session.SetCompileInfo(info);
        return true;
    }
    // TODO(baoxinqi): IsCompatibleCache fail, return false, or reset status.
    if (!status.isOK()) {
        LOG(WARNING) << status;
        status = base::Status::OK();
    }
    DLOG(INFO) << "Compile FESQL ...";
    status = base::Status::OK();
    info = std::shared_ptr<CompileInfo>(new CompileInfo());
    auto& sql_context = info->get_sql_context();
    sql_context.sql = sql;
    sql_context.db = db;
    sql_context.engine_mode = session.engine_mode();
    sql_context.is_performance_sensitive = options_.is_performance_sensitive();
    sql_context.is_cluster_optimized = options_.is_cluster_optimzied();
    sql_context.is_batch_request_optimized =
        options_.is_batch_request_optimized();
    sql_context.enable_expr_optimize = options_.is_enable_expr_optimize();
    sql_context.jit_options = options_.jit_options();

    auto batch_req_sess = dynamic_cast<BatchRequestRunSession*>(&session);
    if (batch_req_sess) {
        sql_context.batch_request_info.common_column_indices =
            batch_req_sess->common_column_indices();
    }

    SQLCompiler compiler(
        std::atomic_load_explicit(&cl_, std::memory_order_acquire),
        options_.is_keep_ir(), false, options_.is_plan_only());
    bool ok = compiler.Compile(info->get_sql_context(), status);
    if (!ok || 0 != status.code) {
        return false;
    }
    if (!options_.is_compile_only()) {
        ok = compiler.BuildClusterJob(info->get_sql_context(), status);
        if (!ok || 0 != status.code) {
            LOG(WARNING) << "fail to build cluster job: " << status.msg;
            return false;
        }
    }

    SetCacheLocked(db, sql, session.engine_mode(), info);
    session.SetCompileInfo(info);
    if (session.is_debug_) {
        std::ostringstream plan_oss;
        if (nullptr != sql_context.physical_plan) {
            sql_context.physical_plan->Print(plan_oss, "");
            LOG(INFO) << "physical plan:\n" << plan_oss.str() << std::endl;
        }
        std::ostringstream runner_oss;
        sql_context.cluster_job.Print(runner_oss, "");
        LOG(INFO) << "cluster job:\n" << runner_oss.str() << std::endl;
    }
    return true;
}

bool Engine::Explain(const std::string& sql, const std::string& db,
                     EngineMode engine_mode,
                     const std::set<size_t>& common_column_indices,
                     ExplainOutput* explain_output, base::Status* status) {
    if (explain_output == NULL || status == NULL) {
        LOG(WARNING) << "input args is invalid";
        return false;
    }
    if (!common_column_indices.empty() && engine_mode != kBatchRequestMode) {
        LOG(WARNING)
            << "common column config can only be valid in batch request mode";
        return false;
    }
    SQLContext ctx;
    ctx.engine_mode = engine_mode;
    ctx.sql = sql;
    ctx.db = db;
    ctx.is_performance_sensitive = options_.is_performance_sensitive();
    ctx.is_cluster_optimized = options_.is_cluster_optimzied();
    ctx.is_batch_request_optimized = !common_column_indices.empty();
    ctx.batch_request_info.common_column_indices = common_column_indices;
    SQLCompiler compiler(
        std::atomic_load_explicit(&cl_, std::memory_order_acquire), true, true,
        true);
    bool ok = compiler.Compile(ctx, *status);
    if (!ok || 0 != status->code) {
        LOG(WARNING) << "fail to compile sql " << sql << " in db " << db
                     << " with error " << *status;
        return false;
    }
    explain_output->input_schema.CopyFrom(ctx.request_schema);
    explain_output->output_schema.CopyFrom(ctx.schema);
    explain_output->logical_plan = ctx.logical_plan_str;
    explain_output->physical_plan = ctx.physical_plan_str;
    explain_output->ir = ctx.ir;
    explain_output->request_name = ctx.request_name;
    if (engine_mode == ::fesql::vm::kBatchMode) {
        std::set<std::string> tables;
        base::Status status;
        for (auto iter = ctx.logical_plan.cbegin();
             iter != ctx.logical_plan.cend(); iter++) {
            if (!GetDependentTables(*iter, &tables, status)) {
                LOG(WARNING) << "fail to get dependent tables " << sql
                             << " in db " << db << " with error " << status;
                break;
            }
        }
        if (!tables.empty()) {
            explain_output->router.SetMainTable(*tables.begin());
        }
    } else {
        explain_output->router.SetMainTable(ctx.request_name);
        explain_output->router.Parse(ctx.physical_plan);
    }
    if (engine_mode == ::fesql::vm::kBatchRequestMode) {
        // fill common output column info
        auto& output_common_indices =
            ctx.batch_request_info.output_common_column_indices;
        size_t schema_size =
            static_cast<size_t>(explain_output->output_schema.size());
        for (size_t idx : output_common_indices) {
            if (idx >= schema_size) {
                LOG(WARNING)
                    << "Output common column indice out of bound: " << idx;
                return false;
            }
            auto* column = explain_output->output_schema.Mutable(idx);
            column->set_is_constant(true);
        }
    }
    return true;
}

bool Engine::Explain(const std::string& sql, const std::string& db,
                     EngineMode engine_mode, ExplainOutput* explain_output,
                     base::Status* status) {
    return Explain(sql, db, engine_mode, {}, explain_output, status);
}

void Engine::ClearCacheLocked(const std::string& db) {
    std::lock_guard<base::SpinMutex> lock(mu_);
    for (auto& cache : lru_cache_) {
        cache.second.erase(db);
    }
}

std::shared_ptr<CompileInfo> Engine::GetCacheLocked(const std::string& db,
                                                    const std::string& sql,
                                                    EngineMode engine_mode) {
    std::lock_guard<base::SpinMutex> lock(mu_);
    auto mode_iter = lru_cache_.find(engine_mode);
    if (mode_iter == lru_cache_.end()) {
        return nullptr;
    }
    auto& mode_cache = mode_iter->second;
    auto db_iter = mode_cache.find(db);
    if (db_iter == mode_cache.end()) {
        return nullptr;
    }
    auto& lru = db_iter->second;
    auto value = lru.get(sql);
    if (value == boost::none) {
        return nullptr;
    } else {
        return value.value();
    }
}

bool Engine::SetCacheLocked(const std::string& db, const std::string& sql,
                            EngineMode engine_mode,
                            std::shared_ptr<CompileInfo> info) {
    std::lock_guard<base::SpinMutex> lock(mu_);
    auto& mode_cache = lru_cache_[engine_mode];
    using BoostLRU =
        boost::compute::detail::lru_cache<std::string,
                                          std::shared_ptr<CompileInfo>>;
    std::map<std::string, BoostLRU>::iterator db_iter = mode_cache.find(db);
    if (db_iter == mode_cache.end()) {
        db_iter = mode_cache.insert(
            db_iter, {db, BoostLRU(options_.max_sql_cache_size())});
    }
    auto& lru = db_iter->second;
    auto value = lru.get(sql);
    if (value == boost::none || engine_mode == kBatchRequestMode) {
        lru.insert(sql, info);
        return true;
    } else {
        // TODO(xxx): Ensure compile result is stable
        DLOG(INFO) << "Engine cache already exists: " << engine_mode << " "
                   << db << "\n"
                   << sql;
        return false;
    }
}

RunSession::RunSession(EngineMode engine_mode)
    : engine_mode_(engine_mode), is_debug_(false), sp_name_("") {}
RunSession::~RunSession() {}

bool RunSession::SetCompileInfo(
    const std::shared_ptr<CompileInfo>& compile_info) {
    compile_info_ = compile_info;
    return true;
}

int32_t RequestRunSession::Run(const Row& in_row, Row* out_row) {
    DLOG(INFO) << "Request Row Run with main task";
    return Run(compile_info_->get_sql_context().cluster_job.main_task_id(),
               in_row, out_row);
}
int32_t RequestRunSession::Run(const uint32_t task_id, const Row& in_row,
                               Row* out_row) {
    auto task =
        compile_info_->get_sql_context().cluster_job.GetTask(task_id).GetRoot();
    if (nullptr == task) {
        LOG(WARNING) << "fail to run request plan: taskid" << task_id
                     << " not exist!";
        return -2;
    }
    DLOG(INFO) << "Request Row Run with task_id " << task_id;
    RunnerContext ctx(&compile_info_->get_sql_context().cluster_job, in_row,
                      sp_name_, is_debug_);
    auto output = task->RunWithCache(ctx);
    if (!output) {
        LOG(WARNING) << "run request plan output is null";
        return -1;
    }
    bool ok = Runner::ExtractRow(output, out_row);
    if (ok) {
        return 0;
    }
    return -1;
}

int32_t BatchRequestRunSession::Run(const std::vector<Row>& request_batch,
                                    std::vector<Row>& output) {
    return Run(compile_info_->get_sql_context().cluster_job.main_task_id(),
               request_batch, output);
}
int32_t BatchRequestRunSession::Run(const uint32_t id,
                                    const std::vector<Row>& request_batch,
                                    std::vector<Row>& output) {
    RunnerContext ctx(&compile_info_->get_sql_context().cluster_job,
                      request_batch, sp_name_, is_debug_);
    auto task =
        compile_info_->get_sql_context().cluster_job.GetTask(id).GetRoot();
    if (nullptr == task) {
        LOG(WARNING) << "fail to run request plan: taskid" << id
                     << " not exist!";
        return -2;
    }
    auto handler = task->BatchRequestRun(ctx);
    if (!handler) {
        LOG(WARNING) << "run request plan output is null";
        return -1;
    }
    bool ok = Runner::ExtractRows(handler, output);
    if (!ok) {
        return -1;
    }
    ctx.ClearCache();
    return 0;
}

std::shared_ptr<TableHandler> BatchRunSession::Run() {
    RunnerContext ctx(&compile_info_->get_sql_context().cluster_job, is_debug_);
    auto output = compile_info_->get_sql_context()
                      .cluster_job.GetMainTask()
                      .GetRoot()
                      ->RunWithCache(ctx);
    if (!output) {
        LOG(WARNING) << "run batch plan output is null";
        return std::shared_ptr<TableHandler>();
    }
    switch (output->GetHanlderType()) {
        case kTableHandler: {
            return std::dynamic_pointer_cast<TableHandler>(output);
        }
        case kRowHandler: {
            auto table =
                std::shared_ptr<MemTableHandler>(new MemTableHandler());
            table->AddRow(
                std::dynamic_pointer_cast<RowHandler>(output)->GetValue());
            return table;
        }
        case kPartitionHandler: {
            LOG(WARNING) << "partition output is invalid";
            return std::shared_ptr<TableHandler>();
        }
    }
    return std::shared_ptr<TableHandler>();
}

int32_t BatchRunSession::Run(std::vector<Row>& rows, uint64_t limit) {
    RunnerContext ctx(&compile_info_->get_sql_context().cluster_job, is_debug_);
    auto output = compile_info_->get_sql_context()
                      .cluster_job.GetTask(0)
                      .GetRoot()
                      ->RunWithCache(ctx);
    if (!output) {
        LOG(WARNING) << "run batch plan output is null";
        return -1;
    }
    switch (output->GetHanlderType()) {
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(output)->GetIterator();
            if (!iter) {
                return 0;
            }
            iter->SeekToFirst();
            while (iter->Valid()) {
                rows.push_back(iter->GetValue());
                iter->Next();
            }
            return 0;
        }
        case kRowHandler: {
            rows.push_back(
                std::dynamic_pointer_cast<RowHandler>(output)->GetValue());
            return 0;
        }
        case kPartitionHandler: {
            LOG(WARNING) << "partition output is invalid";
            return -1;
        }
    }
    return 0;
}

}  // namespace vm
}  // namespace fesql
