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
                               std::shared_ptr<CompileInfo> info) {
    auto& cache_ctx = info->get_sql_context();
    if (cache_ctx.engine_mode != session.engine_mode()) {
        return false;
    }
    if (session.engine_mode() == kBatchRequestMode) {
        auto batch_req_sess = dynamic_cast<BatchRequestRunSession*>(&session);
        if (batch_req_sess == nullptr) {
            return false;
        }
        auto& cache_indices = cache_ctx.common_column_indices;
        auto& sess_indices = batch_req_sess->common_column_indices();
        if (sess_indices.size() != cache_indices.size()) {
            return false;
        }
        for (size_t i = 0; i < sess_indices.size(); ++i) {
            if (sess_indices[i] != cache_indices[i]) {
                return false;
            }
        }
    }
    return true;
}

bool Engine::Get(const std::string& sql, const std::string& db,
                 RunSession& session,
                 base::Status& status) {  // NOLINT (runtime/references)
    std::shared_ptr<CompileInfo> info =
        GetCacheLocked(db, sql, session.engine_mode());
    if (info && IsCompatibleCache(session, info)) {
        session.SetCompileInfo(info);
        return true;
    }

    info = std::shared_ptr<CompileInfo>(new CompileInfo());
    auto& sql_context = info->get_sql_context();
    sql_context.sql = sql;
    sql_context.db = db;
    sql_context.engine_mode = session.engine_mode();
    sql_context.is_performance_sensitive = options_.is_performance_sensitive();

    auto batch_req_sess = dynamic_cast<BatchRequestRunSession*>(&session);
    if (batch_req_sess) {
        sql_context.common_column_indices =
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
            return false;
        }
    }

    bool ovewrite_cache = session.engine_mode() == kBatchRequestMode;
    SetCacheLocked(db, sql, session.engine_mode(), ovewrite_cache, info);
    session.SetCompileInfo(info);
    return true;
}

bool Engine::Explain(const std::string& sql, const std::string& db,
                     EngineMode engine_mode, ExplainOutput* explain_output,
                     base::Status* status) {
    if (explain_output == NULL || status == NULL) {
        LOG(WARNING) << "input args is invalid";
        return false;
    }
    SQLContext ctx;
    ctx.engine_mode = engine_mode;
    ctx.sql = sql;
    ctx.db = db;
    ctx.is_performance_sensitive = options_.is_performance_sensitive();
    SQLCompiler compiler(
        std::atomic_load_explicit(&cl_, std::memory_order_acquire), true, true);
    bool ok = compiler.Compile(ctx, *status);
    if (!ok || 0 != status->code) {
        LOG(WARNING) << "fail to compile sql " << sql << " in db " << db
                     << " with error " << status;
        return false;
    }
    explain_output->input_schema.CopyFrom(ctx.request_schema);
    explain_output->output_schema.CopyFrom(ctx.schema);
    explain_output->logical_plan = ctx.logical_plan_str;
    explain_output->physical_plan = ctx.physical_plan_str;
    explain_output->ir = ctx.ir;
    return true;
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
                            EngineMode engine_mode, bool overwrite,
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
    if (value == boost::none || overwrite) {
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
    : engine_mode_(engine_mode), is_debug_(false) {}
RunSession::~RunSession() {}

bool RunSession::SetCompileInfo(
    const std::shared_ptr<CompileInfo>& compile_info) {
    compile_info_ = compile_info;
    return true;
}

static bool ExtractSingleRow(std::shared_ptr<DataHandler> handler,
                             Row* out_row) {
    switch (handler->GetHanlderType()) {
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(handler)->GetIterator();
            if (!iter) {
                return false;
            }
            iter->SeekToFirst();
            if (iter->Valid()) {
                *out_row = iter->GetValue();
                return true;
            } else {
                return false;
            }
        }
        case kRowHandler: {
            *out_row =
                std::dynamic_pointer_cast<RowHandler>(handler)->GetValue();
            return true;
        }
        case kPartitionHandler: {
            LOG(WARNING) << "partition output is invalid";
            return false;
        }
        default: {
            return false;
        }
    }
}

int32_t RequestRunSession::Run(const Row& in_row, Row* out_row) {
    return Run(0, in_row, out_row);
}
int32_t RequestRunSession::Run(const uint32_t task_id, const Row& in_row,
                               Row* out_row) {
    auto task = compile_info_->get_sql_context().cluster_job.GetTask(task_id);
    if (nullptr == task) {
        LOG(WARNING) << "fail to run request plan: taskid" << task_id
                     << " not exist!";
        return -2;
    }
    RunnerContext ctx(in_row, is_debug_);
    auto output = task->RunWithCache(ctx);
    if (!output) {
        LOG(WARNING) << "run request plan output is null";
        return -1;
    }
    bool ok = ExtractSingleRow(output, out_row);
    if (ok) {
        return 0;
    }
    return -1;
}

int32_t BatchRequestRunSession::Run(const std::vector<Row>& request_batch,
                                    std::vector<Row>& output) {
    return Run(0, request_batch, output);
}
int32_t BatchRequestRunSession::Run(const uint32_t id,
                                    const std::vector<Row>& request_batch,
                                    std::vector<Row>& output) {
    RunnerContext ctx(is_debug_);
    for (size_t i = 0; i < request_batch.size(); ++i) {
        output.push_back(Row());
        int32_t ok = RunSingle(ctx, id, request_batch[i], &output.back());
        if (ok != 0) {
            return -1;
        }
    }
    return 0;
}

int32_t BatchRequestRunSession::RunSingle(RunnerContext& ctx,  // NOLINT
                                          const Row& request,
                                          Row* output) {  // NOLINT
    return RunSingle(ctx, 0, request, output);
}
int32_t BatchRequestRunSession::RunSingle(RunnerContext& ctx,  // NOLINT
                                          const uint32_t task_id,
                                          const Row& request,
                                          Row* output) {  // NOLINT
    auto task = compile_info_->get_sql_context().cluster_job.GetTask(task_id);
    if (nullptr == task) {
        LOG(WARNING) << "fail to run request plan: taskid" << task_id
                     << " not exist!";
        return -2;
    }
    ctx.SetRequest(request);
    auto handler = task->RunWithCache(ctx);
    if (!handler) {
        LOG(WARNING) << "run request plan output is null";
        return -1;
    }
    bool ok = ExtractSingleRow(handler, output);
    if (!ok) {
        return -1;
    }
    return 0;
}

std::shared_ptr<TableHandler> BatchRunSession::Run() {
    RunnerContext ctx(is_debug_);
    auto output =
        compile_info_->get_sql_context().cluster_job.GetTask(0)->RunWithCache(
            ctx);
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
    RunnerContext ctx(is_debug_);
    auto output =
        compile_info_->get_sql_context().cluster_job.GetTask(0)->RunWithCache(
            ctx);
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
