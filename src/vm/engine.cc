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
    : cl_(catalog),
      options_(),
      mu_(),
      batch_lru_cache_(),
      request_lru_cache_() {}

Engine::Engine(const std::shared_ptr<Catalog>& catalog,
               const EngineOptions& options)
    : cl_(catalog),
      options_(options),
      mu_(),
      batch_lru_cache_(),
      request_lru_cache_() {}

Engine::~Engine() {}

void Engine::InitializeGlobalLLVM() {
    if (LLVM_IS_INITIALIZED) return;
    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
    LLVM_IS_INITIALIZED = true;
}

bool Engine::GetDependentTables(const std::string& sql, const std::string& db,
                                bool is_batch_mode,
                                std::set<std::string>* tables,
                                base::Status& status) {
    std::shared_ptr<CompileInfo> info(new CompileInfo());
    info->get_sql_context().sql = sql;
    info->get_sql_context().db = db;
    info->get_sql_context().is_batch_mode = is_batch_mode;
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
bool Engine::Get(const std::string& sql, const std::string& db,
                 RunSession& session,
                 base::Status& status) {  // NOLINT (runtime/references)
    {
        std::shared_ptr<CompileInfo> info =
            GetCacheLocked(db, sql, session.IsBatchRun());
        if (info) {
            session.SetCompileInfo(info);
            return true;
        }
    }

    std::shared_ptr<CompileInfo> info(new CompileInfo());
    info->get_sql_context().sql = sql;
    info->get_sql_context().db = db;
    info->get_sql_context().is_batch_mode = session.IsBatchRun();
    info->get_sql_context().is_performance_sensitive =
        options_.is_performance_sensitive();
    SQLCompiler compiler(
        std::atomic_load_explicit(&cl_, std::memory_order_acquire),
        options_.is_keep_ir(), false, options_.is_plan_only());
    bool ok = compiler.Compile(info->get_sql_context(), status);
    if (!ok || 0 != status.code) {
        return false;
    }
    if (!options_.is_compile_only()) {
        ok = compiler.BuildRunner(info->get_sql_context(), status);
        if (!ok || 0 != status.code) {
            return false;
        }
    }
    {
        std::lock_guard<base::SpinMutex> lock(mu_);
        if (session.IsBatchRun()) {
            auto it = batch_lru_cache_.find(db);
            if (it == batch_lru_cache_.end()) {
                batch_lru_cache_.insert(std::make_pair(
                    db, boost::compute::detail::lru_cache<
                            std::string, std::shared_ptr<CompileInfo>>(
                            options_.max_sql_cache_size())));
                it = batch_lru_cache_.find(db);
            }
            auto value = it->second.get(sql);
            if (value == boost::none) {
                it->second.insert(sql, info);
                session.SetCompileInfo(info);
            } else {
                session.SetCompileInfo(value.value());
            }
        } else {
            auto it = request_lru_cache_.find(db);
            if (it == request_lru_cache_.end()) {
                request_lru_cache_.insert(std::make_pair(
                    db, boost::compute::detail::lru_cache<
                            std::string, std::shared_ptr<CompileInfo>>(
                            options_.max_sql_cache_size())));
                it = request_lru_cache_.find(db);
            }
            auto value = it->second.get(sql);
            if (value == boost::none) {
                it->second.insert(sql, info);
                session.SetCompileInfo(info);
            } else {
                session.SetCompileInfo(value.value());
            }
        }
    }
    return true;
}

bool Engine::Explain(const std::string& sql, const std::string& db,
                     bool is_batch, ExplainOutput* explain_output,
                     base::Status* status) {
    if (explain_output == NULL || status == NULL) {
        LOG(WARNING) << "input args is invalid";
        return false;
    }
    SQLContext ctx;
    ctx.is_batch_mode = is_batch;
    ctx.sql = sql;
    ctx.db = db;
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
    batch_lru_cache_.erase(db);
    request_lru_cache_.erase(db);
}

std::shared_ptr<CompileInfo> Engine::GetCacheLocked(const std::string& db,
                                                    const std::string& sql,
                                                    bool is_batch) {
    std::lock_guard<base::SpinMutex> lock(mu_);
    if (is_batch) {
        auto it = batch_lru_cache_.find(db);
        if (it == batch_lru_cache_.end()) {
            return std::shared_ptr<CompileInfo>();
        }
        auto value = it->second.get(sql);
        if (value == boost::none) {
            return std::shared_ptr<CompileInfo>();
        }
        return value.value();
    } else {
        auto it = request_lru_cache_.find(db);
        if (it == request_lru_cache_.end()) {
            return std::shared_ptr<CompileInfo>();
        }
        auto value = it->second.get(sql);
        if (value == boost::none) {
            return std::shared_ptr<CompileInfo>();
        }
        return value.value();
    }
}

RunSession::RunSession() : is_debug_(false) {}
RunSession::~RunSession() {}

bool RunSession::SetCompileInfo(
    const std::shared_ptr<CompileInfo>& compile_info) {
    compile_info_ = compile_info;
    return true;
}

int32_t RequestRunSession::Run(const Row& in_row, Row* out_row) {
    RunnerContext ctx(in_row, is_debug_);
    auto output = compile_info_->get_sql_context().runner->RunWithCache(ctx);
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
            if (iter->Valid()) {
                *out_row = iter->GetValue();
            }
            return 0;
        }
        case kRowHandler: {
            *out_row =
                std::dynamic_pointer_cast<RowHandler>(output)->GetValue();
            return 0;
        }
        case kPartitionHandler: {
            LOG(WARNING) << "partition output is invalid";
            return -1;
        }
    }
    return 0;
}

std::shared_ptr<TableHandler> BatchRunSession::Run() {
    RunnerContext ctx(is_debug_);
    auto output = compile_info_->get_sql_context().runner->RunWithCache(ctx);
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
    auto output = compile_info_->get_sql_context().runner->RunWithCache(ctx);
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
