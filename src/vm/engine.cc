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
#include "codec/list_iterator_codec.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"
#include "codegen/buf_ir_builder.h"
#include "gflags/gflags.h"
#include "llvm-c/Target.h"
#include "vm/mem_catalog.h"

DECLARE_bool(logtostderr);
DECLARE_string(log_dir);

namespace fesql {
namespace vm {

Engine::Engine(const std::shared_ptr<Catalog>& catalog) : cl_(catalog) {}

Engine::Engine(const std::shared_ptr<Catalog>& catalog,
               const EngineOptions& options)
    : cl_(catalog), options_(options) {}

Engine::~Engine() {}

void Engine::InitializeGlobalLLVM() {
    FLAGS_logtostderr = false;
    FLAGS_log_dir = "/tmp";
    const char* arg = "";
    google::InitGoogleLogging(arg);
    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
}

bool Engine::Get(const std::string& sql, const std::string& db,
                 RunSession& session,
                 base::Status& status) {  // NOLINT (runtime/references)
    {
        std::shared_ptr<CompileInfo> info = GetCacheLocked(db, sql);
        if (info) {
            session.SetCompileInfo(info);
            session.SetCatalog(cl_);
            return true;
        }
    }

    std::shared_ptr<CompileInfo> info(new CompileInfo());
    info->get_sql_context().sql = sql;
    info->get_sql_context().db = db;
    info->get_sql_context().is_batch_mode = session.IsBatchRun();
    SQLCompiler compiler(cl_, &nm_, options_.is_keep_ir());
    bool ok = compiler.Compile(info->get_sql_context(), status);
    if (!ok || 0 != status.code) {
        // TODO(chenjing): do clean
        return false;
    }

    if (!options_.is_compile_only()) {
        ok = compiler.BuildRunner(info->get_sql_context(), status);
        if (!ok || 0 != status.code) {
            // TODO(chenjing): do clean
            return false;
        }
    }

    {
        session.SetCatalog(cl_);
        // check
        std::lock_guard<base::SpinMutex> lock(mu_);
        std::map<std::string, std::shared_ptr<CompileInfo>>& sql_in_db =
            cache_[db];
        std::map<std::string, std::shared_ptr<CompileInfo>>::iterator it =
            sql_in_db.find(sql);
        if (it == sql_in_db.end()) {
            // TODO(wangtaize) clean
            sql_in_db.insert(std::make_pair(sql, info));
            session.SetCompileInfo(info);
        } else {
            session.SetCompileInfo(it->second);
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
    ::fesql::node::NodeManager nm;
    SQLContext ctx;
    ctx.is_batch_mode = is_batch;
    ctx.sql = sql;
    ctx.db = db;
    SQLCompiler compiler(cl_, &nm, true, true);
    bool ok = compiler.Compile(ctx, *status);
    if (!ok || 0 != status->code) {
        LOG(WARNING) << "fail to compile sql " << sql << " in db " << db
                     << " with error " << status->msg;
        return false;
    }
    explain_output->input_schema.CopyFrom(ctx.request_schema);
    explain_output->output_schema.CopyFrom(ctx.schema);
    explain_output->logical_plan = ctx.logical_plan;
    explain_output->physical_plan = ctx.physical_plan;
    explain_output->ir = ctx.ir;
    return true;
}

std::shared_ptr<CompileInfo> Engine::GetCacheLocked(const std::string& db,
                                                    const std::string& sql) {
    std::lock_guard<base::SpinMutex> lock(mu_);
    EngineCache::iterator it = cache_.find(db);
    if (it == cache_.end()) {
        return std::shared_ptr<CompileInfo>();
    }
    std::map<std::string, std::shared_ptr<CompileInfo>>::iterator iit =
        it->second.find(sql);
    if (iit == it->second.end()) {
        return std::shared_ptr<CompileInfo>();
    }
    return iit->second;
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
int32_t BatchRunSession::Run(std::vector<int8_t*>& buf, uint64_t limit) {
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
            while (iter->Valid()) {
                buf.push_back(iter->GetValue().buf());
                iter->Next();
            }
            return 0;
        }
        case kRowHandler: {
            buf.push_back(std::dynamic_pointer_cast<RowHandler>(output)
                              ->GetValue()
                              .buf());
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
