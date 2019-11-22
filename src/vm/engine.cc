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

#include <utility>
#include <string>
#include <vector>
#include "base/strings.h"

namespace fesql {
namespace vm {

Engine::Engine(TableMgr* table_mgr) : table_mgr_(table_mgr) {}

Engine::~Engine() {}

bool Engine::Get(const std::string& sql, const std::string& db,
                 RunSession& session,
                 common::Status& status) {  // NOLINT (runtime/references)
    {
        std::shared_ptr<CompileInfo> info = GetCacheLocked(db, sql);
        if (info) {
            session.SetCompileInfo(info);
            session.SetTableMgr(table_mgr_);
            return true;
        }
    }

    std::shared_ptr<CompileInfo> info(new CompileInfo());
    info->sql_ctx.sql = sql;
    info->sql_ctx.db = db;
    SQLCompiler compiler(table_mgr_);
    bool ok = compiler.Compile(info->sql_ctx, status);
    if (!ok || 0 != status.code()) {
        // do clean
        return false;
    }
    info->row_size = 2 + info->sql_ctx.row_size;
    {
        session.SetTableMgr(table_mgr_);
        // check
        std::lock_guard<base::SpinMutex> lock(mu_);
        std::map<std::string, std::shared_ptr<CompileInfo>>& sql_in_db =
            cache_[db];
        std::map<std::string, std::shared_ptr<CompileInfo>>::iterator it =
            sql_in_db.find(sql);
        if (it == sql_in_db.end()) {
            sql_in_db.insert(std::make_pair(sql, info));
            session.SetCompileInfo(info);
        } else {
            session.SetCompileInfo(it->second);
            // TODO(wangtaize) clean
        }
    }
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

RunSession::RunSession() {}
RunSession::~RunSession() {}

int32_t RunSession::Run(std::vector<int8_t*>& buf, uint32_t limit) {
    // Simple op runner
    ScanOp* scan_op =
        reinterpret_cast<ScanOp*>(compile_info_->sql_ctx.ops.ops[0]);
    ProjectOp* project_op =
        reinterpret_cast<ProjectOp*>(compile_info_->sql_ctx.ops.ops[1]);
    LimitOp* limit_op =
        reinterpret_cast<LimitOp*>(compile_info_->sql_ctx.ops.ops[2]);
    std::shared_ptr<TableStatus> status =
        table_mgr_->GetTableDef(scan_op->db, scan_op->tid);
    if (!status) {
        LOG(WARNING) << "fail to find table with tid " << scan_op->tid;
        return -1;
    }
    ::fesql::storage::TableIterator* it = status->table->NewIterator();
    it->SeekToFirst();
    uint32_t min = limit;
    if (min > limit_op->limit) {
        min = limit_op->limit;
    }
    int32_t (*udf)(int8_t*, int8_t*) =
        (int32_t(*)(int8_t*, int8_t*))project_op->fn;
    uint32_t count = 0;
    while (it->Valid() && count < min) {
        ::fesql::storage::Slice value = it->GetValue();
        DLOG(INFO) << "value " << base::DebugString(value.data(), value.size());
        DLOG(INFO) << "key " << it->GetKey() << " row size "
                   << 2 + project_op->output_size;
        int8_t* output =
            reinterpret_cast<int8_t*>(malloc(2 + project_op->output_size));

        int8_t* row =
            reinterpret_cast<int8_t*>(const_cast<char*>(value.data()));
        uint32_t ret = udf(row, output + 2);
        if (ret != 0) {
            LOG(WARNING) << "fail to run udf " << ret;
            delete it;
            return 1;
        }
        buf.push_back(output);
        it->Next();
        count++;
    }
    delete it;
    return 0;
}

}  // namespace vm
}  // namespace fesql
