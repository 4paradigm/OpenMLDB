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

namespace fesql {
namespace vm {

Engine::Engine(TableMgr* table_mgr):table_mgr_(table_mgr) {}

Engine::~Engine() {}

bool Engine::Get(const std::string& sql, 
        RunSession& session) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::map<std::string, std::shared_ptr<CompileInfo>>::iterator it = cache_.find(sql);
        if (it != cache_.end()) {
            session.SetCompileInfo(it->second);
            session.SetTableMgr(table_mgr_);
            return true;
        }
    }

    std::shared_ptr<CompileInfo> info(new CompileInfo());
    info->sql_ctx.sql = sql;
    SQLCompiler compiler(table_mgr_);
    bool ok = compiler.Compile(info->sql_ctx);
    if (!ok) {
        // do clean
        return false;
    }
    info->row_size = info->sql_ctx.row_size;
    {
        // check 
        std::lock_guard<std::mutex> lock(mu_);
        std::map<std::string, std::shared_ptr<CompileInfo>>::iterator it = cache_.find(sql);
        session.SetTableMgr(table_mgr_);
        if (it == cache_.end()) {
            cache_.insert(std::make_pair(sql, info));
            session.SetCompileInfo(info);
        }else {
            session.SetCompileInfo(it->second);
            // TODO clean
        }
    }
    return true;
}

RunSession::RunSession() {}
RunSession::~RunSession() {}

int32_t RunSession::Run(std::vector<int8_t*>& buf, uint32_t length,
        uint32_t* row_cnt) {
    if (row_cnt) {
        LOG(WARNING) << "buf or row cnt is null ";
        return -1;
    }
    // Simple op runner
    ScanOp* scan_op = (ScanOp*)(compile_info_->sql_ctx.ops.ops[0]);
    ProjectOp* project_op = (ProjectOp*)(compile_info_->sql_ctx.ops.ops[1]);
    LimitOp* limit_op = (LimitOp*)(compile_info_->sql_ctx.ops.ops[2]);
    TableStatus* status = NULL;
    bool ok = table_mgr_->GetTableDef(0, scan_op->tid, &status);
    if (!ok || status == NULL) {
        LOG(WARNING) << "fail to find table with tid " << scan_op->tid;
        return -1;
    }
    ::fesql::storage::TableIterator* it = status->table->NewIterator();
    it->SeekToFirst();
    uint32_t min = length;
    if (min > limit_op->limit) {
        min = limit_op->limit;
    }
    int32_t (*udf)(int8_t*, int8_t*) = (int32_t(*)(int8_t*, int8_t*))project_op->fn;
    uint32_t count = 0;
    while (it->Valid() && count < min) {
        ::fesql::storage::Slice value = it->GetValue();
        // TODO use const int8_t*
        int8_t* output = (int8_t*)malloc(project_op->output_size);
        int8_t* row = reinterpret_cast<int8_t*>(const_cast<char*>(value.data()));
        uint32_t ret = udf(row, output);
        if (ret != 0) {
            return 1;
        }
        buf.push_back(output);
    }
    return 0;
}

}  // namespace vm
}  // namespace fesql



