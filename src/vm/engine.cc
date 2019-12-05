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
#include "base/strings.h"
#include "base/window.h"
#include "codegen/buf_ir_builder.h"

namespace fesql {
namespace vm {

Engine::Engine(TableMgr* table_mgr) : table_mgr_(table_mgr) {}

Engine::~Engine() {}

bool Engine::Get(const std::string& sql, const std::string& db,
                 RunSession& session,
                 base::Status& status) {  // NOLINT (runtime/references)
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
    if (!ok || 0 != status.code) {
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

int32_t RunSession::RunProjectOp(ProjectOp* project_op,
                                 std::shared_ptr<TableStatus> status,
                                 int8_t* row, int8_t* output) {
    return 0;
}
int32_t RunSession::Run(std::vector<int8_t*>& buf, uint32_t limit) {
    // TODO(chenjing): memory managetment
    int index = 0;
    int op_size = compile_info_->sql_ctx.ops.ops.size();
    for (auto op : compile_info_->sql_ctx.ops.ops) {
        switch (op->type) {
            case kOpScan: {
                ScanOp* scan_op = reinterpret_cast<ScanOp*>(op);
                std::shared_ptr<TableStatus> status =
                    table_mgr_->GetTableDef(scan_op->db, scan_op->tid);
                if (!status) {
                    LOG(WARNING)
                        << "fail to find table with tid " << scan_op->tid;
                    return -1;
                }
                std::vector<int8_t*> output_rows;
                ::fesql::storage::TableIterator* it =
                    status->table->NewIterator();
                it->SeekToFirst();
                uint32_t min = limit;
                if (min > scan_op->limit) {
                    min = scan_op->limit;
                }
                uint32_t count = 0;
                while (it->Valid() && count < min) {
                    ::fesql::storage::Slice value = it->GetValue();
                    DLOG(INFO) << "value "
                               << base::DebugString(value.data(), value.size());
                    DLOG(INFO) << "key " << it->GetKey() << " row size ";
                    scan_op->output.push_back(std::make_pair(value.size(), reinterpret_cast<int8_t*>(
                        const_cast<char*>(value.data()))));
                    it->Next();
                }
                break;
            }
            case kOpProject: {
                ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op);
                std::vector<int8_t*> output_rows;

                std::shared_ptr<TableStatus> status =
                    table_mgr_->GetTableDef(project_op->db, project_op->tid);
                uint32_t key_offset;
                ::fesql::type::Type key_type;
                uint32_t order_offset;
                ::fesql::type::Type order_type;
                if (project_op->window_agg) {
                    codegen::BufIRBuilder buf_ir_builder(&status->table_def,
                                                         nullptr, nullptr);
                    if (!buf_ir_builder.GetFieldOffset(project_op->w.keys[0],
                                                       key_offset, key_type)) {
                        LOG(WARNING) << "can not find partition "
                                     << project_op->w.keys[0];
                        return 1;
                    }
                    if (!buf_ir_builder.GetFieldOffset(project_op->w.orders[0],
                                                       order_offset,
                                                       order_type)) {
                        LOG(WARNING)
                            << "can not find order " << project_op->w.orders[0];
                        return 1;
                    }
                }
                int32_t (*udf)(int8_t*, int32_t, int8_t**) =
                    (int32_t(*)(int8_t*, int32_t, int8_t**))project_op->fn;
                OpNode* prev = project_op->children[0];
                for (auto pair: prev->output) {
                    int8_t *row = pair.second;
                    int8_t* output = NULL;
                    int32_t output_size = 0;
                    // handle window
                    if (project_op->window_agg) {
                        std::string key_name;
                        {
                            const int8_t* ptr = row + key_offset;
                            switch (key_type) {
                                case fesql::type::kInt32: {
                                    const int32_t value = *(
                                        reinterpret_cast<const int64_t*>(ptr));
                                    key_name = std::to_string(value);
                                    break;
                                }
                                case fesql::type::kInt64: {
                                    const int64_t value = *(
                                        reinterpret_cast<const int64_t*>(ptr));
                                    key_name = std::to_string(value);
                                    break;
                                }
                                case fesql::type::kInt16: {
                                    const int16_t value = *(
                                        reinterpret_cast<const int16_t*>(ptr));
                                    key_name = std::to_string(value);
                                    break;
                                }
                                default: {
                                }
                            }
                        }
                        int64_t ts;
                        {
                            // TODO(chenjing): handle null ts or timestamp/date
                            // ts
                            const int8_t* ptr = row + order_offset;
                            switch (order_type) {
                                case fesql::type::kInt64: {
                                    ts = *(
                                        reinterpret_cast<const int64_t*>(ptr));
                                    break;
                                }
                                default: {
                                }
                            }
                        }
                        // scan window with single key
                        ::fesql::storage::TableIterator* window_it =
                            status->table->NewIterator(key_name);
                        std::vector<::fesql::base::Row> window;
                        window_it->SeekToFirst();
                        while (window_it->Valid()) {
                            ::fesql::base::Row w_row;
                            ::fesql::storage::Slice value =
                                window_it->GetValue();
                            w_row.buf = reinterpret_cast<int8_t*>(
                                const_cast<char*>(value.data()));
                            window.push_back(w_row);
                            window_it->Next();
                        }
                        fesql::base::WindowIteratorImpl impl(window);
                        uint32_t ret = udf(reinterpret_cast<int8_t*>(&impl), pair.first, &output);
                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }

                    } else {
                        uint32_t ret = udf(row, pair.first, &output);

                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }
                    }
                    project_op->output.push_back(std::make_pair(output_size,output));
                }
                break;
            }
            case kOpMerge: {
                // TODO(chenjing): add merge execute logic
                MergeOp* merge_op = reinterpret_cast<MergeOp*>(op);
                break;
            }
            case kOpLimit: {
                // TODO(chenjing): limit optimized.
                LimitOp* limit_op = reinterpret_cast<LimitOp*>(op);
                OpNode* prev = limit_op->children[0];
                int cnt = 0;
                for (int i = 0; i < limit_op->limit; ++i) {
                    if (cnt >= limit_op->limit) {
                        break;
                    }
                    limit_op->output.push_back(prev->output[i]);
                    cnt++;
                }
                break;
            }
        }
        index++;
        if (index == op_size) {
            for(auto pair: op->output) {
                buf.push_back(pair.second);
            }
        }
    }
    return 0;
}

}  // namespace vm
}  // namespace fesql
