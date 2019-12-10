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
#include "codegen/buf_ir_builder.h"
#include "storage/window.h"

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
                    return 1;
                }
                std::vector<int8_t*> output_rows;
                std::unique_ptr<::fesql::storage::TableIterator> it =
                    status->table->NewIterator();
                it->SeekToFirst();
                uint32_t min = limit;
                if (min > scan_op->limit) {
                    min = scan_op->limit;
                }
                uint32_t count = 0;
                while (it->Valid() && count < min) {
                    ::fesql::storage::Slice value = it->GetValue();
                    scan_op->output.push_back(std::make_pair(
                        value.size(), reinterpret_cast<int8_t*>(
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
                int32_t (*udf)(int8_t*, int32_t, int8_t**) =
                    (int32_t(*)(int8_t*, int32_t, int8_t**))project_op->fn;
                OpNode* prev = project_op->children[0];
                std::unique_ptr<storage::RowView> row_view =
                    std::move(std::unique_ptr<storage::RowView>(
                        new storage::RowView(status->table_def.columns())));
                for (auto pair : prev->output) {
                    row_view->Reset(pair.second, pair.first);
                    int8_t* output = NULL;
                    int32_t output_size = 0;
                    // handle window
                    if (project_op->window_agg) {
                        std::string key_name;
                        {
                            switch (project_op->w.keys[0].first) {
                                case fesql::type::kInt32: {
                                    int32_t value;
                                    if (0 == row_view->GetInt32(
                                                 project_op->w.keys[0].second,
                                                 &value)) {
                                        key_name = std::to_string(value);
                                    } else {
                                        LOG(WARNING) << "fail to get partition "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kInt64: {
                                    int64_t value;
                                    if (0 == row_view->GetInt64(
                                                 project_op->w.keys[0].second,
                                                 &value)) {
                                        key_name = std::to_string(value);
                                    } else {
                                        LOG(WARNING) << "fail to get partition "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kInt16: {
                                    int16_t value;
                                    if (0 == row_view->GetInt16(
                                                 project_op->w.keys[0].second,
                                                 &value)) {
                                        key_name = std::to_string(value);
                                    } else {
                                        LOG(WARNING) << "fail to get partition "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kFloat: {
                                    float value;
                                    if (0 == row_view->GetFloat(
                                                 project_op->w.keys[0].second,
                                                 &value)) {
                                        key_name = std::to_string(value);
                                    } else {
                                        LOG(WARNING) << "fail to get partition "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kDouble: {
                                    double value;
                                    if (0 == row_view->GetDouble(
                                                 project_op->w.keys[0].second,
                                                 &value)) {
                                        key_name = std::to_string(value);
                                    } else {
                                        LOG(WARNING) << "fail to get partition "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kVarchar: {
                                    char* str;
                                    uint32_t str_size;
                                    if (0 == row_view->GetString(
                                                 project_op->w.keys[0].second,
                                                 &str, &str_size)) {
                                        key_name = std::string(str, str_size);
                                    } else {
                                        LOG(WARNING) << "fail to get partition "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                default: {
                                    LOG(WARNING) << "fail to get partition for "
                                                    "current row";
                                    break;
                                }
                            }
                        }
                        int64_t ts;
                        {
                            // TODO(chenjing): handle null ts or timestamp/date
                            // ts
                            switch (project_op->w.orders[0].first) {

                                case fesql::type::kInt64: {
                                    if (0 == row_view->GetInt64(
                                        project_op->w.orders[0].second,
                                        &ts)) {
                                    } else {
                                        LOG(WARNING) << "fail to get order "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                default: {
                                    LOG(WARNING) << "fail to get order for "
                                                    "current row";
                                    break;
                                }
                            }
                        }
                        // scan window with single key
                        std::unique_ptr<::fesql::storage::TableIterator>
                            window_it = status->table->NewIterator(key_name);

                        std::vector<::fesql::storage::Row> window;
                        window_it->SeekToFirst();
                        while (window_it->Valid()) {
                            ::fesql::storage::Row w_row;
                            ::fesql::storage::Slice value =
                                window_it->GetValue();
                            w_row.buf = reinterpret_cast<int8_t*>(
                                const_cast<char*>(value.data()));
                            window.push_back(w_row);
                            window_it->Next();
                        }
                        fesql::storage::WindowIteratorImpl impl(window);
                        uint32_t ret = udf(reinterpret_cast<int8_t*>(&impl),
                                           pair.first, &output);
                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }

                    } else {
                        uint32_t ret = udf(pair.second, pair.first, &output);

                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }
                    }
                    project_op->output.push_back(
                        std::make_pair(output_size, output));
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
            for (auto pair : op->output) {
                buf.push_back(pair.second);
            }
        }
    }
    return 0;
}

}  // namespace vm
}  // namespace fesql
