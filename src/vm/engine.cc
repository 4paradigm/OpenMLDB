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
                                 int8_t* row, int8_t* output) {}

int32_t RunSession::RunOne(Row& in_row, Row& out_row) {
    int op_size = compile_info_->sql_ctx.ops.ops.size();
    std::vector<std::vector<::fesql::storage::Row>> temp_buffers(op_size);
    for (auto op : compile_info_->sql_ctx.ops.ops) {
        switch (op->type) {
            case kOpScan: {
                ScanOp* scan_op = reinterpret_cast<ScanOp*>(op);
                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[scan_op->idx];
                out_buffers.push_back(in_row);
                break;
            }
            case kOpProject: {
                ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op);
                std::shared_ptr<TableStatus> status =
                    table_mgr_->GetTableDef(project_op->db, project_op->tid);
                if (!status) {
                    LOG(WARNING)
                        << "fail to find table with tid " << project_op->tid;
                    return 1;
                }
                std::vector<int8_t*> output_rows;
                int32_t (*udf)(int8_t*, int32_t, int8_t**) =
                    (int32_t(*)(int8_t*, int32_t, int8_t**))project_op->fn;
                OpNode* prev = project_op->children[0];
                std::unique_ptr<storage::RowView> row_view =
                    std::move(std::unique_ptr<storage::RowView>(
                        new storage::RowView(status->table_def.columns())));

                std::vector<::fesql::storage::Row>& in_buffers =
                    temp_buffers[prev->idx];

                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[project_op->idx];

                if (project_op->window_agg) {
                    auto key_iter = project_op->w.keys.cbegin();
                    ::fesql::type::Type key_type = key_iter->first;
                    uint32_t key_idx = key_iter->second;
                    for (auto row : in_buffers) {
                        row_view->Reset(row.buf, row.size);
                        int8_t* output = NULL;
                        size_t output_size = 0;
                        // handle window
                        std::string key_name;
                        {
                            switch (key_type) {
                                case fesql::type::kInt32: {
                                    int32_t value;
                                    if (0 ==
                                        row_view->GetInt32(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetInt64(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetInt16(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetFloat(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetDouble(key_idx, &value)) {
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
                                    if (0 == row_view->GetString(key_idx, &str,
                                                                 &str_size)) {
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
                        if (project_op->w.has_order) {
                            // TODO(chenjing): handle null ts or
                            // timestamp/date ts
                            switch (project_op->w.order.first) {
                                case fesql::type::kInt64: {
                                    if (0 ==
                                        row_view->GetInt64(
                                            project_op->w.order.second, &ts)) {
                                    } else {
                                        LOG(WARNING) << "fail to get order "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kInt32: {
                                    int32_t v;
                                    if (0 ==
                                        row_view->GetInt32(
                                            project_op->w.order.second, &v)) {
                                        ts = static_cast<int64_t>(v);
                                    } else {
                                        LOG(WARNING) << "fail to get order "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kInt16: {
                                    int16_t v;
                                    if (0 ==
                                        row_view->GetInt16(
                                            project_op->w.order.second, &v)) {
                                        ts = static_cast<int64_t>(v);
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
                                    continue;
                                }
                            }
                        }
                        // scan window with single key
                        std::unique_ptr<::fesql::storage::TableIterator>
                            window_it = status->table->NewIterator(
                                key_name, project_op->w.index_name);

                        if (!window_it) {
                            LOG(WARNING)
                                << "fail get table iterator when index_name: "
                                << project_op->w.index_name
                                << " key: " << key_name;
                            return 1;
                        }
                        std::vector<::fesql::storage::Row> window;
                        if (project_op->w.has_order) {
                            window_it->Seek(ts);
                        } else {
                            window_it->SeekToFirst();
                        }
                        while (window_it->Valid()) {
                            int64_t current_ts = window_it->GetKey();
                            if (current_ts > ts + project_op->w.end_offset) {
                                window_it->Next();
                                continue;
                            }
                            if (current_ts <= ts + project_op->w.start_offset) {
                                break;
                            }
                            ::fesql::storage::Slice value =
                                window_it->GetValue();
                            ::fesql::storage::Row w_row;
                            w_row.buf = reinterpret_cast<int8_t*>(
                                const_cast<char*>(value.data()));
                            window.push_back(w_row);
                            window_it->Next();
                        }
                        fesql::storage::WindowIteratorImpl impl(window);
                        uint32_t ret = udf(reinterpret_cast<int8_t*>(&impl),
                                           row.size, &output);
                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }

                        out_buffers.push_back(::fesql::storage::Row{
                            .buf = output, .size = output_size});
                    }
                } else {
                    for (auto row : in_buffers) {
                        row_view->Reset(row.buf, row.size);
                        int8_t* output = NULL;
                        size_t output_size = 0;
                        // handle window
                        uint32_t ret = udf(row.buf, row.size, &output);

                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }
                        out_buffers.push_back(::fesql::storage::Row{
                            .buf = output, .size = output_size});
                    }
                }

                // TODO(chenjing): handle multi keys

                break;
            }
            case kOpMerge: {
                MergeOp* merge_op = reinterpret_cast<MergeOp*>(op);
                if (merge_op->children.size() <= 1) {
                    LOG(WARNING)
                        << "fail to merge when children size less than 2";
                    return 1;
                }
                // TODO(chenjing): add merge execute logic

                break;
            }
            case kOpLimit: {
                // TODO(chenjing): limit optimized.
                LimitOp* limit_op = reinterpret_cast<LimitOp*>(op);
                OpNode* prev = limit_op->children[0];
                std::vector<::fesql::storage::Row>& in_buffers =
                    temp_buffers[prev->idx];
                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[limit_op->idx];
                uint32_t cnt = 0;
                for (uint32_t i = 0; i < limit_op->limit; ++i) {
                    if (cnt >= limit_op->limit) {
                        break;
                    }
                    out_buffers.push_back(in_buffers[i]);
                    cnt++;
                }
                break;
            }
        }
    }
    return 0;
}
int32_t RunSession::RunBatch(std::vector<int8_t*>& buf, uint32_t limit) {
    int op_size = compile_info_->sql_ctx.ops.ops.size();
    std::vector<std::vector<::fesql::storage::Row>> temp_buffers(op_size);
    for (auto op : compile_info_->sql_ctx.ops.ops) {
        switch (op->type) {
            case kOpScan: {
                break;
            }
            case kOpProject: {
                ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op);
                // table
                std::shared_ptr<TableStatus> status =
                    table_mgr_->GetTableDef(project_op->db, project_op->tid);
                if (!status) {
                    LOG(WARNING)
                        << "fail to find table with tid " << project_op->tid;
                    return 1;
                }

                // op function
                int32_t (*udf)(int8_t*, int32_t, int8_t**) =
                    (int32_t(*)(int8_t*, int32_t, int8_t**))project_op->fn;

                std::unique_ptr<storage::RowView> row_view =
                    std::move(std::unique_ptr<storage::RowView>(
                        new storage::RowView(status->table_def.columns())));

                // in out buffers
                OpNode* prev = project_op->children[0];
                std::vector<::fesql::storage::Row>& in_buffers =
                    temp_buffers[prev->idx];
                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[project_op->idx];

                if (project_op->window_agg) {
                    uint64_t min = limit > 0 ? limit : INT64_MAX;
                    if (project_op->scan_limit != 0 &&
                        min > project_op->scan_limit) {
                        min = project_op->scan_limit;
                    }

                    // iterator whole table
                    std::unique_ptr<::fesql::storage::TableIterator> it =
                        status->table->NewTraverseIterator(
                            project_op->w.index_name);
                    it->SeekToFirst();
                    uint32_t count = 0;
                    while (it->Valid() && count < min) {
                        std::vector<std::pair<uint64_t, Row>> buffer;
                        // TODO(chenjing): resize or reserve with count
                        buffer.reserve(10000);
                        while (it->CurrentTsValid()) {
                            ::fesql::storage::Slice value = it->GetValue();
                            ::fesql::storage::Row row(
                                {.buf = reinterpret_cast<int8_t*>(
                                     const_cast<char*>(value.data())),
                                 .size = value.size()});
                            buffer.push_back(std::make_pair(it->GetKey(), row));
                            it->NextTs();
                        }
                        it->NextTsInPks();

                        // TODO(chenjing): decide window type
                        ::fesql::storage::CurrentHistoryWindow window(
                            project_op->w.start_offset);
                        for (auto iter = buffer.rbegin();
                             count < min && iter != buffer.rend(); iter++) {
                            window.BufferData(iter->first, iter->second);
                            int8_t* output = NULL;
                            size_t output_size = 0;
                            // handle window
                            fesql::storage::WindowIteratorImpl impl(window);
                            uint32_t ret = udf(reinterpret_cast<int8_t*>(&impl),
                                               iter->second.size, &output);
                            if (ret != 0) {
                                LOG(WARNING) << "fail to run udf " << ret;
                                return 1;
                            }
                            out_buffers.push_back(::fesql::storage::Row{
                                .buf = output, .size = output_size});
                            count++;
                        }
                    }
                } else {
                    // iterator whole table
                    std::unique_ptr<::fesql::storage::TableIterator> it =
                        status->table->NewTraverseIterator();
                    it->SeekToFirst();
                    uint64_t min = limit > 0 ? limit : INT64_MAX;
                    if (project_op->scan_limit != 0 &&
                        min > project_op->scan_limit) {
                        min = project_op->scan_limit;
                    }
                    uint32_t count = 0;
                    while (it->Valid() && count++ < min) {
                        ::fesql::storage::Slice value = it->GetValue();
                        ::fesql::storage::Row row(
                            {.buf = reinterpret_cast<int8_t*>(
                                 const_cast<char*>(value.data())),
                             .size = value.size()});
                        int8_t* output = NULL;
                        size_t output_size = 0;
                        // handle window
                        uint32_t ret = udf(row.buf, row.size, &output);
                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }
                        out_buffers.push_back(::fesql::storage::Row{
                            .buf = output, .size = output_size});
                        it->Next();
                    }
                }
                // TODO(chenjing): handle multi keys
                break;
            }
            case kOpMerge: {
                MergeOp* merge_op = reinterpret_cast<MergeOp*>(op);
                if (merge_op->children.size() <= 1) {
                    LOG(WARNING)
                        << "fail to merge when children size less than 2";
                    return 1;
                }
                // TODO(chenjing): add merge execute logic

                break;
            }
            case kOpLimit: {
                // TODO(chenjing): limit optimized.
                LimitOp* limit_op = reinterpret_cast<LimitOp*>(op);
                OpNode* prev = limit_op->children[0];
                std::vector<::fesql::storage::Row>& in_buffers =
                    temp_buffers[prev->idx];
                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[limit_op->idx];
                uint32_t cnt = 0;
                for (uint32_t i = 0; i < limit_op->limit; ++i) {
                    if (cnt >= limit_op->limit) {
                        break;
                    }
                    out_buffers.push_back(in_buffers[i]);
                    cnt++;
                }
                break;
            }
        }
    }
    for (auto row : temp_buffers[op_size - 1]) {
        buf.push_back(row.buf);
    }
    return 0;
}
int32_t RunSession::Run(std::vector<int8_t*>& buf, uint32_t limit) {
    int op_size = compile_info_->sql_ctx.ops.ops.size();
    std::vector<std::vector<::fesql::storage::Row>> temp_buffers(op_size);
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
                    status->table->NewTraverseIterator();
                it->SeekToFirst();
                uint32_t min = limit;
                if (min > scan_op->limit) {
                    min = scan_op->limit;
                }
                uint32_t count = 0;
                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[scan_op->idx];
                while (it->Valid() && count++ < min) {
                    ::fesql::storage::Slice value = it->GetValue();
                    out_buffers.push_back(::fesql::storage::Row{
                        .buf = reinterpret_cast<int8_t*>(
                            const_cast<char*>(value.data())),
                        .size = value.size()});
                    it->Next();
                }
                break;
            }
            case kOpProject: {
                ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op);
                std::shared_ptr<TableStatus> status =
                    table_mgr_->GetTableDef(project_op->db, project_op->tid);
                if (!status) {
                    LOG(WARNING)
                        << "fail to find table with tid " << project_op->tid;
                    return 1;
                }
                std::vector<int8_t*> output_rows;
                int32_t (*udf)(int8_t*, int32_t, int8_t**) =
                    (int32_t(*)(int8_t*, int32_t, int8_t**))project_op->fn;
                OpNode* prev = project_op->children[0];
                std::unique_ptr<storage::RowView> row_view =
                    std::move(std::unique_ptr<storage::RowView>(
                        new storage::RowView(status->table_def.columns())));

                std::vector<::fesql::storage::Row>& in_buffers =
                    temp_buffers[prev->idx];

                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[project_op->idx];

                if (project_op->window_agg) {
                    auto key_iter = project_op->w.keys.cbegin();
                    ::fesql::type::Type key_type = key_iter->first;
                    uint32_t key_idx = key_iter->second;
                    for (auto row : in_buffers) {
                        row_view->Reset(row.buf, row.size);
                        int8_t* output = NULL;
                        size_t output_size = 0;
                        // handle window
                        std::string key_name;
                        {
                            switch (key_type) {
                                case fesql::type::kInt32: {
                                    int32_t value;
                                    if (0 ==
                                        row_view->GetInt32(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetInt64(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetInt16(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetFloat(key_idx, &value)) {
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
                                    if (0 ==
                                        row_view->GetDouble(key_idx, &value)) {
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
                                    if (0 == row_view->GetString(key_idx, &str,
                                                                 &str_size)) {
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
                        if (project_op->w.has_order) {
                            // TODO(chenjing): handle null ts or
                            // timestamp/date ts
                            switch (project_op->w.order.first) {
                                case fesql::type::kInt64: {
                                    if (0 ==
                                        row_view->GetInt64(
                                            project_op->w.order.second, &ts)) {
                                    } else {
                                        LOG(WARNING) << "fail to get order "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kInt32: {
                                    int32_t v;
                                    if (0 ==
                                        row_view->GetInt32(
                                            project_op->w.order.second, &v)) {
                                        ts = static_cast<int64_t>(v);
                                    } else {
                                        LOG(WARNING) << "fail to get order "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kInt16: {
                                    int16_t v;
                                    if (0 ==
                                        row_view->GetInt16(
                                            project_op->w.order.second, &v)) {
                                        ts = static_cast<int64_t>(v);
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
                                    continue;
                                }
                            }
                        }
                        // scan window with single key
                        std::unique_ptr<::fesql::storage::TableIterator>
                            window_it = status->table->NewIterator(
                                key_name, project_op->w.index_name);

                        if (!window_it) {
                            LOG(WARNING)
                                << "fail get table iterator when index_name: "
                                << project_op->w.index_name
                                << " key: " << key_name;
                            return 1;
                        }
                        std::vector<::fesql::storage::Row> window;
                        if (project_op->w.has_order) {
                            window_it->Seek(ts);
                        } else {
                            window_it->SeekToFirst();
                        }
                        while (window_it->Valid()) {
                            int64_t current_ts = window_it->GetKey();
                            if (current_ts > ts + project_op->w.end_offset) {
                                window_it->Next();
                                continue;
                            }
                            if (current_ts <= ts + project_op->w.start_offset) {
                                break;
                            }
                            ::fesql::storage::Slice value =
                                window_it->GetValue();
                            ::fesql::storage::Row w_row;
                            w_row.buf = reinterpret_cast<int8_t*>(
                                const_cast<char*>(value.data()));
                            window.push_back(w_row);
                            window_it->Next();
                        }
                        fesql::storage::WindowIteratorImpl impl(window);
                        uint32_t ret = udf(reinterpret_cast<int8_t*>(&impl),
                                           row.size, &output);
                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }

                        out_buffers.push_back(::fesql::storage::Row{
                            .buf = output, .size = output_size});
                    }
                } else {
                    for (auto row : in_buffers) {
                        row_view->Reset(row.buf, row.size);
                        int8_t* output = NULL;
                        size_t output_size = 0;
                        // handle window
                        uint32_t ret = udf(row.buf, row.size, &output);

                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }
                        out_buffers.push_back(::fesql::storage::Row{
                            .buf = output, .size = output_size});
                    }
                }

                // TODO(chenjing): handle multi keys

                break;
            }
            case kOpMerge: {
                MergeOp* merge_op = reinterpret_cast<MergeOp*>(op);
                if (merge_op->children.size() <= 1) {
                    LOG(WARNING)
                        << "fail to merge when children size less than 2";
                    return 1;
                }
                // TODO(chenjing): add merge execute logic

                break;
            }
            case kOpLimit: {
                // TODO(chenjing): limit optimized.
                LimitOp* limit_op = reinterpret_cast<LimitOp*>(op);
                OpNode* prev = limit_op->children[0];
                std::vector<::fesql::storage::Row>& in_buffers =
                    temp_buffers[prev->idx];
                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[limit_op->idx];
                uint32_t cnt = 0;
                for (uint32_t i = 0; i < limit_op->limit; ++i) {
                    if (cnt >= limit_op->limit) {
                        break;
                    }
                    out_buffers.push_back(in_buffers[i]);
                    cnt++;
                }
                break;
            }
        }
    }
    for (auto row : temp_buffers[op_size - 1]) {
        buf.push_back(row.buf);
    }
    return 0;
}

}  // namespace vm
}  // namespace fesql
