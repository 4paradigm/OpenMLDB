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
#include "storage/codec.h"
#include "storage/window.h"
#include "tablet/tablet_catalog.h"
#include "vm/mem_catalog.h"

namespace fesql {
namespace vm {

Engine::Engine(const std::shared_ptr<Catalog>& catalog) : cl_(catalog) {}

Engine::~Engine() {}

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
    info->sql_ctx.sql = sql;
    info->sql_ctx.db = db;
    info->sql_ctx.is_batch_mode = session.IsBatchRun();
    SQLCompiler compiler(cl_, &nm_);
    bool ok = compiler.Compile(info->sql_ctx, status);
    if (!ok || 0 != status.code) {
        // do clean
        return false;
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

int32_t RequestRunSession::Run(const Row& in_row, Row& out_row) {
    int op_size = compile_info_->sql_ctx.ops.ops.size();
    std::vector<std::vector<storage::Row>> temp_buffers(op_size);
    for (auto op : compile_info_->sql_ctx.ops.ops) {
        switch (op->type) {
            case kOpScan: {
                ScanOp* scan_op = reinterpret_cast<ScanOp*>(op);
                std::vector<storage::Row>& out_buffers =
                    temp_buffers[scan_op->idx];
                out_buffers.push_back(in_row);
                break;
            }

            case kOpProject: {
                ProjectOp* project_op = reinterpret_cast<ProjectOp*>(op);
                std::vector<int8_t*> output_rows;
                int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
                    (int32_t(*)(int8_t*, int8_t*, int32_t,
                                int8_t**))project_op->fn;
                OpNode* prev = project_op->children[0];
                std::unique_ptr<storage::RowView> row_view = std::move(
                    std::unique_ptr<storage::RowView>(new storage::RowView(
                        project_op->table_handler->GetSchema())));

                std::vector<::fesql::storage::Row>& in_buffers =
                    temp_buffers[prev->idx];

                std::vector<::fesql::storage::Row>& out_buffers =
                    temp_buffers[project_op->idx];

                if (project_op->window_agg) {
                    auto key_iter = project_op->w.keys.cbegin();
                    ::fesql::type::Type key_type = key_iter->type;
                    uint32_t key_idx = key_iter->pos;
                    for (auto row : in_buffers) {
                        row_view->Reset(row.buf, row.size);
                        int8_t* output = NULL;
                        size_t output_size = 0;
                        // handle window
                        std::string key;
                        {
                            switch (key_type) {
                                case fesql::type::kInt32: {
                                    int32_t value;
                                    if (0 ==
                                        row_view->GetInt32(key_idx, &value)) {
                                        key = std::to_string(value);
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
                                        key = std::to_string(value);
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
                                        key = std::to_string(value);
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
                                        key = std::to_string(value);
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
                                        key = std::to_string(value);
                                    } else {
                                        LOG(WARNING) << "fail to get partition "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kVarchar: {
                                    char* str = nullptr;
                                    uint32_t str_size;
                                    if (0 == row_view->GetString(key_idx, &str,
                                                                 &str_size)) {
                                        key = std::string(str, str_size);
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
                            switch (project_op->w.order.type) {
                                case fesql::type::kInt64: {
                                    if (0 ==
                                        row_view->GetInt64(
                                            project_op->w.order.pos, &ts)) {
                                    } else {
                                        LOG(WARNING) << "fail to get order "
                                                        "for current row";
                                        continue;
                                    }
                                    break;
                                }
                                case fesql::type::kInt32: {
                                    int32_t v;
                                    if (0 == row_view->GetInt32(
                                                 project_op->w.order.pos, &v)) {
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
                                    if (0 == row_view->GetInt16(
                                                 project_op->w.order.pos, &v)) {
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
                        std::unique_ptr<WindowIterator> window_it =
                            project_op->table_handler->GetWindowIterator(
                                project_op->w.index_name);
                        window_it->Seek(key);
                        if (!window_it->Valid()) {
                            LOG(WARNING)
                                << "fail get table iterator when index_name: "
                                << project_op->w.index_name << " key: " << key;
                            return 1;
                        }
                        std::unique_ptr<Iterator> it = window_it->GetValue();
                        std::vector<::fesql::storage::Row> window;
                        if (project_op->w.has_order) {
                            it->Seek(ts);
                        } else {
                            it->SeekToFirst();
                        }
                        while (it->Valid()) {
                            int64_t current_ts = it->GetKey();
                            if (current_ts > ts + project_op->w.end_offset) {
                                it->Next();
                                continue;
                            }
                            if (current_ts <= ts + project_op->w.start_offset) {
                                break;
                            }
                            base::Slice value = it->GetValue();
                            ::fesql::storage::Row w_row;
                            w_row.buf = reinterpret_cast<int8_t*>(
                                const_cast<char*>(value.data()));
                            window.push_back(w_row);
                            window_it->Next();
                        }
                        fesql::storage::ListV<Row> list(&window);
                        fesql::storage::WindowImpl impl(list);
                        uint32_t ret =
                            udf(row.buf, reinterpret_cast<int8_t*>(&impl),
                                row.size, &output);
                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }

                        out_buffers.push_back(Row(output, output_size));
                    }
                } else {
                    for (auto row : in_buffers) {
                        row_view->Reset(row.buf, row.size);
                        int8_t* output = NULL;
                        size_t output_size = 0;
                        // handle window
                        uint32_t ret = udf(row.buf, nullptr, row.size, &output);

                        if (ret != 0) {
                            LOG(WARNING) << "fail to run udf " << ret;
                            return 1;
                        }
                        out_buffers.push_back(
                            ::fesql::storage::Row(output, output_size));
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
                for (uint32_t i = 0; i < in_buffers.size(); ++i) {
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
int32_t BatchRunSession::Run(std::vector<int8_t*>& buf, uint64_t limit) {
    auto output = RunBatchPlan(compile_info_->sql_ctx.plan);
    if (!output) {
        LOG(WARNING) << "run batch plan output is null";
        return -1;
    }
    auto iter = output->GetIterator();
    while (iter->Valid()) {
        buf.push_back(reinterpret_cast<int8_t*>(
            const_cast<char*>(iter->GetValue().data())));
        iter->Next();
    }
    return 0;
}

std::shared_ptr<TableHandler> RunSession::RunBatchPlan(
    const PhysicalOpNode* node) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (nullptr == node) {
        LOG(WARNING) << "run fail: null node";
        return fail_ptr;
    }
    switch (node->type_) {
        case kPhysicalOpDataProvider: {
            auto op = dynamic_cast<const PhysicalDataProviderNode*>(node);
            switch (op->provider_type_) {
                case kProviderTypeTable: {
                    auto provider =
                        dynamic_cast<const PhysicalTableProviderNode*>(node);
                    return provider->table_handler_;
                }
                case kProviderTypeIndexScan: {
                    auto provider =
                        dynamic_cast<const PhysicalScanIndexNode*>(node);
                    return std::shared_ptr<PartitionHandler>(
                        new tablet::TabletPartitionHandler(
                            provider->table_handler_, provider->index_name_));
                }
                default: {
                    LOG(WARNING)
                        << "batch mode doesn't support data provider type "
                        << vm::DataProviderTypeName(op->provider_type_);
                    return fail_ptr;
                }
            }
        }
        case kPhysicalOpProject: {
            auto input = RunBatchPlan(node->GetProducers().at(0));
            if (!input) {
                return fail_ptr;
            }
            auto op = dynamic_cast<const PhysicalProjectNode*>(node);
            switch (op->project_type_) {
                case kTableProject: {
                    auto iter = input->GetIterator();
                    auto output_table = std::shared_ptr<MemTableHandler>(
                        new MemTableHandler(op->output_schema));
                    while (iter->Valid()) {
                        const Row row(
                            RowProject(op->GetFn(), iter->GetValue()));
                        output_table->AddRow(row);
                        iter->Next();
                    }
                    return output_table;
                }
                case kWindowAggregation: {
                    auto output_table = std::shared_ptr<MemTableHandler>(
                        new MemTableHandler(op->output_schema));

                    auto op =
                        dynamic_cast<const PhysicalWindowAggrerationNode*>(
                            node);
                    if (input->IsPartitionTable()) {
                        auto partitions =
                            dynamic_cast<PartitionHandler*>(input.get());
                        auto iter = partitions->GetWindowIterator();
                        while (iter->Valid()) {
                            auto segment = iter->GetValue();
                            storage::CurrentHistoryWindow window(
                                op->start_offset_);
                            while (segment->Valid()) {
                                const Row row(WindowProject(
                                    op->GetFn(), segment->GetKey(),
                                    segment->GetValue(), &window));
                                output_table->AddRow(row);
                                segment->Next();
                            }
                            iter->Next();
                        }
                    } else {
                    }

                    return output_table;
                }
                default: {
                    LOG(WARNING)
                        << "batch mode doesn't support data provider type "
                        << vm::ProjectTypeName(op->project_type_);
                    return fail_ptr;
                }
            }
        }
        case kPhysicalOpGroupBy: {
            auto input = RunBatchPlan(node->GetProducers().at(0));
            if (!input) {
                return fail_ptr;
            }
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            if (node::ExprListNullOrEmpty(op->groups_)) {
                return input;
            }

            std::vector<int> col_idxs;
            for (int j = 0; j < static_cast<int>(op->groups_->children_.size());
                 ++j) {
                col_idxs.push_back(j++);
            }
            if (input->IsPartitionTable()) {
                return PartitionGroup(input, op->GetFnSchema(), op->GetFn(),
                                      col_idxs);
            } else {
                return TableGroup(input, op->GetFnSchema(), op->GetFn(),
                                  col_idxs);
            }
        }
        case kPhysicalOpGroupAndSort: {
            auto input = RunBatchPlan(node->GetProducers().at(0));
            if (!input) {
                return fail_ptr;
            }
            auto op = dynamic_cast<const PhysicalGroupAndSortNode*>(node);
            if (node::ExprListNullOrEmpty(op->groups_) &&
                (nullptr == op->orders_ ||
                 node::ExprListNullOrEmpty(op->orders_->order_by_))) {
                return input;
            }

            return TableSortGroup(input, op->GetFnSchema(), op->GetFn(),
                                  op->groups_, op->orders_);
        }
        case kPhysicalOpLimit: {
            auto input = RunBatchPlan(node->GetProducers().at(0));
            if (!input) {
                return fail_ptr;
            }
            auto op = dynamic_cast<const PhysicalLimitNode*>(node);
            auto iter = input->GetIterator();
            auto output_table = std::shared_ptr<MemTableHandler>(
                new MemTableHandler(op->output_schema));
            int32_t cnt = 0;
            while (cnt++ < op->limit_cnt && iter->Valid()) {
                output_table->AddRow(iter->GetKey(), Row(iter->GetValue()));
                iter->Next();
            }
            return output_table;
        }
        default: {
            LOG(WARNING) << "can't handle node "
                         << vm::PhysicalOpTypeName(node->type_);
            return fail_ptr;
        }
    }
    return fail_ptr;
}

base::Slice RunSession::WindowProject(const int8_t* fn, uint64_t key,
                                      const base::Slice slice,
                                      storage::Window* window) {
    if (slice.empty()) {
        return slice;
    }
    int8_t* data = reinterpret_cast<int8_t*>(const_cast<char*>(slice.data()));
    window->BufferData(key, Row(slice));
    int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(fn);
    int8_t* out_buf = nullptr;
    fesql::storage::WindowImpl impl(*window);
    uint32_t ret =
        udf(data, reinterpret_cast<int8_t*>(&impl), slice.size(), &out_buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return base::Slice();
    }
    return base::Slice(reinterpret_cast<char*>(out_buf),
                       ::fesql::storage::RowView::GetSize(out_buf));
}
base::Slice RunSession::RowProject(const int8_t* fn, const base::Slice slice) {
    if (slice.empty()) {
        return slice;
    }
    int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(fn);
    int8_t* buf = nullptr;
    uint32_t ret =
        udf(reinterpret_cast<int8_t*>(const_cast<char*>(slice.data())), nullptr,
            slice.size(), &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return base::Slice();
    }
    return base::Slice(reinterpret_cast<char*>(buf),
                       storage::RowView::GetSize(buf));
}
base::Slice RunSession::AggProject(const int8_t* fn,
                                   const std::shared_ptr<TableHandler> table) {
    if (!table) {
        return base::Slice();
    }
    auto iter = table->GetIterator();
    iter->SeekToFirst();
    if (!iter->Valid()) {
        return base::Slice();
    }

    auto row = Row(iter->GetValue());

    int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(fn);
    int8_t* buf = nullptr;

    uint32_t ret =
        udf(row.buf, reinterpret_cast<int8_t*>(table->GetIterator().get()),
            row.size, &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return base::Slice();
    }
    return base::Slice(reinterpret_cast<char*>(buf));
}
std::string RunSession::GetColumnString(fesql::storage::RowView* row_view,
                                        int key_idx, type::Type key_type) {
    std::string key = "";
    switch (key_type) {
        case fesql::type::kInt32: {
            int32_t value;
            if (0 == row_view->GetInt32(key_idx, &value)) {
                return std::to_string(value);
            }
            break;
        }
        case fesql::type::kInt64: {
            int64_t value;
            if (0 == row_view->GetInt64(key_idx, &value)) {
                key = std::to_string(value);
            }
            break;
        }
        case fesql::type::kInt16: {
            int16_t value;
            if (0 == row_view->GetInt16(key_idx, &value)) {
                key = std::to_string(value);
            }
            break;
        }
        case fesql::type::kFloat: {
            float value;
            if (0 == row_view->GetFloat(key_idx, &value)) {
                key = std::to_string(value);
            }
            break;
        }
        case fesql::type::kDouble: {
            double value;
            if (0 == row_view->GetDouble(key_idx, &value)) {
                key = std::to_string(value);
            }
            break;
        }
        case fesql::type::kVarchar: {
            char* str = nullptr;
            uint32_t str_size;
            if (0 == row_view->GetString(key_idx, &str, &str_size)) {
                key = std::string(str, str_size);
            }
            break;
        }
        default: {
            LOG(WARNING) << "fail to get partition for "
                            "current row";
            break;
        }
    }
    return key;
}

int64_t RunSession::GetColumnInt64(fesql::storage::RowView* row_view,
                                   int key_idx, type::Type key_type) {
    int64_t key = -1;
    switch (key_type) {
        case fesql::type::kInt32: {
            int32_t value;
            if (0 == row_view->GetInt32(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case fesql::type::kInt64: {
            int64_t value;
            if (0 == row_view->GetInt64(key_idx, &value)) {
                return value;
            }
            break;
        }
        case fesql::type::kInt16: {
            int16_t value;
            if (0 == row_view->GetInt16(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case fesql::type::kFloat: {
            float value;
            if (0 == row_view->GetFloat(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case fesql::type::kDouble: {
            double value;
            if (0 == row_view->GetDouble(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        default: {
            LOG(WARNING) << "fail to get partition for "
                            "current row";
            break;
        }
    }
    return key;
}
std::shared_ptr<TableHandler> RunSession::TableGroup(
    const std::shared_ptr<TableHandler> table, const Schema& schema,
    const int8_t* fn, const std::vector<int>& idxs) {
    if (idxs.empty()) {
        return table;
    }

    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = table->GetIterator();
    std::unique_ptr<storage::RowView> row_view = std::move(
        std::unique_ptr<storage::RowView>(new storage::RowView(schema)));
    while (iter->Valid()) {
        Row value_row(iter->GetValue());
        const Row key_row(RowProject(fn, iter->GetValue()));
        row_view->Reset(key_row.buf, key_row.size);
        std::string keys = "";
        for (auto pos : idxs) {
            std::string key =
                GetColumnString(row_view.get(), pos, schema.Get(pos).type());
            if (!keys.empty()) {
                keys.append("|");
            }
            keys.append(key);
        }
        output_partitions->AddRow(keys, iter->GetKey(), value_row);
        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<TableHandler> RunSession::PartitionGroup(
    const std::shared_ptr<TableHandler> table, const Schema& schema,
    const int8_t* fn, const std::vector<int>& idxs) {
    if (idxs.empty()) {
        return table;
    }
    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));
    auto partitions = dynamic_cast<PartitionHandler*>(table.get());
    auto iter = partitions->GetWindowIterator();
    iter->SeekToFirst();
    std::unique_ptr<storage::RowView> row_view = std::move(
        std::unique_ptr<storage::RowView>(new storage::RowView(schema)));
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            const Row key_row(RowProject(fn, segment_iter->GetValue()));
            row_view->Reset(key_row.buf, key_row.size);
            std::string keys = "";
            for (auto pos : idxs) {
                std::string key = GetColumnString(row_view.get(), pos,
                                                  schema.Get(pos).type());
                if (!keys.empty()) {
                    keys.append("|");
                }
                keys.append(key);
            }
            output_partitions->AddRow(keys, segment_iter->GetKey(),
                                      Row(segment_iter->GetValue()));
        }
        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<TableHandler> RunSession::TableSortGroup(
    std::shared_ptr<TableHandler> table, const Schema& schema, const int8_t* fn,
    const node::ExprListNode* groups, const node::OrderByNode* orders) {
    std::shared_ptr<TableHandler> output;
    std::vector<int> groups_idxs;
    std::vector<int> orders_idxs;

    int idx = 0;

    if (!node::ExprListNullOrEmpty(groups)) {
        for (size_t j = 0; j < groups->children_.size(); ++j) {
            groups_idxs.push_back(idx++);
        }
    }

    if (nullptr != orders && !node::ExprListNullOrEmpty(orders->order_by_)) {
        for (size_t j = 0; j < orders->order_by_->children_.size(); j++) {
            orders_idxs.push_back(idx++);
        }
    }

    if (table->IsPartitionTable()) {
        output = PartitionSort(table, schema, fn, orders_idxs, orders->is_asc_);
    } else {
        output = TableSort(table, schema, fn, orders_idxs, orders->is_asc_);
    }

    if (output->IsPartitionTable()) {
        return PartitionGroup(output, schema, fn, groups_idxs);
    } else {
        return TableGroup(output, schema, fn, groups_idxs);
    }
}
std::shared_ptr<TableHandler> RunSession::PartitionSort(
    std::shared_ptr<TableHandler> table, const Schema& schema, const int8_t* fn,
    std::vector<int> idxs, const bool is_asc) {
    auto partitions = dynamic_cast<PartitionHandler*>(table.get());

    // skip sort, when partition has same order direction
    if (idxs.empty() && partitions->IsAsc() == is_asc) {
        return table;
    }

    std::unique_ptr<storage::RowView> row_view = std::move(
        std::unique_ptr<storage::RowView>(new storage::RowView(schema)));
    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = partitions->GetWindowIterator();
    iter->SeekToFirst();

    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            const Row order_row(RowProject(fn, segment_iter->GetValue()));
            int64_t key = -1;
            if (idxs.empty()) {
                key = segment_iter->GetKey();
            } else {
                row_view->Reset(order_row.buf, order_row.size);
                key = GetColumnInt64(row_view.get(), idxs[0],
                                     schema.Get(idxs[0]).type());
            }
            output_partitions->AddRow(
                std::string(iter->GetKey().data(), iter->GetKey().size()), key,
                Row(segment_iter->GetValue()));
        }
        iter->Next();
    }
    output_partitions->Sort(is_asc);
    return output_partitions;
}
std::shared_ptr<TableHandler> RunSession::TableSort(
    std::shared_ptr<TableHandler> table, const Schema& schema, const int8_t* fn,
    std::vector<int> idxs, const bool is_asc) {
    if (idxs.empty()) {
        return table;
    }
    auto output_table = std::shared_ptr<MemTableHandler>(
        new MemTableHandler(table->GetSchema()));

    std::unique_ptr<storage::RowView> row_view = std::move(
        std::unique_ptr<storage::RowView>(new storage::RowView(schema)));
    auto iter = table->GetIterator();
    while (iter->Valid()) {
        const Row order_row(RowProject(fn, iter->GetValue()));

        row_view->Reset(order_row.buf, order_row.size);

        int64_t key =
            GetColumnInt64(row_view.get(), idxs[0], schema.Get(idxs[0]).type());
        output_table->AddRow(key, Row(iter->GetValue()));
        iter->Next();
    }
    output_table->Sort(is_asc);
    return output_table;
}

}  // namespace vm
}  // namespace fesql
