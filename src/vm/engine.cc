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
#include "codec/list_iterator_codec.h"
#include "codec/row_codec.h"
#include "codegen/buf_ir_builder.h"
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

int32_t RequestRunSession::Run(const Slice& in_row, Slice* out_row) {
    RunnerContext ctx(in_row);
    auto output = compile_info_->sql_ctx.runner->RunWithCache(ctx);
    if (!output) {
        LOG(WARNING) << "run batch plan output is null";
        return -1;
    }
    switch (output->GetHanlderType()) {
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(output)->GetIterator();
            if (iter->Valid()) {
                Slice row(iter->GetValue());
                *out_row = row;
            }
            return 0;
        }
        case kRowHandler: {
            Slice row(
                std::dynamic_pointer_cast<RowHandler>(output)->GetValue());
            *out_row = row;
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
    RunnerContext ctx;
    auto output = compile_info_->sql_ctx.runner->RunWithCache(ctx);
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
    RunnerContext ctx;
    auto output = compile_info_->sql_ctx.runner->RunWithCache(ctx);
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
            Slice row(
                std::dynamic_pointer_cast<RowHandler>(output)->GetValue());
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

std::shared_ptr<DataHandler> RunSession::RunPhysicalPlan(
    const PhysicalOpNode* node, const Slice* row) {
    DLOG(INFO) << "Physical Op " << PhysicalOpTypeName(node->type_) << ">> ";
    auto fail_ptr = std::shared_ptr<DataHandler>();
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
                    return provider->table_handler_;
                }
                case kProviderTypeRequest: {
                    return std::shared_ptr<MemRowHandler>(
                        new MemRowHandler(*row, &(op->output_schema)));
                }
                default: {
                    LOG(WARNING) << "fail to support data provider type "
                                 << DataProviderTypeName(op->provider_type_);
                    return fail_ptr;
                }
            }
        }
        case kPhysicalOpProject: {
            auto input = RunPhysicalPlan(node->GetProducers().at(0), row);
            auto op = dynamic_cast<const PhysicalProjectNode*>(node);
            switch (op->project_type_) {
                case kTableProject: {
                    return TableProject(op->GetFn(), input, op->GetLimitCnt(),
                                        op->output_schema);
                }
                case kAggregation: {
                    return std::shared_ptr<MemRowHandler>(new MemRowHandler(
                        AggProject(op->GetFn(), input), &(op->output_schema)));
                }
                case kWindowAggregation: {
                    auto window_op =
                        dynamic_cast<const PhysicalWindowAggrerationNode*>(
                            node);
                    return WindowAggProject(window_op, input,
                                            op->GetLimitCnt());
                }
                case kRowProject: {
                    if (!input) {
                        LOG(WARNING) << "input is empty";
                        return fail_ptr;
                    }
                    auto row = std::dynamic_pointer_cast<RowHandler>(input);
                    return std::shared_ptr<MemRowHandler>(new MemRowHandler(
                        RowProject(op->GetFn(), row->GetValue()),
                        &(op->output_schema)));
                }
                default: {
                    LOG(WARNING) << "fail to support data provider type "
                                 << ProjectTypeName(op->project_type_);
                    return fail_ptr;
                }
            }
        }
        case kPhysicalOpIndexSeek: {
            auto left = RunPhysicalPlan(node->GetProducers().at(0), row);
            auto right = RunPhysicalPlan(node->GetProducers().at(1), row);
            auto seek_op = dynamic_cast<const PhysicalSeekIndexNode*>(node);
            return IndexSeek(left, right, seek_op);
        }
        case kPhysicalOpRequestUnoin: {
            auto left = RunPhysicalPlan(node->GetProducers().at(0), row);
            auto right = RunPhysicalPlan(node->GetProducers().at(1), row);
            auto request_union_op =
                dynamic_cast<const PhysicalRequestUnionNode*>(node);
            return RequestUnion(left, right, request_union_op);
        }
        case kPhysicalOpGroupBy: {
            auto input = RunPhysicalPlan(node->GetProducers().at(0), row);
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            return Group(input, op);
        }
        case kPhysicalOpGroupAndSort: {
            auto input = RunPhysicalPlan(node->GetProducers().at(0), row);
            auto op = dynamic_cast<const PhysicalGroupAndSortNode*>(node);
            return TableSortGroup(input, op);
        }
        case kPhysicalOpLimit: {
            auto input = RunPhysicalPlan(node->GetProducers().at(0), row);
            auto op = dynamic_cast<const PhysicalLimitNode*>(node);
            return Limit(input, op);
        }
        default: {
            LOG(WARNING) << "can't handle node " << node->type_ << " "
                         << PhysicalOpTypeName(node->type_);
            return fail_ptr;
        }
    }
    return fail_ptr;
}  // namespace vm

Slice RunSession::WindowProject(const int8_t* fn, uint64_t key,
                                const Slice slice, Window* window) {
    if (slice.empty()) {
        return slice;
    }
    window->BufferData(key, Slice(slice));
    int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(fn);
    int8_t* out_buf = nullptr;
    uint32_t ret = udf(slice.buf(), reinterpret_cast<int8_t*>(window),
                       slice.size(), &out_buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Slice();
    }
    return Slice(reinterpret_cast<char*>(out_buf), RowView::GetSize(out_buf));
}
Slice RunSession::RowProject(const int8_t* fn, const Slice slice) {
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
        return Slice();
    }
    return Slice(reinterpret_cast<char*>(buf), RowView::GetSize(buf));
}
Slice RunSession::AggProject(const int8_t* fn,
                             const std::shared_ptr<DataHandler> input) {
    if (!input) {
        LOG(WARNING) << "input is empty";
        return Slice();
    }

    if (kTableHandler != input->GetHanlderType()) {
        return Slice();
    }
    auto table = std::dynamic_pointer_cast<TableHandler>(input);
    auto iter = table->GetIterator();
    iter->SeekToFirst();
    if (!iter->Valid()) {
        return Slice();
    }

    auto row = Slice(iter->GetValue());

    int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(fn);
    int8_t* buf = nullptr;

    uint32_t ret = udf(row.buf(), reinterpret_cast<int8_t*>(table.get()),
                       row.size(), &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Slice();
    }
    return Slice(reinterpret_cast<char*>(buf));
}
std::string RunSession::GetColumnString(RowView* row_view, int key_idx,
                                        type::Type key_type) {
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
int64_t RunSession::GetColumnInt64(RowView* row_view, int key_idx,
                                   type::Type key_type) {
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
std::shared_ptr<DataHandler> RunSession::TableGroup(
    const std::shared_ptr<DataHandler> table, const Schema& schema,
    const int8_t* fn, const std::vector<int>& idxs) {
    if (idxs.empty()) {
        return table;
    }
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (kTableHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    std::unique_ptr<RowView> row_view =
        std::move(std::unique_ptr<RowView>(new RowView(schema)));
    while (iter->Valid()) {
        Slice value_row(iter->GetValue());
        const Slice key_row(RowProject(fn, iter->GetValue()));
        row_view->Reset(key_row.buf(), key_row.size());
        std::string keys = GenerateKeys(row_view.get(), schema, idxs);
        output_partitions->AddRow(keys, iter->GetKey(), value_row);
        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<DataHandler> RunSession::PartitionGroup(
    const std::shared_ptr<DataHandler> table, const Schema& schema,
    const int8_t* fn, const std::vector<int>& idxs) {
    if (idxs.empty()) {
        return table;
    }

    if (!table) {
        return std::shared_ptr<DataHandler>();
    }

    if (kPartitionHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));
    auto partitions = std::dynamic_pointer_cast<PartitionHandler>(table);
    auto iter = partitions->GetWindowIterator();
    iter->SeekToFirst();
    std::unique_ptr<RowView> row_view =
        std::move(std::unique_ptr<RowView>(new RowView(schema)));
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            const Slice key_row(RowProject(fn, segment_iter->GetValue()));
            row_view->Reset(key_row.buf(), key_row.size());
            std::string keys = GenerateKeys(row_view.get(), schema, idxs);
            output_partitions->AddRow(keys, segment_iter->GetKey(),
                                      Slice(segment_iter->GetValue()));
            segment_iter->Next();
        }
        iter->Next();
    }
    return output_partitions;
}

std::shared_ptr<DataHandler> RunSession::TableSortGroup(
    std::shared_ptr<DataHandler> table,
    const PhysicalGroupAndSortNode* group_sort_op) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (node::ExprListNullOrEmpty(group_sort_op->groups_) &&
        nullptr == group_sort_op->orders_) {
        return table;
    }
    const Schema& schema = group_sort_op->GetFnSchema();
    const int8_t* fn = group_sort_op->GetFn();
    return TableSortGroup(table, fn, schema, group_sort_op->GetGroupsIdxs(),
                          group_sort_op->GetOrdersIdxs(),
                          nullptr == group_sort_op->orders_
                              ? true
                              : group_sort_op->orders_->is_asc_);
}

std::shared_ptr<DataHandler> RunSession::TableSortGroup(
    std::shared_ptr<DataHandler> table, const int8_t* fn, const Schema& schema,
    const std::vector<int>& groups_idxs, const std::vector<int>& orders_idxs,
    const bool is_asc) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }

    std::shared_ptr<DataHandler> output;
    switch (table->GetHanlderType()) {
        case kPartitionHandler:
            output = PartitionSort(table, schema, fn, orders_idxs, is_asc);
            break;
        case kTableHandler:
            output = TableSort(table, schema, fn, orders_idxs, is_asc);
            break;
        default: {
            LOG(WARNING) << "fail to sort and group table: input isn't table "
                            "or partition";
            return std::shared_ptr<DataHandler>();
        }
    }

    switch (output->GetHanlderType()) {
        case kPartitionHandler:
            return PartitionGroup(output, schema, fn, groups_idxs);
        case kTableHandler:
            return TableGroup(output, schema, fn, groups_idxs);
        default: {
            LOG(WARNING) << "fail to sort and group table: input isn't table "
                            "or partition";
            return std::shared_ptr<DataHandler>();
        }
    }
}
std::shared_ptr<DataHandler> RunSession::PartitionSort(
    std::shared_ptr<DataHandler> table, const Schema& schema, const int8_t* fn,
    std::vector<int> idxs, const bool is_asc) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (kPartitionHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    auto partitions = std::dynamic_pointer_cast<PartitionHandler>(table);

    // skip sort, when partition has same order direction
    if (idxs.empty() && partitions->IsAsc() == is_asc) {
        return table;
    }

    std::unique_ptr<RowView> row_view =
        std::move(std::unique_ptr<RowView>(new RowView(schema)));
    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = partitions->GetWindowIterator();
    iter->SeekToFirst();

    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            int64_t key = -1;
            if (idxs.empty()) {
                key = segment_iter->GetKey();
            } else {
                const Slice order_row(RowProject(fn, segment_iter->GetValue()));
                row_view->Reset(order_row.buf(), order_row.size());
                key = GetColumnInt64(row_view.get(), idxs[0],
                                     schema.Get(idxs[0]).type());
            }
            output_partitions->AddRow(
                std::string(iter->GetKey().data(), iter->GetKey().size()), key,
                Slice(segment_iter->GetValue()));
            segment_iter->Next();
        }
        iter->Next();
    }
    output_partitions->Sort(is_asc);
    return output_partitions;
}
std::shared_ptr<DataHandler> RunSession::TableSort(
    std::shared_ptr<DataHandler> table, const Schema& schema, const int8_t* fn,
    std::vector<int> idxs, const bool is_asc) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (kTableHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    if (idxs.empty()) {
        return table;
    }
    auto output_table = std::shared_ptr<MemTableHandler>(
        new MemTableHandler(table->GetSchema()));

    std::unique_ptr<RowView> row_view =
        std::move(std::unique_ptr<RowView>(new RowView(schema)));
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    while (iter->Valid()) {
        const Slice order_row(RowProject(fn, iter->GetValue()));

        row_view->Reset(order_row.buf(), order_row.size());

        int64_t key =
            GetColumnInt64(row_view.get(), idxs[0], schema.Get(idxs[0]).type());
        output_table->AddRow(key, Slice(iter->GetValue()));
        iter->Next();
    }
    output_table->Sort(is_asc);
    return output_table;
}
std::shared_ptr<DataHandler> RunSession::TableProject(
    const int8_t* fn, std::shared_ptr<DataHandler> table,
    const int32_t limit_cnt, Schema output_schema) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (kTableHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    auto output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(&output_schema));
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();

    int32_t cnt = 0;
    while (iter->Valid()) {
        if (limit_cnt > 0 && cnt++ >= limit_cnt) {
            break;
        }
        const Slice row(RowProject(fn, iter->GetValue()));
        output_table->AddRow(row);
        iter->Next();
    }
    return output_table;
}

std::shared_ptr<DataHandler> RunSession::WindowAggProject(
    const PhysicalWindowAggrerationNode* op, std::shared_ptr<DataHandler> input,
    const int32_t limit_cnt) {
    return WindowAggProject(input, limit_cnt, op->GetFn(), op->GetFnSchema(),
                            op->start_offset_, op->end_offset_);
}

std::shared_ptr<DataHandler> RunSession::WindowAggProject(
    std::shared_ptr<DataHandler> input, const int32_t limit_cnt,
    const int8_t* fn, const Schema& fn_schema, const int64_t start_offset,
    const int64_t end_offset) {
    if (!input) {
        LOG(WARNING) << "window aggregation fail: input is null";
        return std::shared_ptr<DataHandler>();
    }

    if (kPartitionHandler != input->GetHanlderType()) {
        LOG(WARNING) << "window aggregation requires partition input";
        return std::shared_ptr<DataHandler>();
    }

    auto output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(&fn_schema));

    auto partitions = std::dynamic_pointer_cast<PartitionHandler>(input);
    auto iter = partitions->GetWindowIterator();
    int32_t cnt = 0;
    while (iter->Valid()) {
        auto segment = iter->GetValue();
        CurrentHistoryWindow window(start_offset);
        while (segment->Valid()) {
            if (limit_cnt > 0 && cnt++ >= limit_cnt) {
                break;
            }
            const Slice row(WindowProject(fn, segment->GetKey(),
                                          segment->GetValue(), &window));
            output_table->AddRow(row);
            segment->Next();
        }
        iter->Next();
    }
    return output_table;
}
std::string RunSession::GenerateKeys(RowView* row_view, const Schema& schema,
                                     const std::vector<int>& idxs) {
    std::string keys = "";
    for (auto pos : idxs) {
        std::string key =
            GetColumnString(row_view, pos, schema.Get(pos).type());
        if (!keys.empty()) {
            keys.append("|");
        }
        keys.append(key);
    }
    return keys;
}
std::shared_ptr<DataHandler> RunSession::IndexSeek(
    std::shared_ptr<DataHandler> left, std::shared_ptr<DataHandler> right,
    const PhysicalSeekIndexNode* seek_op) {
    std::vector<int> idxs;
    for (int j = 0; j < static_cast<int>(seek_op->keys_->children_.size());
         ++j) {
        idxs.push_back(j++);
    }
    return IndexSeek(left, right, seek_op->GetFn(), seek_op->GetFnSchema(),
                     idxs);
}

std::shared_ptr<DataHandler> RunSession::IndexSeek(
    std::shared_ptr<DataHandler> left, std::shared_ptr<DataHandler> right,
    const int8_t* fn, const Schema& fn_schema, const std::vector<int>& idxs) {
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    if (kPartitionHandler != right->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    std::shared_ptr<PartitionHandler> partition =
        std::dynamic_pointer_cast<PartitionHandler>(right);

    std::unique_ptr<RowView> row_view =
        std::move(std::unique_ptr<RowView>(new RowView(fn_schema)));
    auto keys_row = Slice(RowProject(
        fn, std::dynamic_pointer_cast<RowHandler>(left)->GetValue()));
    row_view->Reset(keys_row.buf(), keys_row.size());

    std::string key = GenerateKeys(row_view.get(), fn_schema, idxs);
    return partition->GetSegment(partition, key);
}

std::shared_ptr<DataHandler> RunSession::RequestUnion(
    std::shared_ptr<DataHandler> left, std::shared_ptr<DataHandler> right,
    const PhysicalRequestUnionNode* request_union_op) {
    return RequestUnion(
        left, right, request_union_op->GetFn(), request_union_op->GetFnSchema(),
        request_union_op->GetGroupsIdxs(), request_union_op->GetOrdersIdxs(),
        request_union_op->GetKeysIdxs(), request_union_op->start_offset_,
        request_union_op->end_offset_);
}

std::shared_ptr<DataHandler> RunSession::RequestUnion(
    std::shared_ptr<DataHandler> left, std::shared_ptr<DataHandler> right,
    const int8_t* fn, const Schema& fn_schema,
    const std::vector<int>& groups_idxs, const std::vector<int>& orders_idxs,
    const std::vector<int>& keys_idxs, const int64_t start_offset,
    const int64_t end_offset) {
    if (!left || !right) {
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    if (kPartitionHandler == right->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    auto output_schema = left->GetSchema();
    auto request = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();

    auto table = std::dynamic_pointer_cast<TableHandler>(right);
    std::shared_ptr<DataHandler> output = right;
    std::unique_ptr<RowView> row_view =
        std::move(std::unique_ptr<RowView>(new RowView(fn_schema)));
    auto request_fn_row = Slice(RowProject(fn, request));
    // filter by keys if need
    if (!groups_idxs.empty()) {
        row_view->Reset(request_fn_row.buf(), request_fn_row.size());
        std::string request_keys =
            GenerateKeys(row_view.get(), fn_schema, groups_idxs);

        auto mem_table = new MemTableHandler(output_schema);
        auto iter = table->GetIterator();
        while (iter->Valid()) {
            auto row = Slice(RowProject(fn, iter->GetValue()));
            row_view->Reset(row.buf(), row.size());
            std::string keys =
                GenerateKeys(row_view.get(), fn_schema, groups_idxs);
            if (request_keys == keys) {
                mem_table->AddRow(Slice(iter->GetValue()));
            }
            iter->Next();
        }
        output = std::shared_ptr<TableHandler>(mem_table);
    }

    // sort by orders if need
    if (!orders_idxs.empty()) {
        output = TableSort(std::shared_ptr<DataHandler>(output), fn_schema, fn,
                           orders_idxs, false);
    }

    // build window with start and end offset
    auto window_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(output_schema));
    row_view->Reset(request_fn_row.buf(), request_fn_row.size());

    uint64_t start = 0;
    uint64_t end = UINT64_MAX;

    if (!keys_idxs.empty()) {
        auto ts_idx = keys_idxs[0];
        int64_t key = GetColumnInt64(row_view.get(), ts_idx,
                                     fn_schema.Get(ts_idx).type());

        start = (key + start_offset) < 0 ? 0 : (key + start_offset);

        end = (key + end_offset) < 0 ? 0 : (key + end_offset);
        DLOG(INFO) << "request key: " << key;
    }

    window_table->AddRow(Slice(request));

    DLOG(INFO) << "start make window ";
    if (output) {
        auto table_output = std::dynamic_pointer_cast<TableHandler>(output);
        auto table_iter = table_output->GetIterator();
        if (table_iter) {
            table_iter->Seek(end);
            while (table_iter->Valid()) {
                DLOG(INFO) << table_iter->GetKey() << " should >= " << start;
                if (table_iter->GetKey() <= start) {
                    break;
                }
                DLOG(INFO) << table_iter->GetKey() << " add row ";
                window_table->AddRow(table_iter->GetKey(),
                                     Slice(table_iter->GetValue()));
                table_iter->Next();
            }
        }
    }
    return window_table;
}
std::shared_ptr<DataHandler> RunSession::Group(
    std::shared_ptr<DataHandler> input, const PhysicalGroupNode* op) {
    return Group(input, op->GetFn(), op->GetFnSchema(), op->GetGroupsIdxs());
}

std::shared_ptr<DataHandler> RunSession::Group(
    std::shared_ptr<DataHandler> input, const int8_t* fn,
    const Schema& fn_schema, const std::vector<int>& idxs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    if (idxs.empty()) {
        return input;
    }

    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            return PartitionGroup(input, fn_schema, fn, idxs);
        }
        case kTableHandler: {
            return TableGroup(input, fn_schema, fn, idxs);
        }
        default: {
            LOG(WARNING) << "fail group when input type isn't "
                            "partition or table";
            return fail_ptr;
        }
    }
}
std::shared_ptr<DataHandler> RunSession::Limit(
    std::shared_ptr<DataHandler> input, const PhysicalLimitNode* op) {
    if (op->GetLimitOptimized()) {
        DLOG(INFO) << "Limit Optimized";
        return input;
    }
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    switch (input->GetHanlderType()) {
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(input)->GetIterator();
            auto output_table = std::shared_ptr<MemTableHandler>(
                new MemTableHandler(&(op->output_schema)));
            int32_t cnt = 0;
            while (cnt++ < op->GetLimitCnt() && iter->Valid()) {
                output_table->AddRow(iter->GetKey(), Slice(iter->GetValue()));
                iter->Next();
            }
            return output_table;
        }
        case kRowProject: {
            DLOG(INFO) << "limit row handler";
            return input;
        }
        case kPartitionHandler: {
            LOG(WARNING) << "fail limit when input type isn't row or table";
            return fail_ptr;
        }
    }
    return fail_ptr;
}
// namespace vm

}  // namespace vm
}  // namespace fesql
