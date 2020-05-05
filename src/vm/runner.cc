/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * runner.cc
 *
 * Author: chenjing
 * Date: 2020/4/3
 *--------------------------------------------------------------------------
 **/
#include "vm/runner.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "vm/core_api.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace vm {
#define MAX_DEBUG_LINES_CNT 10

Runner* RunnerBuilder::Build(PhysicalOpNode* node, Status& status) {
    if (nullptr == node) {
        status.msg = "fail to build runner : physical node is null";
        status.code = common::kOpGenError;
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    switch (node->type_) {
        case kPhysicalOpDataProvider: {
            auto op = dynamic_cast<const PhysicalDataProviderNode*>(node);
            switch (op->provider_type_) {
                case kProviderTypeTable: {
                    auto provider =
                        dynamic_cast<const PhysicalTableProviderNode*>(node);
                    return new DataRunner(id_++,
                                          node->GetOutputNameSchemaList(),
                                          provider->table_handler_);
                }
                case kProviderTypePartition: {
                    auto provider =
                        dynamic_cast<const PhysicalPartitionProviderNode*>(
                            node);
                    return new DataRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        provider->table_handler_->GetPartition(
                            provider->table_handler_, provider->index_name_));
                }
                case kProviderTypeRequest: {
                    return new RequestRunner(id_++,
                                             node->GetOutputNameSchemaList());
                }
                default: {
                    status.msg = "fail to support data provider type " +
                                 DataProviderTypeName(op->provider_type_);
                    status.code = common::kOpGenError;
                    LOG(WARNING) << status.msg;
                    return nullptr;
                }
            }
        }
        case kPhysicalOpProject: {
            auto input = Build(node->producers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }

            auto op = dynamic_cast<const PhysicalProjectNode*>(node);
            switch (op->project_type_) {
                case kTableProject: {
                    auto runner = new TableProjectRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->project_);
                    runner->AddProducer(input);
                    return runner;
                }
                case kAggregation: {
                    auto runner =
                        new AggRunner(id_++, node->GetOutputNameSchemaList(),
                                      op->GetLimitCnt(), op->project_);
                    runner->AddProducer(input);
                    return runner;
                }
                case kGroupAggregation: {
                    auto op =
                        dynamic_cast<const PhysicalGroupAggrerationNode*>(node);
                    auto runner = new GroupAggRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->group_, op->project_);
                    runner->AddProducer(input);
                    return runner;
                }
                case kWindowAggregation: {
                    auto op =
                        dynamic_cast<const PhysicalWindowAggrerationNode*>(
                            node);
                    auto runner = new WindowAggRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->window_, op->project_);
                    runner->AddProducer(input);
                    return runner;
                }
                case kRowProject: {
                    auto runner = new RowProjectRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->project_);
                    runner->AddProducer(input);
                    return runner;
                }
                default: {
                    status.msg = "fail to support project type " +
                                 ProjectTypeName(op->project_type_);
                    status.code = common::kOpGenError;
                    LOG(WARNING) << status.msg;
                    return nullptr;
                }
            }
        }
        case kPhysicalOpRequestUnoin: {
            auto left = Build(node->producers().at(0), status);
            if (nullptr == left) {
                return nullptr;
            }
            auto right = Build(node->producers().at(1), status);
            if (nullptr == right) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalRequestUnionNode*>(node);
            auto runner = new RequestUnionRunner(
                id_++, node->GetOutputNameSchemaList(), op->GetLimitCnt(),
                op->window_, op->index_key_);
            runner->AddProducer(left);
            runner->AddProducer(right);
            return runner;
        }
        case kPhysicalOpRequestJoin: {
            auto left = Build(node->producers().at(0), status);
            if (nullptr == left) {
                return nullptr;
            }
            auto right = Build(node->producers().at(1), status);
            if (nullptr == right) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalRequestJoinNode*>(node);
            switch (op->join_type_) {
                case node::kJoinTypeLast: {
                    auto runner = new RequestLastJoinRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->join_);
                    runner->AddProducer(left);
                    runner->AddProducer(right);
                    return runner;
                }
                case node::kJoinTypeConcat: {
                    auto runner =
                        new ConcatRunner(id_++, node->GetOutputNameSchemaList(),
                                         op->GetLimitCnt());
                    runner->AddProducer(left);
                    runner->AddProducer(right);
                    return runner;
                }
                default: {
                    status.code = common::kOpGenError;
                    status.msg = "can't handle join type " +
                                 node::JoinTypeName(op->join_type_);
                    LOG(WARNING) << status.msg;
                    return nullptr;
                }
            }
        }
        case kPhysicalOpJoin: {
            auto left = Build(node->producers().at(0), status);
            if (nullptr == left) {
                return nullptr;
            }
            auto right = Build(node->producers().at(1), status);
            if (nullptr == right) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalJoinNode*>(node);
            switch (op->join_type_) {
                case node::kJoinTypeLast: {
                    auto runner = new LastJoinRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->join_);
                    runner->AddProducer(left);
                    runner->AddProducer(right);
                    return runner;
                }
                default: {
                    status.code = common::kOpGenError;
                    status.msg = "can't handle join type " +
                                 node::JoinTypeName(op->join_type_);
                    LOG(WARNING) << status.msg;
                    return nullptr;
                }
            }
        }
        case kPhysicalOpGroupBy: {
            auto input = Build(node->producers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            auto runner =
                new GroupRunner(id_++, node->GetOutputNameSchemaList(),
                                op->GetLimitCnt(), op->group().fn_info_);
            runner->AddProducer(input);
            return runner;
        }
        case kPhysicalOpFilter: {
            auto input = Build(node->producers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalFliterNode*>(node);
            auto runner =
                new FilterRunner(id_++, node->GetOutputNameSchemaList(),
                                 op->GetLimitCnt(), op->filter_.fn_info_);
            runner->AddProducer(input);
            return runner;
        }
        case kPhysicalOpLimit: {
            auto input = Build(node->producers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalLimitNode*>(node);
            if (op->GetLimitCnt() == 0 || op->GetLimitOptimized()) {
                return input;
            }
            auto runner = new LimitRunner(
                id_++, node->GetOutputNameSchemaList(), op->GetLimitCnt());
            runner->AddProducer(input);
            return runner;
        }
        default: {
            status.code = common::kOpGenError;
            status.msg = "can't handle node " + std::to_string(node->type_) +
                         " " + PhysicalOpTypeName(node->type_);
            LOG(WARNING) << status.msg;
            return nullptr;
        }
    }
}

std::string Runner::GetColumnString(RowView* row_view, int key_idx,
                                    type::Type key_type) {
    std::string key = "NA";
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
            LOG(WARNING) << "fail to get string for "
                            "current row";
            break;
        }
    }
    return key;
}
bool Runner::GetColumnBool(RowView* row_view, int idx, type::Type type) {
    bool key = false;
    switch (type) {
        case fesql::type::kInt32: {
            int32_t value;
            if (0 == row_view->GetInt32(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kInt64: {
            int64_t value;
            if (0 == row_view->GetInt64(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kInt16: {
            int16_t value;
            if (0 == row_view->GetInt16(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kFloat: {
            float value;
            if (0 == row_view->GetFloat(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kDouble: {
            double value;
            if (0 == row_view->GetDouble(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kBool: {
            bool value;
            if (0 == row_view->GetBool(idx, &value)) {
                return value;
            }
        }
        default: {
            LOG(WARNING) << "fail to get bool for "
                            "current row";
            break;
        }
    }
    return key;
}

Row Runner::WindowProject(const int8_t* fn, const uint64_t key, const Row row,
                          Window* window) {
    if (row.empty()) {
        return row;
    }
    window->BufferData(key, row);
    int32_t (*udf)(int8_t**, int8_t*, int32_t*, int8_t**) =
        (int32_t(*)(int8_t**, int8_t*, int32_t*, int8_t**))(fn);
    int8_t* out_buf = nullptr;
    int8_t** row_ptrs = row.GetRowPtrs();
    int8_t* window_ptr = reinterpret_cast<int8_t*>(window);
    int32_t* row_sizes = row.GetRowSizes();
    uint32_t ret = udf(row_ptrs, window_ptr, row_sizes, &out_buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(reinterpret_cast<char*>(out_buf), RowView::GetSize(out_buf));
}

int64_t Runner::GetColumnInt64(RowView* row_view, int key_idx,
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
            LOG(WARNING) << "fail to get int64 for "
                            "current row";
            break;
        }
    }
    return key;
}
std::shared_ptr<DataHandler> Runner::TableGroup(
    const std::shared_ptr<DataHandler> table, KeyGenerator& key_gen) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!key_gen.Valid()) {
        return fail_ptr;
    }
    if (!table) {
        return fail_ptr;
    }
    if (kTableHandler != table->GetHanlderType()) {
        return fail_ptr;
    }

    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "fail to group empty table";
        return fail_ptr;
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        std::string keys = key_gen.Gen(iter->GetValue());
        output_partitions->AddRow(keys, iter->GetKey(), iter->GetValue());
        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<DataHandler> Runner::PartitionGroup(
    const std::shared_ptr<DataHandler> table, KeyGenerator& key_gen) {
    if (!key_gen.Valid()) {
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
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        auto segment_key = iter->GetKey().ToString();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            std::string keys = key_gen.Gen(segment_iter->GetValue());
            output_partitions->AddRow(segment_key + "|" + keys,
                                      segment_iter->GetKey(),
                                      segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    return output_partitions;
}

std::shared_ptr<DataHandler> Runner::PartitionSort(
    std::shared_ptr<DataHandler> table, OrderGenerator& order_gen,
    bool is_asc) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (kPartitionHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    auto partitions = std::dynamic_pointer_cast<PartitionHandler>(table);

    // skip sort, when partition has same order direction
    if (!order_gen.Valid() && partitions->IsAsc() == is_asc) {
        return table;
    }

    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = partitions->GetWindowIterator();
    iter->SeekToFirst();

    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            int64_t key = order_gen.Valid()
                              ? order_gen.Gen(segment_iter->GetValue())
                              : segment_iter->GetKey();
            output_partitions->AddRow(
                std::string(iter->GetKey().data(), iter->GetKey().size()), key,
                segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    if (order_gen.Valid()) {
        output_partitions->Sort(is_asc);
    } else {
        output_partitions->Reverse();
    }
    return output_partitions;
}
std::shared_ptr<DataHandler> Runner::TableSort(
    std::shared_ptr<DataHandler> table, OrderGenerator order_gen,
    const bool is_asc) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (kTableHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    if (!order_gen.Valid()) {
        return table;
    }
    auto output_table = std::shared_ptr<MemTimeTableHandler>(
        new MemTimeTableHandler(table->GetSchema()));
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    while (iter->Valid()) {
        int64_t key = order_gen.Gen(iter->GetValue());
        output_table->AddRow(key, iter->GetValue());
        iter->Next();
    }
    output_table->Sort(is_asc);
    return output_table;
}
// TODO(chenjing/baoxinqi): TableHandler support reverse interface
std::shared_ptr<TableHandler> Runner::TableReverse(
    std::shared_ptr<TableHandler> table) {
    if (!table) {
        LOG(WARNING) << "fail to reverse null table";
        return std::shared_ptr<TableHandler>();
    }
    auto output_table = std::shared_ptr<MemTimeTableHandler>(
        new MemTimeTableHandler(table->GetSchema()));
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "fail to reverse empty table";
        return std::shared_ptr<TableHandler>();
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        output_table->AddRow(iter->GetKey(), iter->GetValue());
        iter->Next();
    }
    output_table->Reverse();
    return output_table;
}

std::string Runner::GenerateKeys(RowView* row_view, const Schema& schema,
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

std::shared_ptr<DataHandler> Runner::RunWithCache(RunnerContext& ctx) {
    if (need_cache_) {
        auto iter = ctx.cache_.find(id_);
        if (ctx.cache_.cend() != iter) {
            return iter->second;
        }
    }
    auto res = Run(ctx);
    if (ctx.is_debug_) {
        LOG(INFO) << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
                  << "\n";
        Runner::PrintData(output_schemas_, res);
    }
    if (need_cache_) {
        ctx.cache_.insert(std::make_pair(id_, res));
    }
    return res;
}

std::shared_ptr<DataHandler> DataRunner::Run(RunnerContext& ctx) {
    return data_handler_;
}
std::shared_ptr<DataHandler> RequestRunner::Run(RunnerContext& ctx) {
    return std::shared_ptr<DataHandler>(new MemRowHandler(ctx.request_));
}
std::shared_ptr<DataHandler> GroupRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    if (!group_gen_.Valid()) {
        return input;
    }

    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            return PartitionGroup(input, group_gen_);
        }
        case kTableHandler: {
            return TableGroup(input, group_gen_);
        }
        default: {
            LOG(WARNING) << "fail group when input type isn't "
                            "partition or table";
            return fail_ptr;
        }
    }
}
std::shared_ptr<DataHandler> OrderRunner::Run(RunnerContext& ctx) {
    return TableSort(producers_[0]->RunWithCache(ctx), order_gen_, is_asc_);
}

std::shared_ptr<DataHandler> TableProjectRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    if (!input) {
        return std::shared_ptr<DataHandler>();
    }

    if (kTableHandler != input->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    auto iter = std::dynamic_pointer_cast<TableHandler>(input)->GetIterator();

    int32_t cnt = 0;
    while (iter->Valid()) {
        if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
            break;
        }
        output_table->AddRow(project_gen_.Gen(iter->GetValue()));
        iter->Next();
    }
    return output_table;
}
std::shared_ptr<DataHandler> RowProjectRunner::Run(RunnerContext& ctx) {
    auto row =
        std::dynamic_pointer_cast<RowHandler>(producers_[0]->RunWithCache(ctx));
    return std::shared_ptr<RowHandler>(
        new MemRowHandler(project_gen_.Gen(row->GetValue())));
}

std::shared_ptr<DataHandler> WindowAggRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "window aggregation fail: input is null";
        return fail_ptr;
    }

    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            if (!PartitionRun(
                    std::dynamic_pointer_cast<PartitionHandler>(input),
                    output_table)) {
                return fail_ptr;
            } else {
                return output_table;
            }
        }
        case kTableHandler: {
            if (!TableRun(std::dynamic_pointer_cast<TableHandler>(input),
                          output_table)) {
                return fail_ptr;
            } else {
                return output_table;
            }
        }
        default: {
            LOG(WARNING) << "fail group when input type isn't "
                            "partition or table";
            return fail_ptr;
        }
    }
    return output_table;
}
// Compute window agg with given partition input
bool WindowAggRunner::WindowAggRun(
    std::shared_ptr<PartitionHandler> partition,
    std::shared_ptr<MemTableHandler> output_table) {
    if (!partition) {
        LOG(WARNING) << "Fail run window agg when partition input is empty";
        return false;
    }
    auto iter = partition->GetWindowIterator();
    if (!iter) {
        LOG(WARNING) << "Fail run window agg when partition input is empty";
        return false;
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        auto segment_key = iter->GetKey().ToString();
        auto segment = partition->GetSegment(partition, segment_key);
        if (window_op_.sort_.ValidSort()) {
            if (order_gen_.Valid()) {
                segment = std::dynamic_pointer_cast<TableHandler>(TableSort(
                    segment, order_gen_, window_op_.sort_.GetIsAsc()));
            } else {
                segment = TableReverse(segment);
            }
        }
        int32_t cnt = 0;
        auto segment_iter = segment->GetIterator();
        CurrentHistoryWindow window(window_op_.range_.start_offset_);
        while (segment_iter->Valid()) {
            if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
                break;
            }
            output_table->AddRow(window_gen_.Gen(
                segment_iter->GetKey(), segment_iter->GetValue(), &window));
            segment_iter->Next();
        }
        iter->Next();
    }
    return true;
}

bool WindowAggRunner::PartitionRun(
    std::shared_ptr<PartitionHandler> partition,
    std::shared_ptr<MemTableHandler> output_table) {
    if (!partition) {
        LOG(WARNING) << "PartitionRun Fail: input is empty";
        return false;
    }
    if (group_gen_.Valid()) {
        auto iter = partition->GetWindowIterator();
        if (!iter) {
            LOG(WARNING) << "PartitionRun Fail: input is empty";
            return false;
        }
        iter->SeekToFirst();
        while (iter->Valid()) {
            auto segment_key = iter->GetKey().ToString();
            auto table = partition->GetSegment(partition, segment_key);
            TableRun(table, output_table);

            iter->Next();
        }
    } else {
        return WindowAggRun(partition, output_table);
    }
    return true;
}

bool WindowAggRunner::TableRun(std::shared_ptr<TableHandler> input,
                               std::shared_ptr<MemTableHandler> output_table) {
    if (!group_gen_.Valid()) {
        LOG(WARNING) << "Fail run window agg without partition input";
        return false;
    }
    auto partition = TableGroup(input, group_gen_);
    if (!partition) {
        LOG(WARNING) << "Fail run window agg with empty partition input";
        return false;
    }
    return WindowAggRun(std::dynamic_pointer_cast<PartitionHandler>(partition),
                        output_table);
}

std::shared_ptr<DataHandler> RequestLastJoinRunner::Run(
    RunnerContext& ctx) {  // NOLINT
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
    if (!left || !right) {
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    auto left_row = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();
    switch (right->GetHanlderType()) {
        case kPartitionHandler: {
            Row row = PartitionRun(
                left_row, std::dynamic_pointer_cast<PartitionHandler>(right));
            return std::shared_ptr<RowHandler>(new MemRowHandler(row));
        }
        case kTableHandler: {
            Row row = TableRun(left_row,
                               std::dynamic_pointer_cast<TableHandler>(right));
            return std::shared_ptr<RowHandler>(new MemRowHandler(row));
        }
        default: {
            LOG(WARNING) << "can't join when right is not table or partition";
            return fail_ptr;
        }
    }
}

Row RequestLastJoinRunner::PartitionRun(
    const Row& left_row, std::shared_ptr<PartitionHandler> partition) {
    if (!index_key_gen_.Valid()) {
        LOG(WARNING)
            << "can't join right partition table when partition keys is empty";
        return Row();
    }
    std::string partition_key = index_key_gen_.Gen(left_row);
    auto right_table = partition->GetSegment(partition, partition_key);
    return TableRun(left_row, right_table);
}
Row RequestLastJoinRunner::TableRun(const Row& left_row,
                                    std::shared_ptr<TableHandler> right_table) {
    if (!right_table) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_row, Row());
    }
    auto right_iter = right_table->GetIterator();
    if (!right_iter) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_row, Row());
    }
    right_iter->SeekToFirst();
    if (!right_iter->Valid()) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_row, Row());
    }

    if (!left_key_gen_.Valid() && !condition_gen_.Valid()) {
        return Row(left_row, right_iter->GetValue());
    }

    std::string left_key_str = "";
    if (left_key_gen_.Valid()) {
        left_key_str = left_key_gen_.Gen(left_row);
    }
    while (right_iter->Valid()) {
        auto right_key_str = right_key_gen_.Gen(right_iter->GetValue());
        if (left_key_gen_.Valid() && left_key_str != right_key_str) {
            right_iter->Next();
            continue;
        }
        Row joined_row(left_row, right_iter->GetValue());
        if (!condition_gen_.Valid()) {
            return joined_row;
        }
        if (condition_gen_.Gen(joined_row)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_row, Row());
}

std::shared_ptr<DataHandler> LastJoinRunner::Run(RunnerContext& ctx) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
    if (!left || !right) {
        LOG(WARNING) << "fail to run last join: left|right input is empty";
        return fail_ptr;
    }

    if (right_key_gen_.Valid()) {
        if (kPartitionHandler == right->GetHanlderType()) {
            right = PartitionGroup(
                std::dynamic_pointer_cast<PartitionHandler>(right),
                right_key_gen_);
        } else {
            right = TableGroup(std::dynamic_pointer_cast<TableHandler>(right),
                               right_key_gen_);
        }
    }

    if (kTableHandler == left->GetHanlderType()) {
        auto output_table =
            std::shared_ptr<MemTableHandler>(new MemTableHandler());
        auto left_table = std::dynamic_pointer_cast<TableHandler>(left);

        if (kPartitionHandler == right->GetHanlderType()) {
            if (!TableJoin(left_table,
                           std::dynamic_pointer_cast<PartitionHandler>(right),
                           output_table)) {
                return fail_ptr;
            }
        } else {
            if (!TableJoin(left_table,
                           std::dynamic_pointer_cast<TableHandler>(right),
                           output_table)) {
                return fail_ptr;
            }
        }
        return output_table;
    } else {
        auto output_partition =
            std::shared_ptr<MemPartitionHandler>(new MemPartitionHandler());
        auto left_partition = std::dynamic_pointer_cast<PartitionHandler>(left);
        if (kPartitionHandler == right->GetHanlderType()) {
            if (!PartitionJoin(
                    left_partition,
                    std::dynamic_pointer_cast<PartitionHandler>(right),
                    output_partition)) {
                return fail_ptr;
            }

        } else {
            if (!PartitionJoin(left_partition,
                               std::dynamic_pointer_cast<TableHandler>(right),
                               output_partition)) {
                return fail_ptr;
            }
        }
        return output_partition;
    }
}

bool LastJoinRunner::TableJoin(std::shared_ptr<TableHandler> left,
                               std::shared_ptr<TableHandler> right,
                               std::shared_ptr<MemTableHandler> output) {
    auto left_iter = left->GetIterator();
    while (left_iter->Valid()) {
        const Row& left_row = left_iter->GetValue();
        output->AddRow(RowLastJoin(left_row, right, condition_gen_));
        left_iter->Next();
    }
    return true;
}
bool LastJoinRunner::TableJoin(std::shared_ptr<TableHandler> left,
                               std::shared_ptr<PartitionHandler> right,
                               std::shared_ptr<MemTableHandler> output) {
    if (!left_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when join "
                        "left_key_gen_ is invalid";
        return false;
    }
    auto left_iter = left->GetIterator();
    if (!left_iter || !left_iter->Valid()) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return false;
    }
    left_iter->SeekToFirst();
    while (left_iter->Valid()) {
        const Row& left_row = left_iter->GetValue();
        std::string key_str = "";
        if (index_key_gen_.Valid()) {
            key_str = index_key_gen_.Gen(left_row) + "|" +
                      left_key_gen_.Gen(left_row);
        } else {
            key_str = left_key_gen_.Gen(left_row);
        }
        LOG(INFO) << "key_str " << key_str;
        auto right_table = right->GetSegment(right, key_str);
        output->AddRow(RowLastJoin(left_row, right_table, condition_gen_));
        left_iter->Next();
    }
    return true;
}
bool LastJoinRunner::PartitionJoin(
    std::shared_ptr<PartitionHandler> left, std::shared_ptr<TableHandler> right,
    std::shared_ptr<MemPartitionHandler> output) {
    auto left_window_iter = left->GetWindowIterator();
    left_window_iter->SeekToFirst();
    while (left_window_iter->Valid()) {
        auto left_iter = left_window_iter->GetValue();
        auto left_key = left_window_iter->GetKey();
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            output->AddRow(std::string(left_key.data(), left_key.size()),
                           left_iter->GetKey(),
                           RowLastJoin(left_row, right, condition_gen_));
            left_iter->Next();
        }
    }
    return true;
}
bool LastJoinRunner::PartitionJoin(
    std::shared_ptr<PartitionHandler> left,
    std::shared_ptr<PartitionHandler> right,
    std::shared_ptr<MemPartitionHandler> output) {
    if (!left) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return false;
    }
    auto left_partition_iter = left->GetWindowIterator();
    if (!left_partition_iter) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return false;
    }
    if (!left_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when join "
                        "left_key_gen_ is invalid";
        return false;
    }

    left_partition_iter->SeekToFirst();
    while (left_partition_iter->Valid()) {
        auto left_iter = left_partition_iter->GetValue();
        auto left_key = left_partition_iter->GetKey();
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            const std::string& key_str =
                index_key_gen_.Valid() ? index_key_gen_.Gen(left_row) + "|" +
                                             left_key_gen_.Gen(left_row)
                                       : left_key_gen_.Gen(left_row);
            auto right_table = right->GetSegment(right, key_str);
            output->AddRow(std::string(left_key.data(), left_key.size()),
                           left_iter->GetKey(),
                           RowLastJoin(left_row, right_table, condition_gen_));
            left_iter->Next();
        }
        left_partition_iter->Next();
    }
    return true;
}
const Row Runner::RowLastJoin(const Row& left_row,
                              std::shared_ptr<TableHandler> right_table,
                              ConditionGenerator& cond_gen) {
    if (!right_table) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_row, Row());
    }
    auto right_iter = right_table->GetIterator();
    if (!right_iter) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_row, Row());
    }
    right_iter->SeekToFirst();
    if (!right_iter->Valid()) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_row, Row());
    }

    if (!cond_gen.Valid()) {
        return Row(left_row, right_iter->GetValue());
    }
    while (right_iter->Valid()) {
        Row joined_row(left_row, right_iter->GetValue());
        if (cond_gen.Gen(joined_row)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_row, Row());
}
void Runner::PrintData(const vm::NameSchemaList& schema_list,
                       std::shared_ptr<DataHandler> data) {
    std::ostringstream oss;

    std::vector<RowView> row_view_list;
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    for (auto pair : schema_list) {
        for (int i = 0; i < pair.second->size(); i++) {
            if (pair.first.empty()) {
                t.add(pair.second->Get(i).name());
            } else {
                t.add(pair.first + "." + pair.second->Get(i).name());
            }
        }
        row_view_list.push_back(RowView(*pair.second));
    }

    t.endOfRow();
    if (!data) {
        t.add("Empty set");
        t.endOfRow();
        oss << t << std::endl;
        LOG(INFO) << "\n" << oss.str();
        return;
    }

    switch (data->GetHanlderType()) {
        case kRowHandler: {
            auto row_handler = std::dynamic_pointer_cast<RowHandler>(data);
            auto row = row_handler->GetValue();
            for (size_t id = 0; id < row_view_list.size(); id++) {
                RowView& row_view = row_view_list[id];
                row_view.Reset(row.buf(id), row.size(id));
                for (int idx = 0; idx < schema_list[id].second->size(); idx++) {
                    std::string str = row_view.GetAsString(idx);
                    t.add(str);
                }
            }

            t.endOfRow();
            break;
        }
        case kTableHandler: {
            auto table_handler = std::dynamic_pointer_cast<TableHandler>(data);
            auto iter = table_handler->GetIterator();
            if (!iter) {
                t.add("Empty set");
                t.endOfRow();
                break;
            }
            iter->SeekToFirst();
            if (!iter->Valid()) {
                t.add("Empty set");
                t.endOfRow();
                break;
            } else {
                int cnt = 0;
                while (iter->Valid() && cnt++ < MAX_DEBUG_LINES_CNT) {
                    auto row = iter->GetValue();
                    for (size_t id = 0; id < row_view_list.size(); id++) {
                        RowView& row_view = row_view_list[id];
                        row_view.Reset(row.buf(id));
                        for (int idx = 0; idx < schema_list[id].second->size();
                             idx++) {
                            std::string str = row_view.GetAsString(idx);
                            t.add(str);
                        }
                    }
                    iter->Next();
                    t.endOfRow();
                }
            }

            break;
        }
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(data);
            auto iter = partition->GetWindowIterator();
            int cnt = 0;
            if (!iter || !iter->Valid()) {
                t.add("Empty set");
                t.endOfRow();
            }
            while (iter->Valid() && cnt++ < MAX_DEBUG_LINES_CNT) {
                t.add("KEY: " + std::string(iter->GetKey().data(),
                                            iter->GetKey().size()));
                t.endOfRow();
                auto segment_iter = iter->GetValue();
                if (!segment_iter) {
                    t.add("Empty set");
                    t.endOfRow();
                    break;
                }
                segment_iter->SeekToFirst();
                if (!segment_iter->Valid()) {
                    t.add("Empty set");
                    t.endOfRow();
                    break;
                } else {
                    while (segment_iter->Valid()) {
                        auto row = segment_iter->GetValue();
                        for (size_t id = 0; id < row_view_list.size(); id++) {
                            RowView& row_view = row_view_list[id];
                            row_view.Reset(row.buf(id));
                            for (int idx = 0;
                                 idx < schema_list[id].second->size(); idx++) {
                                std::string str = row_view.GetAsString(idx);
                                t.add(str);
                            }
                        }
                        segment_iter->Next();
                        t.endOfRow();
                    }
                }

                iter->Next();
            }
            break;
        }
        default: {
            oss << "Invalid Set";
        }
    }
    oss << t << std::endl;
    LOG(INFO) << data->GetHandlerTypeName() << " RESULT:\n" << oss.str();
}

std::shared_ptr<DataHandler> ConcatRunner::Run(RunnerContext& ctx) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
    if (!left || !right) {
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    if (kRowHandler == right->GetHanlderType()) {
        auto left_row = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();
        auto right_row =
            std::dynamic_pointer_cast<RowHandler>(right)->GetValue();
        return std::shared_ptr<RowHandler>(
            new MemRowHandler(Row(left_row, right_row)));
    } else if (kTableHandler == right->GetHanlderType()) {
        auto left_row = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();
        auto right_table = std::dynamic_pointer_cast<TableHandler>(right);
        auto right_iter = right_table->GetIterator();
        if (!right_iter) {
            return std::shared_ptr<RowHandler>(
                new MemRowHandler(Row(left_row, Row())));
        }
        return std::shared_ptr<RowHandler>(
            new MemRowHandler(Row(left_row, right_iter->GetValue())));
    } else {
        return std::shared_ptr<DataHandler>();
    }
}
std::shared_ptr<DataHandler> LimitRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
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
                new MemTableHandler(input->GetSchema()));
            int32_t cnt = 0;
            while (cnt++ < limit_cnt_ && iter->Valid()) {
                output_table->AddRow(iter->GetValue());
                iter->Next();
            }
            return output_table;
        }
        case kRowHandler: {
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
std::shared_ptr<DataHandler> FilterRunner::Run(RunnerContext& ctx) {
    LOG(WARNING) << "can't handler filter op";
    return std::shared_ptr<DataHandler>();
}
std::shared_ptr<DataHandler> GroupAggRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    if (!input) {
        LOG(WARNING) << "group aggregation fail: input is null";
        return std::shared_ptr<DataHandler>();
    }

    if (kPartitionHandler != input->GetHanlderType()) {
        LOG(WARNING) << "group aggregation fail: input isn't partition ";
        return std::shared_ptr<DataHandler>();
    }
    auto partition = std::dynamic_pointer_cast<PartitionHandler>(input);
    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    auto iter = partition->GetWindowIterator();
    if (!iter) {
        LOG(WARNING) << "group aggregation fail: input iterator is null";
        return std::shared_ptr<DataHandler>();
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            LOG(WARNING) << "group aggregation fail: segment iterator is null";
            return std::shared_ptr<DataHandler>();
        }
        auto key = std::string(iter->GetKey().data(), iter->GetKey().size());
        auto segment = partition->GetSegment(partition, key);
        output_table->AddRow(agg_gen_.Gen(segment));
        iter->Next();
    }
    return output_table;
}

std::shared_ptr<DataHandler> RequestUnionRunner::Run(RunnerContext& ctx) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
    if (!left || !right) {
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    switch (right->GetHanlderType()) {
        case kTableHandler: {
            return UnionTable(
                std::dynamic_pointer_cast<RowHandler>(left)->GetValue(),
                std::dynamic_pointer_cast<TableHandler>(right));
        }
        case kPartitionHandler: {
            return UnionPartition(
                std::dynamic_pointer_cast<RowHandler>(left)->GetValue(),
                std::dynamic_pointer_cast<PartitionHandler>(right));
        }
        default: {
            LOG(WARNING) << "fail to run request union when right is not table "
                            "or partition";
            return fail_ptr;
        }
    }
}
std::shared_ptr<DataHandler> RequestUnionRunner::UnionTable(
    Row request, std::shared_ptr<TableHandler> table) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!table) {
        LOG(WARNING) << "fail to union table: table is null or empty";
        return fail_ptr;
    }

    std::shared_ptr<TableHandler> temp_table = table;
    // filter by keys if need
    if (group_gen_.Valid()) {
        auto mem_table =
            std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
        std::string request_keys = group_gen_.Gen(request);
        auto iter = table->GetIterator();
        if (iter) {
            iter->SeekToFirst();
            while (iter->Valid()) {
                std::string keys = group_gen_.Gen(iter->GetValue());
                if (request_keys == keys) {
                    mem_table->AddRow(iter->GetKey(), iter->GetValue());
                }
                iter->Next();
            }
        }
        temp_table = std::shared_ptr<TableHandler>(mem_table);
    }

    // sort by orders if need
    if (order_gen_.Valid()) {
        temp_table = std::dynamic_pointer_cast<TableHandler>(TableSort(
            std::shared_ptr<DataHandler>(temp_table), order_gen_, false));
    }

    // build window with start and end offset
    auto window_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    uint64_t start = 0;
    uint64_t end = UINT64_MAX;
    if (ts_gen_.Valid()) {
        int64_t key = ts_gen_.Gen(request);
        start = (key + start_offset_) < 0 ? 0 : (key + start_offset_);
        end = (key + end_offset_) < 0 ? 0 : (key + end_offset_);
        DLOG(INFO) << "request key: " << key;
    }
    window_table->AddRow(request);
    DLOG(INFO) << "start make window ";
    if (temp_table) {
        auto table_output = std::dynamic_pointer_cast<TableHandler>(temp_table);
        auto table_iter = table_output->GetIterator();
        if (table_iter) {
            table_iter->Seek(end);
            while (table_iter->Valid()) {
                if (table_iter->GetKey() <= start) {
                    break;
                }
                window_table->AddRow(table_iter->GetValue());
                table_iter->Next();
            }
        }
        return window_table;
    } else {
        return temp_table;
    }
}
std::shared_ptr<DataHandler> RequestUnionRunner::UnionPartition(
    Row row, std::shared_ptr<PartitionHandler> partition) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!partition) {
        LOG(WARNING) << "fail to union partition: partition is null";
        return fail_ptr;
    }

    if (!key_gen_.Valid()) {
        LOG(WARNING) << "fail to union partition without partition key";
        return fail_ptr;
    }

    auto key_str = key_gen_.Gen(row);
    auto table = partition->GetSegment(partition, key_str);
    return UnionTable(row, table);
}
std::shared_ptr<DataHandler> AggRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    if (!input) {
        LOG(WARNING) << "input is empty";
        return std::shared_ptr<DataHandler>();
    }

    if (kTableHandler != input->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    auto row_handler = std::shared_ptr<RowHandler>(new MemRowHandler(
        agg_gen_.Gen(std::dynamic_pointer_cast<TableHandler>(input))));
    return row_handler;
}

const std::string KeyGenerator::Gen(const Row& row) {
    Row key_row = CoreAPI::RowProject(fn_, row, true);
    row_view_.Reset(key_row.buf());
    std::string keys = "";
    for (auto pos : idxs_) {
        std::string key = Runner::GetColumnString(&row_view_, pos,
                                                  fn_schema_.Get(pos).type());
        if (!keys.empty()) {
            keys.append("|");
        }
        keys.append(key);
    }
    return keys;
}
const int64_t OrderGenerator::Gen(const Row& row) {
    Row order_row = CoreAPI::RowProject(fn_, row, true);
    row_view_.Reset(order_row.buf());
    return Runner::GetColumnInt64(&row_view_, idxs_[0],
                                  fn_schema_.Get(idxs_[0]).type());
}
const bool ConditionGenerator::Gen(const Row& row) {
    Row cond_row = CoreAPI::RowProject(fn_, row, true);
    row_view_.Reset(cond_row.buf());
    return Runner::GetColumnBool(&row_view_, idxs_[0],
                                 fn_schema_.Get(idxs_[0]).type());
}
const Row ProjectGenerator::Gen(const Row& row) {
    return CoreAPI::RowProject(fn_, row, false);
}
const Row AggGenerator::Gen(std::shared_ptr<TableHandler> table) {
    auto iter = table->GetIterator();
    iter->SeekToFirst();
    if (!iter->Valid()) {
        return Row();
    }
    auto& row = iter->GetValue();
    int32_t (*udf)(int8_t**, int8_t*, int32_t*, int8_t**) =
        (int32_t(*)(int8_t**, int8_t*, int32_t*, int8_t**))(fn_);
    int8_t* buf = nullptr;

    int8_t** row_ptrs = row.GetRowPtrs();
    int8_t* window_ptr = reinterpret_cast<int8_t*>(table.get());
    int32_t* row_sizes = row.GetRowSizes();
    uint32_t ret = udf(row_ptrs, window_ptr, row_sizes, &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(buf, RowView::GetSize(buf));
}
const Row WindowProjectGenerator::Gen(const uint64_t key, const Row row,
                                      Window* window) {
    return Runner::WindowProject(fn_, key, row, window);
}

}  // namespace vm
}  // namespace fesql
