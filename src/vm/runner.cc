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
#include "vm/catalog_wrapper.h"
#include "vm/core_api.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace vm {
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_MAX 20

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
        case kPhysicalOpSimpleProject: {
            auto input = Build(node->producers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }

            auto op = dynamic_cast<const PhysicalSimpleProjectNode*>(node);
            auto runner = new SimpleProjectRunner(
                id_++, node->GetOutputNameSchemaList(), op->GetLimitCnt(),
                op->project_.fn_info_);
            runner->AddProducer(input);
            return runner;
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
                        op->GetLimitCnt(), op->window_, op->project_,
                        op->instance_not_in_window());
                    runner->AddProducer(input);
                    size_t input_slices =
                        input->output_schemas().GetSchemaSourceListSize();

                    if (!op->window_joins_.Empty()) {
                        for (auto window_join :
                             op->window_joins_.window_joins_) {
                            auto join_right_runner =
                                Build(window_join.first, status);
                            if (nullptr == join_right_runner) {
                                return nullptr;
                            }
                            runner->AddWindowJoin(window_join.second,
                                                  input_slices,
                                                  join_right_runner);
                        }
                    }

                    if (!op->window_unions_.Empty()) {
                        for (auto window_union :
                             op->window_unions_.window_unions_) {
                            auto union_table =
                                Build(window_union.first, status);
                            if (nullptr == union_table) {
                                return nullptr;
                            }
                            runner->AddWindowUnion(window_union.second,
                                                   union_table);
                        }
                    }
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
            auto runner =
                new RequestUnionRunner(id_++, node->GetOutputNameSchemaList(),
                                       op->GetLimitCnt(), op->window().range_);
            runner->AddProducer(left);
            runner->AddProducer(right);

            if (!op->instance_not_in_window()) {
                runner->AddWindowUnion(op->window_, right);
            }
            if (!op->window_unions_.Empty()) {
                for (auto window_union : op->window_unions_.window_unions_) {
                    auto union_table = Build(window_union.first, status);
                    if (nullptr == union_table) {
                        return nullptr;
                    }
                    runner->AddWindowUnion(window_union.second, union_table);
                }
            }
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
            switch (op->join().join_type()) {
                case node::kJoinTypeLast: {
                    auto runner = new RequestLastJoinRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->join_,
                        left->output_schemas().GetSchemaSourceListSize(),
                        right->output_schemas().GetSchemaSourceListSize());
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
                                 node::JoinTypeName(op->join().join_type());
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
            switch (op->join().join_type()) {
                case node::kJoinTypeLast: {
                    auto runner = new LastJoinRunner(
                        id_++, node->GetOutputNameSchemaList(),
                        op->GetLimitCnt(), op->join_,
                        left->output_schemas().GetSchemaSourceListSize(),
                        right->output_schemas().GetSchemaSourceListSize());
                    runner->AddProducer(left);
                    runner->AddProducer(right);
                    return runner;
                }
                default: {
                    status.code = common::kOpGenError;
                    status.msg = "can't handle join type " +
                                 node::JoinTypeName(op->join().join_type());
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
                                op->GetLimitCnt(), op->group());
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
                          const bool is_instance, Window* window) {
    if (row.empty()) {
        return row;
    }
    window->BufferData(key, row);
    if (!is_instance) {
        return Row();
    }

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
    if (window->instance_not_in_window()) {
        window->PopFrontData();
    }
    return Row(base::RefCountedSlice::CreateManaged(out_buf,
                                                    RowView::GetSize(out_buf)));
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
        case fesql::type::kTimestamp: {
            int64_t value;
            if (0 == row_view->GetTimestamp(key_idx, &value)) {
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
    return partition_gen_.Partition(input);
}
std::shared_ptr<DataHandler> SortRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    return sort_gen_.Sort(input);
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
    if (!iter) return std::shared_ptr<DataHandler>();
    iter->SeekToFirst();
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

std::shared_ptr<DataHandler> SimpleProjectRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "simple project fail: input is null";
        return fail_ptr;
    }

    switch (input->GetHanlderType()) {
        case kTableHandler: {
            return std::shared_ptr<TableHandler>(
                new TableWrapper(std::dynamic_pointer_cast<TableHandler>(input),
                                 &project_gen_.fun_));
        }
        case kPartitionHandler: {
            return std::shared_ptr<TableHandler>(new PartitionWrapper(
                std::dynamic_pointer_cast<PartitionHandler>(input),
                &project_gen_.fun_));
        }
        case kRowHandler: {
            return std::shared_ptr<RowHandler>(
                new RowWrapper(std::dynamic_pointer_cast<RowHandler>(input),
                               &project_gen_.fun_));
        }
        default: {
            LOG(WARNING) << "Fail run simple project, invalid handler type "
                         << input->GetHandlerTypeName();
        }
    }

    return std::shared_ptr<DataHandler>();
}
std::shared_ptr<DataHandler> WindowAggRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "window aggregation fail: input is null";
        return fail_ptr;
    }

    // Partition Instance Table
    auto instance_partition =
        instance_window_gen_.partition_gen_.Partition(input);
    if (!instance_partition) {
        LOG(WARNING) << "Window Aggregation Fail: input partition is empty";
        return fail_ptr;
    }
    auto instance_partition_iter = instance_partition->GetWindowIterator();
    if (!instance_partition_iter) {
        LOG(WARNING)
            << "Window Aggregation Fail: when partition input is empty";
        return fail_ptr;
    }
    instance_partition_iter->SeekToFirst();

    // Partition Union Table
    auto union_inpus = windows_union_gen_.RunInputs(ctx);
    auto union_partitions = windows_union_gen_.PartitionEach(union_inpus);
    // Prepare Join Tables
    auto join_right_tables = windows_join_gen_.RunInputs(ctx);

    // Compute output
    std::shared_ptr<MemTableHandler> output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler());
    while (instance_partition_iter->Valid()) {
        auto key = instance_partition_iter->GetKey().ToString();
        RunWindowAggOnKey(instance_partition, union_partitions,
                          join_right_tables, key, output_table);
        instance_partition_iter->Next();
    }
    return output_table;
}

// Run Window Aggeregation on given key
void WindowAggRunner::RunWindowAggOnKey(
    std::shared_ptr<PartitionHandler> instance_partition,
    std::vector<std::shared_ptr<PartitionHandler>> union_partitions,
    std::vector<std::shared_ptr<DataHandler>> join_right_tables,
    const std::string& key, std::shared_ptr<MemTableHandler> output_table) {
    // Prepare Instance Segment
    auto instance_segment =
        instance_partition->GetSegment(instance_partition, key);
    instance_segment = instance_window_gen_.sort_gen_.Sort(instance_segment);
    if (!instance_segment) {
        LOG(WARNING) << "Instance Segment is Empty";
        return;
    }

    auto instance_segment_iter = instance_segment->GetIterator();
    if (!instance_segment_iter) {
        LOG(WARNING) << "Instance Segment is Empty";
        return;
    }
    instance_segment_iter->SeekToFirst();

    // Prepare Union Segment Iterators
    size_t unions_cnt = windows_union_gen_.inputs_cnt_;
    std::vector<std::shared_ptr<TableHandler>> union_segments(unions_cnt);
    std::vector<std::unique_ptr<RowIterator>> union_segment_iters(unions_cnt);
    std::vector<IteratorStatus> union_segment_status(unions_cnt);

    for (size_t i = 0; i < unions_cnt; i++) {
        if (!union_partitions[i]) {
            continue;
        }
        auto segment =
            union_partitions[i]->GetSegment(union_partitions[i], key);
        segment = windows_union_gen_.windows_gen_[i].sort_gen_.Sort(segment);
        union_segments[i] = segment;
        if (!segment) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i] = segment->GetIterator();
        if (!union_segment_iters[i]) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i]->SeekToFirst();
        if (!union_segment_iters[i]->Valid()) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        uint64_t ts = union_segment_iters[i]->GetKey();
        union_segment_status[i] = IteratorStatus(ts);
    }

    int32_t min_union_pos =
        0 == unions_cnt
            ? -1
            : IteratorStatus::PickIteratorWithMininumKey(&union_segment_status);
    int32_t cnt = output_table->GetCount();
    CurrentHistoryWindow window(instance_window_gen_.range_gen_.start_offset_);
    window.set_instance_not_in_window(instance_not_in_window_);
    window.set_rows_preceding(instance_window_gen_.range_gen_.start_row_);

    while (instance_segment_iter->Valid()) {
        if (limit_cnt_ > 0 && cnt >= limit_cnt_) {
            break;
        }
        const Row& instance_row = instance_segment_iter->GetValue();
        uint64_t instance_order = instance_segment_iter->GetKey();
        while (min_union_pos >= 0 &&
               union_segment_status[min_union_pos].key_ < instance_order) {
            Row row = union_segment_iters[min_union_pos]->GetValue();
            if (windows_join_gen_.Valid()) {
                row = windows_join_gen_.Join(row, join_right_tables);
            }
            window_project_gen_.Gen(
                union_segment_iters[min_union_pos]->GetKey(), row, false,
                &window);

            // Update Iterator Status
            union_segment_iters[min_union_pos]->Next();
            if (!union_segment_iters[min_union_pos]->Valid()) {
                union_segment_status[min_union_pos].MarkInValid();
            } else {
                union_segment_status[min_union_pos].set_key(
                    union_segment_iters[min_union_pos]->GetKey());
            }
            // Pick new mininum union pos
            min_union_pos = IteratorStatus::PickIteratorWithMininumKey(
                &union_segment_status);
        }
        if (windows_join_gen_.Valid()) {
            Row row = instance_row;
            row = windows_join_gen_.Join(instance_row, join_right_tables);
            output_table->AddRow(window_project_gen_.Gen(
                instance_segment_iter->GetKey(), row, true, &window));
        } else {
            output_table->AddRow(window_project_gen_.Gen(
                instance_segment_iter->GetKey(), instance_row, true, &window));
        }

        cnt++;
        instance_segment_iter->Next();
    }
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
    return std::shared_ptr<RowHandler>(
        new MemRowHandler(join_gen_.RowLastJoin(left_row, right)));
}

std::shared_ptr<DataHandler> LastJoinRunner::Run(RunnerContext& ctx) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
    if (!left || !right) {
        LOG(WARNING) << "fail to run last join: left|right input is empty";
        return fail_ptr;
    }
    if (join_gen_.right_group_gen_.Valid()) {
        right = join_gen_.right_group_gen_.Partition(right);
    }

    if (kTableHandler == left->GetHanlderType()) {
        auto output_table =
            std::shared_ptr<MemTableHandler>(new MemTableHandler());
        auto left_table = std::dynamic_pointer_cast<TableHandler>(left);
        output_table->SetOrderType(left_table->GetOrderType());
        if (kPartitionHandler == right->GetHanlderType()) {
            if (!join_gen_.TableJoin(
                    left_table,
                    std::dynamic_pointer_cast<PartitionHandler>(right),
                    output_table)) {
                return fail_ptr;
            }
        } else {
            if (!join_gen_.TableJoin(
                    left_table, std::dynamic_pointer_cast<TableHandler>(right),
                    output_table)) {
                return fail_ptr;
            }
        }
        return output_table;
    } else {
        auto output_partition =
            std::shared_ptr<MemPartitionHandler>(new MemPartitionHandler());
        auto left_partition = std::dynamic_pointer_cast<PartitionHandler>(left);
        output_partition->SetOrderType(left_partition->GetOrderType());
        if (kPartitionHandler == right->GetHanlderType()) {
            if (!join_gen_.PartitionJoin(
                    left_partition,
                    std::dynamic_pointer_cast<PartitionHandler>(right),
                    output_partition)) {
                return fail_ptr;
            }

        } else {
            if (!join_gen_.PartitionJoin(
                    left_partition,
                    std::dynamic_pointer_cast<TableHandler>(right),
                    output_partition)) {
                return fail_ptr;
            }
        }
        return output_partition;
    }
}

std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<DataHandler> input) {
    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            return Partition(
                std::dynamic_pointer_cast<PartitionHandler>(input));
        }
        case kTableHandler: {
            return Partition(std::dynamic_pointer_cast<TableHandler>(input));
        }
        default: {
            LOG(WARNING) << "Partition Fail: input isn't partition or table";
            return std::shared_ptr<PartitionHandler>();
        }
    }
}
std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<PartitionHandler> table) {
    if (!key_gen_.Valid()) {
        return table;
    }
    if (!table) {
        return std::shared_ptr<PartitionHandler>();
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
            std::string keys = key_gen_.Gen(segment_iter->GetValue());
            output_partitions->AddRow(segment_key + "|" + keys,
                                      segment_iter->GetKey(),
                                      segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    output_partitions->SetOrderType(table->GetOrderType());
    return output_partitions;
}
std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<TableHandler> table) {
    auto fail_ptr = std::shared_ptr<PartitionHandler>();
    if (!key_gen_.Valid()) {
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
        std::string keys = key_gen_.Gen(iter->GetValue());
        output_partitions->AddRow(keys, iter->GetKey(), iter->GetValue());
        iter->Next();
    }
    output_partitions->SetOrderType(table->GetOrderType());
    return output_partitions;
}
std::shared_ptr<DataHandler> SortGenerator::Sort(
    std::shared_ptr<DataHandler> input, const bool reverse) {
    if (!input || !is_valid_ || !order_gen_.Valid()) {
        return input;
    }
    switch (input->GetHanlderType()) {
        case kTableHandler:
            return Sort(std::dynamic_pointer_cast<TableHandler>(input),
                        reverse);
        case kPartitionHandler:
            return Sort(std::dynamic_pointer_cast<PartitionHandler>(input),
                        reverse);
        default: {
            LOG(WARNING) << "Sort Fail: input isn't partition or table";
            return std::shared_ptr<PartitionHandler>();
        }
    }
}

std::shared_ptr<PartitionHandler> SortGenerator::Sort(
    std::shared_ptr<PartitionHandler> partition, const bool reverse) {
    bool is_asc = reverse ? !is_asc_ : is_asc_;
    if (!is_valid_) {
        return partition;
    }
    if (!partition) {
        return std::shared_ptr<PartitionHandler>();
    }
    if (!order_gen().Valid() &&
        is_asc == (partition->GetOrderType() == kAscOrder)) {
        DLOG(INFO) << "match the order redirect the table";
        return partition;
    }

    DLOG(INFO) << "mismatch the order and sort it";
    auto output =
        std::shared_ptr<MemPartitionHandler>(new MemPartitionHandler());

    auto iter = partition->GetWindowIterator();
    if (!iter) {
        return std::shared_ptr<PartitionHandler>();
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            iter->Next();
            continue;
        }

        auto key = iter->GetKey().ToString();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            int64_t ts = order_gen_.Gen(segment_iter->GetValue());
            output->AddRow(key, static_cast<uint64_t>(ts),
                           segment_iter->GetValue());
            segment_iter->Next();
        }
    }
    if (order_gen_.Valid()) {
        output->Sort(is_asc);
    } else if (is_asc && OrderType::kDescOrder == partition->GetOrderType()) {
        output->Reverse();
    }
    return output;
}

std::shared_ptr<TableHandler> SortGenerator::Sort(
    std::shared_ptr<TableHandler> table, const bool reverse) {
    bool is_asc = reverse ? !is_asc_ : is_asc_;
    if (!table || !is_valid_) {
        return table;
    }
    if (!order_gen().Valid() &&
        is_asc == (table->GetOrderType() == kAscOrder)) {
        return table;
    }
    auto output_table = std::shared_ptr<MemTimeTableHandler>(
        new MemTimeTableHandler(table->GetSchema()));
    output_table->SetOrderType(table->GetOrderType());
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        return std::shared_ptr<TableHandler>();
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        if (order_gen_.Valid()) {
            int64_t key = order_gen_.Gen(iter->GetValue());
            output_table->AddRow(static_cast<uint64_t>(key), iter->GetValue());
        } else {
            output_table->AddRow(iter->GetKey(), iter->GetValue());
        }
        iter->Next();
    }

    if (order_gen_.Valid()) {
        output_table->Sort(is_asc);
    } else {
        switch (table->GetOrderType()) {
            case kDescOrder:
                if (is_asc) {
                    output_table->Reverse();
                }
                break;
            case kAscOrder:
                if (!is_asc) {
                    output_table->Reverse();
                }
                break;
            default: {
                LOG(WARNING) << "Fail to Sort, order type invalid";
                return std::shared_ptr<TableHandler>();
            }
        }
    }
    return output_table;
}
Row JoinGenerator::RowLastJoin(const Row& left_row,
                               std::shared_ptr<DataHandler> right) {
    switch (right->GetHanlderType()) {
        case kPartitionHandler: {
            return RowLastJoinPartition(
                left_row, std::dynamic_pointer_cast<PartitionHandler>(right));
        }
        case kTableHandler: {
            return RowLastJoinTable(
                left_row, std::dynamic_pointer_cast<TableHandler>(right));
        }
        default: {
            LOG(WARNING) << "Last Join right isn't table or partition";
            return Row(left_slices_, left_row, right_slices_, Row());
        }
    }
}
Row JoinGenerator::RowLastJoinPartition(
    const Row& left_row, std::shared_ptr<PartitionHandler> partition) {
    if (!index_key_gen_.Valid()) {
        LOG(WARNING)
            << "can't join right partition table when partition keys is empty";
        return Row();
    }
    std::string partition_key = index_key_gen_.Gen(left_row);
    auto right_table = partition->GetSegment(partition, partition_key);
    return RowLastJoinTable(left_row, right_table);
}
Row JoinGenerator::RowLastJoinTable(const Row& left_row,
                                    std::shared_ptr<TableHandler> table) {
    if (right_sort_gen_.Valid()) {
        table = right_sort_gen_.Sort(table, true);
    }
    if (!table) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices_, left_row, right_slices_, Row());
    }
    auto right_iter = table->GetIterator();
    if (!right_iter) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices_, left_row, right_slices_, Row());
    }
    right_iter->SeekToFirst();
    if (!right_iter->Valid()) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices_, left_row, right_slices_, Row());
    }

    if (!left_key_gen_.Valid() && !condition_gen_.Valid()) {
        return Row(left_slices_, left_row, right_slices_,
                   right_iter->GetValue());
    }

    std::string left_key_str = "";
    if (left_key_gen_.Valid()) {
        left_key_str = left_key_gen_.Gen(left_row);
    }
    while (right_iter->Valid()) {
        if (right_group_gen_.Valid()) {
            auto right_key_str =
                right_group_gen_.GetKey(right_iter->GetValue());
            if (left_key_gen_.Valid() && left_key_str != right_key_str) {
                right_iter->Next();
                continue;
            }
        }

        Row joined_row(left_slices_, left_row, right_slices_,
                       right_iter->GetValue());
        if (!condition_gen_.Valid()) {
            return joined_row;
        }
        if (condition_gen_.Gen(joined_row)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_slices_, left_row, right_slices_, Row());
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<TableHandler> right,
                              std::shared_ptr<MemTableHandler> output) {
    auto left_iter = left->GetIterator();
    while (left_iter->Valid()) {
        const Row& left_row = left_iter->GetValue();
        output->AddRow(
            Runner::RowLastJoinTable(left_slices_, left_row, right_slices_,
                                     right, right_sort_gen_, condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<PartitionHandler> right,
                              std::shared_ptr<MemTableHandler> output) {
    if (!left_key_gen_.Valid() && !index_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when join "
                        "left_key_gen_ and index_key_gen_ is invalid";
        return false;
    }
    auto left_iter = left->GetIterator();

    if (!left_iter) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return false;
    }

    left_iter->SeekToFirst();
    while (left_iter->Valid()) {
        const Row& left_row = left_iter->GetValue();
        std::string key_str =
            index_key_gen_.Valid() ? index_key_gen_.Gen(left_row) : "";
        if (left_key_gen_.Valid()) {
            key_str = key_str.empty()
                          ? left_key_gen_.Gen(left_row)
                          : key_str + "|" + left_key_gen_.Gen(left_row);
        }
        DLOG(INFO) << "key_str " << key_str;
        auto right_table = right->GetSegment(right, key_str);
        output->AddRow(Runner::RowLastJoinTable(
            left_slices_, left_row, right_slices_, right_table, right_sort_gen_,
            condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
                                  std::shared_ptr<TableHandler> right,
                                  std::shared_ptr<MemPartitionHandler> output) {
    auto left_window_iter = left->GetWindowIterator();
    left_window_iter->SeekToFirst();
    while (left_window_iter->Valid()) {
        auto left_iter = left_window_iter->GetValue();
        auto left_key = left_window_iter->GetKey();
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            auto key_str = std::string(
                reinterpret_cast<const char*>(left_key.buf()), left_key.size());
            output->AddRow(key_str, left_iter->GetKey(),
                           Runner::RowLastJoinTable(
                               left_slices_, left_row, right_slices_, right,
                               right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
    }
    return true;
}
bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
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
            auto left_key_str = std::string(
                reinterpret_cast<const char*>(left_key.buf()), left_key.size());
            output->AddRow(left_key_str, left_iter->GetKey(),
                           Runner::RowLastJoinTable(
                               left_slices_, left_row, right_slices_,
                               right_table, right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
        left_partition_iter->Next();
    }
    return true;
}
const Row Runner::RowLastJoinTable(size_t left_slices, const Row& left_row,
                                   size_t right_slices,
                                   std::shared_ptr<TableHandler> right_table,
                                   SortGenerator& right_sort,
                                   ConditionGenerator& cond_gen) {
    right_table = right_sort.Sort(right_table, true);
    if (!right_table) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices, left_row, right_slices, Row());
    }
    auto right_iter = right_table->GetIterator();
    if (!right_iter) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices, left_row, right_slices, Row());
    }
    right_iter->SeekToFirst();

    if (!right_iter->Valid()) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices, left_row, right_slices, Row());
    }

    if (!cond_gen.Valid()) {
        return Row(left_slices, left_row, right_slices, right_iter->GetValue());
    }

    while (right_iter->Valid()) {
        Row joined_row(left_slices, left_row, right_slices,
                       right_iter->GetValue());
        if (cond_gen.Gen(joined_row)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_slices, left_row, right_slices, Row());
}
void Runner::PrintData(const vm::SchemaSourceList& schema_list,
                       std::shared_ptr<DataHandler> data) {
    std::ostringstream oss;
    std::vector<RowView> row_view_list;
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    t.add("Order");
    for (auto source : schema_list.schema_source_list_) {
        for (int i = 0; i < source.schema_->size(); i++) {
            if (source.table_name_.empty()) {
                t.add(source.schema_->Get(i).name());
            } else {
                t.add(source.table_name_ + "." + source.schema_->Get(i).name());
            }

            if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                break;
            }
        }
        row_view_list.push_back(RowView(*source.schema_));
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
            t.add("...");
            break;
        }
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
            t.add("0");
            for (size_t id = 0; id < row_view_list.size(); id++) {
                RowView& row_view = row_view_list[id];
                row_view.Reset(row.buf(id), row.size(id));
                for (int idx = 0;
                     idx < schema_list.schema_source_list_[id].schema_->size();
                     idx++) {
                    std::string str = row_view.GetAsString(idx);
                    t.add(str);
                    if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                        break;
                    }
                }
                if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                    t.add("...");
                    break;
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
                    t.add(std::to_string(iter->GetKey()));
                    for (size_t id = 0; id < row_view_list.size(); id++) {
                        RowView& row_view = row_view_list[id];
                        row_view.Reset(row.buf(id));
                        for (int idx = 0;
                             idx < schema_list.schema_source_list_[id]
                                       .schema_->size();
                             idx++) {
                            std::string str = row_view.GetAsString(idx);
                            t.add(str);
                            if (t.current_columns_size() >=
                                MAX_DEBUG_COLUMN_MAX) {
                                break;
                            }
                        }
                        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                            t.add("...");
                            break;
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
            if (!iter) {
                t.add("Empty set");
                t.endOfRow();
            }
            iter->SeekToFirst();
            if (!iter->Valid()) {
                t.add("Empty set");
                t.endOfRow();
            }
            while (iter->Valid() && cnt++ < MAX_DEBUG_LINES_CNT) {
                t.add("KEY: " + std::string(reinterpret_cast<const char*>(
                                                iter->GetKey().buf()),
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
                        t.add(std::to_string(segment_iter->GetKey()));
                        for (size_t id = 0; id < row_view_list.size(); id++) {
                            RowView& row_view = row_view_list[id];
                            row_view.Reset(row.buf(id));
                            for (int idx = 0;
                                 idx < schema_list.schema_source_list_[id]
                                           .schema_->size();
                                 idx++) {
                                std::string str = row_view.GetAsString(idx);
                                t.add(str);
                                if (t.current_columns_size() >=
                                    MAX_DEBUG_COLUMN_MAX) {
                                    break;
                                }
                            }
                            if (t.current_columns_size() >=
                                MAX_DEBUG_COLUMN_MAX) {
                                t.add("...");
                                break;
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
    size_t left_slices =
        producers_[0]->output_schemas().GetSchemaSourceListSize();
    size_t right_slices =
        producers_[1]->output_schemas().GetSchemaSourceListSize();
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
        return std::shared_ptr<RowHandler>(new MemRowHandler(
            Row(left_slices, left_row, right_slices, right_row)));
    } else if (kTableHandler == right->GetHanlderType()) {
        auto left_row = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();
        auto right_table = std::dynamic_pointer_cast<TableHandler>(right);
        auto right_iter = right_table->GetIterator();
        if (!right_iter) {
            return std::shared_ptr<RowHandler>(new MemRowHandler(
                Row(left_slices, left_row, right_slices, Row())));
        }
        return std::shared_ptr<RowHandler>(new MemRowHandler(
            Row(left_slices, left_row, right_slices, right_iter->GetValue())));
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
            if (!iter) {
                LOG(WARNING) << "fail to get table it";
                return fail_ptr;
            }
            iter->SeekToFirst();
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
    int32_t cnt = 0;
    while (iter->Valid()) {
        if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
            break;
        }
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            LOG(WARNING) << "group aggregation fail: segment iterator is null";
            return std::shared_ptr<DataHandler>();
        }
        auto key = iter->GetKey().ToString();
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

    auto request = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();
    // build window with start and end offset
    auto window_table =
        std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
    uint64_t start = 0;
    uint64_t end = UINT64_MAX;
    uint64_t rows_start_preceding = 0;
    int64_t request_key = range_gen_.ts_gen_.Gen(request);
    DLOG(INFO) << "request key: " << request_key;
    if (range_gen_.Valid()) {
        start = (request_key + range_gen_.start_offset_) < 0
                    ? 0
                    : (request_key + range_gen_.start_offset_);
        end = (request_key + range_gen_.end_offset_) < 0
                  ? 0
                  : (request_key + range_gen_.end_offset_);
        rows_start_preceding = range_gen_.start_row_;
    }
    DLOG(INFO) << " start " << start << " end " << end;
    window_table->AddRow(request_key, request);
    // Prepare Union Window
    auto union_inputs = windows_union_gen_.RunInputs(ctx);
    auto union_segments =
        windows_union_gen_.GetRequestWindows(request, union_inputs);
    // Prepare Union Segment Iterators
    size_t unions_cnt = windows_union_gen_.inputs_cnt_;

    std::vector<std::unique_ptr<RowIterator>> union_segment_iters(unions_cnt);
    std::vector<IteratorStatus> union_segment_status(unions_cnt);

    for (size_t i = 0; i < unions_cnt; i++) {
        if (!union_segments[i]) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i] = union_segments[i]->GetIterator();
        if (!union_segment_iters[i]) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i]->Seek(end);
        if (!union_segment_iters[i]->Valid()) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        uint64_t ts = union_segment_iters[i]->GetKey();
        union_segment_status[i] = IteratorStatus(ts);
    }
    int32_t max_union_pos = 0 == unions_cnt
                                ? -1
                                : IteratorStatus::PickIteratorWithMaximizeKey(
                                      &union_segment_status);
    uint64_t cnt = 1;
    while (-1 != max_union_pos) {
        if (cnt > rows_start_preceding &&
            union_segment_status[max_union_pos].key_ < start) {
            break;
        }

        window_table->AddRow(union_segment_status[max_union_pos].key_,
                             union_segment_iters[max_union_pos]->GetValue());
        cnt++;
        // Update Iterator Status
        union_segment_iters[max_union_pos]->Next();
        if (!union_segment_iters[max_union_pos]->Valid()) {
            union_segment_status[max_union_pos].MarkInValid();
        } else {
            union_segment_status[max_union_pos].set_key(
                union_segment_iters[max_union_pos]->GetKey());
        }
        // Pick new mininum union pos
        max_union_pos =
            IteratorStatus::PickIteratorWithMaximizeKey(&union_segment_status);
    }
    return window_table;
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
        std::string key = row_view_.GetAsString(pos);
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
    return CoreAPI::ComputeCondition(fn_, row, &row_view_, idxs_[0]);
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
    return Row(
        base::RefCountedSlice::CreateManaged(buf, RowView::GetSize(buf)));
}

const Row WindowProjectGenerator::Gen(const uint64_t key, const Row row,
                                      bool is_instance, Window* window) {
    return Runner::WindowProject(fn_, key, row, is_instance, window);
}

std::vector<std::shared_ptr<DataHandler>> InputsGenerator::RunInputs(
    RunnerContext& ctx) {
    std::vector<std::shared_ptr<DataHandler>> union_inputs;
    if (!input_runners_.empty()) {
        for (auto runner : input_runners_) {
            union_inputs.push_back(runner->RunWithCache(ctx));
        }
    }
    return union_inputs;
}
std::vector<std::shared_ptr<PartitionHandler>>
WindowUnionGenerator::PartitionEach(
    std::vector<std::shared_ptr<DataHandler>> union_inputs) {
    std::vector<std::shared_ptr<PartitionHandler>> union_partitions;
    if (!windows_gen_.empty()) {
        union_partitions.reserve(windows_gen_.size());
        for (size_t i = 0; i < inputs_cnt_; i++) {
            union_partitions.push_back(
                windows_gen_[i].partition_gen_.Partition(union_inputs[i]));
        }
    }
    return union_partitions;
}
int32_t IteratorStatus::PickIteratorWithMininumKey(
    std::vector<IteratorStatus>* status_list_ptr) {
    auto status_list = *status_list_ptr;
    int32_t min_union_pos = -1;
    uint64_t min_union_order = UINT64_MAX;
    for (size_t i = 0; i < status_list.size(); i++) {
        if (status_list[i].is_valid_ && status_list[i].key_ < min_union_order) {
            min_union_order = status_list[i].key_;
            min_union_pos = static_cast<int32_t>(i);
        }
    }
    return min_union_pos;
}
int32_t IteratorStatus::PickIteratorWithMaximizeKey(
    std::vector<IteratorStatus>* status_list_ptr) {
    auto status_list = *status_list_ptr;
    int32_t min_union_pos = -1;
    uint64_t min_union_order = 0;
    for (size_t i = 0; i < status_list.size(); i++) {
        if (status_list[i].is_valid_ &&
            status_list[i].key_ >= min_union_order) {
            min_union_order = status_list[i].key_;
            min_union_pos = static_cast<int32_t>(i);
        }
    }
    return min_union_pos;
}
std::vector<std::shared_ptr<DataHandler>> WindowJoinGenerator::RunInputs(
    RunnerContext& ctx) {
    std::vector<std::shared_ptr<DataHandler>> union_inputs;
    if (!input_runners_.empty()) {
        for (auto runner : input_runners_) {
            union_inputs.push_back(runner->RunWithCache(ctx));
        }
    }
    return union_inputs;
}
Row WindowJoinGenerator::Join(
    const Row& left_row,
    const std::vector<std::shared_ptr<DataHandler>>& join_right_tables) {
    Row row = left_row;
    for (size_t i = 0; i < join_right_tables.size(); i++) {
        row = joins_gen_[i].RowLastJoin(row, join_right_tables[i]);
    }
    return row;
}
std::shared_ptr<TableHandler> IndexSeekGenerator::SegmentOfKey(
    const Row& row, std::shared_ptr<DataHandler> input) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (!input) {
        LOG(WARNING) << "fail to seek segment of key: input is empty";
        return fail_ptr;
    }
    if (row.empty()) {
        LOG(WARNING) << "fail to seek segment: key row is empty";
        return fail_ptr;
    }

    if (!index_key_gen_.Valid()) {
        switch (input->GetHanlderType()) {
            case kPartitionHandler: {
                LOG(WARNING) << "fail to seek segment: index key is empty";
                return fail_ptr;
            }
            case kTableHandler: {
                return std::dynamic_pointer_cast<TableHandler>(input);
            }
            default: {
                LOG(WARNING) << "fail to seek segment when input is row";
                return fail_ptr;
            }
        }
    }

    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(input);
            auto key = index_key_gen_.Gen(row);
            return partition->GetSegment(partition, key);
        }
        default: {
            LOG(WARNING) << "fail to seek segment when input isn't partition";
            return fail_ptr;
        }
    }
}

}  // namespace vm
}  // namespace fesql
