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
#include "vm/mem_catalog.h"
namespace fesql {
namespace vm {
#define MAX_ROW_NUM 20;
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
                    return new DataRunner(id_++, provider->table_handler_);
                }
                case kProviderTypeIndexScan: {
                    auto provider =
                        dynamic_cast<const PhysicalScanIndexNode*>(node);
                    return new DataRunner(id_++, provider->table_handler_);
                }
                case kProviderTypeRequest: {
                    return new RequestRunner(id_++, op->output_schema_);
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
            auto input = Build(node->GetProducers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }

            auto op = dynamic_cast<const PhysicalProjectNode*>(node);
            switch (op->project_type_) {
                case kTableProject: {
                    auto runner = new TableProjectRunner(
                        id_++, op->GetLimitCnt(), op->GetFnInfo());
                    runner->AddProducer(input);
                    return runner;
                }
                case kAggregation: {
                    auto runner = new AggRunner(id_++, op->GetLimitCnt(),
                                                op->GetFnInfo());
                    runner->AddProducer(input);
                    return runner;
                }
                case kGroupAggregation: {
                    auto runner = new GroupAggRunner(id_++, op->GetLimitCnt(),
                                                     op->GetFnInfo());
                    runner->AddProducer(input);
                    return runner;
                }
                case kWindowAggregation: {
                    auto op =
                        dynamic_cast<const PhysicalWindowAggrerationNode*>(
                            node);
                    auto runner = new WindowAggRunner(
                        id_++, op->GetLimitCnt(), op->GetFnInfo(),
                        op->start_offset_, op->end_offset_);
                    runner->AddProducer(input);
                    return runner;
                }
                case kRowProject: {
                    auto runner = new RowProjectRunner(id_++, op->GetLimitCnt(),
                                                       op->GetFnInfo());
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
        case kPhysicalOpIndexSeek: {
            auto left = Build(node->GetProducers().at(0), status);
            if (nullptr == left) {
                return nullptr;
            }
            auto right = Build(node->GetProducers().at(1), status);
            if (nullptr == right) {
                return nullptr;
            }

            auto op = dynamic_cast<const PhysicalSeekIndexNode*>(node);
            auto runner = new IndexSeekRunner(
                id_++, op->GetLimitCnt(), op->GetFnInfo(), op->GetKeysIdxs());
            runner->AddProducer(left);
            runner->AddProducer(right);
            return runner;
        }
        case kPhysicalOpRequestUnoin: {
            auto left = Build(node->GetProducers().at(0), status);
            if (nullptr == left) {
                return nullptr;
            }
            auto right = Build(node->GetProducers().at(1), status);
            if (nullptr == right) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalRequestUnionNode*>(node);
            auto runner = new RequestUnionRunner(
                id_++, op->GetLimitCnt(), op->GetFnInfo(), op->GetGroupsIdxs(),
                op->GetOrdersIdxs(), op->GetKeysIdxs(), op->GetIsAsc(),
                op->start_offset_, op->end_offset_);
            runner->AddProducer(left);
            runner->AddProducer(right);
            return runner;
        }
        case kPhysicalOpRequestJoin: {
            auto left = Build(node->GetProducers().at(0), status);
            if (nullptr == left) {
                return nullptr;
            }
            auto right = Build(node->GetProducers().at(1), status);
            if (nullptr == right) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalRequestJoinNode*>(node);
            switch (op->join_type_) {
                case node::kJoinTypeLast: {
                    auto runner = new RequestLastJoinRunner(
                        id_++, op->GetLimitCnt(), op->GetFnInfo(),
                        op->GetConditionIdxs(), op->GetLeftKeyFnInfo(),
                        op->GetLeftKeysIdxs());
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
            auto left = Build(node->GetProducers().at(0), status);
            if (nullptr == left) {
                return nullptr;
            }
            auto right = Build(node->GetProducers().at(1), status);
            if (nullptr == right) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalJoinNode*>(node);
            switch (op->join_type_) {
                case node::kJoinTypeLast: {
                    auto runner = new LastJoinRunner(
                        id_++, op->GetLimitCnt(), op->GetFnInfo(),
                        op->GetConditionIdxs(), op->GetLeftKeyFnInfo(),
                        op->GetLeftKeysIdxs());
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
            auto input = Build(node->GetProducers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            auto runner = new GroupRunner(id_++, op->GetLimitCnt(),
                                          op->GetFnInfo(), op->GetGroupsIdxs());
            runner->AddProducer(input);
            return runner;
        }
        case kPhysicalOpGroupAndSort: {
            auto input = Build(node->GetProducers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalGroupAndSortNode*>(node);

            auto runner = new GroupAndSortRunner(
                id_++, op->GetLimitCnt(), op->GetFnInfo(), op->GetGroupsIdxs(),
                op->GetOrdersIdxs(), op->GetIsAsc());
            runner->AddProducer(input);
            return runner;
        }
        case kPhysicalOpFilter: {
            auto input = Build(node->GetProducers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalFliterNode*>(node);
            auto runner =
                new FilterRunner(id_++, op->GetLimitCnt(), op->GetFnInfo(),
                                 op->GetConditionIdxs());
            runner->AddProducer(input);
            return runner;
        }
        case kPhysicalOpLimit: {
            auto input = Build(node->GetProducers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalLimitNode*>(node);
            if (op->GetLimitCnt() == 0 || op->GetLimitOptimized()) {
                return input;
            }
            auto runner = new LimitRunner(id_++, op->GetLimitCnt());
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

Row Runner::WindowProject(const int8_t* fn, uint64_t key, const Row row,
                          Window* window) {
    if (row.empty()) {
        return row;
    }
    window->BufferData(key, row);
    int32_t (*udf)(int8_t**, int8_t**, int32_t*, int8_t**) =
        (int32_t(*)(int8_t**, int8_t**, int32_t*, int8_t**))(fn);
    int8_t* out_buf = nullptr;
    int8_t* row_ptrs[1] = {row.buf()};
    int8_t* window_ptrs[1] = {reinterpret_cast<int8_t*>(window)};
    int32_t row_sizes[1] = {static_cast<int32_t>(row.size())};
    uint32_t ret = udf(row_ptrs, window_ptrs, row_sizes, &out_buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(reinterpret_cast<char*>(out_buf), RowView::GetSize(out_buf));
}

Row Runner::RowProject(const int8_t* fn, const Row row, const bool need_free) {
    if (row.empty()) {
        return Row();
    }
    int32_t (*udf)(int8_t**, int8_t**, int32_t*, int8_t**) =
        (int32_t(*)(int8_t**, int8_t**, int32_t*, int8_t**))(fn);

    int8_t* buf = nullptr;
    int8_t** row_ptrs = row.GetRowPtrs();
    int32_t* row_sizes = row.GetRowSizes();
    uint32_t ret = udf(row_ptrs, nullptr, row_sizes, &buf);

    if (nullptr != row_ptrs) delete[] row_ptrs;
    if (nullptr != row_sizes) delete[] row_sizes;
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(reinterpret_cast<char*>(buf), RowView::GetSize(buf), need_free);
}
std::string Runner::GetColumnString(RowView* row_view, int key_idx,
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
            LOG(WARNING) << "fail to get partition for "
                            "current row";
            break;
        }
    }
    return key;
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
            LOG(WARNING) << "fail to get partition for "
                            "current row";
            break;
        }
    }
    return key;
}
std::shared_ptr<DataHandler> Runner::TableGroup(
    const std::shared_ptr<DataHandler> table, const KeyGenerator& key_gen) {
    if (!key_gen.Valid()) {
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
    while (iter->Valid()) {
        std::string keys = key_gen.Gen(iter->GetValue());
        output_partitions->AddRow(keys, iter->GetKey(), iter->GetValue());
        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<DataHandler> Runner::PartitionGroup(
    const std::shared_ptr<DataHandler> table, const KeyGenerator& key_gen_) {
    if (key_gen_.Valid()) {
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
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            std::string keys = key_gen_.Gen(segment_iter->GetValue());
            output_partitions->AddRow(keys, segment_iter->GetKey(),
                                      segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    return output_partitions;
}

std::shared_ptr<DataHandler> Runner::PartitionSort(
    std::shared_ptr<DataHandler> table, const OrderGenerator& order_gen,
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

    if (order_gen.Valid()) {
        return table;
    }
    auto output_table = std::shared_ptr<MemSegmentHandler>(
        new MemSegmentHandler(table->GetSchema()));
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    while (iter->Valid()) {
        int64_t key = order_gen.Gen(iter->GetValue());
        output_table->AddRow(key, iter->GetValue());
        iter->Next();
    }
    output_table->Sort(is_asc);
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
    if (need_cache_) {
        ctx.cache_.insert(std::make_pair(id_, res));
    }
    return res;
}

std::shared_ptr<DataHandler> DataRunner::Run(RunnerContext& ctx) {
    return data_handler_;
}
std::shared_ptr<DataHandler> RequestRunner::Run(RunnerContext& ctx) {
    return std::shared_ptr<DataHandler>(
        new MemRowHandler(ctx.request_, &request_schema));
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
std::shared_ptr<DataHandler> GroupAndSortRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    if (!input) {
        return std::shared_ptr<DataHandler>();
    }
    std::shared_ptr<DataHandler> output;
    switch (input->GetHanlderType()) {
        case kPartitionHandler:
            output = PartitionSort(input, order_gen_, is_asc_);
            break;
        case kTableHandler:
            output = TableSort(input, order_gen_, is_asc_);
            break;
        default: {
            LOG(WARNING) << "fail to sort and group table: input isn't table "
                            "or partition";
            return std::shared_ptr<DataHandler>();
        }
    }
    switch (output->GetHanlderType()) {
        case kPartitionHandler:
            return PartitionGroup(output, group_gen_);
        case kTableHandler:
            return TableGroup(output, group_gen_);
        default: {
            LOG(WARNING) << "fail to sort and group table: input isn't table "
                            "or partition";
            return std::shared_ptr<DataHandler>();
        }
    }
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
    if (!input) {
        LOG(WARNING) << "window aggregation fail: input is null";
        return std::shared_ptr<DataHandler>();
    }
    if (kPartitionHandler != input->GetHanlderType()) {
        LOG(WARNING) << "window aggregation requires partition input";
        return std::shared_ptr<DataHandler>();
    }
    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    auto partitions = std::dynamic_pointer_cast<PartitionHandler>(input);
    auto iter = partitions->GetWindowIterator();
    int32_t cnt = 0;
    while (iter->Valid()) {
        auto segment = iter->GetValue();
        CurrentHistoryWindow window(start_offset_);
        while (segment->Valid()) {
            if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
                break;
            }
            output_table->AddRow(window_gen_.Gen(segment->GetKey(),
                                                 segment->GetValue(), &window));
            segment->Next();
        }
        iter->Next();
    }
    return output_table;
}
std::shared_ptr<DataHandler> IndexSeekRunner::Run(RunnerContext& ctx) {
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
    if (!left || !right) {
        LOG(WARNING) << "Index seek fail: left or right input is null";
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    if (kPartitionHandler != right->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    std::shared_ptr<PartitionHandler> partition =
        std::dynamic_pointer_cast<PartitionHandler>(right);
    std::string key =
        key_gen_.Gen(std::dynamic_pointer_cast<RowHandler>(left)->GetValue());
    return partition->GetSegment(partition, key);
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
    auto table = std::dynamic_pointer_cast<TableHandler>(right);
    RowIterator* right_iter = nullptr;
    if (kPartitionHandler == right->GetHanlderType()) {
        if (!left_key_gen_.Valid()) {
            LOG(WARNING)
                << "can't join right partition table when join keys is empty";
            return fail_ptr;
        }
        auto partition = std::dynamic_pointer_cast<PartitionHandler>(right);
        auto partition_iter = partition->GetWindowIterator();
        const std::string& key_str = left_key_gen_.Gen(left_row);
        partition_iter->Seek(key_str);
        right_iter = partition_iter->GetValue().get();
    } else {
        right_iter = table->GetIterator().get();
    }
    if (nullptr == right_iter) {
        return std::shared_ptr<RowHandler>(
            new MemRowHandler(Row(left_row, Row())));
    } else {
        return std::shared_ptr<RowHandler>(
            new MemRowHandler(RowLastJoin(left_row, right_iter)));
    }
}

std::shared_ptr<DataHandler> LastJoinRunner::Run(RunnerContext& ctx) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
    if (!left || !right) {
        LOG(WARNING) << "fail to run last join: left|right input is empty";
        return fail_ptr;
    }
    if (kTableHandler != left->GetHanlderType()) {
        LOG(WARNING) << "fail to run last join: left input is invalid type";
        return fail_ptr;
    }

    auto left_table = std::dynamic_pointer_cast<TableHandler>(left);
    auto left_iter = left_table->GetIterator();
    if (!left_iter || !left_iter->Valid()) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return fail_ptr;
    }
    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());

    if (kPartitionHandler == right->GetHanlderType()) {
        if (!left_key_gen_.Valid()) {
            LOG(WARNING)
                << "can't join right partition table when join keys is empty";
            return fail_ptr;
        }
        auto partition = std::dynamic_pointer_cast<PartitionHandler>(right);
        auto partition_iter = partition->GetWindowIterator();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            const std::string& key_str = left_key_gen_.Gen(left_row);
            partition_iter->Seek(key_str);
            partition->GetSegment(partition, key_str);
            auto right_iter = partition_iter->GetValue();
            if (!right_iter) {
                output_table->AddRow(Row(left_row, Row()));
            } else {
                output_table->AddRow(RowLastJoin(left_row, right_iter.get()));
            }
            left_iter->Next();
        }
    } else {
        auto table = std::dynamic_pointer_cast<TableHandler>(right);
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            auto right_iter = table->GetIterator();
            if (!right_iter) {
                output_table->AddRow(Row(left_row, Row()));
            } else {
                output_table->AddRow(RowLastJoin(left_row, right_iter.get()));
            }
            left_iter->Next();
        }
    }
    return output_table;
}
const Row LastJoinRunner::RowLastJoin(const Row& left_row,
                                      RowIterator* right_iter) {
    if (!right_iter->Valid()) {
        return Row(left_row, Row());
    }
    if (!condition_gen_.Valid()) {
        return Row(left_row, right_iter->GetValue());
    }
    while (right_iter->Valid()) {
        Row joined_row(left_row, right_iter->GetValue());
        if (condition_gen_.Gen(joined_row)) {
            return joined_row;
        }
    }
    return Row(left_row, Row());
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
    auto left = producers_[0]->RunWithCache(ctx);
    auto right = producers_[1]->RunWithCache(ctx);
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
    // filter by keys if need
    if (group_gen_.Valid()) {
        auto mem_table = std::shared_ptr<MemTableHandler>(
            new MemTableHandler(output_schema));
        std::string request_keys = group_gen_.Gen(request);
        auto iter = table->GetIterator();
        while (iter->Valid()) {
            std::string keys = group_gen_.Gen(iter->GetValue());
            if (request_keys == keys) {
                mem_table->AddRow(iter->GetValue());
            }
            iter->Next();
        }
        output = std::shared_ptr<TableHandler>(mem_table);
    }

    // sort by orders if need
    if (order_gen_.Valid()) {
        output =
            TableSort(std::shared_ptr<DataHandler>(output), order_gen_, false);
    }

    // build window with start and end offset
    auto window_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(output_schema));

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
    if (output) {
        auto table_output = std::dynamic_pointer_cast<TableHandler>(output);
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

const std::string KeyGenerator::Gen(const Row& row) const {
    Row key_row = Runner::RowProject(fn_, row, true);
    std::string key_str = Runner::GenerateKeys(&row_view_, fn_schema_, idxs_);
    return key_str;
}

const bool ConditionGenerator::Gen(const Row& row) const {
    Row cond_row = Runner::RowProject(fn_, row, true);
    return Runner::GetColumnBool(&row_view_, idxs_[0],
                                 fn_schema_.Get(idxs_[0]).type());
}
const Row ProjectGenerator::Gen(const Row& row) const {
    return Runner::RowProject(fn_, row, false);
}
const Row AggGenerator::Gen(std::shared_ptr<TableHandler> table) const {
    auto iter = table->GetIterator();
    iter->SeekToFirst();
    if (!iter->Valid()) {
        return Row();
    }
    auto& row = iter->GetValue();
    int32_t (*udf)(int8_t**, int8_t**, int32_t*, int8_t**) =
        (int32_t(*)(int8_t**, int8_t**, int32_t*, int8_t**))(fn_);
    int8_t* buf = nullptr;

    int8_t* row_ptrs[1] = {row.buf()};
    int8_t* window_ptrs[1] = {reinterpret_cast<int8_t*>(table.get())};
    int32_t row_sizes[1] = {static_cast<int32_t>(row.size())};
    uint32_t ret = udf(row_ptrs, window_ptrs, row_sizes, &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(buf, RowView::GetSize(buf));
}
const Row WindowGenerator::Gen(const uint64_t key, const Row row,
                               Window* window) const {
    return Runner::WindowProject(fn_, key, row, window);
}
}  // namespace vm
}  // namespace fesql
