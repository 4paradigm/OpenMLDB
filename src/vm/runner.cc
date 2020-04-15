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
#define ROW_ARG int8_t*, int8_t*, int32_t
#define ROW_ARGS(N)
#define DECLARE_UDF(FN, N)\
int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) = \
(int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(FN);
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
                    auto runner = new TableProjectRunner(id_++, op->GetFn(),
                                                         op->GetFnSchema(),
                                                         op->GetLimitCnt());
                    runner->AddProducer(input);
                    return runner;
                }
                case kAggregation: {
                    auto runner =
                        new AggRunner(id_++, op->GetFn(), op->GetFnSchema(),
                                      op->GetLimitCnt());
                    runner->AddProducer(input);
                    return runner;
                }
                case kGroupAggregation: {
                    auto runner = new GroupAggRunner(id_++, op->GetFn(),
                                                     op->GetFnSchema(),
                                                     op->GetLimitCnt());
                    runner->AddProducer(input);
                    return runner;
                }
                case kWindowAggregation: {
                    auto op =
                        dynamic_cast<const PhysicalWindowAggrerationNode*>(
                            node);
                    auto runner = new WindowAggRunner(
                        id_++, op->GetFn(), op->GetFnSchema(),
                        op->GetLimitCnt(), op->GetGroupsIdxs(),
                        op->GetOrdersIdxs(), op->start_offset_,
                        op->end_offset_);
                    runner->AddProducer(input);
                    return runner;
                }
                case kRowProject: {
                    auto runner = new RowProjectRunner(id_++, op->GetFn(),
                                                       op->GetFnSchema(), 0);
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

            auto seek_op = dynamic_cast<const PhysicalSeekIndexNode*>(node);
            auto runner = new IndexSeekRunner(
                id_++, seek_op->GetFn(), seek_op->GetFnSchema(),
                seek_op->GetLimitCnt(), seek_op->GetKeysIdxs());
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
                id_++, op->GetFn(), op->GetFnSchema(), op->GetLimitCnt(),
                op->GetGroupsIdxs(), op->GetOrdersIdxs(), op->GetKeysIdxs(),
                op->GetIsAsc(), op->start_offset_, op->end_offset_);
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
            auto runner = new RequestLastJoinRunner(
                id_++, op->GetFn(), op->GetFnSchema(), op->GetLimitCnt(),
                op->GetConditionIdxs());
            runner->AddProducer(left);
            runner->AddProducer(right);
            return runner;
        }
        case kPhysicalOpGroupBy: {
            auto input = Build(node->GetProducers().at(0), status);
            if (nullptr == input) {
                return nullptr;
            }
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            auto runner =
                new GroupRunner(id_++, op->GetFn(), op->GetFnSchema(),
                                op->GetLimitCnt(), op->GetGroupsIdxs());
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
                id_++, op->GetFn(), op->GetFnSchema(), op->GetLimitCnt(),
                op->GetGroupsIdxs(), op->GetOrdersIdxs(), op->GetIsAsc());
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
                new FilterRunner(id_++, op->GetFn(), op->GetFnSchema(),
                                 op->GetLimitCnt(), op->GetConditionIdxs());
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

Slice Runner::WindowProject(const int8_t* fn, uint64_t key, const Slice slice,
                            Window* window) {
    if (slice.empty()) {
        return slice;
    }
    window->BufferData(key, slice);
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
Slice Runner::RowProject(const int8_t* fn, const Slice slice) {
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

Slice Runner::MultiRowsProject(const int8_t* fn,
                               std::vector<const Slice>& slices_ptr) {
    if (slices_ptr.empty()) {
        return Slice();
    }
    int32_t (*udf)(int8_t*, int8_t*, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int8_t**))(fn);
    int8_t* buf = nullptr;
    uint32_t ret = udf(reinterpret_cast<int8_t*>(slices_ptr), nullptr, &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Slice();
    }
    return Slice(reinterpret_cast<char*>(buf), RowView::GetSize(buf));
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
    int64_t key = -1;
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
    const std::shared_ptr<DataHandler> table, const Schema& schema,
    const int8_t* fn, const std::vector<int>& idxs, RowView* row_view) {
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
    while (iter->Valid()) {
        const Slice key_row(RowProject(fn, iter->GetValue()), true);
        row_view->Reset(key_row.buf(), key_row.size());
        std::string keys = GenerateKeys(row_view, schema, idxs);
        output_partitions->AddRow(keys, iter->GetKey(), iter->GetValue());

        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<DataHandler> Runner::PartitionGroup(
    const std::shared_ptr<DataHandler> table, const Schema& schema,
    const int8_t* fn, const std::vector<int>& idxs, RowView* row_view) {
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
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            const Slice key_row(RowProject(fn, segment_iter->GetValue()), true);
            row_view->Reset(key_row.buf(), key_row.size());
            std::string keys = GenerateKeys(row_view, schema, idxs);
            output_partitions->AddRow(keys, segment_iter->GetKey(),
                                      segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    return output_partitions;
}

std::shared_ptr<DataHandler> Runner::PartitionSort(
    std::shared_ptr<DataHandler> table, const Schema& schema, const int8_t* fn,
    std::vector<int> idxs, const bool is_asc, RowView* row_view) {
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
                const Slice order_row(RowProject(fn, segment_iter->GetValue()),
                                      true);
                row_view->Reset(order_row.buf(), order_row.size());
                key = GetColumnInt64(row_view, idxs[0],
                                     schema.Get(idxs[0]).type());
            }
            output_partitions->AddRow(
                std::string(iter->GetKey().data(), iter->GetKey().size()), key,
                segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    if (idxs.empty()) {
        output_partitions->Reverse();
    } else {
        output_partitions->Sort(is_asc);
    }
    return output_partitions;
}
std::shared_ptr<DataHandler> Runner::TableSort(
    std::shared_ptr<DataHandler> table, const Schema& schema, const int8_t* fn,
    std::vector<int> idxs, const bool is_asc, RowView* row_view) {
    if (!table) {
        return std::shared_ptr<DataHandler>();
    }
    if (kTableHandler != table->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    if (idxs.empty()) {
        return table;
    }
    auto output_table = std::shared_ptr<MemSegmentHandler>(
        new MemSegmentHandler(table->GetSchema()));
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    while (iter->Valid()) {
        const Slice order_row(RowProject(fn, iter->GetValue()), true);

        row_view->Reset(order_row.buf(), order_row.size());

        int64_t key =
            GetColumnInt64(row_view, idxs[0], schema.Get(idxs[0]).type());
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
    if (idxs_.empty()) {
        return input;
    }

    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            return PartitionGroup(input, fn_schema_, fn_, idxs_, &row_view_);
        }
        case kTableHandler: {
            return TableGroup(input, fn_schema_, fn_, idxs_, &row_view_);
        }
        default: {
            LOG(WARNING) << "fail group when input type isn't "
                            "partition or table";
            return fail_ptr;
        }
    }
}
std::shared_ptr<DataHandler> OrderRunner::Run(RunnerContext& ctx) {
    return TableSort(producers_[0]->RunWithCache(ctx), fn_schema_, fn_, idxs_,
                     is_asc_, &row_view_);
}
std::shared_ptr<DataHandler> GroupAndSortRunner::Run(RunnerContext& ctx) {
    auto input = producers_[0]->RunWithCache(ctx);
    if (!input) {
        return std::shared_ptr<DataHandler>();
    }

    std::shared_ptr<DataHandler> output;
    switch (input->GetHanlderType()) {
        case kPartitionHandler:
            output = PartitionSort(input, fn_schema_, fn_, order_idxs_, is_asc_,
                                   &row_view_);
            break;
        case kTableHandler:
            output = TableSort(input, fn_schema_, fn_, order_idxs_, is_asc_,
                               &row_view_);

            break;
        default: {
            LOG(WARNING) << "fail to sort and group table: input isn't table "
                            "or partition";
            return std::shared_ptr<DataHandler>();
        }
    }

    switch (output->GetHanlderType()) {
        case kPartitionHandler:
            return PartitionGroup(output, fn_schema_, fn_, groups_idxs_,
                                  &row_view_);
        case kTableHandler:
            return TableGroup(output, fn_schema_, fn_, groups_idxs_,
                              &row_view_);
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
    auto output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(&fn_schema_));
    auto iter = std::dynamic_pointer_cast<TableHandler>(input)->GetIterator();

    int32_t cnt = 0;
    while (iter->Valid()) {
        if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
            break;
        }
        output_table->AddRow(RowProject(fn_, iter->GetValue()));
        iter->Next();
    }
    return output_table;
}
std::shared_ptr<DataHandler> RowProjectRunner::Run(RunnerContext& ctx) {
    auto row =
        std::dynamic_pointer_cast<RowHandler>(producers_[0]->RunWithCache(ctx));
    return std::shared_ptr<DataHandler>(
        new MemRowHandler(RowProject(fn_, row->GetValue()), &fn_schema_));
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

    auto output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(&fn_schema_));

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
            const Slice row(WindowProject(fn_, segment->GetKey(),
                                          segment->GetValue(), &window));
            output_table->AddRow(row);
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

    auto keys_row =
        Slice(RowProject(
                  fn_, std::dynamic_pointer_cast<RowHandler>(left)->GetValue()),
              true);
    row_view_.Reset(keys_row.buf(), keys_row.size());

    std::string key = GenerateKeys(&row_view_, fn_schema_, keys_idxs_);
    return partition->GetSegment(partition, key);
}
std::shared_ptr<DataHandler> RequestLastJoinRunner::Run(
    RunnerContext& ctx) {  // NOLINT
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
    //    auto request_fn_row = Slice(RowProject(fn_, request), true);
    // filter by keys if need
    if (!condition_idxs_.empty()) {
        auto condition_row =
            Slice(RowProject(
                fn_, std::dynamic_pointer_cast<RowHandler>(left)->GetValue()),
                  true);

        auto condition_idx = condition_idxs_[0];
        if (GetColumnBool(&row_view_, condition_idx,
                          fn_schema_.Get(condition_idx).type())) {
        }

        auto mem_table = std::shared_ptr<MemTableHandler>(
            new MemTableHandler(output_schema));
        auto iter = table->GetIterator();
        std::vector<const Slice&> rows;
        auto condition_type = fn_schema_.Get(condition_idx).type();
        rows.reserve(2);
        rows.push_back(request);
        while (iter->Valid()) {
            rows[1] = iter->GetValue();
            auto row = Slice(MultiRowsProject(fn_, &rows), true);
            row_view_.Reset(row.buf(), row.size());
            if (GetColumnBool(&row_view_, condition_idx, condition_type)) {
                mem_table->AddRow(iter->GetValue());
                break;
            }
            iter->Next();
        }
        output = std::shared_ptr<TableHandler>(mem_table);
    }

    // sort by orders if need
    if (!orders_idxs_.empty()) {
        output = TableSort(std::shared_ptr<DataHandler>(output), fn_schema_,
                           fn_, orders_idxs_, false, &row_view_);
    }

    // build window with start and end offset
    auto window_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(output_schema));

    uint64_t start = 0;
    uint64_t end = UINT64_MAX;

    if (!keys_idxs_.empty()) {
        row_view_.Reset(request_fn_row.buf(), request_fn_row.size());
        auto ts_idx = keys_idxs_[0];
        int64_t key =
            GetColumnInt64(&row_view_, ts_idx, fn_schema_.Get(ts_idx).type());

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
    auto output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(&fn_schema_));
    auto iter = partition->GetWindowIterator();
    iter->SeekToFirst();

    int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(fn_);

    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            LOG(WARNING) << "group aggregation fail: segment iterator is null";
            return std::shared_ptr<DataHandler>();
        }
        auto& first_row = segment_iter->GetValue();
        auto key = std::string(iter->GetKey().data(), iter->GetKey().size());
        auto segment = partition->GetSegment(partition, key);
        int8_t* buf = nullptr;
        uint32_t ret =
            udf(first_row.buf(), reinterpret_cast<int8_t*>(segment.get()),
                first_row.size(), &buf);
        if (ret != 0) {
            LOG(WARNING) << "fail to run udf " << ret;
            return std::shared_ptr<DataHandler>();
        }
        iter->Next();
        output_table->AddRow(
            Slice(reinterpret_cast<char*>(buf), RowView::GetSize(buf)));
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
    auto request_fn_row = Slice(RowProject(fn_, request), true);
    // filter by keys if need
    if (!groups_idxs_.empty()) {
        row_view_.Reset(request_fn_row.buf(), request_fn_row.size());
        std::string request_keys =
            GenerateKeys(&row_view_, fn_schema_, groups_idxs_);

        auto mem_table = std::shared_ptr<MemTableHandler>(
            new MemTableHandler(output_schema));
        auto iter = table->GetIterator();
        while (iter->Valid()) {
            auto row = Slice(RowProject(fn_, iter->GetValue()), true);
            row_view_.Reset(row.buf(), row.size());
            std::string keys =
                GenerateKeys(&row_view_, fn_schema_, groups_idxs_);
            if (request_keys == keys) {
                mem_table->AddRow(iter->GetValue());
            }
            iter->Next();
        }
        output = std::shared_ptr<TableHandler>(mem_table);
    }

    // sort by orders if need
    if (!orders_idxs_.empty()) {
        output = TableSort(std::shared_ptr<DataHandler>(output), fn_schema_,
                           fn_, orders_idxs_, false, &row_view_);
    }

    // build window with start and end offset
    auto window_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler(output_schema));

    uint64_t start = 0;
    uint64_t end = UINT64_MAX;

    if (!keys_idxs_.empty()) {
        row_view_.Reset(request_fn_row.buf(), request_fn_row.size());
        auto ts_idx = keys_idxs_[0];
        int64_t key =
            GetColumnInt64(&row_view_, ts_idx, fn_schema_.Get(ts_idx).type());

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
    auto table = std::dynamic_pointer_cast<TableHandler>(input);
    auto iter = table->GetIterator();
    iter->SeekToFirst();
    if (!iter->Valid()) {
        return std::shared_ptr<DataHandler>();
    }

    auto& row = iter->GetValue();

    int32_t (*udf)(int8_t*, int8_t*, int32_t, int8_t**) =
        (int32_t(*)(int8_t*, int8_t*, int32_t, int8_t**))(fn_);
    int8_t* buf = nullptr;

    uint32_t ret = udf(row.buf(), reinterpret_cast<int8_t*>(table.get()),
                       row.size(), &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return std::shared_ptr<DataHandler>();
    }
    return std::shared_ptr<RowHandler>(new MemRowHandler(
        Slice(reinterpret_cast<char*>(buf), RowView::GetSize(buf)),
        &fn_schema_));
}
}  // namespace vm
}  // namespace fesql
