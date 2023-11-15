/**
 * Copyright (c) 2023 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/generator.h"

#include <utility>

#include "node/sql_node.h"
#include "vm/catalog.h"
#include "vm/catalog_wrapper.h"
#include "vm/runner.h"

namespace hybridse {
namespace vm {

std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<DataHandler> input, const Row& parameter) {
    switch (input->GetHandlerType()) {
        case kPartitionHandler: {
            return Partition(
                std::dynamic_pointer_cast<PartitionHandler>(input), parameter);
        }
        case kTableHandler: {
            return Partition(std::dynamic_pointer_cast<TableHandler>(input), parameter);
        }
        default: {
            LOG(WARNING) << "Partition Fail: input isn't partition or table";
            return std::shared_ptr<PartitionHandler>();
        }
    }
}
std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<PartitionHandler> table, const Row& parameter) {
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
    if (!iter) {
        LOG(WARNING) << "Partition Fail: partition is Empty";
        return std::shared_ptr<PartitionHandler>();
    }
    iter->SeekToFirst();
    output_partitions->SetOrderType(table->GetOrderType());
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            iter->Next();
            continue;
        }
        auto segment_key = iter->GetKey().ToString();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            std::string keys = key_gen_.Gen(segment_iter->GetValue(), parameter);
            output_partitions->AddRow(segment_key + "|" + keys,
                                      segment_iter->GetKey(),
                                      segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<TableHandler> table, const Row& parameter) {
    auto fail_ptr = std::shared_ptr<PartitionHandler>();
    if (!key_gen_.Valid()) {
        return fail_ptr;
    }
    if (!table) {
        return fail_ptr;
    }
    if (kTableHandler != table->GetHandlerType()) {
        return fail_ptr;
    }

    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "Fail to group empty table: table is empty";
        return fail_ptr;
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        std::string keys = key_gen_.Gen(iter->GetValue(), parameter);
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
    switch (input->GetHandlerType()) {
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
        LOG(WARNING) << "Sort partition fail: partition is Empty";
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
    auto output_table = std::make_shared<MemTimeTableHandler>(table->GetSchema());
    output_table->SetOrderType(table->GetOrderType());
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "Sort table fail: table is Empty";
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
Row JoinGenerator::RowLastJoinDropLeftSlices(
    const Row& left_row, std::shared_ptr<DataHandler> right, const Row& parameter) {
    Row joined = RowLastJoin(left_row, right, parameter);
    Row right_row(joined.GetSlice(left_slices_));
    for (size_t offset = 1; offset < right_slices_; offset++) {
        right_row.Append(joined.GetSlice(left_slices_ + offset));
    }
    return right_row;
}

std::shared_ptr<TableHandler> JoinGenerator::LazyJoin(std::shared_ptr<DataHandler> left,
                                                      std::shared_ptr<DataHandler> right, const Row& parameter) {
    if (left->GetHandlerType() == kPartitionHandler) {
        return std::make_shared<LazyJoinPartitionHandler>(std::dynamic_pointer_cast<PartitionHandler>(left), right,
                                                          parameter, shared_from_this());
    }

    auto left_tb = std::dynamic_pointer_cast<TableHandler>(left);
    if (left->GetHandlerType() == kRowHandler) {
        auto left_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
        left_table->AddRow(std::dynamic_pointer_cast<RowHandler>(left)->GetValue());
        left_tb = left_table;
    }
    return std::make_shared<LazyJoinTableHandler>(left_tb, right, parameter, shared_from_this());
}

std::shared_ptr<PartitionHandler> JoinGenerator::LazyJoinOptimized(std::shared_ptr<PartitionHandler> left,
                                                                   std::shared_ptr<PartitionHandler> right,
                                                                   const Row& parameter) {
    return std::make_shared<LazyJoinPartitionHandler>(left, right, parameter, shared_from_this());
}

std::unique_ptr<RowIterator> JoinGenerator::InitRight(const Row& left_row, std::shared_ptr<PartitionHandler> right,
                                                      const Row& param) {
    auto partition_key = index_key_gen_.Gen(left_row, param);
    auto right_seg = right->GetSegment(partition_key);
    if (!right_seg) {
        return {};
    }
    auto it = right_seg->GetIterator();
    if (!it) {
        return {};
    }
    it->SeekToFirst();
    return it;
}

Row JoinGenerator::RowLastJoin(const Row& left_row,
                               std::shared_ptr<DataHandler> right,
                               const Row& parameter) {
    switch (right->GetHandlerType()) {
        case kPartitionHandler: {
            return RowLastJoinPartition(
                left_row, std::dynamic_pointer_cast<PartitionHandler>(right), parameter);
        }
        case kTableHandler: {
            return RowLastJoinTable(
                left_row, std::dynamic_pointer_cast<TableHandler>(right), parameter);
        }
        case kRowHandler: {
            auto right_table =
                std::shared_ptr<MemTableHandler>(new MemTableHandler());
            right_table->AddRow(
                std::dynamic_pointer_cast<RowHandler>(right)->GetValue());
            return RowLastJoinTable(left_row, right_table, parameter);
        }
        default: {
            LOG(WARNING) << "Last Join right isn't row or table or partition";
            return Row(left_slices_, left_row, right_slices_, Row());
        }
    }
}
Row JoinGenerator::RowLastJoinPartition(
    const Row& left_row, std::shared_ptr<PartitionHandler> partition,
    const Row& parameter) {
    if (!index_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when partition "
                        "keys is empty";
        return Row();
    }
    std::string partition_key = index_key_gen_.Gen(left_row, parameter);
    auto right_table = partition->GetSegment(partition_key);
    return RowLastJoinTable(left_row, right_table, parameter);
}

Row JoinGenerator::RowLastJoinTable(const Row& left_row,
                                    std::shared_ptr<TableHandler> table,
                                    const Row& parameter) {
    if (right_sort_gen_.Valid()) {
        table = right_sort_gen_.Sort(table, true);
    }
    if (!table) {
        return Row(left_slices_, left_row, right_slices_, Row());
    }
    auto right_iter = table->GetIterator();
    if (!right_iter) {
        return Row(left_slices_, left_row, right_slices_, Row());
    }
    right_iter->SeekToFirst();
    if (!right_iter->Valid()) {
        return Row(left_slices_, left_row, right_slices_, Row());
    }

    if (!left_key_gen_.Valid() && !condition_gen_.Valid()) {
        return Row(left_slices_, left_row, right_slices_,
                   right_iter->GetValue());
    }

    std::string left_key_str = "";
    if (left_key_gen_.Valid()) {
        left_key_str = left_key_gen_.Gen(left_row, parameter);
    }
    while (right_iter->Valid()) {
        if (right_group_gen_.Valid()) {
            auto right_key_str =
                right_group_gen_.GetKey(right_iter->GetValue(), parameter);
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
        if (condition_gen_.Gen(joined_row, parameter)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_slices_, left_row, right_slices_, Row());
}

std::pair<Row, bool> JoinGenerator::RowJoinIterator(const Row& left_row,
                                                    std::unique_ptr<codec::RowIterator>& right_iter,
                                                    const Row& parameter) {
    if (!right_iter || !right_iter ->Valid()) {
        return {Row(left_slices_, left_row, right_slices_, Row()), false};
    }

    if (!left_key_gen_.Valid() && !condition_gen_.Valid()) {
        auto right_value = right_iter->GetValue();
        return {Row(left_slices_, left_row, right_slices_, right_value), true};
    }

    std::string left_key_str = "";
    if (left_key_gen_.Valid()) {
        left_key_str = left_key_gen_.Gen(left_row, parameter);
    }
    while (right_iter->Valid()) {
        if (right_group_gen_.Valid()) {
            auto right_key_str = right_group_gen_.GetKey(right_iter->GetValue(), parameter);
            if (left_key_gen_.Valid() && left_key_str != right_key_str) {
                right_iter->Next();
                continue;
            }
        }

        Row joined_row(left_slices_, left_row, right_slices_, right_iter->GetValue());
        if (!condition_gen_.Valid() || condition_gen_.Gen(joined_row, parameter)) {
            return {joined_row, true};
        }
        right_iter->Next();
    }

    return {Row(left_slices_, left_row, right_slices_, Row()), false};
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<TableHandler> right,
                              const Row& parameter,
                              std::shared_ptr<MemTimeTableHandler> output) {
    auto left_iter = left->GetIterator();
    if (!left_iter) {
        LOG(WARNING) << "Table Join with empty left table";
        return false;
    }
    left_iter->SeekToFirst();
    while (left_iter->Valid()) {
        const Row& left_row = left_iter->GetValue();
        output->AddRow(
            left_iter->GetKey(),
            Runner::RowLastJoinTable(left_slices_, left_row, right_slices_,
                                     right, parameter, right_sort_gen_, condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<PartitionHandler> right,
                              const Row& parameter,
                              std::shared_ptr<MemTimeTableHandler> output) {
    if (!left_key_gen_.Valid() && !index_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when neither left_key_gen_ or index_key_gen_ is valid";
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
            index_key_gen_.Valid() ? index_key_gen_.Gen(left_row, parameter) : "";
        if (left_key_gen_.Valid()) {
            key_str = key_str.empty()
                          ? left_key_gen_.Gen(left_row, parameter)
                          : key_str + "|" + left_key_gen_.Gen(left_row, parameter);
        }
        DLOG(INFO) << "key_str " << key_str;
        auto right_table = right->GetSegment(key_str);
        output->AddRow(left_iter->GetKey(), Runner::RowLastJoinTable(left_slices_, left_row, right_slices_, right_table,
                                                                     parameter, right_sort_gen_, condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
                                  std::shared_ptr<TableHandler> right,
                                  const Row& parameter,
                                  std::shared_ptr<MemPartitionHandler> output) {
    auto left_window_iter = left->GetWindowIterator();
    if (!left_window_iter) {
        LOG(WARNING) << "fail to run last join: left iter empty";
        return false;
    }
    left_window_iter->SeekToFirst();
    while (left_window_iter->Valid()) {
        auto left_iter = left_window_iter->GetValue();
        auto left_key = left_window_iter->GetKey();
        if (!left_iter) {
            left_window_iter->Next();
            continue;
        }
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            auto key_str = std::string(
                reinterpret_cast<const char*>(left_key.buf()), left_key.size());
            output->AddRow(key_str, left_iter->GetKey(),
                           Runner::RowLastJoinTable(
                               left_slices_, left_row, right_slices_, right,
                               parameter,
                               right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
        left_window_iter->Next();
    }
    return true;
}
bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
                                  std::shared_ptr<PartitionHandler> right,
                                  const Row& parameter,
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
    if (!index_key_gen_.Valid() && !left_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when join "
                        "left_key_gen_ and index_key_gen_ are invalid";
        return false;
    }

    left_partition_iter->SeekToFirst();
    while (left_partition_iter->Valid()) {
        auto left_iter = left_partition_iter->GetValue();
        auto left_key = left_partition_iter->GetKey();
        if (!left_iter) {
            left_partition_iter->Next();
            continue;
        }
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();

            std::string key_str = "";
            if (index_key_gen_.Valid()) {
                key_str = index_key_gen_.Gen(left_row, parameter);
            }
            if (left_key_gen_.Valid()) {
                key_str = key_str.empty() ? left_key_gen_.Gen(left_row, parameter) :
                                          key_str.append("|").append(left_key_gen_.Gen(left_row, parameter));
            }
            auto right_table = right->GetSegment(key_str);
            auto left_key_str = std::string(
                reinterpret_cast<const char*>(left_key.buf()), left_key.size());
            output->AddRow(left_key_str, left_iter->GetKey(),
                           Runner::RowLastJoinTable(
                               left_slices_, left_row, right_slices_,
                               right_table, parameter, right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
        left_partition_iter->Next();
    }
    return true;
}

/**
 * TODO(chenjing): GenConst key during compile-time
 * @return
 */
const std::string KeyGenerator::GenConst(const Row& parameter) {
    Row key_row = CoreAPI::RowConstProject(fn_, parameter, true);
    codec::RowView row_view(row_view_);
    if (!row_view.Reset(key_row.buf())) {
        LOG(WARNING) << "fail to gen key: row view reset fail";
        return "NA";
    }
    std::string keys = "";
    for (auto pos : idxs_) {
        std::string key =
            row_view.IsNULL(pos)
                ? codec::NONETOKEN
                : fn_schema_.Get(pos).type() == hybridse::type::kDate
                      ? std::to_string(row_view.GetDateUnsafe(pos))
                      : row_view.GetAsString(pos);
        if (key == "") {
            key = codec::EMPTY_STRING;
        }
        if (!keys.empty()) {
            keys.append("|");
        }
        keys.append(key);
    }
    return keys;
}
const std::string KeyGenerator::Gen(const Row& row, const Row& parameter) {
    // TODO(wtz) 避免不必要的row project
    if (row.size() == 0) {
        return codec::NONETOKEN;
    }
    Row key_row = CoreAPI::RowProject(fn_, row, parameter, true);
    std::string keys = "";
    for (auto pos : idxs_) {
        if (!keys.empty()) {
            keys.append("|");
        }
        if (row_view_.IsNULL(key_row.buf(), pos)) {
            keys.append(codec::NONETOKEN);
            continue;
        }
        ::hybridse::type::Type type = fn_schema_.Get(pos).type();
        switch (type) {
            case ::hybridse::type::kVarchar: {
                const char* buf = nullptr;
                uint32_t size = 0;
                if (row_view_.GetValue(key_row.buf(), pos, &buf, &size) == 0) {
                    if (size == 0) {
                        keys.append(codec::EMPTY_STRING.c_str(),
                                    codec::EMPTY_STRING.size());
                    } else {
                        keys.append(buf, size);
                    }
                }
                break;
            }
            case hybridse::type::kDate: {
                int32_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0) {
                    keys.append(std::to_string(buf));
                }
                break;
            }
            case hybridse::type::kBool: {
                bool buf = false;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0) {
                    keys.append(buf ? "true" : "false");
                }
                break;
            }
            case hybridse::type::kInt16: {
                int16_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0) {
                    keys.append(std::to_string(buf));
                }
                break;
            }
            case hybridse::type::kInt32: {
                int32_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0) {
                    keys.append(std::to_string(buf));
                }
                break;
            }
            case hybridse::type::kInt64:
            case hybridse::type::kTimestamp: {
                int64_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0) {
                    keys.append(std::to_string(buf));
                }
                break;
            }
            default: {
                DLOG(ERROR) << "unsupported: partition key's type is " << node::TypeName(type);
                break;
            }
        }
    }
    return keys;
}

const int64_t OrderGenerator::Gen(const Row& row) {
    Row order_row = CoreAPI::RowProject(fn_, row, Row(), true);
    return Runner::GetColumnInt64(order_row.buf(), &row_view_, idxs_[0],
                                  fn_schema_.Get(idxs_[0]).type());
}

const bool ConditionGenerator::Gen(const Row& row, const Row& parameter) const {
    return CoreAPI::ComputeCondition(fn_, row, parameter, &row_view_, idxs_[0]);
}
const bool ConditionGenerator::Gen(std::shared_ptr<TableHandler> table, const codec::Row& parameter) {
    Row cond_row = Runner::GroupbyProject(fn_, parameter, table.get());
    return Runner::GetColumnBool(cond_row.buf(), &row_view_, idxs_[0],
                                 row_view_.GetSchema()->Get(idxs_[0]).type());
}
const Row ProjectGenerator::Gen(const Row& row, const Row& parameter) {
    return CoreAPI::RowProject(fn_, row, parameter, false);
}

const Row ConstProjectGenerator::Gen(const Row& parameter) {
    return CoreAPI::RowConstProject(fn_, parameter, false);
}

const Row AggGenerator::Gen(const codec::Row& parameter_row, std::shared_ptr<TableHandler> table) {
    return Runner::GroupbyProject(fn_, parameter_row, table.get());
}

const Row WindowProjectGenerator::Gen(const uint64_t key, const Row row,
                                      const codec::Row& parameter,
                                      bool is_instance, size_t append_slices,
                                      Window* window) {
    return Runner::WindowProject(fn_, key, row, parameter, is_instance, append_slices,
                                 window);
}

std::shared_ptr<TableHandler> IndexSeekGenerator::SegmnetOfConstKey(
    const Row& parameter,
    std::shared_ptr<DataHandler> input) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (!input) {
        LOG(WARNING) << "fail to seek segment of key: input is empty";
        return fail_ptr;
    }
    if (!index_key_gen_.Valid()) {
        switch (input->GetHandlerType()) {
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

    switch (input->GetHandlerType()) {
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(input);
            auto key = index_key_gen_.GenConst(parameter);
            return partition->GetSegment(key);
        }
        default: {
            LOG(WARNING) << "fail to seek segment when input isn't partition";
            return fail_ptr;
        }
    }
}
std::shared_ptr<TableHandler> IndexSeekGenerator::SegmentOfKey(
    const Row& row, const Row& parameter, std::shared_ptr<DataHandler> input) {
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
        switch (input->GetHandlerType()) {
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

    switch (input->GetHandlerType()) {
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(input);
            auto key = index_key_gen_.Gen(row, parameter);
            return partition->GetSegment(key);
        }
        default: {
            LOG(WARNING) << "fail to seek segment when input isn't partition";
            return fail_ptr;
        }
    }
}

std::shared_ptr<DataHandler> FilterGenerator::Filter(std::shared_ptr<PartitionHandler> partition, const Row& parameter,
                                                     std::optional<int32_t> limit) {
    if (!partition) {
        LOG(WARNING) << "fail to filter table: input is empty";
        return std::shared_ptr<DataHandler>();
    }
    if (index_seek_gen_.Valid()) {
        return Filter(index_seek_gen_.SegmnetOfConstKey(parameter, partition), parameter, limit);
    } else {
        if (condition_gen_.Valid()) {
            partition = std::make_shared<PartitionFilterWrapper>(partition, parameter, this);
        }

        if (!limit.has_value()) {
            return partition;
        }

        return std::make_shared<LimitTableHandler>(partition, limit.value());
    }
}

std::shared_ptr<DataHandler> FilterGenerator::Filter(std::shared_ptr<TableHandler> table, const Row& parameter,
                                                     std::optional<int32_t> limit) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (!table) {
        LOG(WARNING) << "fail to filter table: input is empty";
        return fail_ptr;
    }

    if (condition_gen_.Valid()) {
        table = std::make_shared<TableFilterWrapper>(table, parameter, this);
    }

    if (!limit.has_value()) {
        return table;
    }

    return std::make_shared<LimitTableHandler>(table, limit.value());
}

bool FilterGenerator::ValidIndex() const {
    return index_seek_gen_.Valid();
}
std::vector<std::shared_ptr<DataHandler>> InputsGenerator::RunInputs(
    RunnerContext& ctx) {
    std::vector<std::shared_ptr<DataHandler>> union_inputs;
    for (auto runner : input_runners_) {
        union_inputs.push_back(runner->RunWithCache(ctx));
    }
    return union_inputs;
}

std::vector<std::shared_ptr<PartitionHandler>> WindowUnionGenerator::PartitionEach(
    std::vector<std::shared_ptr<DataHandler>> union_inputs, const Row& parameter) {
    std::vector<std::shared_ptr<PartitionHandler>> union_partitions;
    if (!windows_gen_.empty()) {
        union_partitions.reserve(windows_gen_.size());
        for (size_t i = 0; i < inputs_cnt_; i++) {
            union_partitions.push_back(
                windows_gen_[i].partition_gen_.Partition(union_inputs[i], parameter));
        }
    }
    return union_partitions;
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
    const std::vector<std::shared_ptr<DataHandler>>& join_right_tables,
    const Row& parameter) {
    Row row = left_row;
    for (size_t i = 0; i < join_right_tables.size(); i++) {
        row = joins_gen_[i]->RowLastJoin(row, join_right_tables[i], parameter);
    }
    return row;
}

void WindowJoinGenerator::AddWindowJoin(const class Join& join, size_t left_slices, Runner* runner) {
    size_t right_slices = runner->output_schemas()->GetSchemaSourceSize();
    joins_gen_.push_back(JoinGenerator::Create(join, left_slices, right_slices));
    AddInput(runner);
}

std::vector<std::shared_ptr<TableHandler>> RequestWindowUnionGenerator::GetRequestWindows(
    const Row& row, const Row& parameter, std::vector<std::shared_ptr<DataHandler>> union_inputs) {
    std::vector<std::shared_ptr<TableHandler>> union_segments(union_inputs.size());
    for (size_t i = 0; i < union_inputs.size(); i++) {
        union_segments[i] = windows_gen_[i].GetRequestWindow(row, parameter, union_inputs[i]);
    }
    return union_segments;
}
void RequestWindowUnionGenerator::AddWindowUnion(const RequestWindowOp& window_op, Runner* runner) {
    windows_gen_.emplace_back(window_op);
    AddInput(runner);
}
void WindowUnionGenerator::AddWindowUnion(const WindowOp& window_op, Runner* runner) {
    windows_gen_.push_back(WindowGenerator(window_op));
    AddInput(runner);
}
std::shared_ptr<TableHandler> RequestWindowGenertor::GetRequestWindow(const Row& row, const Row& parameter,
                                                                      std::shared_ptr<DataHandler> input) {
    auto segment = index_seek_gen_.SegmentOfKey(row, parameter, input);

    if (filter_gen_.Valid()) {
        auto filter_key = filter_gen_.GetKey(row, parameter);
        segment = filter_gen_.Filter(parameter, segment, filter_key);
    }
    if (sort_gen_.Valid()) {
        segment = sort_gen_.Sort(segment, true);
    }
    return segment;
}
std::shared_ptr<TableHandler> FilterKeyGenerator::Filter(const Row& parameter, std::shared_ptr<TableHandler> table,
                                                         const std::string& request_keys) {
    if (!filter_key_.Valid()) {
        return table;
    }
    auto mem_table = std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
    mem_table->SetOrderType(table->GetOrderType());
    auto iter = table->GetIterator();
    if (iter) {
        iter->SeekToFirst();
        while (iter->Valid()) {
            std::string keys = filter_key_.Gen(iter->GetValue(), parameter);
            if (request_keys == keys) {
                mem_table->AddRow(iter->GetKey(), iter->GetValue());
            }
            iter->Next();
        }
    }
    return mem_table;
}
}  // namespace vm
}  // namespace hybridse
