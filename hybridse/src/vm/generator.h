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

// generators for runner

#ifndef HYBRIDSE_SRC_VM_GENERATOR_H_
#define HYBRIDSE_SRC_VM_GENERATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "vm/core_api.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace vm {

class ProjectFun {
 public:
    virtual Row operator()(const Row& row, const Row& parameter) const = 0;
};
class PredicateFun {
 public:
    virtual bool operator()(const Row& row, const Row& parameter) const = 0;
};

class FnGenerator {
 public:
    explicit FnGenerator(const FnInfo& info)
        : fn_(info.fn_ptr()), fn_schema_(*info.fn_schema()), row_view_(fn_schema_) {
        for (int32_t idx = 0; idx < fn_schema_.size(); idx++) {
            idxs_.push_back(idx);
        }
    }
    virtual ~FnGenerator() {}
    inline const bool Valid() const { return nullptr != fn_; }
    const int8_t* fn_;
    const Schema fn_schema_;
    const codec::RowView row_view_;
    std::vector<int32_t> idxs_;
};

class RowProjectFun : public ProjectFun {
 public:
    explicit RowProjectFun(const int8_t* fn) : ProjectFun(), fn_(fn) {}
    ~RowProjectFun() {}
    Row operator()(const Row& row, const Row& parameter) const override {
        return CoreAPI::RowProject(fn_, row, parameter, false);
    }
    const int8_t* fn_;
};

class ProjectGenerator : public FnGenerator {
 public:
    explicit ProjectGenerator(const FnInfo& info) : FnGenerator(info), fun_(info.fn_ptr()) {}
    virtual ~ProjectGenerator() {}
    const Row Gen(const Row& row, const Row& parameter);
    RowProjectFun fun_;
};

class ConstProjectGenerator : public FnGenerator {
 public:
    explicit ConstProjectGenerator(const FnInfo& info) : FnGenerator(info), fun_(info.fn_ptr()) {}
    virtual ~ConstProjectGenerator() {}
    const Row Gen(const Row& parameter);
    RowProjectFun fun_;
};
class AggGenerator : public FnGenerator {
 public:
    explicit AggGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~AggGenerator() {}
    const Row Gen(const codec::Row& parameter_row, std::shared_ptr<TableHandler> table);
};
class WindowProjectGenerator : public FnGenerator {
 public:
    explicit WindowProjectGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~WindowProjectGenerator() {}
    const Row Gen(const uint64_t key, const Row row, const codec::Row& parameter_row, const bool is_instance,
                  size_t append_slices, Window* window);
};
class KeyGenerator : public FnGenerator {
 public:
    explicit KeyGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~KeyGenerator() {}
    const std::string Gen(const Row& row, const Row& parameter);
    const std::string GenConst(const Row& parameter);
};
class OrderGenerator : public FnGenerator {
 public:
    explicit OrderGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~OrderGenerator() {}
    const int64_t Gen(const Row& row);
};
class ConditionGenerator : public FnGenerator {
 public:
    explicit ConditionGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~ConditionGenerator() {}
    const bool Gen(const Row& row, const Row& parameter) const;
    const bool Gen(std::shared_ptr<TableHandler> table, const codec::Row& parameter_row);
};
class RangeGenerator {
 public:
    explicit RangeGenerator(const Range& range) : ts_gen_(range.fn_info()), window_range_() {
        if (range.frame_ != nullptr) {
            switch (range.frame()->frame_type()) {
                case node::kFrameRows:
                    window_range_.frame_type_ = Window::WindowFrameType::kFrameRows;
                    break;
                case node::kFrameRowsRange:
                    window_range_.frame_type_ = Window::WindowFrameType::kFrameRowsRange;
                    break;
                case node::kFrameRowsMergeRowsRange:
                    window_range_.frame_type_ = Window::WindowFrameType::kFrameRowsMergeRowsRange;
                default: {
                    window_range_.frame_type_ = Window::WindowFrameType::kFrameRowsMergeRowsRange;
                    break;
                }
            }
            window_range_.start_offset_ = range.frame_->GetHistoryRangeStart();
            window_range_.end_offset_ = range.frame_->GetHistoryRangeEnd();
            window_range_.start_row_ = (-1 * range.frame_->GetHistoryRowsStart());
            window_range_.end_row_ = (-1 * range.frame_->GetHistoryRowsEnd());
            window_range_.max_size_ = range.frame_->frame_maxsize();
            if (window_range_.max_size_ > 0 && range.frame_->exclude_current_row_ &&
                range.frame_->GetHistoryRangeEnd() == 0) {
                // codegen EXCLUDE CURRENT ROW droped one row, increate maxsize early
                window_range_.max_size_++;
            }
        }
    }
    virtual ~RangeGenerator() {}
    const bool Valid() const { return ts_gen_.Valid(); }
    OrderGenerator ts_gen_;
    WindowRange window_range_;
};
class FilterKeyGenerator {
 public:
    explicit FilterKeyGenerator(const Key& filter_key) : filter_key_(filter_key.fn_info()) {}
    virtual ~FilterKeyGenerator() {}
    const bool Valid() const { return filter_key_.Valid(); }
    std::shared_ptr<TableHandler> Filter(const Row& parameter, std::shared_ptr<TableHandler> table,
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
    const std::string GetKey(const Row& row, const Row& parameter) {
        return filter_key_.Valid() ? filter_key_.Gen(row, parameter) : "";
    }
    KeyGenerator filter_key_;
};

class PartitionGenerator {
 public:
    explicit PartitionGenerator(const Key& partition) : key_gen_(partition.fn_info()) {}
    virtual ~PartitionGenerator() {}

    const bool Valid() const { return key_gen_.Valid(); }
    std::shared_ptr<PartitionHandler> Partition(std::shared_ptr<DataHandler> input, const Row& parameter);
    std::shared_ptr<PartitionHandler> Partition(std::shared_ptr<PartitionHandler> table, const Row& parameter);
    std::shared_ptr<PartitionHandler> Partition(std::shared_ptr<TableHandler> table, const Row& parameter);
    const std::string GetKey(const Row& row, const Row& parameter) { return key_gen_.Gen(row, parameter); }

 private:
    KeyGenerator key_gen_;
};
class SortGenerator {
 public:
    explicit SortGenerator(const Sort& sort)
        : is_valid_(sort.ValidSort()), is_asc_(sort.is_asc()), order_gen_(sort.fn_info()) {}
    virtual ~SortGenerator() {}

    const bool Valid() const { return is_valid_; }

    std::shared_ptr<DataHandler> Sort(std::shared_ptr<DataHandler> input, const bool reverse = false);
    std::shared_ptr<PartitionHandler> Sort(std::shared_ptr<PartitionHandler> partition, const bool reverse = false);
    std::shared_ptr<TableHandler> Sort(std::shared_ptr<TableHandler> table, const bool reverse = false);
    const OrderGenerator& order_gen() const { return order_gen_; }

 private:
    bool is_valid_;
    bool is_asc_;
    OrderGenerator order_gen_;
};

class IndexSeekGenerator {
 public:
    explicit IndexSeekGenerator(const Key& key) : index_key_gen_(key.fn_info()) {}
    virtual ~IndexSeekGenerator() {}
    std::shared_ptr<TableHandler> SegmnetOfConstKey(const Row& parameter, std::shared_ptr<DataHandler> input);
    std::shared_ptr<TableHandler> SegmentOfKey(const Row& row, const Row& parameter,
                                               std::shared_ptr<DataHandler> input);
    const bool Valid() const { return index_key_gen_.Valid(); }

    KeyGenerator index_key_gen_;
};

class FilterGenerator : public PredicateFun {
 public:
    explicit FilterGenerator(const Filter& filter)
        : condition_gen_(filter.condition_.fn_info()), index_seek_gen_(filter.index_key_) {}

    const bool Valid() const { return index_seek_gen_.Valid() || condition_gen_.Valid(); }

    std::shared_ptr<DataHandler> Filter(std::shared_ptr<TableHandler> table, const Row& parameter,
                                        std::optional<int32_t> limit);

    std::shared_ptr<DataHandler> Filter(std::shared_ptr<PartitionHandler> table, const Row& parameter,
                                        std::optional<int32_t> limit);

    bool operator()(const Row& row, const Row& parameter) const override {
        if (!condition_gen_.Valid()) {
            return true;
        }
        return condition_gen_.Gen(row, parameter);
    }

 private:
    ConditionGenerator condition_gen_;
    IndexSeekGenerator index_seek_gen_;
};
class WindowGenerator {
 public:
    explicit WindowGenerator(const WindowOp& window)
        : window_op_(window), partition_gen_(window.partition_), sort_gen_(window.sort_), range_gen_(window.range_) {}
    virtual ~WindowGenerator() {}
    const int64_t OrderKey(const Row& row) { return range_gen_.ts_gen_.Gen(row); }
    const WindowOp window_op_;
    PartitionGenerator partition_gen_;
    SortGenerator sort_gen_;
    RangeGenerator range_gen_;
};

class RequestWindowGenertor {
 public:
    explicit RequestWindowGenertor(const RequestWindowOp& window)
        : window_op_(window),
          filter_gen_(window.partition_),
          sort_gen_(window.sort_),
          range_gen_(window.range_.fn_info()),
          index_seek_gen_(window.index_key_) {}
    virtual ~RequestWindowGenertor() {}
    std::shared_ptr<TableHandler> GetRequestWindow(const Row& row, const Row& parameter,
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
    RequestWindowOp window_op_;
    FilterKeyGenerator filter_gen_;
    SortGenerator sort_gen_;
    OrderGenerator range_gen_;
    IndexSeekGenerator index_seek_gen_;
};

class JoinGenerator : public std::enable_shared_from_this<JoinGenerator> {
 public:
    [[nodiscard]] static std::shared_ptr<JoinGenerator> Create(const Join& join, size_t left_slices,
                                                               size_t right_slices) {
        return std::shared_ptr<JoinGenerator>(new JoinGenerator(join, left_slices, right_slices));
    }

    virtual ~JoinGenerator() {}
    bool TableJoin(std::shared_ptr<TableHandler> left, std::shared_ptr<TableHandler> right, const Row& parameter,
                   std::shared_ptr<MemTimeTableHandler> output);  // NOLINT
    bool TableJoin(std::shared_ptr<TableHandler> left, std::shared_ptr<PartitionHandler> right, const Row& parameter,
                   std::shared_ptr<MemTimeTableHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left, std::shared_ptr<TableHandler> right,
                       const Row& parameter,
                       std::shared_ptr<MemPartitionHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left, std::shared_ptr<PartitionHandler> right,
                       const Row& parameter,
                       std::shared_ptr<MemPartitionHandler>);  // NOLINT

    Row RowLastJoin(const Row& left_row, std::shared_ptr<DataHandler> right, const Row& parameter);
    Row RowLastJoinDropLeftSlices(const Row& left_row, std::shared_ptr<DataHandler> right, const Row& parameter);

    std::shared_ptr<PartitionHandler> LazyLastJoin(std::shared_ptr<PartitionHandler> left,
                                                   std::shared_ptr<PartitionHandler> right, const Row& parameter);

    ConditionGenerator condition_gen_;
    KeyGenerator left_key_gen_;
    PartitionGenerator right_group_gen_;
    KeyGenerator index_key_gen_;
    SortGenerator right_sort_gen_;

 private:
    explicit JoinGenerator(const Join& join, size_t left_slices, size_t right_slices)
        : condition_gen_(join.condition_.fn_info()),
          left_key_gen_(join.left_key_.fn_info()),
          right_group_gen_(join.right_key_),
          index_key_gen_(join.index_key_.fn_info()),
          right_sort_gen_(join.right_sort_),
          left_slices_(left_slices),
          right_slices_(right_slices) {}

    Row RowLastJoinPartition(const Row& left_row, std::shared_ptr<PartitionHandler> partition, const Row& parameter);
    Row RowLastJoinTable(const Row& left_row, std::shared_ptr<TableHandler> table, const Row& parameter);

    size_t left_slices_;
    size_t right_slices_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_GENERATOR_H_
