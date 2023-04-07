/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_VM_RUNNER_H_
#define HYBRIDSE_SRC_VM_RUNNER_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "node/node_manager.h"
#include "vm/aggregator.h"
#include "vm/catalog.h"
#include "vm/catalog_wrapper.h"
#include "vm/core_api.h"
#include "vm/mem_catalog.h"
#include "vm/physical_op.h"
namespace hybridse {
namespace vm {

using base::Status;
using codec::Row;
using codec::RowView;
using vm::DataHandler;
using vm::PartitionHandler;
using vm::Schema;
using vm::TableHandler;
using vm::Window;

class Runner;
class RunnerContext;
class FnGenerator {
 public:
    explicit FnGenerator(const FnInfo& info)
        : fn_(info.fn_ptr()),
          fn_schema_(*info.fn_schema()),
          row_view_(fn_schema_) {
        for (int32_t idx = 0; idx < fn_schema_.size(); idx++) {
            idxs_.push_back(idx);
        }
    }
    virtual ~FnGenerator() {}
    inline const bool Valid() const { return nullptr != fn_; }
    const int8_t* fn_;
    const Schema fn_schema_;
    const RowView row_view_;
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
    explicit ProjectGenerator(const FnInfo& info)
        : FnGenerator(info), fun_(info.fn_ptr()) {}
    virtual ~ProjectGenerator() {}
    const Row Gen(const Row& row, const Row& parameter);
    RowProjectFun fun_;
};

class ConstProjectGenerator : public FnGenerator {
 public:
    explicit ConstProjectGenerator(const FnInfo& info)
        : FnGenerator(info), fun_(info.fn_ptr()) {}
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
    explicit RangeGenerator(const Range& range)
        : ts_gen_(range.fn_info()), window_range_() {
        if (range.frame_ != nullptr) {
            switch (range.frame()->frame_type()) {
                case node::kFrameRows:
                    window_range_.frame_type_ =
                        Window::WindowFrameType::kFrameRows;
                    break;
                case node::kFrameRowsRange:
                    window_range_.frame_type_ =
                        Window::WindowFrameType::kFrameRowsRange;
                    break;
                case node::kFrameRowsMergeRowsRange:
                    window_range_.frame_type_ =
                        Window::WindowFrameType::kFrameRowsMergeRowsRange;
                default: {
                    window_range_.frame_type_ =
                        Window::WindowFrameType::kFrameRowsMergeRowsRange;
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
    explicit FilterKeyGenerator(const Key& filter_key)
        : filter_key_(filter_key.fn_info()) {}
    virtual ~FilterKeyGenerator() {}
    const bool Valid() const { return filter_key_.Valid(); }
    std::shared_ptr<TableHandler> Filter(const Row& parameter, std::shared_ptr<TableHandler> table,
                                         const std::string& request_keys) {
        if (!filter_key_.Valid()) {
            return table;
        }
        auto mem_table =
            std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
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
    explicit PartitionGenerator(const Key& partition)
        : key_gen_(partition.fn_info()) {}
    virtual ~PartitionGenerator() {}

    const bool Valid() const { return key_gen_.Valid(); }
    std::shared_ptr<PartitionHandler> Partition(
        std::shared_ptr<DataHandler> input, const Row& parameter);
    std::shared_ptr<PartitionHandler> Partition(
        std::shared_ptr<PartitionHandler> table, const Row& parameter);
    std::shared_ptr<PartitionHandler> Partition(
        std::shared_ptr<TableHandler> table, const Row& parameter);
    const std::string GetKey(const Row& row, const Row& parameter) { return key_gen_.Gen(row, parameter); }

 private:
    KeyGenerator key_gen_;
};
class SortGenerator {
 public:
    explicit SortGenerator(const Sort& sort)
        : is_valid_(sort.ValidSort()),
          is_asc_(sort.is_asc()),
          order_gen_(sort.fn_info()) {}
    virtual ~SortGenerator() {}

    const bool Valid() const { return is_valid_; }

    std::shared_ptr<DataHandler> Sort(std::shared_ptr<DataHandler> input,
                                      const bool reverse = false);
    std::shared_ptr<PartitionHandler> Sort(
        std::shared_ptr<PartitionHandler> partition,
        const bool reverse = false);
    std::shared_ptr<TableHandler> Sort(std::shared_ptr<TableHandler> table,
                                       const bool reverse = false);
    const OrderGenerator& order_gen() const { return order_gen_; }

 private:
    bool is_valid_;
    bool is_asc_;
    OrderGenerator order_gen_;
};

class IndexSeekGenerator {
 public:
    explicit IndexSeekGenerator(const Key& key)
        : index_key_gen_(key.fn_info()) {}
    virtual ~IndexSeekGenerator() {}
    std::shared_ptr<TableHandler> SegmnetOfConstKey(
        const Row& parameter,
        std::shared_ptr<DataHandler> input);
    std::shared_ptr<TableHandler> SegmentOfKey(
        const Row& row, const Row& parameter, std::shared_ptr<DataHandler> input);
    const bool Valid() const { return index_key_gen_.Valid(); }

    KeyGenerator index_key_gen_;
};

class FilterGenerator : public PredicateFun {
 public:
    explicit FilterGenerator(const Filter& filter)
        : condition_gen_(filter.condition_.fn_info()),
          index_seek_gen_(filter.index_key_) {}

    const bool Valid() const {
        return index_seek_gen_.Valid() || condition_gen_.Valid();
    }

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
        : window_op_(window),
          partition_gen_(window.partition_),
          sort_gen_(window.sort_),
          range_gen_(window.range_) {}
    virtual ~WindowGenerator() {}
    const int64_t OrderKey(const Row& row) {
        return range_gen_.ts_gen_.Gen(row);
    }
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
    std::shared_ptr<TableHandler> GetRequestWindow(
        const Row& row, const Row& parameter, std::shared_ptr<DataHandler> input) {
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
};  // namespace vm

enum RunnerType {
    kRunnerData,
    kRunnerRequest,
    kRunnerGroup,
    kRunnerFilter,
    kRunnerOrder,
    kRunnerGroupAndSort,
    kRunnerConstProject,
    kRunnerTableProject,
    kRunnerRowProject,
    kRunnerSimpleProject,
    kRunnerSelectSlice,
    kRunnerGroupAgg,
    kRunnerAgg,
    kRunnerReduce,
    kRunnerWindowAgg,
    kRunnerRequestUnion,
    kRunnerRequestAggUnion,
    kRunnerPostRequestUnion,
    kRunnerIndexSeek,
    kRunnerLastJoin,
    kRunnerConcat,
    kRunnerRequestRunProxy,
    kRunnerRequestLastJoin,
    kRunnerBatchRequestRunProxy,
    kRunnerLimit,
    kRunnerUnknow,
};
inline const std::string RunnerTypeName(const RunnerType& type) {
    switch (type) {
        case kRunnerData:
            return "DATA";
        case kRunnerRequest:
            return "REQUEST";
        case kRunnerGroup:
            return "GROUP";
        case kRunnerGroupAndSort:
            return "GROUP_AND_SORT";
        case kRunnerFilter:
            return "FILTER";
        case kRunnerConstProject:
            return "CONST_PROJECT";
        case kRunnerTableProject:
            return "TABLE_PROJECT";
        case kRunnerRowProject:
            return "ROW_PROJECT";
        case kRunnerSimpleProject:
            return "SIMPLE_PROJECT";
        case kRunnerSelectSlice:
            return "SELECT_SLICE";
        case kRunnerGroupAgg:
            return "GROUP_AGG_PROJECT";
        case kRunnerAgg:
            return "AGG_PROJECT";
        case kRunnerReduce:
            return "REDUCE_PROJECT";
        case kRunnerWindowAgg:
            return "WINDOW_AGG_PROJECT";
        case kRunnerRequestUnion:
            return "REQUEST_UNION";
        case kRunnerRequestAggUnion:
            return "REQUEST_AGG_UNION";
        case kRunnerPostRequestUnion:
            return "POST_REQUEST_UNION";
        case kRunnerIndexSeek:
            return "INDEX_SEEK";
        case kRunnerLastJoin:
            return "LASTJOIN";
        case kRunnerConcat:
            return "CONCAT";
        case kRunnerRequestLastJoin:
            return "REQUEST_LASTJOIN";
        case kRunnerLimit:
            return "LIMIT";
        case kRunnerRequestRunProxy:
            return "REQUEST_RUN_PROXY";
        case kRunnerBatchRequestRunProxy:
            return "BATCH_REQUEST_RUN_PROXY";
        default:
            return "UNKNOW";
    }
}

class Runner : public node::NodeBase<Runner> {
 public:
    explicit Runner(const int32_t id)
        : id_(id),
          type_(kRunnerUnknow),
          limit_cnt_(std::nullopt),
          is_lazy_(false),
          need_cache_(false),
          need_batch_cache_(false),
          producers_(),
          output_schemas_() {}
    Runner(const int32_t id, const RunnerType type,
           const vm::SchemasContext* output_schemas)
        : id_(id),
          type_(type),
          limit_cnt_(std::nullopt),
          is_lazy_(false),
          need_cache_(false),
          need_batch_cache_(false),
          producers_(),
          output_schemas_(output_schemas),
          row_parser_(std::make_unique<RowParser>(output_schemas)) {}
    Runner(const int32_t id, const RunnerType type, const vm::SchemasContext* output_schemas,
           const std::optional<int32_t> limit_cnt)
        : id_(id),
          type_(type),
          limit_cnt_(limit_cnt),
          is_lazy_(false),
          need_cache_(false),
          need_batch_cache_(false),
          producers_(),
          output_schemas_(output_schemas),
          row_parser_(std::make_unique<RowParser>(output_schemas)) {}
    virtual ~Runner() {}
    void AddProducer(Runner* runner) { producers_.push_back(runner); }
    bool SetProducer(size_t idx, Runner* runner) {
        if (idx >= producers_.size()) {
            return false;
        }
        producers_[idx] = runner;
        return true;
    }
    const std::vector<Runner*>& GetProducers() const { return producers_; }
    virtual void PrintRunnerInfo(std::ostream& output,
                                 const std::string& tab) const {
        output << tab << "[" << id_ << "]" << RunnerTypeName(type_);
        if (is_lazy_) {
            output << " lazy";
        }
    }
    virtual void Print(std::ostream& output, const std::string& tab,
                       std::set<int32_t>* visited_ids) const {  // NOLINT
        PrintRunnerInfo(output, tab);
        PrintCacheInfo(output);
        if (nullptr != visited_ids &&
            visited_ids->find(id_) != visited_ids->cend()) {
            output << "\n";
            output << "  " << tab << "...";
            return;
        }
        if (nullptr != visited_ids) {
            visited_ids->insert(id_);
        }
        if (!producers_.empty()) {
            for (auto producer : producers_) {
                output << "\n";
                producer->Print(output, "  " + tab, visited_ids);
            }
        }
    }
    const bool need_cache() { return need_cache_; }
    const bool need_batch_cache() { return need_batch_cache_; }
    void EnableCache() { need_cache_ = true; }
    void DisableCache() { need_cache_ = false; }
    void EnableBatchCache() { need_batch_cache_ = true; }
    void DisableBatchCache() { need_batch_cache_ = false; }

    const int32_t id_;
    const RunnerType type_;
    const std::optional<int32_t> limit_cnt_;
    virtual std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs) = 0;
    virtual std::shared_ptr<DataHandlerList> BatchRequestRun(
        RunnerContext& ctx);  // NOLINT
    virtual std::shared_ptr<DataHandler> RunWithCache(
        RunnerContext& ctx);  // NOLINT

    static int64_t GetColumnInt64(const int8_t* buf, const RowView* view,
                                  int pos, type::Type type);
    static bool GetColumnBool(const int8_t* buf, const RowView* view, int idx,
                              type::Type type);
    static Row RowConstProject(
        const int8_t* fn, const bool need_free = false);
    static Row RowProject(const int8_t* fn, const hybridse::codec::Row& row, const hybridse::codec::Row& parameter,
                          const bool need_free);
    static Row WindowProject(const int8_t* fn, const uint64_t key,
                             const Row row, const Row& parameter,
                             const bool is_instance,
                             size_t append_slices, Window* window);
    static Row GroupbyProject(const int8_t* fn, const Row& parameter, TableHandler* table);
    static const Row RowLastJoinTable(size_t left_slices, const Row& left_row,
                                      size_t right_slices,
                                      std::shared_ptr<TableHandler> right_table,
                                      const hybridse::codec::Row& parameter,
                                      SortGenerator& right_sort,    // NOLINT
                                      ConditionGenerator& filter);  // NOLINT
    static std::shared_ptr<TableHandler> TableReverse(
        std::shared_ptr<TableHandler> table);

    static void PrintData(std::ostringstream& oss,
                          const vm::SchemasContext* schema_list,
                          std::shared_ptr<DataHandler> data);
    static void PrintRow(std::ostringstream& oss, const vm::SchemasContext* schema_list, const Row& row);
    static std::string GetPrettyRow(const vm::SchemasContext* schema_list, const Row& row);

    static const bool IsProxyRunner(const RunnerType& type) {
        return kRunnerRequestRunProxy == type ||
               kRunnerBatchRequestRunProxy == type;
    }
    static bool ExtractRows(std::shared_ptr<DataHandlerList> handlers,
                            std::vector<Row>& out_rows);  // NOLINT
    static bool ExtractRow(std::shared_ptr<DataHandler> handler,
                           Row* out_row);  // NOLINT
    static bool ExtractRows(std::shared_ptr<DataHandler> handler,
                            std::vector<Row>& out_rows);  // NOLINT
    const vm::SchemasContext* output_schemas() const { return output_schemas_; }

    void set_output_schemas(const vm::SchemasContext* schemas) {
        output_schemas_ = schemas;
        row_parser_.reset(new RowParser(output_schemas_));
    }

    virtual const std::string GetTypeName() const {
        return RunnerTypeName(type_);
    }
    virtual bool Equals(const Runner* other) const { return this == other; }

    const RowParser* row_parser() const {
        return row_parser_.get();
    }

 protected:
    bool is_lazy_;

    void PrintCacheInfo(std::ostream& output) const {
        if (need_cache_ && need_batch_cache_) {
            output << " (cache_enable, batch_common)";
        } else if (need_cache_) {
            output << " (cache_enable)";
        } else if (need_batch_cache_) {
            output << " (batch_common)";
        }
    }

    bool need_cache_;
    bool need_batch_cache_;
    std::vector<Runner*> producers_;
    const vm::SchemasContext* output_schemas_;
    std::unique_ptr<RowParser> row_parser_ = nullptr;
};

class IteratorStatus {
 public:
    IteratorStatus() : is_valid_(false), key_(0) {}
    explicit IteratorStatus(uint64_t key) : is_valid_(true), key_(key) {}
    virtual ~IteratorStatus() {}

    /// \brief find the vaild iterators whose iterator key are the minium of all iterators given
    ///
    /// \param status_list: a list of iterators
    /// \return index of last found iterators, -1 if not found
    static int32_t FindLastIteratorWithMininumKey(const std::vector<IteratorStatus>& status_list);

    /// \brief find the vaild iterators whose iterator key are the maximum of all iterators given
    ///
    /// \param status_list: a list of iterators
    /// \return index of first found iterators, -1 if not found
    static int32_t FindFirstIteratorWithMaximizeKey(const std::vector<IteratorStatus>& status_list);

    void MarkInValid() {
        is_valid_ = false;
        key_ = 0;
    }
    void set_key(uint64_t key) { key_ = key; }
    bool is_valid_;
    uint64_t key_;
};  // namespace vm

class InputsGenerator {
 public:
    InputsGenerator() : inputs_cnt_(0), input_runners_() {}
    virtual ~InputsGenerator() {}

    std::vector<std::shared_ptr<DataHandler>> RunInputs(
        RunnerContext& ctx);  // NOLINT
    const bool Valid() const { return 0 != inputs_cnt_; }
    void AddInput(Runner* runner) {
        input_runners_.push_back(runner);
        inputs_cnt_++;
    }
    size_t inputs_cnt_;
    std::vector<Runner*> input_runners_;
};
class WindowUnionGenerator : public InputsGenerator {
 public:
    WindowUnionGenerator() : InputsGenerator() {}
    virtual ~WindowUnionGenerator() {}
    std::vector<std::shared_ptr<PartitionHandler>> PartitionEach(
        std::vector<std::shared_ptr<DataHandler>> union_inputs,
        const Row& parameter);
    void AddWindowUnion(const WindowOp& window_op, Runner* runner) {
        windows_gen_.push_back(WindowGenerator(window_op));
        AddInput(runner);
    }
    std::vector<WindowGenerator> windows_gen_;
};

class RequestWindowUnionGenerator : public InputsGenerator {
 public:
    RequestWindowUnionGenerator() : InputsGenerator() {}
    virtual ~RequestWindowUnionGenerator() {}

    std::vector<std::shared_ptr<PartitionHandler>> PartitionEach(
        std::vector<std::shared_ptr<DataHandler>> union_inputs);
    void AddWindowUnion(const RequestWindowOp& window_op, Runner* runner) {
        windows_gen_.push_back(RequestWindowGenertor(window_op));
        AddInput(runner);
    }
    std::vector<std::shared_ptr<TableHandler>> GetRequestWindows(
        const Row& row, const Row& parameter,
        std::vector<std::shared_ptr<DataHandler>> union_inputs) {
        std::vector<std::shared_ptr<TableHandler>> union_segments(union_inputs.size());
        if (!windows_gen_.empty()) {
            for (size_t i = 0; i < union_inputs.size(); i++) {
                union_segments[i] =
                    windows_gen_[i].GetRequestWindow(row, parameter, union_inputs[i]);
            }
        }
        return union_segments;
    }
    std::vector<RequestWindowGenertor> windows_gen_;
};
class JoinGenerator {
 public:
    explicit JoinGenerator(const Join& join, size_t left_slices,
                           size_t right_slices)
        : condition_gen_(join.condition_.fn_info()),
          left_key_gen_(join.left_key_.fn_info()),
          right_group_gen_(join.right_key_),
          index_key_gen_(join.index_key_.fn_info()),
          right_sort_gen_(join.right_sort_),
          left_slices_(left_slices),
          right_slices_(right_slices) {}
    virtual ~JoinGenerator() {}
    bool TableJoin(std::shared_ptr<TableHandler> left, std::shared_ptr<TableHandler> right,
                   const Row& parameter,
                   std::shared_ptr<MemTimeTableHandler> output);  // NOLINT
    bool TableJoin(std::shared_ptr<TableHandler> left, std::shared_ptr<PartitionHandler> right,
                   const Row& parameter,
                   std::shared_ptr<MemTimeTableHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left,
                       std::shared_ptr<TableHandler> right,
                       const Row& parameter,
                       std::shared_ptr<MemPartitionHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left,
                       std::shared_ptr<PartitionHandler> right,
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
    Row RowLastJoinPartition(
        const Row& left_row,
        std::shared_ptr<PartitionHandler> partition,
        const Row& parameter);
    Row RowLastJoinTable(const Row& left_row,
                         std::shared_ptr<TableHandler> table,
                         const Row& parameter);

    size_t left_slices_;
    size_t right_slices_;
};
class WindowJoinGenerator : public InputsGenerator {
 public:
    WindowJoinGenerator() : InputsGenerator() {}
    virtual ~WindowJoinGenerator() {}
    void AddWindowJoin(const Join& join, size_t left_slices, Runner* runner) {
        size_t right_slices = runner->output_schemas()->GetSchemaSourceSize();
        joins_gen_.push_back(JoinGenerator(join, left_slices, right_slices));
        AddInput(runner);
    }
    std::vector<std::shared_ptr<DataHandler>> RunInputs(
        RunnerContext& ctx);  // NOLINT
    Row Join(
        const Row& left_row,
        const std::vector<std::shared_ptr<DataHandler>>& join_right_tables,
        const Row& parameter);
    std::vector<JoinGenerator> joins_gen_;
};

class DataRunner : public Runner {
 public:
    DataRunner(const int32_t id, const SchemasContext* schema,
               std::shared_ptr<DataHandler> data_hander)
        : Runner(id, kRunnerData, schema), data_handler_(data_hander) {}
    ~DataRunner() {}
    virtual std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs);
    std::shared_ptr<DataHandlerList> BatchRequestRun(
        RunnerContext& ctx) override;  // NOLINT
    const std::shared_ptr<DataHandler> data_handler_;
};

class RequestRunner : public Runner {
 public:
    RequestRunner(const int32_t id, const SchemasContext* schema)
        : Runner(id, kRunnerRequest, schema) {}
    ~RequestRunner() {}
    virtual std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs);
    std::shared_ptr<DataHandlerList> BatchRequestRun(
        RunnerContext& ctx);  // NOLINT
};
class GroupRunner : public Runner {
 public:
    GroupRunner(const int32_t id, const SchemasContext* schema,
                const std::optional<int32_t> limit_cnt, const Key& group)
        : Runner(id, kRunnerGroup, schema, limit_cnt), partition_gen_(group) {}
    ~GroupRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    PartitionGenerator partition_gen_;
};
class FilterRunner : public Runner {
 public:
    FilterRunner(const int32_t id, const SchemasContext* schema,
                 const std::optional<int32_t> limit_cnt, const Filter& filter)
        : Runner(id, kRunnerFilter, schema, limit_cnt), filter_gen_(filter) {
        is_lazy_ = true;
    }
    ~FilterRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    FilterGenerator filter_gen_;
};

class SortRunner : public Runner {
 public:
    SortRunner(const int32_t id, const SchemasContext* schema,
               const std::optional<int32_t> limit_cnt, const Sort& sort)
        : Runner(id, kRunnerOrder, schema, limit_cnt), sort_gen_(sort) {}
    ~SortRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    SortGenerator sort_gen_;
};
class ConstProjectRunner : public Runner {
 public:
    ConstProjectRunner(const int32_t id, const SchemasContext* schema,
                       const std::optional<int32_t> limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerConstProject, schema, limit_cnt),
          project_gen_(fn_info) {}
    ~ConstProjectRunner() {}

    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    ConstProjectGenerator project_gen_;
};
class TableProjectRunner : public Runner {
 public:
    TableProjectRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                       const FnInfo& fn_info)
        : Runner(id, kRunnerTableProject, schema, limit_cnt), project_gen_(fn_info) {}
    ~TableProjectRunner() {}

    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    ProjectGenerator project_gen_;
};
class RowProjectRunner : public Runner {
 public:
    RowProjectRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                     const FnInfo& fn_info)
        : Runner(id, kRunnerRowProject, schema, limit_cnt), project_gen_(fn_info) {}
    ~RowProjectRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    ProjectGenerator project_gen_;
};

class SimpleProjectRunner : public Runner {
 public:
    SimpleProjectRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                        const FnInfo& fn_info)
        : Runner(id, kRunnerSimpleProject, schema, limit_cnt), project_gen_(fn_info) {
        is_lazy_ = true;
    }
    SimpleProjectRunner(const int32_t id, const SchemasContext* schema,
                        const std::optional<int32_t> limit_cnt,
                        const ProjectGenerator& project_gen)
        : Runner(id, kRunnerSimpleProject, schema, limit_cnt),
          project_gen_(project_gen) {
        is_lazy_ = true;
    }
    ~SimpleProjectRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    ProjectGenerator project_gen_;
};

class SelectSliceRunner : public Runner {
 public:
    SelectSliceRunner(const int32_t id, const SchemasContext* schema,
                      const std::optional<int32_t> limit_cnt, size_t slice)
        : Runner(id, kRunnerSelectSlice, schema, limit_cnt),
          get_slice_fn_(slice) {
        is_lazy_ = true;
    }

    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs) override;

    size_t slice() const { return get_slice_fn_.slice_; }

 private:
    struct GetSliceFn : public ProjectFun {
        explicit GetSliceFn(size_t slice) : slice_(slice) {}
        Row operator()(const Row& row, const Row& parameter) const override;
        size_t slice_;
    } get_slice_fn_;
};

class GroupAggRunner : public Runner {
 public:
    GroupAggRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                   const Key& group, const ConditionFilter& having_condition, const FnInfo& project)
        : Runner(id, kRunnerGroupAgg, schema, limit_cnt),
          group_(group.fn_info()),
          having_condition_(having_condition.fn_info()),
          agg_gen_(project) {}
    ~GroupAggRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    KeyGenerator group_;
    ConditionGenerator having_condition_;
    AggGenerator agg_gen_;
};
class AggRunner : public Runner {
 public:
    AggRunner(const int32_t id, const SchemasContext* schema,
              const std::optional<int32_t> limit_cnt,
              const ConditionFilter& having_condition,
              const FnInfo& fn_info)
        : Runner(id, kRunnerAgg, schema, limit_cnt),
          having_condition_(having_condition.fn_info()),
          agg_gen_(fn_info) {}
    ~AggRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    ConditionGenerator having_condition_;
    AggGenerator agg_gen_;
};

class ReduceRunner : public Runner {
 public:
    ReduceRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                 const ConditionFilter& having_condition, const FnInfo& fn_info)
        : Runner(id, kRunnerReduce, schema, limit_cnt),
          having_condition_(having_condition.fn_info()),
          agg_gen_(fn_info) {}
    ~ReduceRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx,
                                     const std::vector<std::shared_ptr<DataHandler>>& inputs) override;
    ConditionGenerator having_condition_;
    AggGenerator agg_gen_;
};

class WindowAggRunner : public Runner {
 public:
    WindowAggRunner(int32_t id, const SchemasContext* schema,
                    std::optional<int32_t> limit_cnt, const WindowOp& window_op,
                    const FnInfo& fn_info,
                    bool instance_not_in_window,
                    bool exclude_current_time,
                    size_t append_slices)
        : Runner(id, kRunnerWindowAgg, schema, limit_cnt),
          instance_not_in_window_(instance_not_in_window),
          exclude_current_time_(exclude_current_time),
          append_slices_(append_slices),
          instance_window_gen_(window_op),
          windows_union_gen_(),
          windows_join_gen_(),
          window_project_gen_(fn_info) {}
    ~WindowAggRunner() {}
    void AddWindowJoin(const Join& join, size_t left_slices, Runner* runner) {
        windows_join_gen_.AddWindowJoin(join, left_slices, runner);
    }
    void AddWindowUnion(const WindowOp& window, Runner* runner) {
        windows_union_gen_.AddWindowUnion(window, runner);
    }
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    void RunWindowAggOnKey(
        const Row& parameter,
        std::shared_ptr<PartitionHandler> instance_partition,
        std::vector<std::shared_ptr<PartitionHandler>> union_partitions,
        std::vector<std::shared_ptr<DataHandler>> joins, const std::string& key,
        std::shared_ptr<MemTableHandler> output_table);

    const bool instance_not_in_window_;
    const bool exclude_current_time_;

    // slice size outputed of the first producer node
    const size_t append_slices_;
    WindowGenerator instance_window_gen_;
    WindowUnionGenerator windows_union_gen_;
    WindowJoinGenerator windows_join_gen_;
    WindowProjectGenerator window_project_gen_;
};

class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const SchemasContext* schema,
                       const std::optional<int32_t> limit_cnt, const Range& range,
                       bool exclude_current_time, bool output_request_row)
        : Runner(id, kRunnerRequestUnion, schema, limit_cnt),
          range_gen_(range),
          exclude_current_time_(exclude_current_time),
          output_request_row_(output_request_row) {}

    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    static std::shared_ptr<TableHandler> RequestUnionWindow(const Row& request,
                                                            std::vector<std::shared_ptr<TableHandler>> union_segments,
                                                            int64_t request_ts, const WindowRange& window_range,
                                                            bool output_request_row, bool exclude_current_time);
    void AddWindowUnion(const RequestWindowOp& window, Runner* runner) {
        windows_union_gen_.AddWindowUnion(window, runner);
    }
    RequestWindowUnionGenerator windows_union_gen_;
    RangeGenerator range_gen_;
    bool exclude_current_time_;
    bool output_request_row_;
};

class RequestAggUnionRunner : public Runner {
 public:
    RequestAggUnionRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                          const Range& range, bool exclude_current_time, bool output_request_row,
                          const node::CallExprNode* project)
        : Runner(id, kRunnerRequestAggUnion, schema, limit_cnt),
          range_gen_(range),
          exclude_current_time_(exclude_current_time),
          output_request_row_(output_request_row),
          func_(project->GetFnDef()),
          agg_col_(project->GetChild(0)) {
        if (agg_col_->GetExprType() == node::kExprColumnRef) {
            agg_col_name_ = dynamic_cast<const node::ColumnRefNode*>(agg_col_)->GetColumnName();
        } /* for kAllExpr like count(*), agg_col_name_ is empty */

        if (project->GetChildNum() >= 2) {
            // assume second kid of project as filter condition
            // function support check happens in compile
            cond_ = project->GetChild(1);
        }
    }

    bool InitAggregator();
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx,
                                     const std::vector<std::shared_ptr<DataHandler>>& inputs) override;
    std::shared_ptr<TableHandler> RequestUnionWindow(const Row& request,
                                                     std::vector<std::shared_ptr<TableHandler>> union_segments,
                                                     int64_t request_ts, const WindowRange& window_range,
                                                     const bool output_request_row,
                                                     const bool exclude_current_time) const;
    void AddWindowUnion(const RequestWindowOp& window, Runner* runner) {
        windows_union_gen_.AddWindowUnion(window, runner);
    }

    static std::string PrintEvalValue(const absl::StatusOr<std::optional<bool>>& val);

 private:
    enum AggType {
        kSum,
        kCount,
        kAvg,
        kMin,
        kMax,
        kCountWhere,
        kSumWhere,
        kAvgWhere,
        kMinWhere,
        kMaxWhere,
    };

    RequestWindowUnionGenerator windows_union_gen_;
    RangeGenerator range_gen_;
    bool exclude_current_time_;

    // include request row from union.
    // turn to false if `EXCLUDE CURRENT_ROW` from window definition
    bool output_request_row_;

    const node::FnDefNode* func_ = nullptr;
    AggType agg_type_;
    const node::ExprNode* agg_col_ = nullptr;
    std::string agg_col_name_;
    type::Type agg_col_type_;

    // the filter condition for count_where
    // simple compassion binary expr like col < 0 is supported
    node::ExprNode* cond_ = nullptr;

    std::unique_ptr<BaseAggregator> CreateAggregator() const;

    static inline const absl::flat_hash_map<absl::string_view, AggType> agg_type_map_ = {
        {"sum", kSum},
        {"count", kCount},
        {"avg", kAvg},
        {"min", kMin},
        {"max", kMax},
        {"count_where", kCountWhere},
        {"sum_where", kSumWhere},
        {"avg_where", kAvgWhere},
        {"min_where", kMinWhere},
        {"max_where", kMaxWhere}};
};

class PostRequestUnionRunner : public Runner {
 public:
    PostRequestUnionRunner(const int32_t id, const SchemasContext* schema,
                           const Range& request_ts)
        : Runner(id, kRunnerPostRequestUnion, schema),
          request_ts_gen_(request_ts.fn_info()) {}

    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
 private:
    OrderGenerator request_ts_gen_;
};

class LastJoinRunner : public Runner {
 public:
    LastJoinRunner(const int32_t id, const SchemasContext* schema,
                   const std::optional<int32_t> limit_cnt, const Join& join,
                   size_t left_slices, size_t right_slices)
        : Runner(id, kRunnerLastJoin, schema, limit_cnt),
          join_gen_(join, left_slices, right_slices) {}
    ~LastJoinRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT

    JoinGenerator join_gen_;
};
class RequestLastJoinRunner : public Runner {
 public:
    RequestLastJoinRunner(const int32_t id, const SchemasContext* schema,
                          const std::optional<int32_t> limit_cnt, const Join& join,
                          const size_t left_slices, const size_t right_slices,
                          const bool output_right_only)
        : Runner(id, kRunnerRequestLastJoin, schema, limit_cnt),
          join_gen_(join, left_slices, right_slices),
          output_right_only_(output_right_only) {}
    ~RequestLastJoinRunner() {}

    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,                                        // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs);  // NOLINT
    virtual void PrintRunnerInfo(std::ostream& output,
                                 const std::string& tab) const {
        output << tab << "[" << id_ << "]" << RunnerTypeName(type_);
        if (is_lazy_) {
            output << " lazy";
        }
        if (output_right_only_) {
            output << " OUTPUT_RIGHT_ONLY";
        }
    }
    JoinGenerator join_gen_;
    const bool output_right_only_;
};
class ConcatRunner : public Runner {
 public:
    ConcatRunner(const int32_t id, const SchemasContext* schema,
                 const std::optional<int32_t> limit_cnt)
        : Runner(id, kRunnerConcat, schema, limit_cnt) {
        is_lazy_ = true;
    }
    ~ConcatRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
};
class LimitRunner : public Runner {
 public:
    LimitRunner(int32_t id, const SchemasContext* schema, int32_t limit_cnt)
        : Runner(id, kRunnerLimit, schema, limit_cnt) {}
    ~LimitRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,                                        // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs);  // NOLINT
};

class ProxyRequestRunner : public Runner {
 public:
    ProxyRequestRunner(int32_t id, uint32_t task_id,
                       const SchemasContext* schema_ctx)
        : Runner(id, kRunnerRequestRunProxy, schema_ctx),
          task_id_(task_id),
          index_input_(nullptr) {
        is_lazy_ = true;
    }
    ProxyRequestRunner(int32_t id, uint32_t task_id, Runner* index_input,
                       const SchemasContext* schema_ctx)
        : Runner(id, kRunnerRequestRunProxy, schema_ctx),
          task_id_(task_id),
          index_input_(index_input) {
        is_lazy_ = true;
    }
    ~ProxyRequestRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs) override;
    std::shared_ptr<DataHandlerList> BatchRequestRun(
        RunnerContext& ctx) override;  // NOLINT
    virtual void PrintRunnerInfo(std::ostream& output,
                                 const std::string& tab) const {
        output << tab << "[" << id_ << "]" << RunnerTypeName(type_)
               << "(TASK_ID=" << task_id_ << ")";
        if (is_lazy_) {
            output << " lazy";
        }
    }
    virtual void Print(std::ostream& output, const std::string& tab,
                       std::set<int32_t>* visited_ids) const {  // NOLINT
        PrintRunnerInfo(output, tab);
        PrintCacheInfo(output);
        if (nullptr != index_input_) {
            output << "\n    " << tab << "proxy_index_input:\n";
            index_input_->Print(output, "    " + tab + "+-", nullptr);
        }
        if (nullptr != visited_ids &&
            visited_ids->find(id_) != visited_ids->cend()) {
            output << "\n";
            output << "  " << tab << "...";
            return;
        }
        if (nullptr != visited_ids) {
            visited_ids->insert(id_);
        }
        if (!producers_.empty()) {
            for (auto producer : producers_) {
                output << "\n";
                producer->Print(output, "  " + tab, visited_ids);
            }
        }
    }

    const int32_t task_id() const { return task_id_; }

 private:
    std::shared_ptr<DataHandlerList> RunBatchInput(
        RunnerContext& ctx,  // NOLINT
        std::shared_ptr<DataHandlerList> input,
        std::shared_ptr<DataHandlerList> index_input);
    std::shared_ptr<DataHandler> RunWithRowInput(RunnerContext& ctx,  // NOLINT
                                                 const Row& row,
                                                 const Row& index_row);
    std::shared_ptr<TableHandler> RunWithRowsInput(
        RunnerContext& ctx,  // NOLINT
        const std::vector<Row>& rows, const std::vector<Row>& index_rows,
        const bool request_is_common);
    uint32_t task_id_;
    Runner* index_input_;
};
class ClusterTask;
class RouteInfo {
 public:
    RouteInfo()
        : index_(),
          index_key_(),
          index_key_input_runner_(nullptr),
          input_(),
          table_handler_() {}
    RouteInfo(const std::string index,
              std::shared_ptr<TableHandler> table_handler)
        : index_(index),
          index_key_(),
          index_key_input_runner_(nullptr),
          input_(),
          table_handler_(table_handler) {}
    RouteInfo(const std::string index, const Key& index_key,
              std::shared_ptr<ClusterTask> input,
              std::shared_ptr<TableHandler> table_handler)
        : index_(index),
          index_key_(index_key),
          index_key_input_runner_(nullptr),
          input_(input),
          table_handler_(table_handler) {}
    ~RouteInfo() {}
    const bool IsCompleted() const {
        return table_handler_ && !index_.empty() && index_key_.ValidKey();
    }
    const bool IsCluster() const { return table_handler_ && !index_.empty(); }
    static const bool EqualWith(const RouteInfo& info1,
                                const RouteInfo& info2) {
        return info1.input_ == info2.input_ &&
               info1.table_handler_ == info2.table_handler_ &&
               info1.index_ == info2.index_ &&
               node::ExprEquals(info1.index_key_.keys_, info2.index_key_.keys_);
    }

    const std::string ToString() const {
        if (IsCompleted()) {
            std::ostringstream oss;
            oss << ", routing index = " << table_handler_->GetDatabase() << "."
                << table_handler_->GetName() << "." << index_ << ", "
                << index_key_.ToString();
            return oss.str();
        } else {
            return "";
        }
    }
    std::string index_;
    Key index_key_;
    Runner* index_key_input_runner_;
    std::shared_ptr<ClusterTask> input_;
    std::shared_ptr<TableHandler> table_handler_;
};

// task info of cluster job
// partitoin/index info
// index key generator
// request generator
class ClusterTask {
 public:
    ClusterTask() : root_(nullptr), input_runners_(), route_info_() {}
    explicit ClusterTask(Runner* root)
        : root_(root), input_runners_(), route_info_() {}
    ClusterTask(Runner* root, const std::shared_ptr<TableHandler> table_handler,
                std::string index)
        : root_(root), input_runners_(), route_info_(index, table_handler) {}
    ClusterTask(Runner* root, const std::vector<Runner*>& input_runners,
                const RouteInfo& route_info)
        : root_(root), input_runners_(input_runners), route_info_(route_info) {}
    ~ClusterTask() {}
    void Print(std::ostream& output, const std::string& tab) const {
        output << route_info_.ToString() << "\n";
        if (nullptr == root_) {
            output << tab << "NULL RUNNER\n";
        } else {
            std::set<int32_t> visited_ids;
            root_->Print(output, tab, &visited_ids);
        }
    }

    void ResetInputs(std::shared_ptr<ClusterTask> input) {
        for (auto input_runner : input_runners_) {
            input_runner->SetProducer(0, route_info_.input_->GetRoot());
        }
        route_info_.index_key_input_runner_ = route_info_.input_->GetRoot();
        route_info_.input_ = input;
    }
    Runner* GetRoot() const { return root_; }
    void SetRoot(Runner* root) { root_ = root; }
    Runner* GetInputRunner(size_t idx) const {
        return idx >= input_runners_.size() ? nullptr : input_runners_[idx];
    }
    Runner* GetIndexKeyInput() const {
        return route_info_.index_key_input_runner_;
    }
    std::shared_ptr<ClusterTask> GetInput() const { return route_info_.input_; }
    Key GetIndexKey() const { return route_info_.index_key_; }
    void SetIndexKey(const Key& key) { route_info_.index_key_ = key; }
    void SetInput(std::shared_ptr<ClusterTask> input) {
        route_info_.input_ = input;
    }

    const bool IsValid() const { return nullptr != root_; }

    const bool IsCompletedClusterTask() const {
        return IsValid() && route_info_.IsCompleted();
    }
    const bool IsUnCompletedClusterTask() const {
        return IsClusterTask() && !route_info_.IsCompleted();
    }
    const bool IsClusterTask() const { return route_info_.IsCluster(); }
    const std::string& index() { return route_info_.index_; }
    std::shared_ptr<TableHandler> table_handler() {
        return route_info_.table_handler_;
    }

    // Cluster tasks with same input runners and index keys can be merged
    static const bool TaskCanBeMerge(const ClusterTask& task1,
                                     const ClusterTask& task2) {
        return RouteInfo::EqualWith(task1.route_info_, task2.route_info_);
    }
    static const ClusterTask TaskMerge(Runner* root, const ClusterTask& task1,
                                       const ClusterTask& task2) {
        return TaskMergeToLeft(root, task1, task2);
    }
    static const ClusterTask TaskMergeToLeft(Runner* root,
                                             const ClusterTask& task1,
                                             const ClusterTask& task2) {
        std::vector<Runner*> input_runners;
        for (auto runner : task1.input_runners_) {
            input_runners.push_back(runner);
        }
        for (auto runner : task2.input_runners_) {
            input_runners.push_back(runner);
        }
        return ClusterTask(root, input_runners, task1.route_info_);
    }
    static const ClusterTask TaskMergeToRight(Runner* root,
                                              const ClusterTask& task1,
                                              const ClusterTask& task2) {
        std::vector<Runner*> input_runners;
        for (auto runner : task1.input_runners_) {
            input_runners.push_back(runner);
        }
        for (auto runner : task2.input_runners_) {
            input_runners.push_back(runner);
        }
        return ClusterTask(root, input_runners, task2.route_info_);
    }

    static const Runner* GetRequestInput(const ClusterTask& task) {
        if (!task.IsValid()) {
            return nullptr;
        }
        auto input_task = task.GetInput();
        if (input_task) {
            return input_task->GetRoot();
        }
        return nullptr;
    }

    const RouteInfo& GetRouteInfo() const { return route_info_; }

 protected:
    Runner* root_;
    std::vector<Runner*> input_runners_;
    RouteInfo route_info_;
};

class ClusterJob {
 public:
    ClusterJob()
        : tasks_(), main_task_id_(-1), sql_(""), common_column_indices_() {}
    explicit ClusterJob(const std::string& sql, const std::string& db,
                        const std::set<size_t>& common_column_indices)
        : tasks_(),
          main_task_id_(-1),
          sql_(sql),
          db_(db),
          common_column_indices_(common_column_indices) {}
    ClusterTask GetTask(int32_t id) {
        if (id < 0 || id >= static_cast<int32_t>(tasks_.size())) {
            LOG(WARNING) << "fail get task: task " << id << " not exist";
            return ClusterTask();
        }
        return tasks_[id];
    }

    ClusterTask GetMainTask() { return GetTask(main_task_id_); }
    int32_t AddTask(const ClusterTask& task) {
        if (!task.IsValid()) {
            LOG(WARNING) << "fail to add invalid task";
            return -1;
        }
        tasks_.push_back(task);
        return tasks_.size() - 1;
    }
    bool AddRunnerToTask(Runner* runner, const int32_t id) {
        if (id < 0 || id >= static_cast<int32_t>(tasks_.size())) {
            LOG(WARNING) << "fail update task: task " << id << " not exist";
            return false;
        }
        runner->AddProducer(tasks_[id].GetRoot());
        tasks_[id].SetRoot(runner);
        return true;
    }

    void AddMainTask(const ClusterTask& task) { main_task_id_ = AddTask(task); }
    void Reset() { tasks_.clear(); }
    const size_t GetTaskSize() const { return tasks_.size(); }
    const bool IsValid() const { return !tasks_.empty(); }
    const int32_t main_task_id() const { return main_task_id_; }
    const std::string& sql() const { return sql_; }
    const std::string& db() const { return db_; }
    void Print(std::ostream& output, const std::string& tab) const {
        if (tasks_.empty()) {
            output << "EMPTY CLUSTER JOB\n";
            return;
        }
        for (size_t i = 0; i < tasks_.size(); i++) {
            if (main_task_id_ == static_cast<int32_t>(i)) {
                output << "MAIN TASK ID " << i;
            } else {
                output << "TASK ID " << i;
            }
            tasks_[i].Print(output, tab);
            output << "\n";
        }
    }
    const std::set<size_t>& common_column_indices() const {
        return common_column_indices_;
    }
    void Print() const { this->Print(std::cout, "    "); }

 private:
    std::vector<ClusterTask> tasks_;
    int32_t main_task_id_;
    std::string sql_;
    std::string db_;
    std::set<size_t> common_column_indices_;
};
class RunnerBuilder {
    enum TaskBiasType { kLeftBias, kRightBias, kNoBias };

 public:
    explicit RunnerBuilder(node::NodeManager* nm, const std::string& sql,
                           const std::string& db,
                           bool support_cluster_optimized,
                           const std::set<size_t>& common_column_indices,
                           const std::set<size_t>& batch_common_node_set)
        : nm_(nm),
          support_cluster_optimized_(support_cluster_optimized),
          id_(0),
          cluster_job_(sql, db, common_column_indices),
          task_map_(),
          proxy_runner_map_(),
          batch_common_node_set_(batch_common_node_set) {}
    virtual ~RunnerBuilder() {}
    ClusterTask RegisterTask(PhysicalOpNode* node, ClusterTask task) {
        task_map_[node] = task;
        if (batch_common_node_set_.find(node->node_id()) !=
            batch_common_node_set_.end()) {
            task.GetRoot()->EnableBatchCache();
        }
        return task;
    }
    ClusterTask Build(PhysicalOpNode* node,  // NOLINT
                      Status& status);       // NOLINT
    ClusterJob BuildClusterJob(PhysicalOpNode* node,
                               Status& status) {  // NOLINT
        id_ = 0;
        cluster_job_.Reset();
        auto task =  // NOLINT whitespace/braces
            Build(node, status);
        if (!status.isOK()) {
            return cluster_job_;
        }

        if (task.IsCompletedClusterTask()) {
            auto proxy_task = BuildProxyRunnerForClusterTask(task);
            if (!proxy_task.IsValid()) {
                status.code = common::kExecutionPlanError;
                status.msg = "Fail to build proxy cluster task";
                LOG(WARNING) << status;
                return cluster_job_;
            }
            cluster_job_.AddMainTask(proxy_task);
        } else if (task.IsUnCompletedClusterTask()) {
            status.code = common::kExecutionPlanError;
            status.msg =
                "Fail to build main task, can't handler "
                "uncompleted cluster task";
            LOG(WARNING) << status;
            return cluster_job_;
        } else {
            cluster_job_.AddMainTask(task);
        }
        return cluster_job_;
    }

    template <typename Op, typename... Args>
    Op* CreateRunner(Args&&... args) {
        return nm_->MakeNode<Op>(std::forward<Args>(args)...);
    }

 private:
    node::NodeManager* nm_;
    bool support_cluster_optimized_;
    int32_t id_;
    ClusterJob cluster_job_;

    std::unordered_map<::hybridse::vm::PhysicalOpNode*,
                       ::hybridse::vm::ClusterTask>
        task_map_;
    std::shared_ptr<ClusterTask> request_task_;
    std::unordered_map<hybridse::vm::Runner*, ::hybridse::vm::Runner*>
        proxy_runner_map_;
    std::set<size_t> batch_common_node_set_;
    ClusterTask MultipleInherit(const std::vector<const ClusterTask*>& children, Runner* runner,
                                                const Key& index_key, const TaskBiasType bias);
    ClusterTask BinaryInherit(const ClusterTask& left, const ClusterTask& right,
                              Runner* runner, const Key& index_key,
                              const TaskBiasType bias = kNoBias);
    ClusterTask BuildLocalTaskForBinaryRunner(const ClusterTask& left,
                                              const ClusterTask& right,
                                              Runner* runner);
    ClusterTask BuildClusterTaskForBinaryRunner(const ClusterTask& left,
                                                const ClusterTask& right,
                                                Runner* runner,
                                                const Key& index_key,
                                                const TaskBiasType bias);
    ClusterTask BuildProxyRunnerForClusterTask(const ClusterTask& task);
    ClusterTask InvalidTask() { return ClusterTask(); }
    ClusterTask CommonTask(Runner* runner) { return ClusterTask(runner); }
    ClusterTask UnCompletedClusterTask(
        Runner* runner, const std::shared_ptr<TableHandler> table_handler,
        std::string index);
    ClusterTask BuildRequestTask(RequestRunner* runner);
    ClusterTask UnaryInheritTask(const ClusterTask& input, Runner* runner);
    ClusterTask BuildRequestAggUnionTask(PhysicalOpNode* node, Status& status);  // NOLINT
};

class RunnerContext {
 public:
    explicit RunnerContext(hybridse::vm::ClusterJob* cluster_job,
                           const hybridse::codec::Row& parameter,
                           const bool is_debug = false)
        : cluster_job_(cluster_job),
          sp_name_(""),
          request_(),
          requests_(),
          parameter_(parameter),
          is_debug_(is_debug),
          batch_cache_() {}
    explicit RunnerContext(hybridse::vm::ClusterJob* cluster_job,
                           const hybridse::codec::Row& request,
                           const std::string& sp_name = "",
                           const bool is_debug = false)
        : cluster_job_(cluster_job),
          sp_name_(sp_name),
          request_(request),
          requests_(),
          parameter_(),
          is_debug_(is_debug),
          batch_cache_() {}
    explicit RunnerContext(hybridse::vm::ClusterJob* cluster_job,
                           const std::vector<Row>& request_batch,
                           const std::string& sp_name = "",
                           const bool is_debug = false)
        : cluster_job_(cluster_job),
          sp_name_(sp_name),
          request_(),
          requests_(request_batch),
          parameter_(),
          is_debug_(is_debug),
          batch_cache_() {}

    const size_t GetRequestSize() const { return requests_.size(); }
    const hybridse::codec::Row& GetRequest() const { return request_; }
    const hybridse::codec::Row& GetRequest(size_t idx) const {
        return requests_[idx];
    }
    const hybridse::codec::Row& GetParameterRow() const { return parameter_; }
    hybridse::vm::ClusterJob* cluster_job() { return cluster_job_; }
    void SetRequest(const hybridse::codec::Row& request);
    void SetRequests(const std::vector<hybridse::codec::Row>& requests);
    bool is_debug() const { return is_debug_; }

    const std::string& sp_name() { return sp_name_; }
    std::shared_ptr<DataHandler> GetCache(int64_t id) const;
    void SetCache(int64_t id, std::shared_ptr<DataHandler> data);
    void ClearCache() { cache_.clear(); }
    std::shared_ptr<DataHandlerList> GetBatchCache(int64_t id) const;
    void SetBatchCache(int64_t id, std::shared_ptr<DataHandlerList> data);

 private:
    hybridse::vm::ClusterJob* cluster_job_;
    const std::string sp_name_;
    hybridse::codec::Row request_;
    std::vector<hybridse::codec::Row> requests_;
    hybridse::codec::Row parameter_;
    size_t idx_;
    const bool is_debug_;
    // TODO(chenjing): optimize
    std::map<int64_t, std::shared_ptr<DataHandler>> cache_;
    std::map<int64_t, std::shared_ptr<DataHandlerList>> batch_cache_;
};
}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_VM_RUNNER_H_
