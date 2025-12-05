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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "vm/aggregator.h"
#include "vm/catalog.h"
#include "vm/core_api.h"
#include "vm/generator.h"
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
    kRunnerJoin,
    kRunnerConcat,
    kRunnerRequestRunProxy,
    kRunnerRequestJoin,
    kRunnerBatchRequestRunProxy,
    kRunnerLimit,
    kRunnerSetOperation,
    kRunnerUnknow,
};

std::string RunnerTypeName(RunnerType type);

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
          agg_gen_(AggGenerator::Create(project)) {}
    ~GroupAggRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    KeyGenerator group_;
    ConditionGenerator having_condition_;
    std::shared_ptr<AggGenerator> agg_gen_;
};
class AggRunner : public Runner {
 public:
    AggRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
              const ConditionFilter& having_condition, const FnInfo& fn_info)
        : Runner(id, kRunnerAgg, schema, limit_cnt),
          having_condition_(having_condition.fn_info()),
          agg_gen_(AggGenerator::Create(fn_info)) {}
    ~AggRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT
    ConditionGenerator having_condition_;
    std::shared_ptr<AggGenerator> agg_gen_;
};

class ReduceRunner : public Runner {
 public:
    ReduceRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                 const ConditionFilter& having_condition, const FnInfo& fn_info)
        : Runner(id, kRunnerReduce, schema, limit_cnt),
          having_condition_(having_condition.fn_info()),
          agg_gen_(AggGenerator::Create(fn_info)) {}
    ~ReduceRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx,
                                     const std::vector<std::shared_ptr<DataHandler>>& inputs) override;
    ConditionGenerator having_condition_;
    std::shared_ptr<AggGenerator> agg_gen_;
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

    bool without_order_by() const { return !instance_window_gen_.sort_gen_.Valid(); }

    // slice size outputed of the first producer node
    const size_t append_slices_;
    WindowGenerator instance_window_gen_;
    WindowUnionGenerator windows_union_gen_;
    WindowJoinGenerator windows_join_gen_;
    WindowProjectGenerator window_project_gen_;
};

class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                       const Range& range, bool exclude_current_time, bool output_request_row)
        : Runner(id, kRunnerRequestUnion, schema, limit_cnt),
          range_gen_(RangeGenerator::Create(range)),
          exclude_current_time_(exclude_current_time),
          output_request_row_(output_request_row) {
        windows_union_gen_ = RequestWindowUnionGenerator::Create();
    }

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx,  // NOLINT
                                     const std::vector<std::shared_ptr<DataHandler>>& inputs) override;

    std::shared_ptr<TableHandler> RunOneRequest(RunnerContext* ctx, const Row& request);

    static std::shared_ptr<TableHandler> RequestUnionWindow(const Row& request,
                                                            std::vector<std::shared_ptr<TableHandler>> union_segments,
                                                            int64_t request_ts, const WindowRange& window_range,
                                                            bool output_request_row, bool exclude_current_time);
    void AddWindowUnion(const RequestWindowOp& window, Runner* runner) {
        windows_union_gen_->AddWindowUnion(window, runner);
    }

    void Print(std::ostream& output, const std::string& tab,
                       std::set<int32_t>* visited_ids) const override {
        Runner::Print(output, tab, visited_ids);
        output << "\n" << tab << "window unions:\n";
        for (auto& r : windows_union_gen_->input_runners_) {
            r->Print(output, tab + "  ", visited_ids);
        }
    }

    std::shared_ptr<RequestWindowUnionGenerator> windows_union_gen_;
    std::shared_ptr<RangeGenerator> range_gen_;
    bool exclude_current_time_;
    bool output_request_row_;
};

class RequestAggUnionRunner : public Runner {
 public:
    RequestAggUnionRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                          const Range& range, bool exclude_current_time, bool output_request_row,
                          const node::CallExprNode* project)
        : Runner(id, kRunnerRequestAggUnion, schema, limit_cnt),
          range_gen_(RangeGenerator::Create(range)),
          exclude_current_time_(exclude_current_time),
          output_request_row_(output_request_row),
          func_(project->GetFnDef()),
          agg_col_(project->GetChild(0)) {
        windows_union_gen_ = RequestWindowUnionGenerator::Create();
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
        windows_union_gen_->AddWindowUnion(window, runner);
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

    std::shared_ptr<RequestWindowUnionGenerator> windows_union_gen_;
    std::shared_ptr<RangeGenerator> range_gen_;
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

class JoinRunner : public Runner {
 public:
    JoinRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt, const Join& join,
               size_t left_slices, size_t right_slices)
        : Runner(id, kRunnerJoin, schema, limit_cnt) {
        join_gen_ = JoinGenerator::Create(join, left_slices, right_slices);
    }
    ~JoinRunner() {}
    std::shared_ptr<DataHandler> Run(
        RunnerContext& ctx,  // NOLINT
        const std::vector<std::shared_ptr<DataHandler>>& inputs)
        override;  // NOLINT

    std::shared_ptr<JoinGenerator> join_gen_;
};
class RequestJoinRunner : public Runner {
 public:
    RequestJoinRunner(const int32_t id, const SchemasContext* schema, const std::optional<int32_t> limit_cnt,
                      const Join& join, const size_t left_slices, const size_t right_slices,
                      const bool output_right_only)
        : Runner(id, kRunnerRequestJoin, schema, limit_cnt), output_right_only_(output_right_only) {
        join_gen_ = JoinGenerator::Create(join, left_slices, right_slices);
    }
    ~RequestJoinRunner() {}

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
    std::shared_ptr<JoinGenerator> join_gen_;
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
    void PrintRunnerInfo(std::ostream& output, const std::string& tab) const override {
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

class SetOperationRunner : public Runner {
 public:
    SetOperationRunner(const int32_t id, const SchemasContext* schema, node::SetOperationType type, bool distinct)
        : Runner(id, kRunnerSetOperation, schema), op_type_(type), distinct_(distinct) {
        is_lazy_ = true;
    }
    ~SetOperationRunner() {}

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx,                                                 // NOLINT
                                     const std::vector<std::shared_ptr<DataHandler>>& inputs) override;  // NOLINT

 private:
    node::SetOperationType op_type_;
    bool distinct_ = false;
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_VM_RUNNER_H_
