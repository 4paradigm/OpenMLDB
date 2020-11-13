/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * runner.h
 *
 * Author: chenjing
 * Date: 2020/4/3
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_RUNNER_H_
#define SRC_VM_RUNNER_H_

#include <map>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "node/node_manager.h"
#include "vm/catalog.h"
#include "vm/catalog_wrapper.h"
#include "vm/core_api.h"
#include "vm/mem_catalog.h"
#include "vm/physical_op.h"
namespace fesql {
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
class FnGenerator {
 public:
    explicit FnGenerator(const FnInfo& info)
        : fn_(info.fn_), fn_schema_(info.fn_schema_), row_view_(fn_schema_) {
        std::vector<int32_t> idxs;
        for (int32_t idx = 0; idx < info.fn_schema_.size(); idx++) {
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
    explicit RowProjectFun(int8_t* fn) : ProjectFun(), fn_(fn) {}
    ~RowProjectFun() {}
    Row operator()(const Row& row) const override {
        return CoreAPI::RowProject(fn_, row, false);
    }
    int8_t* fn_;
};

class ProjectGenerator : public FnGenerator {
 public:
    explicit ProjectGenerator(const FnInfo& info)
        : FnGenerator(info), fun_(info.fn_) {}
    virtual ~ProjectGenerator() {}
    const Row Gen(const Row& row);
    RowProjectFun fun_;
};

class ConstProjectGenerator : public FnGenerator {
 public:
    explicit ConstProjectGenerator(const FnInfo& info)
        : FnGenerator(info), fun_(info.fn_) {}
    virtual ~ConstProjectGenerator() {}
    const Row Gen();
    RowProjectFun fun_;
};
class AggGenerator : public FnGenerator {
 public:
    explicit AggGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~AggGenerator() {}
    const Row Gen(std::shared_ptr<TableHandler> table);
};
class WindowProjectGenerator : public FnGenerator {
 public:
    explicit WindowProjectGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~WindowProjectGenerator() {}
    const Row Gen(const uint64_t key, const Row row, const bool is_instance,
                  size_t append_slices, Window* window);
};
class KeyGenerator : public FnGenerator {
 public:
    explicit KeyGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~KeyGenerator() {}
    const std::string Gen(const Row& row);
    const std::string GenConst();
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
    const bool Gen(const Row& row) const;
};
class RangeGenerator {
 public:
    explicit RangeGenerator(const Range& range) : ts_gen_(range.fn_info_) {
        if (range.frame_ != nullptr) {
            start_offset_ = range.frame_->GetHistoryRangeStart();
            end_offset_ = range.frame_->GetHistoryRangeEnd();
            start_row_ = (-1 * range.frame_->GetHistoryRowsStart());
            end_row_ = (-1 * range.frame_->GetHistoryRowsEnd());
        }
    }
    virtual ~RangeGenerator() {}
    const bool Valid() const { return ts_gen_.Valid(); }
    OrderGenerator ts_gen_;
    int64_t start_offset_;
    int64_t end_offset_;
    uint64_t start_row_;
    uint64_t end_row_;
};
class FilterKeyGenerator {
 public:
    explicit FilterKeyGenerator(const Key& filter_key)
        : filter_key_(filter_key.fn_info_) {}
    virtual ~FilterKeyGenerator() {}
    const bool Valid() const { return filter_key_.Valid(); }
    std::shared_ptr<TableHandler> Filter(std::shared_ptr<TableHandler> table,
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
                std::string keys = filter_key_.Gen(iter->GetValue());
                if (request_keys == keys) {
                    mem_table->AddRow(iter->GetKey(), iter->GetValue());
                }
                iter->Next();
            }
        }
        return mem_table;
    }
    const std::string GetKey(const Row& row) {
        return filter_key_.Valid() ? filter_key_.Gen(row) : "";
    }
    KeyGenerator filter_key_;
};

class PartitionGenerator {
 public:
    explicit PartitionGenerator(const Key& partition)
        : key_gen_(partition.fn_info_) {}
    virtual ~PartitionGenerator() {}

    const bool Valid() const { return key_gen_.Valid(); }
    std::shared_ptr<PartitionHandler> Partition(
        std::shared_ptr<DataHandler> input);
    std::shared_ptr<PartitionHandler> Partition(
        std::shared_ptr<PartitionHandler> table);
    std::shared_ptr<PartitionHandler> Partition(
        std::shared_ptr<TableHandler> table);
    const std::string GetKey(const Row& row) { return key_gen_.Gen(row); }

 private:
    KeyGenerator key_gen_;
};
class SortGenerator {
 public:
    explicit SortGenerator(const Sort& sort)
        : is_valid_(sort.ValidSort()),
          is_asc_(sort.is_asc()),
          order_gen_(sort.fn_info_) {}
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
        : index_key_gen_(key.fn_info_) {}
    virtual ~IndexSeekGenerator() {}
    std::shared_ptr<TableHandler> SegmnetOfConstKey(
        std::shared_ptr<DataHandler> input);
    std::shared_ptr<TableHandler> SegmentOfKey(
        const Row& row, std::shared_ptr<DataHandler> input);
    const bool Valid() const { return index_key_gen_.Valid(); }

 private:
    KeyGenerator index_key_gen_;
};

class FilterGenerator : public PredicateFun {
 public:
    explicit FilterGenerator(const Filter& filter)
        : condition_gen_(filter.condition_.fn_info_),
          index_seek_gen_(filter.index_key_) {}

    const bool Valid() const {
        return index_seek_gen_.Valid() || condition_gen_.Valid();
    }
    std::shared_ptr<TableHandler> Filter(std::shared_ptr<TableHandler> table);
    std::shared_ptr<TableHandler> Filter(
        std::shared_ptr<PartitionHandler> table);
    bool operator()(const Row& row) const override {
        if (!Valid()) {
            return true;
        }
        return condition_gen_.Gen(row);
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
          range_gen_(window.range_.fn_info_),
          index_seek_gen_(window.index_key_) {}
    virtual ~RequestWindowGenertor() {}
    std::shared_ptr<TableHandler> GetRequestWindow(
        const Row& row, std::shared_ptr<DataHandler> input) {
        auto segment = index_seek_gen_.SegmentOfKey(row, input);

        if (filter_gen_.Valid()) {
            auto filter_key = filter_gen_.GetKey(row);
            segment = filter_gen_.Filter(segment, filter_key);
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
    kRunnerGroupAgg,
    kRunnerAgg,
    kRunnerWindowAgg,
    kRunnerRequestUnion,
    kRunnerIndexSeek,
    kRunnerLastJoin,
    kRunnerConcat,
    kRunnerRequestLastJoin,
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
        case kRunnerGroupAgg:
            return "GROUP_AGG_PROJECT";
        case kRunnerAgg:
            return "AGG_PROJECT";
        case kRunnerWindowAgg:
            return "WINDOW_AGG_PROJECT";
        case kRunnerRequestUnion:
            return "REQUEST_UNION";
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
        default:
            return "UNKNOW";
    }
}
class Runner : public node::NodeBase<Runner> {
 public:
    explicit Runner(const int32_t id)
        : id_(id),
          type_(kRunnerUnknow),
          limit_cnt_(0),
          need_cache_(false),
          producers_(),
          output_schemas_() {}
    explicit Runner(const int32_t id, const RunnerType type,
                    const vm::SchemaSourceList& output_schemas)
        : id_(id),
          type_(type),
          limit_cnt_(0),
          need_cache_(false),
          producers_(),
          output_schemas_(output_schemas) {}
    Runner(const int32_t id, const RunnerType type,
           const vm::SchemaSourceList& output_schemas, const int32_t limit_cnt)
        : id_(id),
          type_(type),
          limit_cnt_(limit_cnt),
          need_cache_(false),
          producers_(),
          output_schemas_(output_schemas) {}
    virtual ~Runner() {}
    void AddProducer(Runner* runner) { producers_.push_back(runner); }
    const std::vector<Runner*>& GetProducers() const { return producers_; }
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]" << RunnerTypeName(type_);
        if (!producers_.empty()) {
            for (auto producer : producers_) {
                output << "\n";
                producer->Print(output, "  " + tab);
            }
        }
    }
    const int32_t id_;
    const RunnerType type_;
    const int32_t limit_cnt_;
    virtual std::shared_ptr<DataHandler> Run(RunnerContext& ctx) = 0;  // NOLINT
    virtual std::shared_ptr<DataHandler> RunWithCache(
        RunnerContext& ctx);  // NOLINT

    static int64_t GetColumnInt64(RowView* view, int pos, type::Type type);
    static bool GetColumnBool(RowView* view, int idx, type::Type type);
    static Row WindowProject(const int8_t* fn, const uint64_t key,
                             const Row row, const bool is_instance,
                             size_t append_slices, Window* window);
    static Row GroupbyProject(const int8_t* fn, TableHandler* table);
    static const Row RowLastJoinTable(size_t left_slices, const Row& left_row,
                                      size_t right_slices,
                                      std::shared_ptr<TableHandler> right_table,
                                      SortGenerator& right_sort,    // NOLINT
                                      ConditionGenerator& filter);  // NOLINT
    static std::shared_ptr<TableHandler> TableReverse(
        std::shared_ptr<TableHandler> table);

    static void PrintData(const vm::SchemaSourceList& schema_list,
                          std::shared_ptr<DataHandler> data);
    const vm::SchemaSourceList& output_schemas() const {
        return output_schemas_;
    }
    virtual const std::string GetTypeName() const {
        return RunnerTypeName(type_);
    }
    virtual bool Equals(const Runner* other) const { return this == other; }

 protected:
    bool need_cache_;
    std::vector<Runner*> producers_;
    const vm::SchemaSourceList output_schemas_;
};
class IteratorStatus {
 public:
    IteratorStatus() : is_valid_(false), key_(0) {}
    explicit IteratorStatus(uint64_t key) : is_valid_(true), key_(key) {}
    virtual ~IteratorStatus() {}
    static int32_t PickIteratorWithMininumKey(
        std::vector<IteratorStatus>* status_list_ptr);
    static int32_t PickIteratorWithMaximizeKey(
        std::vector<IteratorStatus>* status_list_ptr);
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
        std::vector<std::shared_ptr<DataHandler>> union_inputs);
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
        const Row& row,
        std::vector<std::shared_ptr<DataHandler>> union_inputs) {
        std::vector<std::shared_ptr<TableHandler>> union_segments(inputs_cnt_);
        if (!windows_gen_.empty()) {
            for (size_t i = 0; i < inputs_cnt_; i++) {
                union_segments[i] =
                    windows_gen_[i].GetRequestWindow(row, union_inputs[i]);
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
        : condition_gen_(join.condition_.fn_info_),
          left_key_gen_(join.left_key_.fn_info_),
          right_group_gen_(join.right_key_),
          index_key_gen_(join.index_key_.fn_info_),
          right_sort_gen_(join.right_sort_),
          left_slices_(left_slices),
          right_slices_(right_slices) {}
    virtual ~JoinGenerator() {}
    bool TableJoin(std::shared_ptr<TableHandler> left,
                   std::shared_ptr<TableHandler> right,
                   std::shared_ptr<MemTimeTableHandler> output);  // NOLINT
    bool TableJoin(std::shared_ptr<TableHandler> left,
                   std::shared_ptr<PartitionHandler> right,
                   std::shared_ptr<MemTimeTableHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left,
                       std::shared_ptr<TableHandler> right,
                       std::shared_ptr<MemPartitionHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left,
                       std::shared_ptr<PartitionHandler> right,
                       std::shared_ptr<MemPartitionHandler>);  // NOLINT

    Row RowLastJoin(const Row& left_row, std::shared_ptr<DataHandler> right);

    ConditionGenerator condition_gen_;
    KeyGenerator left_key_gen_;
    PartitionGenerator right_group_gen_;
    KeyGenerator index_key_gen_;
    SortGenerator right_sort_gen_;

 private:
    Row RowLastJoinPartition(
        const Row& left_row,
        std::shared_ptr<PartitionHandler> partition);  // NOLINT
    Row RowLastJoinTable(const Row& left_row,
                         std::shared_ptr<TableHandler> table);  // NOLINT

    size_t left_slices_;
    size_t right_slices_;
};
class WindowJoinGenerator : public InputsGenerator {
 public:
    WindowJoinGenerator() : InputsGenerator() {}
    virtual ~WindowJoinGenerator() {}
    void AddWindowJoin(const Join& join, size_t left_slices, Runner* runner) {
        size_t right_slices =
            runner->output_schemas().GetSchemaSourceListSize();
        joins_gen_.push_back(JoinGenerator(join, left_slices, right_slices));
        AddInput(runner);
    }
    std::vector<std::shared_ptr<DataHandler>> RunInputs(
        RunnerContext& ctx);  // NOLINT
    Row Join(
        const Row& left_row,
        const std::vector<std::shared_ptr<DataHandler>>& join_right_tables);
    std::vector<JoinGenerator> joins_gen_;
};

class DataRunner : public Runner {
 public:
    DataRunner(const int32_t id, const SchemaSourceList& schema,
               std::shared_ptr<DataHandler> data_hander)
        : Runner(id, kRunnerData, schema), data_handler_(data_hander) {}
    ~DataRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::shared_ptr<DataHandler> data_handler_;
};

class RequestRunner : public Runner {
 public:
    RequestRunner(const int32_t id, const SchemaSourceList& schema)
        : Runner(id, kRunnerRequest, schema) {}
    ~RequestRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
class GroupRunner : public Runner {
 public:
    GroupRunner(const int32_t id, const SchemaSourceList& schema,
                const int32_t limit_cnt, const Key& group)
        : Runner(id, kRunnerGroup, schema, limit_cnt), partition_gen_(group) {}
    ~GroupRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    PartitionGenerator partition_gen_;
};
class FilterRunner : public Runner {
 public:
    FilterRunner(const int32_t id, const SchemaSourceList& schema,
                 const int32_t limit_cnt, const Filter& filter)
        : Runner(id, kRunnerFilter, schema, limit_cnt), filter_gen_(filter) {}
    ~FilterRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    FilterGenerator filter_gen_;
};

class ProxyRequestRunner : public Runner {
 public:
     std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;
};

class SortRunner : public Runner {
 public:
    SortRunner(const int32_t id, const SchemaSourceList& schema,
               const int32_t limit_cnt, const Sort& sort)
        : Runner(id, kRunnerOrder, schema, limit_cnt), sort_gen_(sort) {}
    ~SortRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    SortGenerator sort_gen_;
};
class ConstProjectRunner : public Runner {
 public:
    ConstProjectRunner(const int32_t id, const SchemaSourceList& schema,
                       const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerConstProject, schema, limit_cnt),
          project_gen_(fn_info) {}
    ~ConstProjectRunner() {}

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ConstProjectGenerator project_gen_;
};
class TableProjectRunner : public Runner {
 public:
    TableProjectRunner(const int32_t id, const SchemaSourceList& schema,
                       const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerTableProject, schema, limit_cnt),
          project_gen_(fn_info) {}
    ~TableProjectRunner() {}

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ProjectGenerator project_gen_;
};
class RowProjectRunner : public Runner {
 public:
    RowProjectRunner(const int32_t id, const SchemaSourceList& schema,
                     const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerRowProject, schema, limit_cnt),
          project_gen_(fn_info) {}
    ~RowProjectRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ProjectGenerator project_gen_;
};

class SimpleProjectRunner : public Runner {
 public:
    SimpleProjectRunner(const int32_t id, const SchemaSourceList& schema,
                        const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerSimpleProject, schema, limit_cnt),
          project_gen_(fn_info) {}
    ~SimpleProjectRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ProjectGenerator project_gen_;
};
class GroupAggRunner : public Runner {
 public:
    GroupAggRunner(const int32_t id, const SchemaSourceList& schema,
                   const int32_t limit_cnt, const Key& group,
                   const FnInfo& project)
        : Runner(id, kRunnerGroupAgg, schema, limit_cnt),
          group_(group.fn_info_),
          agg_gen_(project) {}
    ~GroupAggRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator group_;
    AggGenerator agg_gen_;
};
class AggRunner : public Runner {
 public:
    AggRunner(const int32_t id, const SchemaSourceList& schema,
              const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerAgg, schema, limit_cnt), agg_gen_(fn_info) {}
    ~AggRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    AggGenerator agg_gen_;
};
class WindowAggRunner : public Runner {
 public:
    WindowAggRunner(const int32_t id, const SchemaSourceList& schema,
                    const int32_t limit_cnt, const WindowOp& window_op,
                    const FnInfo& fn_info, const bool instance_not_in_window,
                    const bool need_append_input)
        : Runner(id, kRunnerWindowAgg, schema, limit_cnt),
          instance_not_in_window_(instance_not_in_window),
          need_append_input_(need_append_input),
          append_slices_(need_append_input ? schema.GetSchemaSourceListSize()
                                           : 0),
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
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    void RunWindowAggOnKey(
        std::shared_ptr<PartitionHandler> instance_partition,
        std::vector<std::shared_ptr<PartitionHandler>> union_partitions,
        std::vector<std::shared_ptr<DataHandler>> joins, const std::string& key,
        std::shared_ptr<MemTableHandler> output_table);

    const bool instance_not_in_window_;
    const bool need_append_input_;
    const size_t append_slices_;
    WindowGenerator instance_window_gen_;
    WindowUnionGenerator windows_union_gen_;
    WindowJoinGenerator windows_join_gen_;
    WindowProjectGenerator window_project_gen_;
};
class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const SchemaSourceList& schema,
                       const int32_t limit_cnt, const Range& range)
        : Runner(id, kRunnerRequestUnion, schema, limit_cnt),
          range_gen_(range) {}

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    void AddWindowUnion(const RequestWindowOp& window, Runner* runner) {
        windows_union_gen_.AddWindowUnion(window, runner);
    }
    RequestWindowUnionGenerator windows_union_gen_;
    RangeGenerator range_gen_;
};
class LastJoinRunner : public Runner {
 public:
    LastJoinRunner(const int32_t id, const SchemaSourceList& schema,
                   const int32_t limit_cnt, const Join& join,
                   size_t left_slices, size_t right_slices)
        : Runner(id, kRunnerLastJoin, schema, limit_cnt),
          join_gen_(join, left_slices, right_slices) {}
    ~LastJoinRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT

    JoinGenerator join_gen_;
};
class RequestLastJoinRunner : public Runner {
 public:
    RequestLastJoinRunner(const int32_t id, const SchemaSourceList& schema,
                          const int32_t limit_cnt, const Join& join,
                          size_t left_slices, size_t right_slices)
        : Runner(id, kRunnerRequestLastJoin, schema, limit_cnt),
          join_gen_(join, left_slices, right_slices) {}
    ~RequestLastJoinRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT

    JoinGenerator join_gen_;
};
class ConcatRunner : public Runner {
 public:
    ConcatRunner(const int32_t id, const SchemaSourceList& schema,
                 const int32_t limit_cnt)
        : Runner(id, kRunnerConcat, schema, limit_cnt) {}
    ~ConcatRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
class LimitRunner : public Runner {
 public:
    LimitRunner(int32_t id, const SchemaSourceList& schema, int32_t limit_cnt)
        : Runner(id, kRunnerLimit, schema, limit_cnt) {}
    ~LimitRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
class RunnerBuilder {
 public:
    explicit RunnerBuilder(node::NodeManager* nm) : id_(0), nm_(nm) {}
    virtual ~RunnerBuilder() {}
    Runner* Build(PhysicalOpNode* node,  // NOLINT
                  Status& status);       // NOLINT
 private:
    int32_t id_;
    node::NodeManager* nm_;
};
class ClusterJob {
 public:
    ClusterJob() : tasks_() {}
    Runner* GetTask(uint32_t id) {
        return id >= tasks_.size() ? nullptr : tasks_[id];
    }
    bool AddTask(Runner* task) {
        if (nullptr == task) {
            return false;
        }
        tasks_.push_back(task);
        return true;
    }
    void Reset() { tasks_.clear(); }
    const size_t GetTaskSize() const { return tasks_.size(); }
    const bool IsValid() const { return !tasks_.empty(); }
    void Print(std::ostream& output, const std::string& tab) const {
        if (tasks_.empty()) {
            return;
        }
        uint32_t id = 0;
        for (auto iter = tasks_.cbegin(); iter != tasks_.cend(); iter++) {
            output << "TASK ID " << id++;
            (*iter)->Print(output, tab);
            output << "\n";
        }
    }
    void Print() const { this->Print(std::cout, "    "); }

 private:
    std::vector<Runner*> tasks_;
};
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_RUNNER_H_
