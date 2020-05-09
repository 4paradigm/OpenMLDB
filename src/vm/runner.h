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
#include "base/status.h"
#include "codec/row_codec.h"
#include "vm/catalog.h"
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
    RowView row_view_;
    std::vector<int32_t> idxs_;
};

class ProjectGenerator : public FnGenerator {
 public:
    explicit ProjectGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~ProjectGenerator() {}
    const Row Gen(const Row& row);
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
                  Window* window);
};

class KeyGenerator : public FnGenerator {
 public:
    explicit KeyGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~KeyGenerator() {}
    const std::string Gen(const Row& row);
};

class OrderGenerator : public FnGenerator {
 public:
    explicit OrderGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~OrderGenerator() {}
    const int64_t Gen(const Row& row);
    bool is_asc_;
};

class ConditionGenerator : public FnGenerator {
 public:
    explicit ConditionGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~ConditionGenerator() {}
    const bool Gen(const Row& row);
};

class FilterGenerator {
 public:
    explicit FilterGenerator(const Key& filter_key)
        : filter_key_(filter_key.fn_info_) {}
    virtual ~FilterGenerator() {}
    const bool Valid() const { return filter_key_.Valid(); }
    std::shared_ptr<TableHandler> Filter(std::shared_ptr<TableHandler> table,
                                         const std::string& request_keys) {
        if (!filter_key_.Valid()) {
            return table;
        }
        auto mem_table =
            std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
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
        std::shared_ptr<DataHandler> input) {
        switch (input->GetHanlderType()) {
            case kPartitionHandler: {
                return Partition(
                    std::dynamic_pointer_cast<PartitionHandler>(input));
            }
            case kTableHandler: {
                return Partition(
                    std::dynamic_pointer_cast<TableHandler>(input));
            }
            default: {
                LOG(WARNING)
                    << "Partition Fail: input isn't partition or table";
                return std::shared_ptr<PartitionHandler>();
            }
        }
    }
    std::shared_ptr<PartitionHandler> Partition(
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
        return output_partitions;
    }
    std::shared_ptr<PartitionHandler> Partition(
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

        auto iter =
            std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
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
        return output_partitions;
    }
    const std::string GetKey(const Row& row) { return key_gen_.Gen(row); }

 private:
    KeyGenerator key_gen_;
};

class SortGenerator {
 public:
    SortGenerator(const Sort& sort)
        : order_gen_(sort.fn_info_),
          is_valid_(sort.ValidSort()),
          is_asc_(sort.is_asc()) {}
    virtual ~SortGenerator() {}

    const bool Valid() const { return is_valid_; }
    std::shared_ptr<DataHandler> Sort(std::shared_ptr<DataHandler> input) {
        if (!input || !is_valid_ || !order_gen_.Valid()) {
            return input;
        }
        switch (input->GetHanlderType()) {
            case kTableHandler:
                return Sort(std::dynamic_pointer_cast<TableHandler>(input));
            case kPartitionHandler:
                return Sort(std::dynamic_pointer_cast<PartitionHandler>(input));
            default: {
                LOG(WARNING) << "Sort Fail: input isn't partition or table";
                return std::shared_ptr<PartitionHandler>();
            }
        }
    }

    std::shared_ptr<PartitionHandler> Sort(
        std::shared_ptr<PartitionHandler> partition) {
        if (!is_valid_) {
            return partition;
        }
        if (!partition) {
            return std::shared_ptr<PartitionHandler>();
        }
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
                output->AddRow(key, static_cast<uint64_t >(ts), segment_iter->GetValue());
                segment_iter->Next();
            }
        }
        if (order_gen_.Valid()) {
            output->Sort(is_asc_);
        } else {
            output->Reverse();
        }
        return output;
    }

    std::shared_ptr<TableHandler> Sort(std::shared_ptr<TableHandler> table) {
        if (!table || !is_valid_) {
            return table;
        }
        auto output_table = std::shared_ptr<MemTimeTableHandler>(
            new MemTimeTableHandler(table->GetSchema()));
        auto iter =
            std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
        if (!iter) {
            return std::shared_ptr<TableHandler>();
        }
        iter->SeekToFirst();
        while (iter->Valid()) {
            if (order_gen_.Valid()) {
                int64_t key = order_gen_.Gen(iter->GetValue());
                output_table->AddRow(static_cast<uint64_t >(key), iter->GetValue());
            } else {
                output_table->AddRow(iter->GetKey(), iter->GetValue());
            }
            iter->Next();
        }

        if (order_gen_.Valid()) {
            output_table->Sort(is_asc_);
        } else {
            output_table->Reverse();
        }
        return output_table;
    }

 private:
    bool is_valid_;
    bool is_asc_;
    OrderGenerator order_gen_;
};

class WindowGenerator {
 public:
    explicit WindowGenerator(const WindowOp& window)
        : sort_gen_(window.sort_),
          partition_gen_(window.partition_),
          window_op_(window),
          range_gen_(window.range_.fn_info_) {}
    virtual ~WindowGenerator() {}
    const int64_t OrderKey(const Row& row) { return range_gen_.Gen(row); }
    const WindowOp window_op_;
    PartitionGenerator partition_gen_;
    SortGenerator sort_gen_;
    OrderGenerator range_gen_;
};

enum RunnerType {
    kRunnerData,
    kRunnerRequest,
    kRunnerGroup,
    kRunnerFilter,
    kRunnerOrder,
    kRunnerGroupAndSort,
    kRunnerTableProject,
    kRunnerRowProject,
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
        case kRunnerTableProject:
            return "TABLE_PROJECT";
        case kRunnerRowProject:
            return "ROW_PROJECT";
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
class Runner {
 public:
    explicit Runner(const int32_t id)
        : id_(id),
          type_(kRunnerUnknow),
          limit_cnt_(0),
          need_cache_(false),
          producers_(),
          output_schemas_() {}
    explicit Runner(const int32_t id, const RunnerType type,
                    const vm::NameSchemaList& output_schemas)
        : id_(id),
          type_(type),
          limit_cnt_(0),
          need_cache_(false),
          producers_(),
          output_schemas_(output_schemas) {}
    Runner(const int32_t id, const RunnerType type,
           const vm::NameSchemaList& output_schemas, const int32_t limit_cnt)
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
                             Window* window);
    static const Row RowLastJoinTable(const Row& left_row,
                                      std::shared_ptr<TableHandler> right_table,
                                      ConditionGenerator& filter);  // NOLINT
    static std::shared_ptr<TableHandler> TableReverse(
        std::shared_ptr<TableHandler> table);

    static void PrintData(const vm::NameSchemaList& schema_list,
                          std::shared_ptr<DataHandler> data);
    const vm::NameSchemaList& output_schemas() const {
        return output_schemas_;
    }

 protected:
    bool need_cache_;
    std::vector<Runner*> producers_;
    const vm::NameSchemaList output_schemas_;
};
class IteratorStatus {
 public:
    IteratorStatus() : is_valid_(false), key_(0) {}
    IteratorStatus(uint64_t key) : is_valid_(true), key_(key) {}
    virtual ~IteratorStatus() {}
    void MarkInValid() {
        is_valid_ = false;
        key_ = 0;
    }
    void set_key(uint64_t key) { key_ = key; }
    bool is_valid_;
    uint64_t key_;
};  // namespace vm
class WindowUnionGenerator {
 public:
    WindowUnionGenerator() : unions_cnt_(0) {}
    virtual ~WindowUnionGenerator() {}

    std::vector<std::shared_ptr<DataHandler>> RunInputs(RunnerContext& ctx) {
        std::vector<std::shared_ptr<DataHandler>> union_inputs;
        if (!union_input_runners_.empty()) {
            for (auto runner : union_input_runners_) {
                union_inputs.push_back(runner->RunWithCache(ctx));
            }
        }
        return union_inputs;
    }
    std::vector<std::shared_ptr<PartitionHandler>> PartitionEach(
        std::vector<std::shared_ptr<DataHandler>> union_inputs) {
        std::vector<std::shared_ptr<PartitionHandler>> union_partitions;
        if (!windows_gen_.empty()) {
            union_partitions.reserve(windows_gen_.size());
            auto input_iter = union_inputs.cbegin();
            for (size_t i = 0; i < unions_cnt_; i++) {
                union_partitions.push_back(
                    windows_gen_[i].partition_gen_.Partition(union_inputs[i]));
            }
        }
        return union_partitions;
    }
    void AddWindowUnion(const WindowOp& window_op, Runner* runner) {
        windows_gen_.push_back(WindowGenerator(window_op));
        union_input_runners_.push_back(runner);
        unions_cnt_++;
    }

    int32_t PickIteratorWithMininumKey(
        std::vector<IteratorStatus>* status_list_ptr) {
        auto status_list = *status_list_ptr;
        int32_t min_union_pos = -1;
        int64_t min_union_order = INT64_MAX;
        for (size_t i = 0; i < unions_cnt_; i++) {
            if (status_list[i].is_valid_ &&
                status_list[i].key_ < min_union_order) {
                min_union_order = status_list[i].key_;
                min_union_pos = static_cast<int32_t>(i);
            }
        }
        return min_union_pos;
    }
    size_t unions_cnt_;
    std::vector<WindowGenerator> windows_gen_;
    std::vector<Runner*> union_input_runners_;
};
class JoinGenerator {
 public:
    explicit JoinGenerator(const Join& join)
        : condition_gen_(join.filter_.fn_info_),
          left_key_gen_(join.left_key_.fn_info_),
          index_key_gen_(join.index_key_.fn_info_),
          right_group_gen_(join.right_key_) {}
    virtual ~JoinGenerator() {}
    bool TableJoin(std::shared_ptr<TableHandler> left,
                   std::shared_ptr<TableHandler> right,
                   std::shared_ptr<MemTableHandler> output);  // NOLINT
    bool TableJoin(std::shared_ptr<TableHandler> left,
                   std::shared_ptr<PartitionHandler> right,
                   std::shared_ptr<MemTableHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left,
                       std::shared_ptr<TableHandler> right,
                       std::shared_ptr<MemPartitionHandler> output);  // NOLINT
    bool PartitionJoin(std::shared_ptr<PartitionHandler> left,
                       std::shared_ptr<PartitionHandler> right,
                       std::shared_ptr<MemPartitionHandler>);  // NOLINT

    Row RowLastJoin(const Row& left_row, std::shared_ptr<DataHandler> right);

    ConditionGenerator condition_gen_;
    KeyGenerator left_key_gen_;
    KeyGenerator index_key_gen_;
    PartitionGenerator right_group_gen_;

 private:
    Row RowLastJoinPartition(
        const Row& left_row,
        std::shared_ptr<PartitionHandler> partition);  // NOLINT
    Row RowLastJoinTable(const Row& left_row,
                         std::shared_ptr<TableHandler> table);  // NOLINT
};

class WindowJoinGenerator {
 public:
    WindowJoinGenerator() : joins_cnt_(0) {}
    virtual ~WindowJoinGenerator() {}
    void AddWindowJoin(const Join& join, Runner* runner) {
        joins_gen_.push_back(JoinGenerator(join));
        join_input_runners_.push_back(runner);
        joins_cnt_++;
    }
    const bool Valid() const { return 0 != joins_cnt_; }
    std::vector<std::shared_ptr<DataHandler>> RunInputs(RunnerContext& ctx) {
        std::vector<std::shared_ptr<DataHandler>> union_inputs;
        if (!join_input_runners_.empty()) {
            for (auto runner : join_input_runners_) {
                union_inputs.push_back(runner->RunWithCache(ctx));
            }
        }
        return union_inputs;
    }
    Row Join(
        const Row& left_row,
        const std::vector<std::shared_ptr<DataHandler>>& join_right_tables) {
        Row row = left_row;
        for (size_t i = 0; i < join_right_tables.size(); i++) {
            row = joins_gen_[i].RowLastJoin(row, join_right_tables[i]);
        }
        return row;
    }

    size_t joins_cnt_;
    std::vector<JoinGenerator> joins_gen_;
    std::vector<Runner*> join_input_runners_;
};

class DataRunner : public Runner {
 public:
    DataRunner(const int32_t id, const NameSchemaList& schema,
               std::shared_ptr<DataHandler> data_hander)
        : Runner(id, kRunnerData, schema), data_handler_(data_hander) {}
    ~DataRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::shared_ptr<DataHandler> data_handler_;
};

class RequestRunner : public Runner {
 public:
    RequestRunner(const int32_t id, const NameSchemaList& schema)
        : Runner(id, kRunnerRequest, schema) {}
    ~RequestRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
class GroupRunner : public Runner {
 public:
    GroupRunner(const int32_t id, const NameSchemaList& schema,
                const int32_t limit_cnt, const Key& group)
        : Runner(id, kRunnerGroup, schema, limit_cnt), partition_gen_(group) {}
    ~GroupRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    PartitionGenerator partition_gen_;
};

class FilterRunner : public Runner {
 public:
    FilterRunner(const int32_t id, const NameSchemaList& schema,
                 const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerFilter, schema, limit_cnt), cond_gen_(fn_info) {}
    ~FilterRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ConditionGenerator cond_gen_;
};
class OrderRunner : public Runner {
 public:
    OrderRunner(const int32_t id, const NameSchemaList& schema,
                const int32_t limit_cnt, const Sort& sort)
        : Runner(id, kRunnerOrder, schema, limit_cnt), sort_gen_(sort) {}
    ~OrderRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    SortGenerator sort_gen_;
};

class TableProjectRunner : public Runner {
 public:
    TableProjectRunner(const int32_t id, const NameSchemaList& schema,
                       const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerTableProject, schema, limit_cnt),
          project_gen_(fn_info) {}
    ~TableProjectRunner() {}

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ProjectGenerator project_gen_;
};

class RowProjectRunner : public Runner {
 public:
    RowProjectRunner(const int32_t id, const NameSchemaList& schema,
                     const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerRowProject, schema, limit_cnt),
          project_gen_(fn_info) {}
    ~RowProjectRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ProjectGenerator project_gen_;
};

class GroupAggRunner : public Runner {
 public:
    GroupAggRunner(const int32_t id, const NameSchemaList& schema,
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
    AggRunner(const int32_t id, const NameSchemaList& schema,
              const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerAgg, schema, limit_cnt), agg_gen_(fn_info) {}
    ~AggRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    AggGenerator agg_gen_;
};
class WindowAggRunner : public Runner {
 public:
    WindowAggRunner(const int32_t id, const NameSchemaList& schema,
                    const int32_t limit_cnt, const WindowOp& window_op,
                    const FnInfo& fn_info)
        : Runner(id, kRunnerWindowAgg, schema, limit_cnt),
          window_project_gen_(fn_info),
          instance_window_gen_(window_op) {}
    ~WindowAggRunner() {}
    void AddWindowJoin(const Join& join, Runner* runner) {
        windows_join_gen_.AddWindowJoin(join, runner);
    }
    void AddWindowUnion(Runner* runner) {
        windows_union_gen_.AddWindowUnion(instance_window_gen_.window_op_,
                                          runner);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    void RunWindowAggOnKey(
        std::shared_ptr<PartitionHandler> instance_partition,
        std::vector<std::shared_ptr<PartitionHandler>> union_partitions,
        std::vector<std::shared_ptr<DataHandler>> joins, const std::string& key,
        std::shared_ptr<MemTableHandler> output_table);

    WindowUnionGenerator windows_union_gen_;
    WindowJoinGenerator windows_join_gen_;
    WindowGenerator instance_window_gen_;
    WindowProjectGenerator window_project_gen_;
};

class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const NameSchemaList& schema,
                       const int32_t limit_cnt, const WindowOp& window_op,
                       const Key& hash)
        : Runner(id, kRunnerRequestUnion, schema, limit_cnt),
          is_asc_(window_op.sort_.is_asc()),
          partition_filter_gen_(window_op.partition_),
          sort_gen_(window_op.sort_),
          key_gen_(hash.fn_info_),
          ts_gen_(window_op.range_.fn_info_),
          start_offset_(window_op.range_.start_offset_),
          end_offset_(window_op.range_.end_offset_) {}
    ~RequestUnionRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT

    std::shared_ptr<DataHandler> UnionTable(
        Row row, std::shared_ptr<TableHandler> table);
    std::shared_ptr<DataHandler> UnionPartition(
        Row row, std::shared_ptr<PartitionHandler> partition);
    const bool is_asc_;
    FilterGenerator partition_filter_gen_;
    SortGenerator sort_gen_;
    KeyGenerator key_gen_;
    OrderGenerator ts_gen_;
    const int64_t start_offset_;
    const int64_t end_offset_;
};

class LastJoinRunner : public Runner {
 public:
    LastJoinRunner(const int32_t id, const NameSchemaList& schema,
                   const int32_t limit_cnt, const Join& join)
        : Runner(id, kRunnerLastJoin, schema, limit_cnt), join_gen_(join) {}
    ~LastJoinRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT

    JoinGenerator join_gen_;
};

class RequestLastJoinRunner : public Runner {
 public:
    RequestLastJoinRunner(const int32_t id, const NameSchemaList& schema,
                          const int32_t limit_cnt, const Join& join)
        : Runner(id, kRunnerRequestLastJoin, schema, limit_cnt),
          join_gen_(join) {}
    ~RequestLastJoinRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT

    JoinGenerator join_gen_;
};

class ConcatRunner : public Runner {
 public:
    ConcatRunner(const int32_t id, const NameSchemaList& schema,
                 const int32_t limit_cnt)
        : Runner(id, kRunnerConcat, schema, limit_cnt) {}
    ~ConcatRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
class LimitRunner : public Runner {
 public:
    LimitRunner(int32_t id, const NameSchemaList& schema, int32_t limit_cnt)
        : Runner(id, kRunnerLimit, schema, limit_cnt) {}
    ~LimitRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
class RunnerBuilder {
 public:
    RunnerBuilder() : id_(0) {}
    virtual ~RunnerBuilder() {}

    Runner* Build(PhysicalOpNode* node, Status& status);  // NOLINT

 private:
    int32_t id_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_RUNNER_H_
