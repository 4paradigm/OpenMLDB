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
    const Row Gen(const uint64_t key, const Row row, Window* window);
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
};

class ConditionGenerator : public FnGenerator {
 public:
    explicit ConditionGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~ConditionGenerator() {}
    const bool Gen(const Row& row);
};

struct JoinGenerator {
 public:
    explicit JoinGenerator(const Join& join)
        : condition_gen_(join.filter_.fn_info_),
          left_key_gen_(join.left_key_.fn_info_),
          index_key_gen_(join.index_key_.fn_info_),
          right_key_gen_(join.right_key_.fn_info_) {}
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
    KeyGenerator right_key_gen_;

 private:
    Row RowLastJoinPartition(
        const Row& left_row,
        std::shared_ptr<PartitionHandler> partition);  // NOLINT
    Row RowLastJoinTable(const Row& left_row,
                         std::shared_ptr<TableHandler> table);  // NOLINT
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
    static std::string GetColumnString(RowView* view, int pos, type::Type type);
    static int64_t GetColumnInt64(RowView* view, int pos, type::Type type);
    static bool GetColumnBool(RowView* view, int idx, type::Type type);
    static std::string GenerateKeys(RowView* row_view, const Schema& schema,
                                    const std::vector<int>& idxs);
    static Row WindowProject(const int8_t* fn, const uint64_t key,
                             const Row row, Window* window);
    static const Row RowLastJoinTable(const Row& left_row,
                                      std::shared_ptr<TableHandler> right_table,
                                      ConditionGenerator& filter);  // NOLINT
    static std::shared_ptr<DataHandler> TableGroup(
        const std::shared_ptr<DataHandler> table,
        KeyGenerator& key_gen);  // NOLINT
    static std::shared_ptr<DataHandler> PartitionGroup(
        const std::shared_ptr<DataHandler> partitions,
        KeyGenerator& key_gen);  // NOLINT

    static std::shared_ptr<DataHandler> PartitionSort(
        std::shared_ptr<DataHandler> table,
        OrderGenerator& order_gen,  // NOLINT
        const bool is_asc);
    static std::shared_ptr<DataHandler> TableSort(
        std::shared_ptr<DataHandler> table, OrderGenerator order_gen,  // NOLINT
        const bool is_asc);
    static std::shared_ptr<TableHandler> TableReverse(
        std::shared_ptr<TableHandler> table);

    static void PrintData(const vm::NameSchemaList& schema_list,
                          std::shared_ptr<DataHandler> data);

 protected:
    bool need_cache_;
    std::vector<Runner*> producers_;
    const vm::NameSchemaList output_schemas_;
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
                const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerGroup, schema, limit_cnt), group_gen_(fn_info) {}
    ~GroupRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator group_gen_;
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
        : Runner(id, kRunnerOrder, schema, limit_cnt),
          order_gen_(sort.fn_info_),
          is_asc_(sort.is_asc()) {}
    ~OrderRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    OrderGenerator order_gen_;
    bool is_asc_;
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
          window_op_(window_op),
          window_gen_(fn_info),
          group_gen_(window_op.partition_.fn_info_),
          order_gen_(window_op.sort_.fn_info_),
          range_gen_(window_op.range_.fn_info_) {}
    ~WindowAggRunner() {}
    void AddWindowJoin(const Join& join, Runner* runner) {
        joins_gen_.push_back(std::make_pair(JoinGenerator(join), runner));
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    bool WindowAggRun(RunnerContext& ctx,                           // NOLINT
                      std::shared_ptr<PartitionHandler> partition,
                      std::shared_ptr<MemTableHandler> output_table);
    bool PartitionRun(RunnerContext& ctx,  // NOLINT
                      std::shared_ptr<PartitionHandler> partition,
                      std::shared_ptr<MemTableHandler> output_table);
    bool TableRun(RunnerContext& ctx,  // NOLINT
                  std::shared_ptr<TableHandler> table,
                  std::shared_ptr<MemTableHandler> output_table);

    WindowOp window_op_;
    WindowJoinList window_joins_;
    WindowProjectGenerator window_gen_;
    KeyGenerator group_gen_;
    OrderGenerator order_gen_;
    OrderGenerator range_gen_;
    std::vector<std::pair<JoinGenerator, Runner*>> joins_gen_;
};

class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const NameSchemaList& schema,
                       const int32_t limit_cnt, const WindowOp& window_op,
                       const Key& hash)
        : Runner(id, kRunnerRequestUnion, schema, limit_cnt),
          is_asc_(window_op.sort_.is_asc()),
          group_gen_(window_op.partition_.fn_info_),
          key_gen_(hash.fn_info_),
          order_gen_(window_op.sort_.fn_info_),
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
    KeyGenerator group_gen_;
    KeyGenerator key_gen_;
    OrderGenerator order_gen_;
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
