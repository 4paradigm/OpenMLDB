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
#include <stdint-gcc.h>
#include <map>
#include <string>
#include <vector>
#include "base/status.h"
#include "codec/row_codec.h"
#include "vm/catalog.h"
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
class RunnerContext {
 public:
    RunnerContext(const bool is_debug = false)
        : request_(), is_debug_(is_debug), cache_() {}
    RunnerContext(const Row& request, const bool is_debug = false)
        : request_(request), is_debug_(is_debug), cache_() {}
    const Row request_;
    const bool is_debug_;
    std::map<int32_t, std::shared_ptr<DataHandler>> cache_;
};

class FnGenerator {
 public:
    explicit FnGenerator(const FnInfo& info)
        : fn_(info.fn_), fn_schema_(info.fn_schema_), row_view_(fn_schema_) {}
    virtual ~FnGenerator() {}
    virtual const bool Valid() const = 0;
    const int8_t* fn_;
    const Schema fn_schema_;
    RowView row_view_;
};

class ProjectGenerator : public FnGenerator {
 public:
    explicit ProjectGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~ProjectGenerator() {}
    inline const bool Valid() const override { return nullptr != fn_; }
    const Row Gen(const Row& row);
};

class AggGenerator : public FnGenerator {
 public:
    explicit AggGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~AggGenerator() {}
    const Row Gen(std::shared_ptr<TableHandler> table);
    inline const bool Valid() const override { return nullptr != fn_; }
};

class WindowGenerator : public FnGenerator {
 public:
    explicit WindowGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~WindowGenerator() {}
    const Row Gen(const uint64_t key, const Row row, Window* window);
    inline const bool Valid() const override { return nullptr != fn_; }
};

class KeyGenerator : public FnGenerator {
 public:
    KeyGenerator(const FnInfo& info, const std::vector<int32_t> idxs)
        : FnGenerator(info), idxs_(idxs) {}
    virtual ~KeyGenerator() {}
    const std::string Gen(const Row& row);
    const std::vector<int32_t> idxs_;
    inline const bool Valid() const override {
        return nullptr != fn_ && !idxs_.empty();
    }
};

class OrderGenerator : public FnGenerator {
 public:
    OrderGenerator(const FnInfo& info, const std::vector<int32_t> idxs)
        : FnGenerator(info), idxs_(idxs) {}
    virtual ~OrderGenerator() {}
    const int64_t Gen(const Row& row);
    const std::vector<int32_t> idxs_;
    inline const bool Valid() const override {
        return nullptr != fn_ && !idxs_.empty();
    }
};

class ConditionGenerator : public FnGenerator {
 public:
    ConditionGenerator(const FnInfo& info, const std::vector<int32_t> idxs)
        : FnGenerator(info), idxs_(idxs) {}
    virtual ~ConditionGenerator() {}
    const bool Gen(const Row& row);
    const std::vector<int32_t> idxs_;
    inline const bool Valid() const override {
        return nullptr != fn_ && !idxs_.empty();
    }
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
    kRunnerRequestLastJoin,
    kRunnerLimit
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
    static Row RowProject(const int8_t* fn, const Row row,
                          const bool need_free = false);
    const Row RowLastJoin(const Row& left_row,
                          std::shared_ptr<TableHandler> right_table,
                          ConditionGenerator& cond_gen);  // NOLINT
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

    static void PrintData(const vm::NameSchemaList& schema_list, std::shared_ptr<DataHandler> data);
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
    GroupRunner(const int32_t id, const NameSchemaList& schema, const int32_t limit_cnt,
                const FnInfo& fn_info, const std::vector<int32_t>& idxs)
        : Runner(id, kRunnerGroup, schema, limit_cnt),
          group_gen_(fn_info, idxs) {}
    ~GroupRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator group_gen_;
};

class FilterRunner : public Runner {
 public:
    FilterRunner(const int32_t id, const NameSchemaList& schema,
                 const int32_t limit_cnt, const FnInfo& fn_info,
                 const std::vector<int32_t>& idxs)
        : Runner(id, kRunnerFilter, schema, limit_cnt),
          cond_gen_(fn_info, idxs) {}
    ~FilterRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> idxs_;
    ConditionGenerator cond_gen_;
};
class OrderRunner : public Runner {
 public:
    OrderRunner(const int32_t id, const NameSchemaList& schema, const int32_t limit_cnt,
                const FnInfo& fn_info, const std::vector<int32_t>& idxs,
                const bool is_asc)
        : Runner(id, kRunnerOrder, schema, limit_cnt),
          order_gen_(fn_info, idxs),
          is_asc_(is_asc) {}
    ~OrderRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    OrderGenerator order_gen_;
    const bool is_asc_;
};

class GroupAndSortRunner : public Runner {
 public:
    GroupAndSortRunner(const int32_t id, const NameSchemaList& schema,
                       const int32_t limit_cnt, const FnInfo& fn_info,
                       const std::vector<int32_t>& groups_idxs,
                       const std::vector<int32_t>& orders_idxs,
                       const bool is_asc)
        : Runner(id, kRunnerGroupAndSort, schema, limit_cnt),
          group_gen_(fn_info, groups_idxs),
          order_gen_(fn_info, orders_idxs),
          is_asc_(is_asc) {}
    ~GroupAndSortRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator group_gen_;
    OrderGenerator order_gen_;
    const bool is_asc_;
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
                   const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, kRunnerGroupAgg, schema, limit_cnt), agg_gen_(fn_info) {}
    ~GroupAggRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    AggGenerator agg_gen_;
};

class AggRunner : public Runner {
 public:
    AggRunner(const int32_t id, const NameSchemaList& schema, const int32_t limit_cnt,
              const FnInfo& fn_info)
        : Runner(id, kRunnerAgg, schema, limit_cnt), agg_gen_(fn_info) {}
    ~AggRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    AggGenerator agg_gen_;
};
class WindowAggRunner : public Runner {
 public:
    WindowAggRunner(const int32_t id, const NameSchemaList& schema,
                    const int32_t limit_cnt, const FnInfo& fn_info,
                    const int64_t start_offset, const int64_t end_offset)
        : Runner(id, kRunnerWindowAgg, schema, limit_cnt),
          window_gen_(fn_info),
          start_offset_(start_offset),
          end_offset_(end_offset) {}
    ~WindowAggRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    WindowGenerator window_gen_;
    const int64_t start_offset_;
    const int64_t end_offset_;
};

class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const NameSchemaList& schema,
                       const int32_t limit_cnt, const FnInfo& fn_info,
                       const std::vector<int32_t>& groups_idxs,
                       const std::vector<int32_t>& orders_idxs,
                       const std::vector<int32_t>& ts_idxs, const bool is_asc,
                       const int64_t start_offset, const int64_t end_offset)
        : Runner(id, kRunnerRequestUnion, schema, limit_cnt),
          is_asc_(is_asc),
          group_gen_(fn_info, groups_idxs),
          order_gen_(fn_info, orders_idxs),
          ts_gen_(fn_info, ts_idxs),
          start_offset_(start_offset),
          end_offset_(end_offset) {}
    ~RequestUnionRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const bool is_asc_;
    KeyGenerator group_gen_;
    OrderGenerator order_gen_;
    OrderGenerator ts_gen_;
    const int64_t start_offset_;
    const int64_t end_offset_;
};
class IndexSeekRunner : public Runner {
 public:
    IndexSeekRunner(const int32_t id, const NameSchemaList& schema,
                    const int32_t limit_cnt, const FnInfo& fn_info,
                    const std::vector<int32_t>& keys_idxs)
        : Runner(id, kRunnerIndexSeek, schema, limit_cnt),
          key_gen_(fn_info, keys_idxs) {}

    ~IndexSeekRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator key_gen_;
};

class LastJoinRunner : public Runner {
 public:
    LastJoinRunner(const int32_t id, const NameSchemaList& schema,
                   const int32_t limit_cnt, const FnInfo& fn_info,
                   const std::vector<int32_t>& condition_idxs,
                   const FnInfo& left_key_info,
                   const std::vector<int32_t>& left_keys_idxs)
        : Runner(id, kRunnerLastJoin, schema, limit_cnt),
          condition_gen_(fn_info, condition_idxs),
          left_key_gen_(left_key_info, left_keys_idxs) {}
    ~LastJoinRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ConditionGenerator condition_gen_;
    KeyGenerator left_key_gen_;
};

class RequestLastJoinRunner : public Runner {
 public:
    RequestLastJoinRunner(const int32_t id, const NameSchemaList& schema,
                          const int32_t limit_cnt, const FnInfo& fn_info,
                          const std::vector<int32_t>& condition_idxs,
                          const FnInfo& left_key_info,
                          const std::vector<int32_t>& left_keys_idxs)
        : Runner(id, kRunnerRequestLastJoin, schema, limit_cnt),
          condition_gen_(fn_info, condition_idxs),
          left_key_gen_(left_key_info, left_keys_idxs) {}
    ~RequestLastJoinRunner() {}
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ConditionGenerator condition_gen_;
    KeyGenerator left_key_gen_;
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
