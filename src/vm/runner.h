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
    RunnerContext() : request_(), cache_() {}
    explicit RunnerContext(const Row& request) : request_(request), cache_() {}
    const Row request_;
    std::map<int32_t, std::shared_ptr<DataHandler>> cache_;
};

class FnGenerator {
 public:
    FnGenerator(const FnInfo& info)
        : fn_(info.fn_), fn_schema_(info.fn_schema_), row_view_(fn_schema_) {}
    virtual ~FnGenerator() {}
    inline const bool Valid() const { return fn_ == nullptr; }
    const int8_t* fn_;
    const Schema fn_schema_;
    RowView row_view_;
};

class ProjectGenerator : public FnGenerator {
 public:
    ProjectGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~ProjectGenerator() {}
    const Row Gen(const Row& row);
};

class AggGenerator : public FnGenerator {
 public:
    AggGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~AggGenerator() {}
    const Row Gen(std::shared_ptr<TableHandler> table);
};

class WindowGenerator : public FnGenerator {
 public:
    WindowGenerator(const FnInfo& info) : FnGenerator(info) {}
    virtual ~WindowGenerator() {}
    const Row Gen(const uint64_t key, const Row row, Window* window);
};

class KeyGenerator : public FnGenerator {
 public:
    KeyGenerator(const FnInfo& info, const std::vector<int32_t> idxs)
        : FnGenerator(info), idxs_(idxs) {}
    virtual ~KeyGenerator() {}
    const std::string Gen(const Row& row);
    const std::vector<int32_t> idxs_;
};

class OrderGenerator : public FnGenerator {
 public:
    OrderGenerator(const FnInfo& info, const std::vector<int32_t> idxs)
        : FnGenerator(info), idxs_(idxs) {}
    virtual ~OrderGenerator() {}
    const int64_t Gen(const Row& row);
    const std::vector<int32_t> idxs_;
};

class ConditionGenerator : public FnGenerator {
 public:
    ConditionGenerator(const FnInfo& info, const std::vector<int32_t> idxs)
        : FnGenerator(info), idxs_(idxs) {}
    virtual ~ConditionGenerator() {}
    const bool Gen(const Row& row);
    const std::vector<int32_t> idxs_;
};
class Runner {
 public:
    explicit Runner(const int32_t id)
        : id_(id), limit_cnt_(0), need_cache_(false), producers_() {}
    Runner(const int32_t id, const int32_t limit_cnt)
        : id_(id), limit_cnt_(limit_cnt), need_cache_(false), producers_() {}
    virtual ~Runner() {}
    void AddProducer(Runner* runner) { producers_.push_back(runner); }
    const std::vector<Runner*>& GetProducers() const { return producers_; }
    virtual void Print(std::ostream& output, const std::string& tab) const {
        if (!producers_.empty()) {
            for (auto producer : producers_) {
                output << "\n";
                producer->Print(output, "  " + tab);
            }
        }
    }
    const int32_t id_;
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
    static std::shared_ptr<DataHandler> TableGroup(
        const std::shared_ptr<DataHandler> table, KeyGenerator& key_gen);
    static std::shared_ptr<DataHandler> PartitionGroup(
        const std::shared_ptr<DataHandler> partitions, KeyGenerator& key_gen);

    static std::shared_ptr<DataHandler> PartitionSort(
        std::shared_ptr<DataHandler> table, OrderGenerator& order_gen,
        const bool is_asc);
    static std::shared_ptr<DataHandler> TableSort(
        std::shared_ptr<DataHandler> table, OrderGenerator order_gen,
        const bool is_asc);

 protected:
    bool need_cache_;
    std::vector<Runner*> producers_;
};

class DataRunner : public Runner {
 public:
    DataRunner(const int32_t id, std::shared_ptr<DataHandler> data_hander)
        : Runner(id), data_handler_(data_hander) {}
    ~DataRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "DATA";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::shared_ptr<DataHandler> data_handler_;
};

class RequestRunner : public Runner {
 public:
    RequestRunner(const int32_t id, const Schema& schema)
        : Runner(id), request_schema(schema) {}
    ~RequestRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "REQUEST";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const Schema request_schema;
};
class GroupRunner : public Runner {
 public:
    GroupRunner(const int32_t id, const int32_t limit_cnt,
                const FnInfo& fn_info, const std::vector<int32_t>& idxs)
        : Runner(id, limit_cnt), group_gen_(fn_info, idxs) {}
    ~GroupRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "GROUP";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator group_gen_;
};

class FilterRunner : public Runner {
 public:
    FilterRunner(const int32_t id, const int32_t limit_cnt,
                 const FnInfo& fn_info, const std::vector<int32_t>& idxs)
        : Runner(id, limit_cnt), cond_gen_(fn_info, idxs) {}
    ~FilterRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "FILTER";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> idxs_;
    ConditionGenerator cond_gen_;
};
class OrderRunner : public Runner {
 public:
    OrderRunner(const int32_t id, const int32_t limit_cnt,
                const FnInfo& fn_info, const std::vector<int32_t>& idxs,
                const bool is_asc)
        : Runner(id, limit_cnt), order_gen_(fn_info, idxs), is_asc_(is_asc) {}
    ~OrderRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "ORDER";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    OrderGenerator order_gen_;
    const bool is_asc_;
};

class GroupAndSortRunner : public Runner {
 public:
    GroupAndSortRunner(const int32_t id, const int32_t limit_cnt,
                       const FnInfo& fn_info,
                       const std::vector<int32_t>& groups_idxs,
                       const std::vector<int32_t>& orders_idxs,
                       const bool is_asc)
        : Runner(id, limit_cnt),
          group_gen_(fn_info, groups_idxs),
          order_gen_(fn_info, orders_idxs),
          is_asc_(is_asc) {}
    ~GroupAndSortRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "GROUP_AND_SORT";
        Runner::Print(output, tab);
    }

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator group_gen_;
    OrderGenerator order_gen_;
    const bool is_asc_;
};

class TableProjectRunner : public Runner {
 public:
    TableProjectRunner(const int32_t id, const int32_t limit_cnt,
                       const FnInfo& fn_info)
        : Runner(id, limit_cnt), project_gen_(fn_info) {}
    ~TableProjectRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "TABLE_PROJECT";
        Runner::Print(output, tab);
    }

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ProjectGenerator project_gen_;
};

class RowProjectRunner : public Runner {
 public:
    RowProjectRunner(const int32_t id, const int32_t limit_cnt,
                     const FnInfo& fn_info)
        : Runner(id, limit_cnt), project_gen_(fn_info) {}
    ~RowProjectRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "ROW_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ProjectGenerator project_gen_;
};

class GroupAggRunner : public Runner {
 public:
    GroupAggRunner(const int32_t id, const int32_t limit_cnt,
                   const FnInfo& fn_info)
        : Runner(id, limit_cnt), agg_gen_(fn_info) {}
    ~GroupAggRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "GROUP_AGG_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    AggGenerator agg_gen_;
};

class AggRunner : public Runner {
 public:
    AggRunner(const int32_t id, const int32_t limit_cnt, const FnInfo& fn_info)
        : Runner(id, limit_cnt), agg_gen_(fn_info) {}
    ~AggRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "AGG_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    AggGenerator agg_gen_;
};
class WindowAggRunner : public Runner {
 public:
    WindowAggRunner(const int32_t id, const int32_t limit_cnt,
                    const FnInfo& fn_info, const int64_t start_offset,
                    const int64_t end_offset)
        : Runner(id, limit_cnt),
          window_gen_(fn_info),
          start_offset_(start_offset),
          end_offset_(end_offset) {}
    ~WindowAggRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "WINDOW_AGG_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    WindowGenerator window_gen_;
    const int64_t start_offset_;
    const int64_t end_offset_;
};

class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const int32_t limit_cnt,
                       const FnInfo& fn_info,
                       const std::vector<int32_t>& groups_idxs,
                       const std::vector<int32_t>& orders_idxs,
                       const std::vector<int32_t>& ts_idxs, const bool is_asc,
                       const int64_t start_offset, const int64_t end_offset)
        : Runner(id, limit_cnt),
          is_asc_(is_asc),
          group_gen_(fn_info, groups_idxs),
          order_gen_(fn_info, orders_idxs),
          ts_gen_(fn_info, ts_idxs),
          start_offset_(start_offset),
          end_offset_(end_offset) {}
    ~RequestUnionRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "REQUEST_UNION";
        Runner::Print(output, tab);
    }
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
    IndexSeekRunner(const int32_t id, const int32_t limit_cnt,
                    const FnInfo& fn_info,
                    const std::vector<int32_t>& keys_idxs)
        : Runner(id, limit_cnt), key_gen_(fn_info, keys_idxs) {}

    ~IndexSeekRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "INDEX_SEEK";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    KeyGenerator key_gen_;
};

class LastJoinRunner : public Runner {
 public:
    LastJoinRunner(const int32_t id, const int32_t limit_cnt,
                   const FnInfo& fn_info,
                   const std::vector<int32_t>& condition_idxs,
                   const FnInfo& left_key_info,
                   const std::vector<int32_t>& left_keys_idxs)
        : Runner(id, limit_cnt),
          condition_gen_(fn_info, condition_idxs),
          left_key_gen_(left_key_info, left_keys_idxs) {}
    ~LastJoinRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "LASTJOIN";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    ConditionGenerator condition_gen_;
    KeyGenerator left_key_gen_;
    const Row RowLastJoin(const Row& left_row, RowIterator* right_iter);
};

class RequestLastJoinRunner : public LastJoinRunner {
 public:
    RequestLastJoinRunner(const int32_t id, const int32_t limit_cnt,
                          const FnInfo& fn_info,
                          const std::vector<int32_t>& condition_idxs,
                          const FnInfo& left_key_info,
                          const std::vector<int32_t>& left_keys_idxs)
        : LastJoinRunner(id, limit_cnt, fn_info, condition_idxs, left_key_info,
                         left_keys_idxs) {}
    ~RequestLastJoinRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "REQUEST_LASTJOIN";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};

class LimitRunner : public Runner {
 public:
    LimitRunner(int32_t id, int32_t limit_cnt) : Runner(id, limit_cnt) {}
    ~LimitRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "LIMIT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
;
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
