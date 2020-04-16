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
class Runner {
 public:
    explicit Runner(const int32_t id)
        : id_(id),
          fn_(nullptr),
          fn_schema_(),
          row_view_(fn_schema_),
          limit_cnt_(0),
          need_cache_(false),
          producers_() {}
    Runner(const int32_t id, const int32_t limit_cnt)
        : id_(id),
          fn_(nullptr),
          fn_schema_(),
          row_view_(fn_schema_),
          limit_cnt_(limit_cnt),
          need_cache_(false),
          producers_() {}
    Runner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
           const int32_t limit_cnt)
        : id_(id),
          fn_(fn),
          fn_schema_(fn_schema),
          row_view_(fn_schema_),
          limit_cnt_(limit_cnt),
          need_cache_(false),
          producers_() {}
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
    const int8_t* fn_;
    const Schema fn_schema_;
    RowView row_view_;
    const int32_t limit_cnt_;
    virtual std::shared_ptr<DataHandler> Run(RunnerContext& ctx) = 0;  // NOLINT
    virtual std::shared_ptr<DataHandler> RunWithCache(
        RunnerContext& ctx);  // NOLINT

 protected:
    Row WindowProject(const int8_t* fn, const uint64_t key, const Row row,
                      Window* window);
    Row RowProject(const int8_t* fn, const Row row,
                   const bool need_free = false);
    Row MultiRowsProject(const int8_t* fn, const std::vector<Row>& rows,
                         const bool need_free = false);

    std::string GetColumnString(RowView* view, int pos, type::Type type);
    int64_t GetColumnInt64(RowView* view, int pos, type::Type type);
    bool GetColumnBool(RowView* view, int idx, type::Type type);
    std::string GenerateKeys(RowView* row_view, const Schema& schema,
                             const std::vector<int>& idxs);
    std::shared_ptr<DataHandler> TableGroup(
        const std::shared_ptr<DataHandler> table, const Schema& schema,
        const int8_t* fn, const std::vector<int>& idxs, RowView* row_view);
    std::shared_ptr<DataHandler> PartitionGroup(
        const std::shared_ptr<DataHandler> partitions, const Schema& schema,
        const int8_t* fn, const std::vector<int>& idxs, RowView* row_view);

    std::shared_ptr<DataHandler> PartitionSort(
        std::shared_ptr<DataHandler> table, const Schema& schema,
        const int8_t* fn, std::vector<int> idxs, const bool is_asc,
        RowView* row_view);
    std::shared_ptr<DataHandler> TableSort(std::shared_ptr<DataHandler> table,
                                           const Schema& schema,
                                           const int8_t* fn,
                                           std::vector<int> idxs,
                                           const bool is_asc,
                                           RowView* row_view);
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
    GroupRunner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
                const int32_t limit_cnt, const std::vector<int32_t>& idxs)
        : Runner(id, fn, fn_schema, limit_cnt), idxs_(idxs) {}
    ~GroupRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "GROUP";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> idxs_;
};

class FilterRunner : public Runner {
 public:
    FilterRunner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
                 const int32_t limit_cnt, const std::vector<int32_t>& idxs)
        : Runner(id, fn, fn_schema, limit_cnt), idxs_(idxs) {}
    ~FilterRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "FILTER";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> idxs_;
};
class OrderRunner : public Runner {
 public:
    OrderRunner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
                const int32_t limit_cnt, const std::vector<int32_t>& idxs,
                const bool is_asc)
        : Runner(id, fn, fn_schema, limit_cnt), is_asc_(is_asc), idxs_(idxs) {}
    ~OrderRunner() {}

    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "ORDER";
        Runner::Print(output, tab);
    }

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const bool is_asc_;
    const std::vector<int32_t> idxs_;
};

class GroupAndSortRunner : public Runner {
 public:
    GroupAndSortRunner(const int32_t id, const int8_t* fn,
                       const Schema& fn_schema, const int32_t limit_cnt,
                       const std::vector<int32_t>& groups_idxs,
                       const std::vector<int32_t>& orders_idxs,
                       const bool is_asc)
        : Runner(id, fn, fn_schema, limit_cnt),
          groups_idxs_(groups_idxs),
          order_idxs_(orders_idxs),
          is_asc_(is_asc) {}
    ~GroupAndSortRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "GROUP_AND_SORT";
        Runner::Print(output, tab);
    }

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> groups_idxs_;
    const std::vector<int32_t> order_idxs_;
    const bool is_asc_;
};

class TableProjectRunner : public Runner {
 public:
    TableProjectRunner(const int32_t id, const int8_t* fn,
                       const Schema& fn_schema, const int32_t limit_cnt)
        : Runner(id, fn, fn_schema, limit_cnt) {}
    ~TableProjectRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "TABLE_PROJECT";
        Runner::Print(output, tab);
    }

    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};

class RowProjectRunner : public Runner {
 public:
    RowProjectRunner(const int32_t id, const int8_t* fn,
                     const Schema& fn_schema, const int32_t limit_cnt)
        : Runner(id, fn, fn_schema, limit_cnt) {}
    ~RowProjectRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "ROW_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};

class GroupAggRunner : public Runner {
 public:
    GroupAggRunner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
                   const int32_t limit_cnt)
        : Runner(id, fn, fn_schema, limit_cnt) {}
    ~GroupAggRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "GROUP_AGG_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};

class AggRunner : public Runner {
 public:
    AggRunner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
              const int32_t limit_cnt)
        : Runner(id, fn, fn_schema, limit_cnt) {}
    ~AggRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "AGG_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
};
class WindowAggRunner : public Runner {
 public:
    WindowAggRunner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
                    const int32_t limit_cnt,
                    const std::vector<int32_t>& groups_idxs,
                    const std::vector<int32_t>& orders_idxs,
                    const int64_t start_offset, const int64_t end_offset)
        : Runner(id, fn, fn_schema, limit_cnt),
          groups_idxs_(groups_idxs),
          order_idxs_(orders_idxs),
          start_offset_(start_offset),
          end_offset_(end_offset) {}
    ~WindowAggRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "WINDOW_AGG_PROJECT";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> groups_idxs_;
    const std::vector<int32_t> order_idxs_;
    const int64_t start_offset_;
    const int64_t end_offset_;
};

class RequestUnionRunner : public Runner {
 public:
    RequestUnionRunner(const int32_t id, const int8_t* fn,
                       const Schema& fn_schema, const int32_t limit_cnt,
                       const std::vector<int32_t>& groups_idxs,
                       const std::vector<int32_t>& orders_idxs,
                       const std::vector<int32_t>& keys_idxs, const bool is_asc,
                       const int64_t start_offset, const int64_t end_offset)
        : Runner(id, fn, fn_schema, limit_cnt),
          is_asc_(is_asc),
          groups_idxs_(groups_idxs),
          orders_idxs_(orders_idxs),
          keys_idxs_(keys_idxs),
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
    const std::vector<int32_t> groups_idxs_;
    const std::vector<int32_t> orders_idxs_;
    const std::vector<int32_t> keys_idxs_;
    const int64_t start_offset_;
    const int64_t end_offset_;
};

class RequestLastJoinRunner : public Runner {
 public:
    RequestLastJoinRunner(const int32_t id, const int8_t* fn,
                          const Schema& fn_schema, const int32_t limit_cnt,
                          const std::vector<int32_t>& condition_idxs)
        : Runner(id, fn, fn_schema, limit_cnt),
          condition_idxs_(condition_idxs) {}
    ~RequestLastJoinRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "REQUEST_LASTJOIN";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> condition_idxs_;
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
class IndexSeekRunner : public Runner {
 public:
    IndexSeekRunner(const int32_t id, const int8_t* fn, const Schema& fn_schema,
                    const int32_t limit_cnt,
                    const std::vector<int32_t>& keys_idxs)
        : Runner(id, fn, fn_schema, limit_cnt), keys_idxs_(keys_idxs) {}

    ~IndexSeekRunner() {}
    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << "[" << id_ << "]"
               << "INDEX_SEEK";
        Runner::Print(output, tab);
    }
    std::shared_ptr<DataHandler> Run(RunnerContext& ctx) override;  // NOLINT
    const std::vector<int32_t> keys_idxs_;
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
