/*
 * engine.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_VM_ENGINE_H_
#define SRC_VM_ENGINE_H_

#include <map>
#include <memory>
#include <mutex>  //NOLINT
#include <string>
#include <vector>
#include "base/spin_lock.h"
#include "codec/list_iterator_codec.h"
#include "codec/row_codec.h"
#include "proto/common.pb.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace vm {

using ::fesql::base::Slice;
using ::fesql::codec::RowView;

class Engine;

struct CompileInfo {
    SQLContext sql_ctx;
};

class RunSession {
 public:
    RunSession();

    ~RunSession();

    virtual inline const Schema& GetSchema() const {
        return compile_info_->sql_ctx.schema;
    }
    virtual inline vm::PhysicalOpNode* GetPhysicalPlan() {
        return compile_info_->sql_ctx.plan;
    }
    virtual inline vm::Runner* GetRunner() {
        return compile_info_->sql_ctx.runner;
    }


    virtual const bool IsBatchRun() const = 0;

 protected:
    inline void SetCompileInfo(
        const std::shared_ptr<CompileInfo>& compile_info) {
        compile_info_ = compile_info;
    }
    inline void SetCatalog(const std::shared_ptr<Catalog>& cl) { cl_ = cl; }
    std::shared_ptr<CompileInfo> compile_info_;
    std::shared_ptr<Catalog> cl_;
    friend Engine;
    std::shared_ptr<DataHandler> RunPhysicalPlan(const PhysicalOpNode* node,
                                                 const Slice* in_row = nullptr);

    Slice WindowProject(const int8_t* fn, const uint64_t key, const Slice slice,
                        Window* window);
    Slice RowProject(const int8_t* fn, const Slice slice);
    Slice AggProject(const int8_t* fn,
                     const std::shared_ptr<DataHandler> table);
    std::string GetColumnString(RowView* view, int pos, type::Type type);
    int64_t GetColumnInt64(RowView* view, int pos, type::Type type);
    std::shared_ptr<DataHandler> TableGroup(
        const std::shared_ptr<DataHandler> table, const Schema& schema,
        const int8_t* fn, const std::vector<int>& idxs);
    std::shared_ptr<DataHandler> PartitionGroup(
        const std::shared_ptr<DataHandler> partitions, const Schema& schema,
        const int8_t* fn, const std::vector<int>& idxs);

    std::shared_ptr<DataHandler> PartitionSort(
        std::shared_ptr<DataHandler> table, const Schema& schema,
        const int8_t* fn, std::vector<int> idxs, const bool is_asc);
    std::shared_ptr<DataHandler> TableSort(std::shared_ptr<DataHandler> table,
                                           const Schema& schema,
                                           const int8_t* fn,
                                           std::vector<int> idxs,
                                           const bool is_asc);
    std::shared_ptr<DataHandler> TableProject(
        const int8_t* fn, std::shared_ptr<DataHandler> table,
        const int32_t limit_cnt, Schema output_schema);

    std::string GenerateKeys(RowView* row_view, const Schema& schema,
                             const std::vector<int>& idxs);

    std::shared_ptr<DataHandler> IndexSeek(std::shared_ptr<DataHandler> left,
                                           std::shared_ptr<DataHandler> right,
                                           const int8_t* fn,
                                           const Schema& fn_schema,
                                           const std::vector<int>& idxs);
    std::shared_ptr<DataHandler> RequestUnion(
        std::shared_ptr<DataHandler> left, std::shared_ptr<DataHandler> right,
        const int8_t* fn, const Schema& fn_schema,
        const std::vector<int>& groups_idxs,
        const std::vector<int>& orders_idxs, const std::vector<int>& keys_idxs,
        const int64_t start_offset, const int64_t end_offset);

    std::shared_ptr<DataHandler> WindowAggProject(
        std::shared_ptr<DataHandler> input, const int32_t limit_cnt,
        const int8_t* fn, const Schema& fn_schema, const int64_t start_offset,
        const int64_t end_offset);

    std::shared_ptr<DataHandler> TableSortGroup(
        std::shared_ptr<DataHandler> table, const int8_t* fn,
        const Schema& schema, const std::vector<int>& groups_idxs,
        const std::vector<int>& orders_idxs, const bool is_asc);

    std::shared_ptr<DataHandler> Group(
        std::shared_ptr<DataHandler> input, const int8_t* fn,
        const Schema& fn_schema, const std::vector<int>& idxs);

    std::shared_ptr<DataHandler> TableSortGroup(
        std::shared_ptr<DataHandler> table,
        const PhysicalGroupAndSortNode* grouo_sort_op);
    std::shared_ptr<DataHandler> WindowAggProject(
        const PhysicalWindowAggrerationNode* op,
        std::shared_ptr<DataHandler> input, const int32_t limit_cnt);

    std::shared_ptr<DataHandler> IndexSeek(
        std::shared_ptr<DataHandler> left, std::shared_ptr<DataHandler> right,
        const PhysicalSeekIndexNode* seek_op);
    std::shared_ptr<DataHandler> RequestUnion(
        std::shared_ptr<DataHandler> left, std::shared_ptr<DataHandler> right,
        const PhysicalRequestUnionNode* request_union_op);
    std::shared_ptr<DataHandler> Group(std::shared_ptr<DataHandler> input,
                                       const PhysicalGroupNode* op);
    std::shared_ptr<DataHandler> Limit(std::shared_ptr<DataHandler> input,
                                       const PhysicalLimitNode* op);
};

class BatchRunSession : public RunSession {
 public:
    explicit BatchRunSession(bool mini_batch = false)
        : RunSession(), mini_batch_(mini_batch) {}
    ~BatchRunSession() {}
    virtual int32_t Run(std::vector<int8_t*>& buf, uint64_t limit);  // NOLINT
    const bool IsBatchRun() const override { return true; }

 private:
    const bool mini_batch_;
};

class RequestRunSession : public RunSession {
 public:
    RequestRunSession() : RunSession() {}
    ~RequestRunSession() {}
    virtual int32_t Run(const Slice& in_row, Slice* output);  // NOLINT
    const bool IsBatchRun() const override { return false; }
    std::shared_ptr<TableHandler> RunRequestPlan(const Slice& request,
                                                 PhysicalOpNode* node);
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<CompileInfo>>>
    EngineCache;
class Engine {
 public:
    explicit Engine(const std::shared_ptr<Catalog>& cl);

    ~Engine();

    bool Get(const std::string& sql, const std::string& db,
             RunSession& session,    // NOLINT
             base::Status& status);  // NOLINT

    std::shared_ptr<CompileInfo> GetCacheLocked(const std::string& db,
                                                const std::string& sql);

 private:
    const std::shared_ptr<Catalog> cl_;
    base::SpinMutex mu_;
    EngineCache cache_;
    ::fesql::node::NodeManager nm_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_ENGINE_H_
