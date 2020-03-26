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

#include <storage/codec.h>
#include <map>
#include <memory>
#include <mutex>  //NOLINT
#include <string>
#include <vector>
#include "base/spin_lock.h"
#include "proto/common.pb.h"
#include "vm/catalog.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace vm {

using ::fesql::storage::Row;

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

 protected:
    inline void SetCompileInfo(
        const std::shared_ptr<CompileInfo>& compile_info) {
        compile_info_ = compile_info;
    }
    inline void SetCatalog(const std::shared_ptr<Catalog>& cl) { cl_ = cl; }
    std::shared_ptr<CompileInfo> compile_info_;
    std::shared_ptr<Catalog> cl_;
    friend Engine;
    std::shared_ptr<TableHandler> RunBatchPlan(const PhysicalOpNode* node);
    base::Slice WindowProject(const int8_t* fn,
                                          const uint64_t key,
                                          const base::Slice slice,
                                          storage::Window* window);
    base::Slice RowProject(const int8_t* fn, const base::Slice slice);
    base::Slice AggProject(const int8_t* fn,
                           const std::shared_ptr<TableHandler> table);
    std::string GetColumnString(fesql::storage::RowView* view, int pos,
                                type::Type type);
    int64_t GetColumnInt64(fesql::storage::RowView* view, int pos,
                           type::Type type);
    std::shared_ptr<TableHandler> TableGroup(
        const std::shared_ptr<TableHandler> table, const Schema& schema,
        const int8_t* fn, const std::vector<int>& idxs);
    std::shared_ptr<TableHandler> PartitionGroup(
        const std::shared_ptr<TableHandler> partitions, const Schema& schema,
        const int8_t* fn, const std::vector<int>& idxs);
    std::shared_ptr<TableHandler> TableSortGroup(
        std::shared_ptr<TableHandler> table, const Schema& schema,
        const int8_t* fn, const node::ExprListNode* groups,
        const node::OrderByNode* orders);
    std::shared_ptr<TableHandler> PartitionSort(
        std::shared_ptr<TableHandler> table, const Schema& schema,
        const int8_t* fn, std::vector<int> idxs);
    std::shared_ptr<TableHandler> TableSort(std::shared_ptr<TableHandler> table,
                                            const Schema& schema,
                                            const int8_t* fn,
                                            std::vector<int> idxs);
};

class BatchRunSession : public RunSession {
 public:
    BatchRunSession(bool mini_batch_ = false)
        : RunSession(), mini_batch_(mini_batch_) {}
    ~BatchRunSession() {}
    virtual int32_t Run(std::vector<int8_t*>& buf, uint64_t limit);  // NOLINT
 private:
    const bool mini_batch_;
};

class RequestRunSession : public RunSession {
 public:
    RequestRunSession() : RunSession() {}
    ~RequestRunSession() {}
    virtual int32_t Run(const Row& in_row, Row& out_row);  // NOLINT
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<CompileInfo>>>
    EngineCache;
class Engine {
 public:
    explicit Engine(const std::shared_ptr<Catalog>& cl,
                    const node::PlanModeType mode = node::kPlanModeBatch);

    ~Engine();

    bool Get(const std::string& sql, const std::string& db,
             RunSession& session,    // NOLINT
             base::Status& status);  // NOLINT

    std::shared_ptr<CompileInfo> GetCacheLocked(const std::string& db,
                                                const std::string& sql);

 private:
    const std::shared_ptr<Catalog> cl_;
    const node::PlanModeType mode_;
    base::SpinMutex mu_;
    EngineCache cache_;
    ::fesql::node::NodeManager nm_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_ENGINE_H_
