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
#include <set>
#include <string>
#include <vector>
#include "base/raw_buffer.h"
#include "base/spin_lock.h"
#include "boost/compute/detail/lru_cache.hpp"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "llvm-c/Target.h"
#include "proto/fe_common.pb.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace vm {

using ::fesql::codec::Row;
using ::fesql::codec::RowView;

class Engine;

class EngineOptions {
 public:
    EngineOptions()
        : keep_ir_(false),
          compile_only_(false),
          plan_only_(false),
          performance_sensitive_(true),
          max_sql_cache_size_(50) {}
    inline void set_keep_ir(bool flag) { this->keep_ir_ = flag; }
    inline bool is_keep_ir() const { return this->keep_ir_; }
    inline void set_compile_only(bool flag) { this->compile_only_ = flag; }
    inline bool is_compile_only() const { return compile_only_; }
    inline bool is_plan_only() const { return plan_only_; }
    inline void set_plan_only(bool flag) { plan_only_ = flag; }
    inline bool is_performance_sensitive() const {
        return performance_sensitive_;
    }
    inline uint32_t max_sql_cache_size() const { return max_sql_cache_size_; }
    inline void set_performance_sensitive(bool flag) {
        performance_sensitive_ = flag;
    }

    inline void set_max_sql_cache_size(uint32_t size) {
        max_sql_cache_size_ = size;
    }

 private:
    bool keep_ir_;
    bool compile_only_;
    bool plan_only_;
    bool performance_sensitive_;
    uint32_t max_sql_cache_size_;
};

class CompileInfo {
 public:
    SQLContext& get_sql_context() { return this->sql_ctx; }

    bool get_ir_buffer(const base::RawBuffer& buf) {
        auto& str = this->sql_ctx.ir;
        return buf.CopyFrom(str.data(), str.size());
    }

    size_t get_ir_size() { return this->sql_ctx.ir.size(); }

 private:
    SQLContext sql_ctx;
};

class RunSession {
 public:
    explicit RunSession(EngineMode engine_mode);

    virtual ~RunSession();

    virtual const Schema& GetSchema() const {
        return compile_info_->get_sql_context().schema;
    }

    virtual const std::string& GetEncodedSchema() const {
        return compile_info_->get_sql_context().encoded_schema;
    }

    virtual fesql::vm::PhysicalOpNode* GetPhysicalPlan() {
        return compile_info_->get_sql_context().physical_plan;
    }

    virtual fesql::vm::Runner* GetMainTask() {
        return compile_info_->get_sql_context().cluster_job.GetTask(0);
    }
    virtual fesql::vm::ClusterJob& GetClusterJob() {
        return compile_info_->get_sql_context().cluster_job;
    }

    virtual std::shared_ptr<CompileInfo> GetCompileInfo() {
        return compile_info_;
    }

    void EnableDebug() { is_debug_ = true; }
    void DisableDebug() { is_debug_ = false; }

    EngineMode engine_mode() const { return engine_mode_; }

    void SetIsProcedure(bool flag) { is_procedure_ = flag; }
    bool IsProcedure() { return is_procedure_; }

 protected:
    bool SetCompileInfo(const std::shared_ptr<CompileInfo>& compile_info);
    std::shared_ptr<CompileInfo> compile_info_;
    EngineMode engine_mode_;
    bool is_debug_;
    bool is_procedure_;
    friend Engine;
};

class BatchRunSession : public RunSession {
 public:
    explicit BatchRunSession(bool mini_batch = false)
        : RunSession(kBatchMode), mini_batch_(mini_batch) {}
    ~BatchRunSession() {}
    int32_t Run(std::vector<Row>& output,  // NOLINT
                uint64_t limit = 0);
    std::shared_ptr<TableHandler> Run();

 private:
    const bool mini_batch_;
};

class RequestRunSession : public RunSession {
 public:
    RequestRunSession() : RunSession(kRequestMode) {}
    ~RequestRunSession() {}
    int32_t Run(const Row& in_row, Row* output);                    // NOLINT
    int32_t Run(uint32_t task_id, const Row& in_row, Row* output);  // NOLINT
    virtual const Schema& GetRequestSchema() const {
        return compile_info_->get_sql_context().request_schema;
    }
    virtual const std::string& GetRequestName() const {
        return compile_info_->get_sql_context().request_name;
    }
};

class BatchRequestRunSession : public RunSession {
 public:
    BatchRequestRunSession() : RunSession(kBatchRequestMode) {}
    ~BatchRequestRunSession() {}

    const Schema& GetRequestSchema() const {
        return compile_info_->get_sql_context().request_schema;
    }
    const std::string& GetRequestName() const {
        return compile_info_->get_sql_context().request_name;
    }
    int32_t Run(const uint32_t id, const std::vector<Row>& request_batch,
                std::vector<Row>& output);  // NOLINT
    int32_t Run(const std::vector<Row>& request_batch,
                std::vector<Row>& output);  // NOLINT
    // TODO(baoxinqi): remove
    int32_t RunSingle(fesql::vm::RunnerContext& ctx,  // NOLINT
                      const Row& request,
                      Row* output);  // NOLINT
 private:
    int32_t RunSingle(fesql::vm::RunnerContext& ctx,  // NOLINT
                      const uint32_t id, const Row& request,
                      Row* output);  // NOLINT
};

struct ExplainOutput {
    // just for request mode
    vm::Schema input_schema;
    std::string logical_plan;
    std::string physical_plan;
    std::string ir;
    vm::Schema output_schema;
};

typedef std::map<
    EngineMode,
    std::map<std::string, boost::compute::detail::lru_cache<
                              std::string, std::shared_ptr<CompileInfo>>>>
    EngineLRUCache;

typedef std::map<std::string, std::map<
    std::string, std::shared_ptr<CompileInfo>>> ProcedureCache;

class Engine {
 public:
    Engine(const std::shared_ptr<Catalog>& cl, const EngineOptions& options);
    explicit Engine(const std::shared_ptr<Catalog>& cl);

    // Initialize LLVM environments
    static void InitializeGlobalLLVM();

    ~Engine();

    bool Get(const std::string& sql, const std::string& db,
             RunSession& session,    // NOLINT
             base::Status& status);  // NOLINT

    bool GetDependentTables(const std::string& sql, const std::string& db,
                            EngineMode engine_mode,
                            std::set<std::string>* tables,
                            base::Status& status);  // NOLINT
    bool Explain(const std::string& sql, const std::string& db,
                 EngineMode engine_mode, ExplainOutput* explain_output,
                 base::Status* status);
    inline void UpdateCatalog(std::shared_ptr<Catalog> cl) {
        std::atomic_store_explicit(&cl_, cl, std::memory_order_release);
    }

    void ClearCacheLocked(const std::string& db);

    void ClearSpCacheLocked(const std::string& db, const std::string& sp_name);

 private:
    bool GetDependentTables(node::PlanNode* node, std::set<std::string>* tables,
                            base::Status& status);  // NOLINT
    std::shared_ptr<CompileInfo> GetCacheLocked(const std::string& db,
                                                const std::string& sql,
                                                EngineMode engine_mode,
                                                bool is_procedure);
    bool SetCacheLocked(const std::string& db, const std::string& sql,
                        EngineMode engine_mode,
                        std::shared_ptr<CompileInfo> info,
                        bool is_procedure);

    std::shared_ptr<Catalog> cl_;
    EngineOptions options_;
    base::SpinMutex mu_;
    EngineLRUCache lru_cache_;
    ProcedureCache procedure_cache_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_ENGINE_H_
