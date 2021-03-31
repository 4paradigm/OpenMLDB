/*
 * Copyright 2021 4Paradigm
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

#ifndef INCLUDE_VM_ENGINE_H_
#define INCLUDE_VM_ENGINE_H_

#include <map>
#include <memory>
#include <mutex>  //NOLINT
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/raw_buffer.h"
#include "base/spin_lock.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "gflags/gflags.h"
#include "llvm-c/Target.h"
#include "proto/fe_common.pb.h"
#include "vm/catalog.h"
#include "vm/engine_context.h"
#include "vm/router.h"

namespace hybridse {
namespace vm {

using ::hybridse::codec::Row;

class Engine;

class EngineOptions {
 public:
    EngineOptions();
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

    inline bool is_cluster_optimzied() const { return cluster_optimized_; }
    inline EngineOptions* set_cluster_optimized(bool flag) {
        cluster_optimized_ = flag;
        return this;
    }
    bool is_batch_request_optimized() const { return batch_request_optimized_; }
    EngineOptions* set_batch_request_optimized(bool flag) {
        batch_request_optimized_ = flag;
        return this;
    }
    inline void set_max_sql_cache_size(uint32_t size) {
        max_sql_cache_size_ = size;
    }
    static EngineOptions NewEngineOptionWithClusterEnable(bool flag) {
        EngineOptions options;
        options.set_cluster_optimized(flag);
        LOG(INFO) << "Engine Options with cluster_optimized_ " << flag;
        return options;
    }

    bool is_enable_expr_optimize() const { return enable_expr_optimize_; }
    inline EngineOptions* set_enable_expr_optimize(bool flag) {
        enable_expr_optimize_ = flag;
        return this;
    }

    inline EngineOptions* set_enable_batch_window_parallelization(bool flag) {
        enable_batch_window_parallelization_ = flag;
        return this;
    }
    inline bool is_enable_batch_window_parallelization() const {
        return enable_batch_window_parallelization_;
    }

    EngineOptions* set_enable_spark_unsaferow_format(bool flag);

    inline bool is_enable_spark_unsaferow_format() const {
        return enable_spark_unsaferow_format_;
    }

    hybridse::vm::JITOptions& jit_options() { return jit_options_; }

 private:
    bool keep_ir_;
    bool compile_only_;
    bool plan_only_;
    bool performance_sensitive_;
    bool cluster_optimized_;
    bool batch_request_optimized_;
    bool enable_expr_optimize_;
    bool enable_batch_window_parallelization_;
    uint32_t max_sql_cache_size_;
    bool enable_spark_unsaferow_format_;
    JITOptions jit_options_;
};

class RunSession {
 public:
    explicit RunSession(EngineMode engine_mode);

    virtual ~RunSession();

    virtual const Schema& GetSchema() const {
        return compile_info_->GetSchema();
    }

    virtual const std::string& GetEncodedSchema() const {
        return compile_info_->GetEncodedSchema();
    }

    virtual std::shared_ptr<hybridse::vm::CompileInfo> GetCompileInfo() {
        return compile_info_;
    }
    bool SetCompileInfo(
        const std::shared_ptr<hybridse::vm::CompileInfo>& compile_info);

    void EnableDebug() { is_debug_ = true; }
    void DisableDebug() { is_debug_ = false; }
    bool IsDebug() { return is_debug_; }

    void SetSpName(const std::string& sp_name) { sp_name_ = sp_name; }
    EngineMode engine_mode() const { return engine_mode_; }

 protected:
    std::shared_ptr<hybridse::vm::CompileInfo> compile_info_;
    hybridse::vm::EngineMode engine_mode_;
    bool is_debug_;
    std::string sp_name_;
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
        return compile_info_->GetRequestSchema();
    }
    virtual const std::string& GetRequestName() const {
        return compile_info_->GetRequestName();
    }
};

class BatchRequestRunSession : public RunSession {
 public:
    BatchRequestRunSession() : RunSession(kBatchRequestMode) {}
    ~BatchRequestRunSession() {}

    const Schema& GetRequestSchema() const {
        return compile_info_->GetRequestSchema();
    }
    const std::string& GetRequestName() const {
        return compile_info_->GetRequestName();
    }
    int32_t Run(const uint32_t id, const std::vector<Row>& request_batch,
                std::vector<Row>& output);  // NOLINT
    int32_t Run(const std::vector<Row>& request_batch,
                std::vector<Row>& output);  // NOLINT
    void AddCommonColumnIdx(size_t idx) { common_column_indices_.insert(idx); }

    const std::set<size_t>& common_column_indices() const {
        return common_column_indices_;
    }

 private:
    std::set<size_t> common_column_indices_;
};

struct ExplainOutput {
    // just for request mode
    vm::Schema input_schema;
    std::string logical_plan;
    std::string physical_plan;
    std::string ir;
    vm::Schema output_schema;
    std::string request_name;
    vm::Router router;
};

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
    bool Explain(const std::string& sql, const std::string& db,
                 EngineMode engine_mode,
                 const std::set<size_t>& common_column_indices,
                 ExplainOutput* explain_output, base::Status* status);

    inline void UpdateCatalog(std::shared_ptr<Catalog> cl) {
        std::atomic_store_explicit(&cl_, cl, std::memory_order_release);
    }

    void ClearCacheLocked(const std::string& db);

 private:
    bool GetDependentTables(node::PlanNode* node, std::set<std::string>* tables,
                            base::Status& status);  // NOLINT
    std::shared_ptr<CompileInfo> GetCacheLocked(const std::string& db,
                                                const std::string& sql,
                                                EngineMode engine_mode);
    bool SetCacheLocked(const std::string& db, const std::string& sql,
                        EngineMode engine_mode,
                        std::shared_ptr<CompileInfo> info);

    bool IsCompatibleCache(RunSession& session,  // NOLINT
                           std::shared_ptr<CompileInfo> info,
                           base::Status& status);  // NOLINT

    std::shared_ptr<Catalog> cl_;
    EngineOptions options_;
    base::SpinMutex mu_;
    EngineLRUCache lru_cache_;
};

class LocalTablet : public Tablet {
 public:
    explicit LocalTablet(
        hybridse::vm::Engine* engine,
        std::shared_ptr<hybridse::vm::CompileInfoCache> sp_cache)
        : Tablet(),
          name_("LocalTablet"),
          engine_(engine),
          sp_cache_(sp_cache) {}
    ~LocalTablet() {}
    std::shared_ptr<RowHandler> SubQuery(uint32_t task_id,
                                         const std::string& db,
                                         const std::string& sql, const Row& row,
                                         const bool is_procedure,
                                         const bool is_debug) override;
    virtual std::shared_ptr<TableHandler> SubQuery(
        uint32_t task_id, const std::string& db, const std::string& sql,
        const std::set<size_t>& common_column_indices,
        const std::vector<Row>& in_rows, const bool request_is_common,
        const bool is_procedure, const bool is_debug);
    const std::string& GetName() const { return name_; }

 private:
    const std::string name_;
    vm::Engine* engine_;
    std::shared_ptr<hybridse::vm::CompileInfoCache> sp_cache_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // INCLUDE_VM_ENGINE_H_
