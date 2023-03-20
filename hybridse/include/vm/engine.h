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

#ifndef HYBRIDSE_INCLUDE_VM_ENGINE_H_
#define HYBRIDSE_INCLUDE_VM_ENGINE_H_

#include <map>
#include <memory>
#include <mutex>  //NOLINT
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>
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

inline constexpr const char* LONG_WINDOWS = "long_windows";

class Engine;
/// \brief An options class for controlling engine behaviour.
class EngineOptions {
 public:
    EngineOptions();

    /// Set `true` to enable storing ir results into SqlContext, default `false`.
    inline void SetKeepIr(bool flag) { this->keep_ir_ = flag; }
    /// Return if support to store ir results into SqlContext.
    inline bool IsKeepIr() const { return this->keep_ir_; }

    /// Set `true` if only support to compile SQL, default `false`
    ///
    /// If set `true`, the engine won't generate runner plan as well.
    inline void SetCompileOnly(bool flag) { this->compile_only_ = flag; }
    /// Return if only support to compile physical plan.
    inline bool IsCompileOnly() const { return compile_only_; }


    /// Set `true` if the engine only generate physical plan, default `false`.
    ///
    /// If set `true`, the engine won't build llvm jit.
    inline void SetPlanOnly(bool flag) { plan_only_ = flag; }
    /// Return `true` if the engine only generate physical plan.
    inline bool IsPlanOnly() const { return plan_only_; }

    /// Set `true` to enable cluster optimization, default `false`
    inline EngineOptions* SetClusterOptimized(bool flag) {
        cluster_optimized_ = flag;
        return this;
    }
    /// Return if the engine support cluster optimization.
    inline bool IsClusterOptimzied() const { return cluster_optimized_; }

    /// Set `true` to enable batch request optimization, default `true`.
    inline EngineOptions* SetBatchRequestOptimized(bool flag) {
        batch_request_optimized_ = flag;
        return this;
    }
    /// Return if the engine support batch request optimization.
    inline bool IsBatchRequestOptimized() const { return batch_request_optimized_; }

    /// Set `true` to enable expression optimization, default `true`.
    inline EngineOptions* SetEnableExprOptimize(bool flag) {
        enable_expr_optimize_ = flag;
        return this;
    }
    /// Return if the engine support expression optimization
    inline bool IsEnableExprOptimize() const { return enable_expr_optimize_; }

    /// Set `true` to enable batch window parallelization, default `false`.
    inline EngineOptions* SetEnableBatchWindowParallelization(bool flag) {
        enable_batch_window_parallelization_ = flag;
        return this;
    }
    /// Return if the engine support batch window parallelization.
    inline bool IsEnableBatchWindowParallelization() const {
        return enable_batch_window_parallelization_;
    }

    /// Set `true` to enable window column purning
    inline EngineOptions* SetEnableWindowColumnPruning(bool flag) {
        enable_window_column_pruning_ = flag;
        return this;
    }
    /// Return if the engine window column purning
    inline bool IsEnableWindowColumnPruning() const {
        return enable_window_column_pruning_;
    }

    /// Set the maximum number of cache entries, default is `50`.
    inline void SetMaxSqlCacheSize(uint32_t size) {
        max_sql_cache_size_ = size;
    }
    /// Return the maximum number of entries we can hold for compiling cache.
    inline uint32_t GetMaxSqlCacheSize() const { return max_sql_cache_size_; }

    /// Return JitOptions
    inline hybridse::vm::JitOptions& jit_options() { return jit_options_; }

 private:
    bool keep_ir_;
    bool compile_only_;
    bool plan_only_;
    bool cluster_optimized_;
    bool batch_request_optimized_;
    bool enable_expr_optimize_;
    bool enable_batch_window_parallelization_;
    bool enable_window_column_pruning_;
    uint32_t max_sql_cache_size_;
    JitOptions jit_options_;
};

/// \brief A RunSession maintain SQL running context, including compile information, procedure name.
///
class RunSession {
 public:
    explicit RunSession(EngineMode engine_mode);

    virtual ~RunSession();

    /// Return query result schema.
    virtual const Schema& GetSchema() const {
        return compile_info_->GetSchema();
    }

    /// Return query schema string.
    virtual const std::string& GetEncodedSchema() const {
        return compile_info_->GetEncodedSchema();
    }

    /// Return query related compile information.
    virtual std::shared_ptr<hybridse::vm::CompileInfo> GetCompileInfo() {
        return compile_info_;
    }

    /// Update query related compile information.
    bool SetCompileInfo(
        const std::shared_ptr<hybridse::vm::CompileInfo>& compile_info);

    /// Enable printing debug information while running a query.
    void EnableDebug() { is_debug_ = true; }
    /// Disable printing debug information while running a query.
    void DisableDebug() { is_debug_ = false; }
    /// Return if this run session support printing debug information.
    bool IsDebug() { return is_debug_; }

    /// Bind this run session with specific procedure
    void SetSpName(const std::string& sp_name) { sp_name_ = sp_name; }
    /// Return the engine mode of this run session
    EngineMode engine_mode() const { return engine_mode_; }

    const std::shared_ptr<const std::unordered_map<std::string, std::string>>& GetOptions() const {
        return options_;
    }

    void SetOptions(const std::shared_ptr<const std::unordered_map<std::string, std::string>>& options) {
        options_ = options;
    }

 protected:
    std::shared_ptr<hybridse::vm::CompileInfo> compile_info_;
    hybridse::vm::EngineMode engine_mode_;
    bool is_debug_;
    std::string sp_name_;
    std::shared_ptr<const std::unordered_map<std::string, std::string>> options_ = nullptr;
    friend Engine;
};

/// \brief BatchRunSession is a kind of RunSession designed for batch mode query.
class BatchRunSession : public RunSession {
 public:
    explicit BatchRunSession(bool mini_batch = false)
        : RunSession(kBatchMode), parameter_schema_() {}
    ~BatchRunSession() {}
    /// \brief Query sql with parameter row in batch mode.
    /// Query results will be returned as std::vector<Row> in output
    int32_t Run(const Row& parameter_row, std::vector<Row>& output,  // NOLINT
                uint64_t limit = 0);

    /// \brief Query sql in batch mode.
    /// Query results will be returned as std::vector<Row> in output
    int32_t Run(std::vector<Row>& output,  // NOLINT
                uint64_t limit = 0);
    /// Bing the run session with specific parameter schema
    void SetParameterSchema(const codec::Schema& schema) { parameter_schema_ = schema; }
    /// Return query parameter schema.
    virtual const Schema& GetParameterSchema() const { return parameter_schema_; }
 private:
    codec::Schema parameter_schema_;
};

/// \brief MockRequestRunSession is a kind of mock RuSession design for request query
/// disable performance sensitive. Since it is a mock RunSession for SQL compiling and explain
/// we are not going to support Run() method for it
class MockRequestRunSession : public RunSession {
 public:
    MockRequestRunSession() : RunSession(kMockRequestMode) {
    }
    ~MockRequestRunSession() {}
    /// \brief Return the schema of request row
    virtual const Schema& GetRequestSchema() const {
        return compile_info_->GetRequestSchema();
    }
    /// \brief Return the name of request table name
    virtual const std::string& GetRequestName() const {
        return compile_info_->GetRequestName();
    }
    /// \brief Return the name of request table db
    virtual const std::string& GetRequestDbName() const {
        return compile_info_->GetRequestDbName();
    }
};
/// \brief RequestRunSession is a kind of RunSession designed for request mode query.
///
/// Request-mode query is widely used in OLAD database. It requires a request Row.
class RequestRunSession : public RunSession {
 public:
    RequestRunSession() : RunSession(kRequestMode) {}
    ~RequestRunSession() {}
    /// \brief Query sql in request mode.
    ///
    /// \param in_row request row
    /// \param output query result will be returned as Row in output
    /// \return `0` if run successfully else negative integer
    int32_t Run(const Row& in_row, Row* output);  // NOLINT

    /// \brief Run a task specified by task_id in request mode.
    ///
    /// \param task_id: task id of task
    /// \param in_row: request row
    /// \param[out] output: result is written to this variable
    /// \return `0` if run successfully else negative integer
    int32_t Run(uint32_t task_id, const Row& in_row, Row* output);  // NOLINT

    /// \brief Return the schema of request row
    virtual const Schema& GetRequestSchema() const {
        return compile_info_->GetRequestSchema();
    }
    /// \brief Return the name of request table name
    virtual const std::string& GetRequestName() const {
        return compile_info_->GetRequestName();
    }
    /// \brief Return the name of request table db
    virtual const std::string& GetRequestDbName() const {
        return compile_info_->GetRequestDbName();
    }
};
/// \brief BatchRequestRunSession is a kind of RunSession designed for batch request mode query.
///
/// BatchRequest mode query is widely used in OLAD database. It requires a batch of request Rows.
class BatchRequestRunSession : public RunSession {
 public:
    BatchRequestRunSession() : RunSession(kBatchRequestMode) {}
    ~BatchRequestRunSession() {}

    /// \brief Return the schema of request row
    const Schema& GetRequestSchema() const {
        return compile_info_->GetRequestSchema();
    }
    /// \brief Return the name of request table name
    const std::string& GetRequestName() const {
        return compile_info_->GetRequestName();
    }
    /// \brief Return the name of request db name
    const std::string& GetRequestDbName() const {
        return compile_info_->GetRequestDbName();
    }

    /// \brief Run query in batch request mode.
    /// \param request_batch: a batch of request rows
    /// \param output: query results will be returned as std::vector<Row> in output
    /// \return 0 if runs successfully else negative integer
    int32_t Run(const std::vector<Row>& request_batch, std::vector<Row>& output);  // NOLINT

    /// \brief Run a task specified by task_id in request mode.
    /// \param id: id of task
    /// \param request_batch: a batch of request rows
    /// \param output: query results will be returned as std::vector<Row> in output
    /// \return 0 if runs successfully else negative integer
    int32_t Run(const uint32_t id, const std::vector<Row>& request_batch, std::vector<Row>& output);  // NOLINT

    /// \brief Add common column idx
    void AddCommonColumnIdx(size_t idx) { common_column_indices_.insert(idx); }

    /// \brief Return a set of common column indices
    const std::set<size_t>& common_column_indices() const {
        return common_column_indices_;
    }

 private:
    std::set<size_t> common_column_indices_;
};

/// An options class for controlling runtime interpreter behavior.
struct ExplainOutput {
    vm::Schema input_schema;      ///< The schema of request row for request-mode query
    std::string request_db_name;  ///< The name of request db for request-mode query
    std::string request_name;     ///< The name of request for request-mode query
    std::string logical_plan;     ///< Logical plan string
    std::string physical_plan;    ///< Physical plan string
    std::string ir;               ///< Codegen IR String
    vm::Schema output_schema;     ///< The schema of query result
    vm::Router router;            ///< The Router for request-mode query
    uint32_t limit_cnt;           ///< The limit count
    std::set<std::pair<std::string, std::string>> dependent_tables;
};


/// \brief An engine is responsible to compile SQL on the specific Catalog.
///
/// An engine can be used to `compile sql and explain the compiling result.
/// It maintains a LRU cache for compiling result.
///
/// **Example**
/// ```
/// // Assuming the catalog has been created and initialized before
/// base::Status status;
/// EngineOptions options;
/// Engine engine(catalog, options);
/// BatchRunSession session;
/// std::string db = "test_db";
/// std::string sql = "select col0, col1, col2, col1+col2 as col12 from t1;";
/// engine.Get(sql, db, session, status);
/// engine.Explain(sql, db, EngineMode::kBatchMode, &output, &status);
/// ```
class Engine {
 public:
    /// \brief Create an Engine with a specific Catalog object.
    explicit Engine(const std::shared_ptr<Catalog>& cl);

    /// \brief Create an Engine a specific Catalog object, configuring it with EngineOptions
    Engine(const std::shared_ptr<Catalog>& cl, const EngineOptions& options);

    /// \brief Initialize LLVM environments
    static void InitializeGlobalLLVM();

    static void InitializeUnsafeRowOptFlag(bool isUnsafeRowOpt);

    ~Engine();

    /// \brief Compile sql in db and stored the results in the session
    bool Get(const std::string& sql, const std::string& db,
             RunSession& session,    // NOLINT
             base::Status& status);  // NOLINT

    /// \brief Search all tables related to the specific sql in db.
    ///
    /// The tables' names are returned in tables
    bool GetDependentTables(const std::string& sql, const std::string& db,
                            std::set<std::pair<std::string, std::string>>* db_tables,
                            base::Status& status);  // NOLINT

    /// \brief Explain sql compiling result.
    ///
    /// The results are returned as ExplainOutput in explain_output.
    /// The success or fail status message is returned as Status in status.
    /// TODO: base::Status* status -> base::Status& status
    bool Explain(const std::string& sql, const std::string& db,
                 EngineMode engine_mode, ExplainOutput* explain_output, base::Status* status);
    /// \brief Explain sql compiling result.
    ///
    /// The results are returned as ExplainOutput in explain_output.
    /// The success or fail status message is returned as Status in status.
    /// TODO: base::Status* status -> base::Status& status
    bool Explain(const std::string& sql, const std::string& db,
                 EngineMode engine_mode, const codec::Schema& parameter_schema,
                 ExplainOutput* explain_output,
                 base::Status* status);

    static base::Status RegisterExternalFunction(const std::string& name,
                                     node::DataType return_type, bool return_nullable,
                                     const std::vector<node::DataType>& arg_types, bool arg_nullable,
                                     bool is_aggregate, const std::string& file);

    static base::Status RemoveExternalFunction(const std::string& name,
                                     const std::vector<node::DataType>& arg_types,
                                     const std::string& file);

    /// \brief Same as above, but allowing compiling with configuring common column indices.
    ///
    /// The common column indices are used for common column optimization under EngineMode::kBatchRequestMode
    bool Explain(const std::string& sql, const std::string& db, EngineMode engine_mode,
                 const std::set<size_t>& common_column_indices, ExplainOutput* explain_output, base::Status* status);

    /// \brief Update engine's catalog
    inline void UpdateCatalog(std::shared_ptr<Catalog> cl) {
        std::atomic_store_explicit(&cl_, cl, std::memory_order_release);
    }

    /// \brief Clear engine's compiling result cache
    void ClearCacheLocked(const std::string& db);

    /// \brief Get engine's options
    EngineOptions GetEngineOptions();

 private:
    // Get all dependent (db, table) info from physical plan
    Status GetDependentTables(const PhysicalOpNode*, std::set<std::pair<std::string, std::string>>*);

    std::shared_ptr<CompileInfo> GetCacheLocked(const std::string& db,
                                                const std::string& sql,
                                                EngineMode engine_mode);
    bool SetCacheLocked(const std::string& db, const std::string& sql,
                        EngineMode engine_mode,
                        std::shared_ptr<CompileInfo> info);

    bool IsCompatibleCache(RunSession& session,  // NOLINT
                           std::shared_ptr<CompileInfo> info,
                           base::Status& status);  // NOLINT

    bool Explain(const std::string& sql, const std::string& db,
                 EngineMode engine_mode, const codec::Schema& parameter_schema,
                 const std::set<size_t>& common_column_indices,
                 ExplainOutput* explain_output, base::Status* status);
    std::shared_ptr<Catalog> cl_;
    EngineOptions options_;
    base::SpinMutex mu_;
    EngineLRUCache lru_cache_;
};

/// \brief Local tablet is responsible to run a task locally.
///
/// Local tablet won't invoke rpc to run a task remotely.
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

    /// Run a task in request mode locally
    /// \param task_id: id of task
    /// \param db: name of database
    /// \param sql: represents a sql string if `is_procedure` is ture, or represents a procedure name
    /// \param row: request row
    /// \param is_procedure: whether sql is a procedure or not
    /// \param is_debug: whether printing debug information while running
    /// \return result row as RowHandler pointer
    std::shared_ptr<RowHandler> SubQuery(uint32_t task_id,
                                         const std::string& db,
                                         const std::string& sql, const Row& row,
                                         const bool is_procedure,
                                         const bool is_debug) override;

    /// Run a task in batch-request mode locally
    /// \param task_id: id of task
    /// \param db: name of database
    /// \param sql: represents a sql string if `is_procedure` is ture, or represents a procedure name
    /// \param common_column_indices: a set of common column indices
    /// \param in_rows: a batch of request rows
    /// \param request_is_common: whether request is common or not
    /// \param is_procedure: whether run procedure or not
    /// \param is_debug: whether printing debug information while running
    /// \return result rows as TableHandler pointer
    virtual std::shared_ptr<TableHandler> SubQuery(
        uint32_t task_id, const std::string& db, const std::string& sql,
        const std::set<size_t>& common_column_indices,
        const std::vector<Row>& in_rows, const bool request_is_common,
        const bool is_procedure, const bool is_debug);

    /// Return the name of tablet
    const std::string& GetName() const { return name_; }

 private:
    const std::string name_;
    vm::Engine* engine_;
    std::shared_ptr<hybridse::vm::CompileInfoCache> sp_cache_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_VM_ENGINE_H_
