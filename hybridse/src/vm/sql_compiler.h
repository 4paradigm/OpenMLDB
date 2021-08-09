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

#ifndef SRC_VM_SQL_COMPILER_H_
#define SRC_VM_SQL_COMPILER_H_

#include <memory>
#include <set>
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "llvm/IR/Module.h"
#include "proto/fe_common.pb.h"
#include "udf/udf_library.h"
#include "vm/catalog.h"
#include "vm/engine_context.h"
#include "vm/jit_wrapper.h"
#include "vm/physical_op.h"
#include "vm/runner.h"

namespace hybridse {
namespace vm {

using hybridse::base::Status;

struct SqlContext {
    // mode: batch|request|batch request
    ::hybridse::vm::EngineMode engine_mode;
    bool is_performance_sensitive = false;
    bool is_cluster_optimized = false;
    bool is_batch_request_optimized = false;
    bool enable_expr_optimize = false;
    bool enable_batch_window_parallelization = false;

    // the sql content
    std::string sql;
    // the database
    std::string db;
    // the logical plan
    ::hybridse::node::PlanNodeList logical_plan;
    ::hybridse::vm::PhysicalOpNode* physical_plan = nullptr;
    hybridse::vm::ClusterJob cluster_job;
    // TODO(wangtaize) add a light jit engine
    // eg using bthead to compile ir
    hybridse::vm::JitOptions jit_options;
    std::shared_ptr<hybridse::vm::HybridSeJitWrapper> jit = nullptr;
    Schema schema;
    Schema request_schema;
    std::string request_name;
    Schema parameter_types;
    uint32_t row_size;
    std::string ir;
    std::string logical_plan_str;
    std::string physical_plan_str;
    std::string encoded_schema;
    std::string encoded_request_schema;
    ::hybridse::node::NodeManager nm;
    ::hybridse::udf::UdfLibrary* udf_library = nullptr;

    ::hybridse::vm::BatchRequestInfo batch_request_info;

    SqlContext() {}
    ~SqlContext() {}
};

class SqlCompileInfo : public CompileInfo {
 public:
    SqlCompileInfo() : sql_ctx() {}
    virtual ~SqlCompileInfo() {}
    hybridse::vm::SqlContext& get_sql_context() { return this->sql_ctx; }

    bool GetIRBuffer(const base::RawBuffer& buf) {
        auto& str = this->sql_ctx.ir;
        return buf.CopyFrom(str.data(), str.size());
    }
    size_t GetIRSize() { return this->sql_ctx.ir.size(); }

    const hybridse::vm::Schema& GetSchema() const { return sql_ctx.schema; }

    const hybridse::vm::ComileType GetCompileType() const {
        return ComileType::kCompileSql;
    }
    const hybridse::vm::EngineMode GetEngineMode() const {
        return sql_ctx.engine_mode;
    }
    const std::string& GetEncodedSchema() const {
        return sql_ctx.encoded_schema;
    }

    const std::string& GetSql() const { return sql_ctx.sql; }

    virtual const Schema& GetRequestSchema() const {
        return sql_ctx.request_schema;
    }
    virtual const Schema& GetParameterSchema() const {
        return sql_ctx.parameter_types;
    }
    virtual const std::string& GetRequestName() const {
        return sql_ctx.request_name;
    }
    virtual const hybridse::vm::BatchRequestInfo& GetBatchRequestInfo() const {
        return sql_ctx.batch_request_info;
    }
    virtual const hybridse::vm::PhysicalOpNode* GetPhysicalPlan() const {
        return sql_ctx.physical_plan;
    }
    virtual hybridse::vm::Runner* GetMainTask() {
        return sql_ctx.cluster_job.GetMainTask().GetRoot();
    }
    virtual hybridse::vm::ClusterJob& GetClusterJob() {
        return sql_ctx.cluster_job;
    }
    virtual void DumpPhysicalPlan(std::ostream& output,
                                  const std::string& tab) {
        sql_ctx.physical_plan->Print(output, tab);
    }
    virtual void DumpClusterJob(std::ostream& output, const std::string& tab) {
        sql_ctx.cluster_job.Print(output, tab);
    }
    static SqlCompileInfo* CastFrom(CompileInfo* node) {
        return dynamic_cast<SqlCompileInfo*>(node);
    }

 private:
    hybridse::vm::SqlContext sql_ctx;
};

class SqlCompiler {
 public:
    SqlCompiler(const std::shared_ptr<Catalog>& cl, bool keep_ir = false,
                bool dump_plan = false, bool plan_only = false);

    ~SqlCompiler();

    bool Compile(SqlContext& ctx,                 // NOLINT
                 Status& status);                 // NOLINT
    bool Parse(SqlContext& ctx, Status& status);  // NOLINT
    bool BuildClusterJob(SqlContext& ctx,         // NOLINT
                         Status& status);         // NOLINT

 private:
    void KeepIR(SqlContext& ctx, llvm::Module* m);  // NOLINT

    bool ResolvePlanFnAddress(
        PhysicalOpNode* node,
        std::shared_ptr<HybridSeJitWrapper>& jit,  // NOLINT
        Status& status);                           // NOLINT

    Status BuildPhysicalPlan(SqlContext* ctx,
                             const ::hybridse::node::PlanNodeList& plan_list,
                             ::llvm::Module* llvm_module,
                             PhysicalOpNode** output);
    Status BuildBatchModePhysicalPlan(
        SqlContext* ctx, const ::hybridse::node::PlanNodeList& plan_list,
        ::llvm::Module* llvm_module, udf::UdfLibrary* library,
        PhysicalOpNode** output);
    Status BuildRequestModePhysicalPlan(
        SqlContext* ctx, const ::hybridse::node::PlanNodeList& plan_list,
        ::llvm::Module* llvm_module, udf::UdfLibrary* library,
        PhysicalOpNode** output);
    Status BuildBatchRequestModePhysicalPlan(
        SqlContext* ctx, const ::hybridse::node::PlanNodeList& plan_list,
        ::llvm::Module* llvm_module, udf::UdfLibrary* library,
        PhysicalOpNode** output);

 private:
    const std::shared_ptr<Catalog> cl_;
    bool keep_ir_;
    bool dump_plan_;
    bool plan_only_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // SRC_VM_SQL_COMPILER_H_
