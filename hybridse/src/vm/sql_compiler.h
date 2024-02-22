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

#ifndef HYBRIDSE_SRC_VM_SQL_COMPILER_H_
#define HYBRIDSE_SRC_VM_SQL_COMPILER_H_

#include <memory>
#include <string>

#include "base/fe_status.h"
#include "llvm/IR/Module.h"
#include "udf/udf_library.h"
#include "vm/catalog.h"
#include "vm/cluster_task.h"
#include "vm/engine_context.h"
#include "vm/jit_wrapper.h"
#include "vm/physical_op.h"
#include "vm/physical_plan_context.h"
#include "vm/runner.h"
#include "vm/sql_ctx.h"

namespace hybridse {
namespace vm {

using hybridse::base::Status;

class SqlCompileInfo : public CompileInfo {
 public:
    SqlCompileInfo() : sql_ctx() {}
    ~SqlCompileInfo() override {}

    hybridse::vm::SqlContext& get_sql_context() { return this->sql_ctx; }

    bool GetIRBuffer(const base::RawBuffer& buf) override {
        auto& str = this->sql_ctx.ir;
        return buf.CopyFrom(str.data(), str.size());
    }
    size_t GetIRSize() override { return this->sql_ctx.ir.size(); }

    const hybridse::vm::Schema& GetSchema() const override { return sql_ctx.schema; }

    const hybridse::vm::ComileType GetCompileType() const override { return ComileType::kCompileSql; }
    const hybridse::vm::EngineMode GetEngineMode() const override { return sql_ctx.engine_mode; }
    const std::string& GetEncodedSchema() const override { return sql_ctx.encoded_schema; }

    const std::string& GetSql() const override { return sql_ctx.sql; }

    const Schema& GetRequestSchema() const override { return sql_ctx.request_schema; }
    const Schema& GetParameterSchema() const override { return sql_ctx.parameter_types; }
    const std::string& GetRequestName() const override { return sql_ctx.request_name; }
    const std::string& GetRequestDbName() const override { return sql_ctx.request_db_name; }
    const hybridse::vm::BatchRequestInfo& GetBatchRequestInfo() const override { return sql_ctx.batch_request_info; }
    const hybridse::vm::PhysicalOpNode* GetPhysicalPlan() const override { return sql_ctx.physical_plan; }
    hybridse::vm::Runner* GetMainTask() { return sql_ctx.cluster_job->GetMainTask().GetRoot(); }
    std::shared_ptr<hybridse::vm::ClusterJob> GetClusterJob() { return sql_ctx.cluster_job; }
    void DumpPhysicalPlan(std::ostream& output, const std::string& tab) override {
        sql_ctx.physical_plan->Print(output, tab);
    }
    void DumpClusterJob(std::ostream& output, const std::string& tab) override {
        sql_ctx.cluster_job->Print(output, tab);
    }
    static SqlCompileInfo* CastFrom(CompileInfo* node) { return dynamic_cast<SqlCompileInfo*>(node); }

 private:
    hybridse::vm::SqlContext sql_ctx;
};

class SqlCompiler {
 public:
    explicit SqlCompiler(const std::shared_ptr<Catalog>& cl, bool keep_ir = false,
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
        const bool enable_request_performance_sensitive,
        ::llvm::Module* llvm_module, udf::UdfLibrary* library, PhysicalOpNode** output);
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
#endif  // HYBRIDSE_SRC_VM_SQL_COMPILER_H_
