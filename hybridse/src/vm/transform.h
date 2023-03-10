/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_VM_TRANSFORM_H_
#define HYBRIDSE_SRC_VM_TRANSFORM_H_

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "base/fe_hash.h"
#include "base/fe_status.h"
#include "base/graph.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "passes/physical/transform_up_physical_pass.h"
#include "udf/udf_library.h"
#include "vm/physical_op.h"
#include "vm/schemas_context.h"
#include "vm/sql_compiler.h"

namespace hybridse {
namespace vm {

using hybridse::passes::PhysicalPlanPassType;

class LogicalOp {
 public:
    // param node can't be null
    explicit LogicalOp(const node::PlanNode* node, const internal::Closure* ctx) ABSL_ATTRIBUTE_NONNULL(2)
        : node_(node), ctx_(ctx) {}

    const size_t Hash() const { return static_cast<size_t>(node_->GetType()); }

    const bool Equals(const LogicalOp& that) const {
        return node::PlanEquals(node_, that.node_) && base::GeneralPtrEq(ctx_, that.ctx_);
    }

    friend std::ostream& operator<<(std::ostream& output,
                                    const LogicalOp& thiz);
    const node::PlanNode* node_;
    const internal::Closure* ctx_;
};

struct HashLogicalOp {
    size_t operator()(const class LogicalOp& v) const {
        //  return  hash<int>(classA.getvalue());
        return v.Hash();
    }
};
struct EqualLogicalOp {
    bool operator()(const class LogicalOp& a1,
                    const class LogicalOp& a2) const {
        return a1.Equals(a2);
    }
};

class PhysicalOpVertex {
 public:
    explicit PhysicalOpVertex(size_t id, const PhysicalOpNode* node)
        : id_(id), node_(node) {}
    const size_t Hash() const { return id_ % 100; }
    const bool Equals(const PhysicalOpVertex& that) const {
        return id_ == that.id_;
    }
    const size_t id_;
    const PhysicalOpNode* node_;
};
struct HashPhysicalOp {
    size_t operator()(const class PhysicalOpVertex& v) const {
        //  return  hash<int>(classA.getvalue());
        return v.Hash();
    }
};
struct EqualPhysicalOp {
    bool operator()(const class PhysicalOpVertex& a1,
                    const class PhysicalOpVertex& a2) const {
        return a1.Equals(a2);
    }
};

using hybridse::base::Status;

typedef hybridse::base::Graph<LogicalOp, HashLogicalOp, EqualLogicalOp>
    LogicalGraph;

class BatchModeTransformer {
 public:
    BatchModeTransformer(node::NodeManager* node_manager, const std::string& db,
                         const std::shared_ptr<Catalog>& catalog, const codec::Schema* parameter_types,
                         ::llvm::Module* module, const udf::UdfLibrary* library,
                         bool cluster_optimized_mode = false, bool enable_expr_opt = false,
                         bool enable_window_parallelization = true,
                         bool enable_window_column_pruning = false,
                         const std::unordered_map<std::string, std::string>* options = nullptr);
    virtual ~BatchModeTransformer();
    bool AddDefaultPasses();

    Status TransformPhysicalPlan(const ::hybridse::node::PlanNodeList& trees, ::hybridse::vm::PhysicalOpNode** output);
    ABSL_MUST_USE_RESULT
    Status TransformQueryPlan(const ::hybridse::node::QueryPlanNode*, ::hybridse::vm::PhysicalOpNode**);

    virtual Status ValidatePlan(PhysicalOpNode* in);

    bool AddPass(PhysicalPlanPassType type);

    // Generate function info for node's all components
    Status InitFnInfo(PhysicalOpNode* node, std::set<PhysicalOpNode*>* visited);

    Status GenJoin(Join* join, PhysicalOpNode* in);
    Status GenFilter(Filter* filter, PhysicalOpNode* in);
    Status GenHavingFilter(ConditionFilter* filter,
                              const SchemasContext* schemas_ctx);
    Status GenConditionFilter(ConditionFilter* filter,
                              const SchemasContext* schemas_ctx);
    Status GenKey(Key* hash, const SchemasContext* schemas_ctx);
    Status GenWindow(WindowOp* window, PhysicalOpNode* in);
    Status GenRequestWindow(RequestWindowOp* window, PhysicalOpNode* in);

    Status GenSort(Sort* sort, const SchemasContext* schemas_ctx);
    Status GenRange(Range* sort, const SchemasContext* schemas_ctx);

    bool isSourceFromTableOrPartition(PhysicalOpNode* in);
    bool isSourceFromTable(PhysicalOpNode* in);
    Status ValidateTableProvider(PhysicalOpNode* physical_plan);
    Status ValidatePartitionDataProvider(PhysicalOpNode* physical_plan);
    std::string ExtractSchemaName(PhysicalOpNode* in);
    Status ValidateRequestDataProvider(PhysicalOpNode* physical_plan);
    Status ValidateWindowIndexOptimization(const WindowOp& window,
                                           PhysicalOpNode* in);
    Status ValidateJoinIndexOptimization(const Join& join, PhysicalOpNode* in);
    Status ValidateRequestJoinIndexOptimization(const Join& join, PhysicalOpNode* in);
    Status ValidateIndexOptimization(PhysicalOpNode* physical_plan);
    Status ValidateOnlyFullGroupBy(const node::ProjectListNode* project_list, const node::ExprListNode* group_keys,
                                   const SchemasContext* schemas_ctx);
    PhysicalPlanContext* GetPlanContext() { return &plan_ctx_; }

 protected:
    Status TransformPlanOp(const ::hybridse::node::PlanNode* node, ::hybridse::vm::PhysicalOpNode** ouput);

    virtual Status TransformLimitOp(const node::LimitPlanNode* node,
                                    PhysicalOpNode** output);
    virtual Status TransformProjectPlanOp(const node::ProjectPlanNode*, PhysicalOpNode**);

    virtual Status TransformJoinOp(const node::JoinPlanNode* node,
                                   PhysicalOpNode** output);
    virtual Status TransformGroupOp(const node::GroupPlanNode* node,
                                    PhysicalOpNode** output);
    virtual Status TransformSortOp(const node::SortPlanNode* node,
                                   PhysicalOpNode** output);
    virtual Status TransformFilterOp(const node::FilterPlanNode* node,
                                     PhysicalOpNode** output);
    virtual Status TransformScanOp(const node::TablePlanNode* node,
                                   PhysicalOpNode** output);
    virtual Status TransformRenameOp(const node::RenamePlanNode* node,
                                     PhysicalOpNode** output);
    virtual Status TransformDistinctOp(const node::DistinctPlanNode* node,
                                       PhysicalOpNode** output);
    virtual Status TransformDeleteOp(const node::DeletePlanNode* node, PhysicalOpNode** output);

    virtual Status TransformSelectIntoOp(const node::SelectIntoPlanNode* node, PhysicalOpNode* child,
                                         PhysicalOpNode** output);

    virtual Status TransformLoadDataOp(const node::LoadDataPlanNode* node,
                                       PhysicalOpNode** output);

    Status TransformCreateTableOp(const node::CreatePlanNode* create, PhysicalOpNode** output);

    virtual Status CreatePhysicalConstProjectNode(
        node::ProjectListNode* project_list, PhysicalOpNode** output);

    Status TransformWithClauseEntry(const node::WithClauseEntryPlanNode* node, PhysicalOpNode** out);

    virtual Status CreatePhysicalProjectNode(
        const ProjectType project_type, PhysicalOpNode* node,
        node::ProjectListNode* project_list, bool append_input,
        PhysicalOpNode** output);

    virtual Status TransformProjectOp(node::ProjectListNode* node,
                                      PhysicalOpNode* depend, bool append_input,
                                      PhysicalOpNode** output);
    virtual void ApplyPasses(PhysicalOpNode* node, PhysicalOpNode** output);

    Status ValidatePlanSupported(const PhysicalOpNode* in);

    template <typename Op, typename... Args>
    Status CreateOp(Op** op, Args&&... args) {
        return plan_ctx_.CreateOp<Op>(op, args...);
    }

    Status GenFnDef(const node::FuncDefPlanNode* fn_plan);

    /**
     * Instantiate underlying llvm function with specified fn info.
     */
    Status InstantiateLLVMFunction(const FnInfo* fn_info);

    Status GenWindowJoinList(PhysicalWindowAggrerationNode* window_agg_op,
                             PhysicalOpNode* in);
    Status GenWindowUnionList(WindowUnionList* window_union_list,
                              PhysicalOpNode* in);
    Status GenRequestWindowUnionList(RequestWindowUnionList* window_unions,
                                     PhysicalOpNode* in);
    bool IsSimpleProject(const ColumnProjects& project);

    Status CheckHistoryWindowFrame(const node::WindowPlanNode* w_ptr);
    Status CheckWindow(const node::WindowPlanNode* w_ptr,
                       const vm::SchemasContext* schemas_ctx);

    base::Status CheckTimeOrIntegerOrderColumn(
        const node::OrderByNode* orders, const SchemasContext* schemas_ctx);
    Status CheckPartitionColumn(const node::ExprListNode* partition, const SchemasContext* ctx);

    base::Status ExtractGroupKeys(vm::PhysicalOpNode* depend, const node::ExprListNode** keys);
    Status CompleteProjectList(const node::ProjectPlanNode* project_node, PhysicalOpNode* depend) const;

    // in stack the new CTE environment and replace the current closure as `Closure(old_closure, cte_env)`
    void PushCTEEnv(const internal::CTEEnv& clu);

    // Replace `closure_` with new `clu` as the current captured CTEs
    void ReplaceClosure(internal::Closure* clu);

    // Pop all `CTEContext`s who all has the same parent
    ABSL_MUST_USE_RESULT
    Status PopCTEs();

    virtual absl::StatusOr<PhysicalOpNode*> ResolveCTERef(absl::string_view tb_name);

    absl::StatusOr<PhysicalOpNode*> ResolveCTERefImpl(absl::string_view tb_name, bool request_mode);

 protected:
    node::NodeManager* node_manager_;
    const std::string db_;
    const std::shared_ptr<Catalog> catalog_;

    typedef std::unordered_map<LogicalOp, ::hybridse::vm::PhysicalOpNode*, HashLogicalOp, EqualLogicalOp> LogicalOpMap;

    // Captured CTEs during the transform process
    //   pointer value changes between different transform steps internally
    //   lifetime of all `Closure`s managed by NodeManager
    internal::Closure* closure_ = nullptr;

    PhysicalPlanContext plan_ctx_;

 private:
    virtual Status TransformProjectPlanOpWithWindowParallel(const node::ProjectPlanNode* node, PhysicalOpNode** output);
    virtual Status TransformProjectPlanOpWindowSerial(const node::ProjectPlanNode* node, PhysicalOpNode** output);

    ::llvm::Module* module_;
    uint32_t id_;
    // window partition and order should be optimized under
    // `index_opt_strict_mode_` join key should be optimized under
    // `index_opt_strict_mode_`
    bool cluster_optimized_mode_;
    bool enable_batch_window_parallelization_;
    bool enable_batch_window_column_pruning_;
    std::vector<PhysicalPlanPassType> passes;
    LogicalOpMap op_map_;
    const udf::UdfLibrary* library_;
};

class RequestModeTransformer : public BatchModeTransformer {
 public:
    RequestModeTransformer(node::NodeManager* node_manager, const std::string& db,
                           const std::shared_ptr<Catalog>& catalog, const codec::Schema* parameter_types,
                           ::llvm::Module* module, udf::UdfLibrary* library,
                           const std::set<size_t>& common_column_indices,
                           const bool cluster_optimized, const bool enable_batch_request_opt, bool enable_expr_opt,
                           bool performance_sensitive = true,
                           const std::unordered_map<std::string, std::string>* options = nullptr);
    virtual ~RequestModeTransformer();

    const Schema& request_schema() const { return request_schema_; }
    const std::string& request_name() const { return request_name_; }
    const std::string& request_db_name() const { return request_db_name_; }
    const BatchRequestInfo& batch_request_info() const {
        return batch_request_info_;
    }
    Status ValidatePlan(PhysicalOpNode* in) override;

 protected:
    void ApplyPasses(PhysicalOpNode* node, PhysicalOpNode** output) override;
    Status TransformProjectPlanOp(const node::ProjectPlanNode* node, PhysicalOpNode** output) override;
    Status TransformProjectOp(node::ProjectListNode* node, PhysicalOpNode* depend, bool append_input,
                              PhysicalOpNode** output) override;
    Status TransformJoinOp(const node::JoinPlanNode* node, PhysicalOpNode** output) override;
    Status TransformScanOp(const node::TablePlanNode* node, PhysicalOpNode** output) override;
    Status TransformGroupOp(const node::GroupPlanNode* node, PhysicalOpNode** output) override;

    Status TransformLoadDataOp(const node::LoadDataPlanNode* node, PhysicalOpNode** output) override;

    Status TransformWindowOp(PhysicalOpNode* depend, const node::WindowPlanNode* w_ptr, PhysicalOpNode** output);

    base::Status CreateRequestUnionNode(PhysicalOpNode* request, PhysicalOpNode* right, const std::string& db_name,
                                        const std::string& primary_name, const codec::Schema* primary_schema,
                                        const node::ExprListNode* partition, const node::WindowPlanNode* window_plan,
                                        PhysicalRequestUnionNode** output);

    absl::StatusOr<PhysicalOpNode*> ResolveCTERef(absl::string_view tb_name) override;

    Status ValidateRequestTable(PhysicalOpNode* in, PhysicalOpNode** request_table);

 private:
    // Optimize simple project node which is the producer of window project
    Status OptimizeSimpleProjectAsWindowProducer(PhysicalSimpleProjectNode* depend,
                                                 const SchemasContext* window_depend_sc,
                                                 const node::WindowPlanNode* w_ptr, PhysicalOpNode** output);

    Status OptimizeRequestJoinAsWindowProducer(PhysicalRequestJoinNode* depend, const SchemasContext* window_depend_sc,
                                               const node::WindowPlanNode* w_ptr, PhysicalOpNode** output);

 private:
    bool enable_batch_request_opt_;
    bool performance_sensitive_;
    vm::Schema request_schema_;
    std::string request_name_ = "";
    std::string request_db_name_ = "";
    BatchRequestInfo batch_request_info_;
    node::TablePlanNode* request_table_ = nullptr;
};

inline bool SchemaType2DataType(const ::hybridse::type::Type type,
                                ::hybridse::node::DataType* output) {
    switch (type) {
        case ::hybridse::type::kBool: {
            *output = ::hybridse::node::kBool;
            break;
        }
        case ::hybridse::type::kInt16: {
            *output = ::hybridse::node::kInt16;
            break;
        }
        case ::hybridse::type::kInt32: {
            *output = ::hybridse::node::kInt32;
            break;
        }
        case ::hybridse::type::kInt64: {
            *output = ::hybridse::node::kInt64;
            break;
        }
        case ::hybridse::type::kFloat: {
            *output = ::hybridse::node::kFloat;
            break;
        }
        case ::hybridse::type::kDouble: {
            *output = ::hybridse::node::kDouble;
            break;
        }
        case ::hybridse::type::kVarchar: {
            *output = ::hybridse::node::kVarchar;
            break;
        }
        case ::hybridse::type::kTimestamp: {
            *output = ::hybridse::node::kTimestamp;
            break;
        }
        case ::hybridse::type::kDate: {
            *output = ::hybridse::node::kDate;
            break;
        }
        default: {
            LOG(WARNING) << "unrecognized schema type "
                         << ::hybridse::type::Type_Name(type);
            return false;
        }
    }
    return true;
}

Status ExtractProjectInfos(const node::PlanNodeList& projects, const node::FrameNode* primary_frame,
                           ColumnProjects* output);
}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_VM_TRANSFORM_H_
