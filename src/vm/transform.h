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


#ifndef SRC_VM_TRANSFORM_H_
#define SRC_VM_TRANSFORM_H_

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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

namespace fesql {
namespace vm {

using fesql::passes::PhysicalPlanPassType;

class LogicalOp {
 public:
    explicit LogicalOp(const node::PlanNode* node) : node_(node) {}
    const size_t Hash() const { return static_cast<size_t>(node_->GetType()); }
    const bool Equals(const LogicalOp& that) const {
        return node::PlanEquals(node_, that.node_);
    }

    friend std::ostream& operator<<(std::ostream& output,
                                    const LogicalOp& thiz);
    const node::PlanNode* node_;
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

using fesql::base::Status;

typedef fesql::base::Graph<LogicalOp, HashLogicalOp, EqualLogicalOp>
    LogicalGraph;

class BatchModeTransformer {
 public:
    BatchModeTransformer(node::NodeManager* node_manager, const std::string& db,
                         const std::shared_ptr<Catalog>& catalog,
                         ::llvm::Module* module,
                         const udf::UDFLibrary* library);
    BatchModeTransformer(node::NodeManager* node_manager, const std::string& db,
                         const std::shared_ptr<Catalog>& catalog,
                         ::llvm::Module* module, const udf::UDFLibrary* library,
                         bool performance_sensitive,
                         bool cluster_optimized_mode, bool enable_expr_opt,
                         bool enable_window_parallelization);
    virtual ~BatchModeTransformer();
    bool AddDefaultPasses();
    virtual Status TransformPhysicalPlan(
        const ::fesql::node::PlanNodeList& trees,
        ::fesql::vm::PhysicalOpNode** output);
    virtual Status TransformQueryPlan(const ::fesql::node::PlanNode* node,
                                      ::fesql::vm::PhysicalOpNode** output);

    bool AddPass(PhysicalPlanPassType type);

    typedef std::unordered_map<LogicalOp, ::fesql::vm::PhysicalOpNode*,
                               HashLogicalOp, EqualLogicalOp>
        LogicalOpMap;

    // Generate function info for node's all components
    Status InitFnInfo(PhysicalOpNode* node, std::set<PhysicalOpNode*>* visited);

    Status GenJoin(Join* join, PhysicalOpNode* in);
    Status GenFilter(Filter* filter, PhysicalOpNode* in);
    Status GenConditionFilter(ConditionFilter* filter,
                              const SchemasContext* schemas_ctx);
    Status GenKey(Key* hash, const SchemasContext* schemas_ctx);
    Status GenWindow(WindowOp* window, PhysicalOpNode* in);
    Status GenRequestWindow(RequestWindowOp* window, PhysicalOpNode* in);

    Status GenSort(Sort* sort, const SchemasContext* schemas_ctx);
    Status GenRange(Range* sort, const SchemasContext* schemas_ctx);

    Status ValidatePartitionDataProvider(PhysicalOpNode* physical_plan);
    std::string ExtractSchameName(PhysicalOpNode* physical_plan);
    Status ValidateRequestDataProvider(PhysicalOpNode* physical_plan);
    Status ValidateWindowIndexOptimization(const WindowOp& window,
                                           PhysicalOpNode* in);
    Status ValidateJoinIndexOptimization(const Join& join, PhysicalOpNode* in);
    Status ValidateRequestJoinIndexOptimization(const Join& join,
                                                PhysicalOpNode* in);
    Status ValidateIndexOptimization(PhysicalOpNode* physical_plan);

    PhysicalPlanContext* GetPlanContext() { return &plan_ctx_; }

 protected:
    virtual Status TransformPlanOp(const ::fesql::node::PlanNode* node,
                                   ::fesql::vm::PhysicalOpNode** ouput);
    virtual Status TransformLimitOp(const node::LimitPlanNode* node,
                                    PhysicalOpNode** output);
    virtual Status TransformProjectPlanOp(const node::ProjectPlanNode* node,
                                          PhysicalOpNode** output);
    virtual Status TransformWindowOp(PhysicalOpNode* depend,
                                     const node::WindowPlanNode* w_ptr,
                                     PhysicalOpNode** output);
    virtual Status TransformJoinOp(const node::JoinPlanNode* node,
                                   PhysicalOpNode** output);
    virtual Status TransformUnionOp(const node::UnionPlanNode* node,
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

    virtual Status CreatePhysicalConstProjectNode(
        node::ProjectListNode* project_list, PhysicalOpNode** output);

    base::Status CreateRequestUnionNode(PhysicalOpNode* request,
                                        PhysicalOpNode* right,
                                        const std::string& primary_name,
                                        const codec::Schema* primary_schema,
                                        const node::ExprListNode* partition,
                                        const node::WindowPlanNode* window_plan,
                                        PhysicalRequestUnionNode** output);

    virtual Status CreatePhysicalProjectNode(
        const ProjectType project_type, PhysicalOpNode* node,
        node::ProjectListNode* project_list, bool append_input,
        PhysicalOpNode** output);

    virtual Status TransformProjectOp(node::ProjectListNode* node,
                                      PhysicalOpNode* depend, bool append_input,
                                      PhysicalOpNode** output);
    virtual void ApplyPasses(PhysicalOpNode* node, PhysicalOpNode** output);

    template <typename Op, typename... Args>
    Status CreateOp(Op** op, Args&&... args) {
        return plan_ctx_.CreateOp<Op>(op, args...);
    }

    Status GenFnDef(const node::FuncDefPlanNode* fn_plan);

    /**
     * Instantiate underlying llvm function with specified fn info.
     */
    Status InstantiateLLVMFunction(const FnInfo& fn_info);

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

    node::NodeManager* node_manager_;
    const std::string db_;
    const std::shared_ptr<Catalog> catalog_;

 private:
    virtual Status TransformProjectPlanOpWithWindowParallel(
        const node::ProjectPlanNode* node, PhysicalOpNode** output);
    virtual Status TransformProjectPlanOpWindowSerial(
        const node::ProjectPlanNode* node, PhysicalOpNode** output);
    ::llvm::Module* module_;
    uint32_t id_;
    // window partition and order should be optimized under
    // `index_opt_strict_mode_` join key should be optimized under
    // `index_opt_strict_mode_`
    bool performance_sensitive_mode_;
    bool cluster_optimized_mode_;
    bool enable_batch_window_parallelization_;
    std::vector<PhysicalPlanPassType> passes;
    LogicalOpMap op_map_;
    const udf::UDFLibrary* library_;
    PhysicalPlanContext plan_ctx_;
};

class RequestModeTransformer : public BatchModeTransformer {
 public:
    RequestModeTransformer(
        node::NodeManager* node_manager, const std::string& db,
        const std::shared_ptr<Catalog>& catalog, ::llvm::Module* module,
        udf::UDFLibrary* library, const std::set<size_t>& common_column_indices,
        const bool performance_sensitive, const bool cluster_optimized,
        const bool enable_batch_request_opt, bool enable_expr_opt);
    virtual ~RequestModeTransformer();

    const Schema& request_schema() const { return request_schema_; }
    const std::string& request_name() const { return request_name_; }
    const BatchRequestInfo& batch_request_info() const {
        return batch_request_info_;
    }

 protected:
    void ApplyPasses(PhysicalOpNode* node, PhysicalOpNode** output) override;

    virtual Status TransformProjectOp(node::ProjectListNode* node,
                                      PhysicalOpNode* depend, bool append_input,
                                      PhysicalOpNode** output);
    virtual Status TransformProjectPlanOp(const node::ProjectPlanNode* node,
                                          PhysicalOpNode** output);
    virtual Status TransformJoinOp(const node::JoinPlanNode* node,
                                   PhysicalOpNode** output);
    virtual Status TransformScanOp(const node::TablePlanNode* node,
                                   PhysicalOpNode** output);

 private:
    bool enable_batch_request_opt_;
    vm::Schema request_schema_;
    std::string request_name_;
    BatchRequestInfo batch_request_info_;
};

inline bool SchemaType2DataType(const ::fesql::type::Type type,
                                ::fesql::node::DataType* output) {
    switch (type) {
        case ::fesql::type::kBool: {
            *output = ::fesql::node::kBool;
            break;
        }
        case ::fesql::type::kInt16: {
            *output = ::fesql::node::kInt16;
            break;
        }
        case ::fesql::type::kInt32: {
            *output = ::fesql::node::kInt32;
            break;
        }
        case ::fesql::type::kInt64: {
            *output = ::fesql::node::kInt64;
            break;
        }
        case ::fesql::type::kFloat: {
            *output = ::fesql::node::kFloat;
            break;
        }
        case ::fesql::type::kDouble: {
            *output = ::fesql::node::kDouble;
            break;
        }
        case ::fesql::type::kVarchar: {
            *output = ::fesql::node::kVarchar;
            break;
        }
        case ::fesql::type::kTimestamp: {
            *output = ::fesql::node::kTimestamp;
            break;
        }
        case ::fesql::type::kDate: {
            *output = ::fesql::node::kDate;
            break;
        }
        default: {
            LOG(WARNING) << "unrecognized schema type "
                         << ::fesql::type::Type_Name(type);
            return false;
        }
    }
    return true;
}

bool TransformLogicalTreeToLogicalGraph(const ::fesql::node::PlanNode* node,
                                        LogicalGraph* graph,
                                        fesql::base::Status& status);  // NOLINT

Status ExtractProjectInfos(const node::PlanNodeList& projects,
                           const node::FrameNode* primary_frame,
                           const SchemasContext* schemas_ctx,
                           node::NodeManager* node_manager,
                           ColumnProjects* output);
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_TRANSFORM_H_
