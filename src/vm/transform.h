/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * transform.h
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/

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
#include "passes/physical/physical_pass.h"
#include "udf/udf_library.h"
#include "vm/physical_op.h"
#include "vm/schemas_context.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace vm {

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

class TransformUpPysicalPass : public PhysicalPass {
 public:
    explicit TransformUpPysicalPass(PhysicalPlanContext* plan_ctx)
        : plan_ctx_(plan_ctx),
          node_manager_(plan_ctx->node_manager()),
          db_(plan_ctx->db()),
          catalog_(plan_ctx->catalog()) {}
    ~TransformUpPysicalPass() {}

    virtual bool Apply(PhysicalOpNode* in, PhysicalOpNode** out);
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** out) = 0;

    Status Apply(PhysicalPlanContext* ctx, PhysicalOpNode* in,
                 PhysicalOpNode** out) override {
        return base::Status(common::kPlanError, "Not implemented");
    }

 protected:
    PhysicalPlanContext* plan_ctx_;
    node::NodeManager* node_manager_;
    const std::string db_;
    std::shared_ptr<Catalog> catalog_;
};

class GroupAndSortOptimized : public TransformUpPysicalPass {
 public:
    explicit GroupAndSortOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

    ~GroupAndSortOptimized() {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);

    bool FilterOptimized(const SchemasContext* root_schemas_ctx,
                         PhysicalOpNode* in, Filter* filter,
                         PhysicalOpNode** new_in);
    bool JoinKeysOptimized(const SchemasContext* schemas_ctx,
                           PhysicalOpNode* in, Join* join,
                           PhysicalOpNode** new_in);
    bool KeysFilterOptimized(const SchemasContext* root_schemas_ctx,
                             PhysicalOpNode* in, Key* group, Key* hash,
                             PhysicalOpNode** new_in);
    bool GroupOptimized(const SchemasContext* root_schemas_ctx,
                        PhysicalOpNode* in, Key* group,
                        PhysicalOpNode** new_in);
    bool SortOptimized(const SchemasContext* root_schemas_ctx,
                       PhysicalOpNode* in, Sort* sort);
    bool TransformGroupExpr(const SchemasContext* schemas_ctx,
                            const node::ExprListNode* group,
                            const std::string& table_name,
                            const IndexHint& index_hint, std::string* index,
                            std::vector<bool>* best_bitmap);
    bool TransformOrderExpr(const SchemasContext* schemas_ctx,
                            const node::OrderByNode* order,
                            const Schema& schema, const IndexSt& index_st,
                            const node::OrderByNode** output);
    bool MatchBestIndex(const std::vector<std::string>& columns,
                        const std::string& table_name, const IndexHint& catalog,
                        std::vector<bool>* bitmap, std::string* index_name,
                        std::vector<bool>* best_bitmap);  // NOLINT
};

class LimitOptimized : public TransformUpPysicalPass {
 public:
    explicit LimitOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}
    ~LimitOptimized() {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);

    static bool ApplyLimitCnt(PhysicalOpNode* node, int32_t limit_cnt);
};

class SimpleProjectOptimized : public TransformUpPysicalPass {
 public:
    explicit SimpleProjectOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}
    ~SimpleProjectOptimized() {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
};

struct ExprPair {
    node::ExprNode* left_expr_ = nullptr;
    node::ExprNode* right_expr_ = nullptr;
};

// Optimize filter condition
// for FilterNode, JoinNode
class ConditionOptimized : public TransformUpPysicalPass {
 public:
    explicit ConditionOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

    static bool TransfromAndConditionList(
        const node::ExprNode* condition,
        node::ExprListNode* and_condition_list);
    static bool ExtractEqualExprPair(
        node::ExprNode* condition,
        std::pair<node::ExprNode*, node::ExprNode*>* expr_pair);
    static bool TransformJoinEqualExprPair(
        const SchemasContext* left_schemas_ctx,
        const SchemasContext* right_schemas_ctx,
        node::ExprListNode* and_conditions,
        node::ExprListNode* out_condition_list,
        std::vector<ExprPair>& condition_eq_pair);  // NOLINT
    static bool TransformConstEqualExprPair(
        node::ExprListNode* and_conditions,
        node::ExprListNode* out_condition_list,
        std::vector<ExprPair>& condition_eq_pair);  // NOLINT
    static bool MakeConstEqualExprPair(
        const std::pair<node::ExprNode*, node::ExprNode*> expr_pair,
        const SchemasContext* right_schemas_ctx, ExprPair* output);

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool JoinConditionOptimized(PhysicalBinaryNode* in, Join* join);
    void SkipConstExpression(node::ExprListNode input,
                             node::ExprListNode* output);
    bool FilterConditionOptimized(PhysicalOpNode* in, Filter* filter);
};

class LeftJoinOptimized : public TransformUpPysicalPass {
 public:
    explicit LeftJoinOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

 private:
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool ColumnExist(const Schema& schema, const std::string& column);
    bool CheckExprListFromSchema(const node::ExprListNode* expr_list,
                                 const Schema* schema);
};

class ClusterOptimized : public TransformUpPysicalPass {
 public:
    explicit ClusterOptimized(PhysicalPlanContext* plan_ctx)
        : TransformUpPysicalPass(plan_ctx) {}

 private:
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool SimplifyJoinLeftInput(PhysicalOpNode* join_op, const Join& join,
                               const SchemasContext* joined_schema_ctx,
                               PhysicalOpNode** out);
};

typedef fesql::base::Graph<LogicalOp, HashLogicalOp, EqualLogicalOp>
    LogicalGraph;

enum PhysicalPlanPassType {
    kPassColumnProjectsOptimized,
    kPassFilterOptimized,
    kPassGroupAndSortOptimized,
    kPassLeftJoinOptimized,
    kPassClusterOptimized,
    kPassLimitOptimized
};

inline std::string PhysicalPlanPassTypeName(PhysicalPlanPassType type) {
    switch (type) {
        case kPassColumnProjectsOptimized:
            return "PassColumnProjectsOptimized";
        case kPassFilterOptimized:
            return "PassFilterOptimized";
        case kPassGroupAndSortOptimized:
            return "PassGroupByOptimized";
        case kPassLeftJoinOptimized:
            return "PassLeftJoinOptimized";
        case kPassLimitOptimized:
            return "PassLimitOptimized";
        default:
            return "unknowPass";
    }
    return "";
}

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
                         bool cluster_optimized_mode);
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
    ::llvm::Module* module_;
    uint32_t id_;
    // window partition and order should be optimized under
    // `index_opt_strict_mode_` join key should be optimized under
    // `index_opt_strict_mode_`
    bool performance_sensitive_mode_;
    bool cluster_optimized_mode_;
    std::vector<PhysicalPlanPassType> passes;
    LogicalOpMap op_map_;
    const udf::UDFLibrary* library_;
    PhysicalPlanContext plan_ctx_;
};

class RequestModeTransformer : public BatchModeTransformer {
 public:
    RequestModeTransformer(node::NodeManager* node_manager,
                           const std::string& db,
                           const std::shared_ptr<Catalog>& catalog,
                           ::llvm::Module* module, udf::UDFLibrary* library,
                           const std::set<size_t>& common_column_indices,
                           const bool performance_sensitive,
                           const bool cluster_optimized,
                           const bool enable_batch_request_opt);
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
