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
#include "udf/udf_library.h"
#include "vm/physical_op.h"
#include "vm/schemas_context.h"

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

template <class T>
class TransformPass {
 public:
    TransformPass(node::NodeManager* node_manager, const std::string& db,
                  const std::shared_ptr<Catalog>& catalog)
        : db_(db), catalog_(catalog), node_manager_(node_manager) {}
    virtual ~TransformPass() {}
    const std::string db_;
    const std::shared_ptr<Catalog> catalog_;

 protected:
    node::NodeManager* node_manager_;
    virtual bool Transform(T in, T* output) = 0;
    virtual bool Apply(T in, T* out) = 0;
};
class TransformUpPysicalPass : public TransformPass<PhysicalOpNode*> {
 public:
    TransformUpPysicalPass(node::NodeManager* node_manager,
                           const std::string& db,
                           const std::shared_ptr<Catalog>& catalog)
        : TransformPass<PhysicalOpNode*>(node_manager, db, catalog) {}
    ~TransformUpPysicalPass() {}
    virtual bool Apply(PhysicalOpNode* in, PhysicalOpNode** out);
};

class ExprTransformPass : public TransformPass<node::ExprNode*> {
 public:
    ExprTransformPass(node::NodeManager* node_manager, const std::string& db,
                      const std::shared_ptr<Catalog>& catalog)
        : TransformPass<node::ExprNode*>(node_manager, db, catalog) {}
    ~ExprTransformPass() {}
};

class CanonicalizeExprTransformPass : public ExprTransformPass {
 public:
    CanonicalizeExprTransformPass(node::NodeManager* node_manager,
                                  const std::string& db,
                                  const std::shared_ptr<Catalog>& catalog)
        : ExprTransformPass(node_manager, db, catalog) {}
    ~CanonicalizeExprTransformPass() {}
    virtual bool Transform(node::ExprNode* in, node::ExprNode** output);
};

class GroupAndSortOptimized : public TransformUpPysicalPass {
 public:
    GroupAndSortOptimized(node::NodeManager* node_manager,
                          const std::string& db,
                          const std::shared_ptr<Catalog>& catalog)
        : TransformUpPysicalPass(node_manager, db, catalog) {}

    ~GroupAndSortOptimized() {}

 private:
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);

    bool JoinKeysOptimized(const vm::SchemaSourceList& column_sources,
                           PhysicalOpNode* in, Join* join,
                           PhysicalOpNode** new_in);
    bool KeysFilterOptimized(const vm::SchemaSourceList& column_sources,
                             PhysicalOpNode* in, Key* group, Key* hash,
                             PhysicalOpNode** new_in);
    bool GroupOptimized(const vm::SchemaSourceList& column_sources,
                        PhysicalOpNode* in, Key* group,
                        PhysicalOpNode** new_in);
    bool SortOptimized(const vm::SchemaSourceList& column_sources,
                       PhysicalOpNode* in, Sort* sort);
    bool TransformGroupExpr(const vm::SchemaSourceList& column_sources,
                            const node::ExprListNode* group,
                            const IndexHint& index_hint, std::string* index,
                            const node::ExprListNode** keys,
                            const node::ExprListNode** output);
    bool TransformOrderExpr(const vm::SchemaSourceList& column_sources,
                            const node::OrderByNode* order,
                            const Schema& schema, const IndexSt& index_st,
                            const node::OrderByNode** output);
    bool MatchBestIndex(const std::vector<std::string>& columns,
                        const IndexHint& catalog, std::vector<bool>* bitmap,
                        std::string* index_name);  // NOLINT
};

class LimitOptimized : public TransformUpPysicalPass {
 public:
    LimitOptimized(node::NodeManager* node_manager, const std::string& db,
                   const std::shared_ptr<Catalog>& catalog)
        : TransformUpPysicalPass(node_manager, db, catalog) {}
    ~LimitOptimized() {}

 private:
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);

    static bool ApplyLimitCnt(PhysicalOpNode* node, int32_t limit_cnt);
};

class SimpleProjectOptimized : public TransformUpPysicalPass {
 public:
    SimpleProjectOptimized(node::NodeManager* node_manager,
                           const std::string& db,
                           const std::shared_ptr<Catalog>& catalog)
        : TransformUpPysicalPass(node_manager, db, catalog) {}
    ~SimpleProjectOptimized() {}

 private:
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
};
struct ExprPair {
    node::ExprNode* left_expr_ = nullptr;
    uint32_t left_idx_ = 0u;
    node::ExprNode* right_expr_ = nullptr;
    uint32_t right_idx_ = 0u;
};
// Optimize filter condition
// for FilterNode, JoinNode
class ConditionOptimized : public TransformUpPysicalPass {
 public:
    ConditionOptimized(node::NodeManager* node_manager, const std::string& db,
                       const std::shared_ptr<Catalog>& catalog)
        : TransformUpPysicalPass(node_manager, db, catalog) {}
    static bool TransfromAndConditionList(
        const node::ExprNode* condition,
        node::ExprListNode* and_condition_list);
    static bool ExtractEqualExprPair(
        node::ExprNode* condition,
        std::pair<node::ExprNode*, node::ExprNode*>* expr_pair);
    static bool TransformEqualExprPair(
        const SchemaSourceList& name_schema_list, const size_t left_schema_cnt,
        node::ExprListNode* and_conditions,
        node::ExprListNode* out_condition_list,
        std::vector<ExprPair>& condition_eq_pair);  // NOLINT

 private:
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool JoinConditionOptimized(PhysicalOpNode* in, Join* join);
    void SkipConstExpression(node::ExprListNode input,
                             node::ExprListNode* output);
};
class LeftJoinOptimized : public TransformUpPysicalPass {
 public:
    LeftJoinOptimized(node::NodeManager* node_manager, const std::string& db,
                      const std::shared_ptr<Catalog>& catalog)
        : TransformUpPysicalPass(node_manager, db, catalog) {}

 private:
    virtual bool Transform(PhysicalOpNode* in, PhysicalOpNode** output);
    bool ColumnExist(const Schema& schema, const std::string& column);
    bool CheckExprListFromSchema(const node::ExprListNode* expr_list,
                                 const Schema& schema);
};
typedef fesql::base::Graph<LogicalOp, HashLogicalOp, EqualLogicalOp>
    LogicalGraph;

enum PhysicalPlanPassType {
    kPassColumnProjectOptimized,
    kPassFilterOptimized,
    kPassGroupAndSortOptimized,
    kPassLeftJoinOptimized,
    kPassLimitOptimized,
};

inline std::string PhysicalPlanPassTypeName(PhysicalPlanPassType type) {
    switch (type) {
        case kPassColumnProjectOptimized:
            return "PassColumnProjectOptimized";
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
                         ::llvm::Module* module, udf::UDFLibrary* library);
    virtual ~BatchModeTransformer();
    bool AddDefaultPasses();
    bool TransformPhysicalPlan(const ::fesql::node::PlanNodeList& trees,
                               ::fesql::vm::PhysicalOpNode** output,
                               ::fesql::base::Status& status);  // NOLINT
    virtual bool TransformQueryPlan(const ::fesql::node::PlanNode* node,
                                    ::fesql::vm::PhysicalOpNode** output,
                                    ::fesql::base::Status& status);  // NOLINT

    bool AddPass(PhysicalPlanPassType type);

    typedef std::unordered_map<LogicalOp, ::fesql::vm::PhysicalOpNode*,
                               HashLogicalOp, EqualLogicalOp>
        LogicalOpMap;
    bool GenPlanNode(PhysicalOpNode* node, base::Status& status);  // NOLINT
    bool GenJoin(Join* join, PhysicalOpNode* in,
                 base::Status& status);  // NOLINT
    bool GenFilter(ConditionFilter* filter,
                   const SchemaSourceList& input_name_schema_list,
                   base::Status& status);  // NOLINT
    bool GenKey(Key* hash, const SchemaSourceList& input_name_schema_list,
                base::Status& status);  // NOLINT
    bool GenWindow(WindowOp* window, PhysicalOpNode* in,
                   base::Status& status);  // NOLINT
    bool GenRequestWindow(RequestWindowOp* window, PhysicalOpNode* in,
                          base::Status& status);  // NOLINT

    bool GenSort(Sort* sort, const SchemaSourceList& input_name_schema_list,
                 base::Status& status);  // NOLINT
    bool GenRange(Range* sort, const SchemaSourceList& input_name_schema_list,
                  base::Status& status);  // NOLINT
    bool GenSimpleProject(ColumnProject* project, PhysicalOpNode* in,
                          base::Status& status);  // NOLINT

 protected:
    virtual bool TransformPlanOp(const ::fesql::node::PlanNode* node,
                                 ::fesql::vm::PhysicalOpNode** ouput,
                                 ::fesql::base::Status& status);  // NOLINT
    virtual bool TransformLimitOp(const node::LimitPlanNode* node,
                                  PhysicalOpNode** output,
                                  base::Status& status);  // NOLINT

    virtual bool TransformProjecPlantOp(const node::ProjectPlanNode* node,
                                        PhysicalOpNode** output,
                                        base::Status& status);  // NOLINT
    virtual bool TransformWindowOp(PhysicalOpNode* depend,
                                   const node::WindowPlanNode* w_ptr,
                                   PhysicalOpNode** output,
                                   base::Status& status);  // NOLINT

    virtual bool TransformJoinOp(const node::JoinPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status);  // NOLINT
    virtual bool TransformUnionOp(const node::UnionPlanNode* node,
                                  PhysicalOpNode** output,
                                  base::Status& status);  // NOLINT
    virtual bool TransformGroupOp(const node::GroupPlanNode* node,
                                  PhysicalOpNode** output,
                                  base::Status& status);  // NOLINT
    virtual bool TransformSortOp(const node::SortPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status);  // NOLINT
    virtual bool TransformFilterOp(const node::FilterPlanNode* node,
                                   PhysicalOpNode** output,
                                   base::Status& status);  // NOLINT
    virtual bool TransformScanOp(const node::TablePlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status);  // NOLINT
    virtual bool TransformRenameOp(const node::RenamePlanNode* node,
                                   PhysicalOpNode** output,
                                   base::Status& status);  // NOLINT
    virtual bool TransformDistinctOp(const node::DistinctPlanNode* node,
                                     PhysicalOpNode** output,
                                     base::Status& status);  // NOLINT
    virtual bool CreatePhysicalProjectNode(const ProjectType project_type,
                                           PhysicalOpNode* node,
                                           node::ProjectListNode* project_list,
                                           PhysicalOpNode** output,
                                           base::Status& status);  // NOLINT
    virtual bool TransformProjectOp(node::ProjectListNode* node,
                                    PhysicalOpNode* depend,
                                    PhysicalOpNode** output,
                                    base::Status& status);  // NOLINT
    virtual void ApplyPasses(PhysicalOpNode* node, PhysicalOpNode** output);
    bool GenFnDef(const node::FuncDefPlanNode* fn_plan,
                  base::Status& status);  // NOLINT
    bool CodeGenExprList(const SchemaSourceList& input_name_schema_list,
                         const node::ExprListNode* expr_list, bool row_mode,
                         FnInfo* fn_info,
                         base::Status& status);  // NOLINT
    bool GenProjects(const SchemaSourceList& input_name_schema_list,
                     const node::PlanNodeList& projects, const bool row_mode,
                     const node::FrameNode* frame,
                     std::string& fn_name,   // NOLINT
                     Schema* output_schema,  // NOLINT
                     ColumnSourceList* output_column_sources,
                     base::Status& status);  // NOLINT
    bool GenWindowJoinList(WindowJoinList* window_join_list, PhysicalOpNode* in,
                           base::Status& status);  // NOLINT
    bool GenWindowUnionList(WindowUnionList* window_union_list,
                            PhysicalOpNode* in,
                            base::Status& status);  // NOLINT
    bool GenRequestWindowUnionList(RequestWindowUnionList* window_unions,
                                   PhysicalOpNode* in,
                                   base::Status& status);  // NOLINT
    bool IsSimpleProject(const ColumnSourceList& source);
    bool BuildExprListFromSchemaSource(const ColumnSourceList column_sources,
                                       const SchemaSourceList& schema_souces,
                                       node::ExprListNode* expr_list);
    bool CheckHistoryWindowFrame(const node::WindowPlanNode* w_ptr,
                                 base::Status& status);  // NOLINT
    base::Status CheckTimeOrIntegerOrderColumn(
        const node::OrderByNode* orders,
        const vm::SchemaSourceList& schema_source_list);  // NOLINT
    node::NodeManager* node_manager_;
    const std::string db_;
    const std::shared_ptr<Catalog> catalog_;

 private:
    ::llvm::Module* module_;
    uint32_t id_;
    std::vector<PhysicalPlanPassType> passes;
    LogicalOpMap op_map_;
    udf::UDFLibrary* library_;
};

class RequestModeransformer : public BatchModeTransformer {
 public:
    RequestModeransformer(node::NodeManager* node_manager,
                          const std::string& db,
                          const std::shared_ptr<Catalog>& catalog,
                          ::llvm::Module* module, udf::UDFLibrary* library);
    virtual ~RequestModeransformer();

    const Schema& request_schema() const { return request_schema_; }
    const std::string& request_name() const { return request_name_; }

 protected:
    virtual bool TransformProjectOp(node::ProjectListNode* node,
                                    PhysicalOpNode* depend,
                                    PhysicalOpNode** output,
                                    base::Status& status);  // NOLINT
    virtual bool TransformProjecPlantOp(const node::ProjectPlanNode* node,
                                        PhysicalOpNode** output,
                                        base::Status& status);  // NOLINT
    virtual bool TransformJoinOp(const node::JoinPlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status);  // NOLINT
    virtual bool TransformScanOp(const node::TablePlanNode* node,
                                 PhysicalOpNode** output,
                                 base::Status& status);  // NOLINT

 private:
    vm::Schema request_schema_;
    std::string request_name_;
};

inline bool SchemaType2DataType(const ::fesql::type::Type type,
                                ::fesql::node::DataType* output) {
    switch (type) {
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

class FnDefResolver {
 public:
    FnDefResolver(udf::UDFLibrary* library, node::NodeManager* nm)
        : library_(library), nm_(nm) {}

    bool Visit(node::ExprNode* expr, node::ExprNode** output) {
        for (size_t i = 0; i < expr->GetChildNum(); ++i) {
            node::ExprNode* new_child = nullptr;
            bool updated = Visit(expr->GetChild(i), &new_child);
            if (updated && new_child != nullptr) {
                expr->SetChild(i, new_child);
            }
        }
        *output = expr;  // default
        switch (expr->GetExprType()) {
            case node::kExprCall: {
                auto call = dynamic_cast<node::CallExprNode*>(expr);
                auto fn = call->GetFnDef();
                if (fn->GetType() != node::kExternalFnDef) {
                    return false;
                }
                auto external_fn =
                    dynamic_cast<const node::ExternalFnDefNode*>(fn);
                if (external_fn->IsResolved()) {
                    return false;
                }
                node::ExprNode* result = nullptr;
                auto status = library_->Transform(
                    external_fn->function_name(), call->GetArgs(),
                    call->GetOver(), nm_, &result);
                if (status.isOK() && result != nullptr) {
                    *output = result;
                    return true;
                } else {
                    LOG(WARNING)
                        << "Resolve function '" << external_fn->function_name()
                        << "' failed: " << status.msg;
                    return false;  // fallback to legacy fn gen
                }
            }
            default:
                return false;
        }
    }

 private:
    udf::UDFLibrary* library_;
    node::NodeManager* nm_;
};

class ExpressionTypeAnalyzer {
 public:
    ExpressionTypeAnalyzer(bool is_in_window,
                           const vm::SchemasContext* schemas_context,
                           node::NodeManager* nm)
        : is_in_window_(is_in_window),
          schemas_context_(schemas_context),
          nm_(nm) {}

    bool Visit(node::ExprNode* expr) {
        for (size_t i = 0; i < expr->GetChildNum(); ++i) {
            if (!Visit(expr->GetChild(i))) return false;
        }
        switch (expr->GetExprType()) {
            case node::kExprColumnRef:
                return VisitColumnRef(dynamic_cast<node::ColumnRefNode*>(expr));
            default:
                return true;
        }
    }

 protected:
    bool VisitColumnRef(node::ColumnRefNode* column) {
        const vm::RowSchemaInfo* schema_info = nullptr;
        if (!schemas_context_->ColumnRefResolved(column->GetRelationName(),
                                                 column->GetColumnName(),
                                                 &schema_info)) {
            LOG(WARNING) << "Fail to resolve column "
                         << column->GetRelationName() << "."
                         << column->GetColumnName();
            return false;
        }
        codec::RowDecoder decoder(*schema_info->schema_);
        codec::ColInfo col_info;
        if (!decoder.ResolveColumn(column->GetColumnName(), &col_info)) {
            LOG(WARNING) << "Fail to resolve column "
                         << column->GetRelationName() << "."
                         << column->GetColumnName();
            return false;
        }
        node::DataType dtype;
        if (!SchemaType2DataType(col_info.type, &dtype)) {
            return false;
        }
        if (is_in_window_) {
            column->SetOutputType(nm_->MakeTypeNode(node::kList, dtype));
        } else {
            column->SetOutputType(nm_->MakeTypeNode(dtype));
        }
        return true;
    }

 private:
    bool is_in_window_;
    const vm::SchemasContext* schemas_context_;
    node::NodeManager* nm_;
};

bool TransformLogicalTreeToLogicalGraph(const ::fesql::node::PlanNode* node,
                                        LogicalGraph* graph,
                                        fesql::base::Status& status);  // NOLINT
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_TRANSFORM_H_
