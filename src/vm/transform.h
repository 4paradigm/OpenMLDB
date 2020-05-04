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
#include "base/graph.h"
#include "base/status.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "vm/physical_op.h"
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

    bool JoinKeysOptimized(PhysicalOpNode* in, Join* join,
                           PhysicalOpNode** new_in);
    bool KeysFilterOptimized(PhysicalOpNode* in, Group* group, Hash* hash,
                             PhysicalOpNode** new_in);
    bool GroupOptimized(PhysicalOpNode* in, Group* group,
                        PhysicalOpNode** new_in);
    bool SortOptimized(PhysicalOpNode* in, Sort* sort);
    bool TransformGroupExpr(const node::ExprListNode* group,
                            const IndexHint& index_hint, std::string* index,
                            const node::ExprListNode** keys,
                            const node::ExprListNode** output);
    bool TransformOrderExpr(const node::OrderByNode* order,
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
        const std::vector<std::pair<const std::string, const vm::Schema*>>
            name_schema_list,
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
    kPassFilterOptimized,
    kPassGroupAndSortOptimized,
    kPassLeftJoinOptimized,
    kPassLimitOptimized,
};

inline std::string PhysicalPlanPassTypeName(PhysicalPlanPassType type) {
    switch (type) {
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
                         ::llvm::Module* module);
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
    bool GenFilter(ConditionFilter* filter, PhysicalOpNode* in,
                   base::Status& status);
    bool GenHash(Hash* hash, PhysicalOpNode*& in,
                 base::Status& status);  // NOLINT
    bool GenWindow(WindowOp* window, PhysicalOpNode* in,
                   base::Status& status);  // NOLINT
    bool GenGroup(Group* group, PhysicalOpNode* in, base::Status& status);
    bool GenSort(Sort* sort, PhysicalOpNode* in, base::Status& status);
    bool GenRange(Range* sort, PhysicalOpNode* in, base::Status& status);

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
    bool CodeGenExprList(const NameSchemaList& input_name_schema_list,
                         const node::ExprListNode* expr_list, bool row_mode,
                         std::string& fn_name, Schema* output_schema,  // NOLINT
                         base::Status& status);                        // NOLINT
    bool CodeGenExprList(const NameSchemaList& input_name_schema_list,
                         const node::ExprListNode* expr_list, bool row_mode,
                         FnInfo* fn_info,  // NOLINT
                         base::Status& status);

    node::NodeManager* node_manager_;
    const std::string db_;
    const std::shared_ptr<Catalog> catalog_;

 private:
    bool GenProjects(
        const std::vector<std::pair<const std::string, const Schema*>>&
            input_name_schema_list,
        const node::PlanNodeList& projects, const bool row_mode,
        std::string& fn_name,   // NOLINT
        Schema* output_schema,  // NOLINT
        base::Status& status);  // NOLINT

    ::llvm::Module* module_;
    uint32_t id_;
    std::vector<PhysicalPlanPassType> passes;
    LogicalOpMap op_map_;
};

class RequestModeransformer : public BatchModeTransformer {
 public:
    RequestModeransformer(node::NodeManager* node_manager,
                          const std::string& db,
                          const std::shared_ptr<Catalog>& catalog,
                          ::llvm::Module* module);
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
bool TransformLogicalTreeToLogicalGraph(const ::fesql::node::PlanNode* node,
                                        LogicalGraph* graph,
                                        fesql::base::Status& status);  // NOLINT
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_TRANSFORM_H_
