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
#include <node/plan_node.h>
#include <node/sql_node.h>
#include "base/graph.h"
#include "base/status.h"
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

typedef fesql::base::Graph<LogicalOp, HashLogicalOp, EqualLogicalOp>
    LogicalGraph;

class Transform {
 public:
    Transform(const std::string &db, const std::shared_ptr<Catalog>& catalog);
    virtual ~Transform();
    bool TransformPhysicalPlan(const ::fesql::node::PlanNode* node,
                               ::fesql::vm::PhysicalOpNode** ouput,
                               ::fesql::base::Status& status);  // NOLINT
    typedef std::unordered_map<LogicalOp, ::fesql::vm::PhysicalOpNode*,
                               HashLogicalOp, EqualLogicalOp>
        LogicalOpMap;
    bool TransformLimitOp(const node::LimitPlanNode* node,
                          PhysicalOpNode** output, base::Status& status);

 private:
    bool TransformProjectOp(const node::ProjectPlanNode* node,
                            PhysicalOpNode** output, base::Status& status);
    bool TransformWindowProject(const node::ProjectListNode* project_list,
                                PhysicalOpNode* depend,
                                PhysicalOpNode** output, base::Status& status);

 private:
    const std::string db_;
    const std::shared_ptr<Catalog> catalog_;
    LogicalOpMap op_map_;

    bool TransformJoinOp(const node::JoinPlanNode* node,
                         PhysicalOpNode** output, base::Status& status);
    bool TransformUnionOp(const node::UnionPlanNode* node,
                          PhysicalOpNode** output, base::Status& status);
    bool TransformGroupOp(const node::GroupPlanNode* node,
                          PhysicalOpNode** output, base::Status& status);
    bool TransformSortOp(const node::SortPlanNode* node,
                         PhysicalOpNode** output, base::Status& status);
    bool TransformFilterOp(const node::FilterPlanNode* node,
                           PhysicalOpNode** output, base::Status& status);
    bool TryOptimizedFilterCondition(const IndexHint& index_map,
                                     const node::ExprNode* condition,
                                     std::string& index_name,
                                     node::ExprNode** output);
    bool TransformScanOp(const node::TablePlanNode* node,
                         PhysicalOpNode** output, base::Status& status);
    bool TransformRenameOp(const node::RenamePlanNode* node,
                           PhysicalOpNode** output, base::Status& status);
    bool TransformQueryPlan(const node::QueryPlanNode* node,
                            PhysicalOpNode** output, base::Status& status);
};
bool TransformLogicalTreeToLogicalGraph(const ::fesql::node::PlanNode* node,
                                        fesql::base::Status& status,  // NOLINT
                                        LogicalGraph& graph);
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_TRANSFORM_H_
