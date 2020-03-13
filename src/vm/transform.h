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
namespace fesql {
namespace vm {
class LogicalOp {
 public:
    LogicalOp(const node::PlanNode* node) : node_(node) {}
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
typedef fesql::base::Graph<LogicalOp, HashLogicalOp, EqualLogicalOp>
    LogicalGraph;

class Transform {
 public:
    Transform();
    virtual ~Transform();
    bool TransformLogicalTreeToLogicalGraph(
        const ::fesql::node::PlanNode* node, fesql::base::Status& status,
        LogicalGraph& grash);  // NOLINT
};
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_TRANSFORM_H_
