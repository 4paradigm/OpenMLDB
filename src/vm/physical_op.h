/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * physical_op.h
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_PHYSICAL_OP_H_
#define SRC_VM_PHYSICAL_OP_H_
#include <node/plan_node.h>
#include "base/graph.h"
namespace fesql {
namespace vm {
enum PhysicalOpType {
//    kOpScan,
    kOpGroupBy,
    kOpSortBy,
    kOpLoops,
    kOpAggrerate,
    kOpWindowAggrerate,
//    kOpProject,
    kOpJoin,
    kOpUnoin
};


class PhysicalOpNode {
 public:
    const size_t Hash() const { return id_ % 100; }
    const bool Equals(const PhysicalOpNode& that) const {
        return id_ == that.id_;
    }
    const size_t id_;
};
struct HashPhysicalOp {
    size_t operator()(const class PhysicalOpNode& v) const {
        //  return  hash<int>(classA.getvalue());
        return v.Hash();
    }
};
struct EqualPhysicalOp {
    bool operator()(const class PhysicalOpNode& a1,
                    const class PhysicalOpNode& a2) const {
        return a1.Equals(a2);
    }
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_PHYSICAL_OP_H_
