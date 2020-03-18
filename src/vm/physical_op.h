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
#include <memory>
#include <string>
#include <vector>
#include "base/graph.h"
#include "vm/catalog.h"
namespace fesql {
namespace vm {

// new and delete physical node manage
enum PhysicalOpType {
    kPhysicalOpScan,
    kPhysicalOpFilter,
    kPhysicalOpGroupBy,
    kPhysicalOpSortBy,
    kPhysicalOpLoops,
    kPhysicalOpAggrerate,
    kPhysicalOpBuffer,
    kPhysicalOpProject,
    kPhysicalOpLimit,
    kPhysicalOpRename,
    kPhysicalOpDistinct,
    kPhysicalOpJoin,
    kPhysicalOpUnoin
};

inline const std::string PhysicalOpTypeName(const PhysicalOpType &type) {
    switch (type) {
        case kPhysicalOpScan:
            return "SCAN";
        case kPhysicalOpGroupBy:
            return "GROUP_BY";
        case kPhysicalOpSortBy:
            return "SORT_BY";
        case kPhysicalOpFilter:
            return "FILTER_BY";
        case kPhysicalOpLoops:
            return "LOOPS";
        case kPhysicalOpProject:
            return "PROJECT";
        case kPhysicalOpAggrerate:
            return "AGGRERATE";
        case kPhysicalOpBuffer:
            return "BUFFER";
        case kPhysicalOpLimit:
            return "LIMIT";
        case kPhysicalOpRename:
            return "RENAME";
        case kPhysicalOpDistinct:
            return "DISTINCT";
        case kPhysicalOpJoin:
            return "JOIN";
        case kPhysicalOpUnoin:
            return "UNION";
        default:
            return "UNKNOW";
    }
}
class PhysicalOpNode;

class PhysicalOpNode {
 public:
    PhysicalOpNode(PhysicalOpType type, bool is_block, bool is_lazy)
        : type_(type), is_block_(is_block), is_lazy_(is_lazy) {}
    virtual bool consume() { return true; }
    virtual bool produce() { return true; }
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
    std::vector<PhysicalOpNode *> &GetProducers() { return producers_; }
    void UpdateProducer(int i, PhysicalOpNode *producer);

    void AddConsumer(PhysicalOpNode *consumer) {
        consumers_.push_back(consumer);
    }

    void AddProducer(PhysicalOpNode *producer) {
        producers_.push_back(producer);
    }
    const PhysicalOpType type_;
    const bool is_block_;
    const bool is_lazy_;
    vm::Schema output_schema;

 protected:
    std::vector<PhysicalOpNode *> consumers_;
    std::vector<PhysicalOpNode *> producers_;
};

class PhysicalUnaryNode : public PhysicalOpNode {
 public:
    PhysicalUnaryNode(PhysicalOpNode *node, PhysicalOpType type, bool is_block,
                      bool is_lazy)
        : PhysicalOpNode(type, is_block, is_lazy) {
        AddProducer(node);
        producers_[0]->AddConsumer(this);
    }
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
};

class PhysicalBinaryNode : public PhysicalOpNode {
 public:
    PhysicalBinaryNode(PhysicalOpNode *left, PhysicalOpNode *right,
                       PhysicalOpType type, bool is_block, bool is_lazy)
        : PhysicalOpNode(type, is_block, is_lazy) {
        AddProducer(left);
        AddProducer(right);
        left->AddConsumer(this);
        right->AddConsumer(this);
    }
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
};

enum ScanType { kScanTypeTableScan, kScanTypeIndexScan };

inline const std::string ScanTypeName(const ScanType &type) {
    switch (type) {
        case kScanTypeTableScan:
            return "TableScan";
        case kScanTypeIndexScan:
            return "IndexScan";
        default:
            return "UNKNOW";
    }
}
class PhysicalScanNode : public PhysicalOpNode {
 public:
    PhysicalScanNode(const std::shared_ptr<TableHandler> &table_handler,
                     ScanType scan_type)
        : PhysicalOpNode(kPhysicalOpScan, false, false),
          scan_type_(scan_type),
          table_handler_(table_handler) {
        output_schema = table_handler->GetSchema();
    }
    const ScanType scan_type_;
    const std::shared_ptr<TableHandler> table_handler_;
};

class PhysicalScanTableNode : public PhysicalScanNode {
 public:
    explicit PhysicalScanTableNode(
        const std::shared_ptr<TableHandler> &table_handler)
        : PhysicalScanNode(table_handler, kScanTypeTableScan) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
};

class PhysicalScanIndexNode : public PhysicalScanNode {
 public:
    PhysicalScanIndexNode(const std::shared_ptr<TableHandler> &table_handler,
                          const std::string &index_name)
        : PhysicalScanNode(table_handler, kScanTypeIndexScan),
          index_name_(index_name) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string index_name_;
};

class PhysicalGroupNode : public PhysicalUnaryNode {
 public:
    PhysicalGroupNode(PhysicalOpNode *node, const node::ExprListNode *groups)
        : PhysicalUnaryNode(node, kPhysicalOpGroupBy, true, false),
          groups_(groups) {}
    PhysicalGroupNode(PhysicalOpNode *node,
                      const std::vector<std::string> &groups)
        : PhysicalUnaryNode(node, kPhysicalOpGroupBy, true, false) {
        // TODO(chenjing): remove 临时适配, 有mem泄漏问题
        node::ExprListNode *expr = new node::ExprListNode();
        for (auto id : groups) {
            expr->AddChild(new node::ColumnRefNode(id, ""));
        }
        groups_ = expr;
    }
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
};

enum ProjectType {
    kProjectRow,
    kProjectAggregation,
};
inline const std::string ProjectTypeName(const ProjectType &type) {
    switch (type) {
        case kProjectRow:
            return "ProjectRow";
        case kProjectAggregation:
            return "Aggregation";
        default:
            return "Unknow";
    }
}

class PhysicalProjectNode : public PhysicalUnaryNode {
 public:
    PhysicalProjectNode(PhysicalOpNode *node, const node::PlanNodeList *project,
                        ProjectType project_type)
        : PhysicalUnaryNode(node, kPhysicalOpProject, false, false),
          project_type_(project_type),
          project_(project) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const ProjectType project_type_;
    const node::PlanNodeList *project_;
};

class PhysicalRowProjectNode : public PhysicalProjectNode {
 public:
    PhysicalRowProjectNode(PhysicalOpNode *node,
                           const node::PlanNodeList *project)
        : PhysicalProjectNode(node, project, kProjectRow) {}
};

class PhysicalAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalAggrerationNode(PhysicalOpNode *node,
                            const node::PlanNodeList *project)
        : PhysicalProjectNode(node, project, kProjectAggregation) {}
};

class PhysicalBufferNode : public PhysicalUnaryNode {
 public:
    PhysicalBufferNode(PhysicalOpNode *node, const int64_t start,
                       const int64_t end)
        : PhysicalUnaryNode(node, kPhysicalOpBuffer, false, false),
          start_offset_(start),
          end_offset_(end) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const int64_t start_offset_;
    const int64_t end_offset_;
};
class PhysicalLoopsNode : public PhysicalUnaryNode {
 public:
    explicit PhysicalLoopsNode(PhysicalOpNode *node)
        : PhysicalUnaryNode(node, kPhysicalOpLoops, false, false) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
};

class PhysicalJoinNode : public PhysicalBinaryNode {
 public:
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type,
                     const node::ExprNode *condition)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false, true),
          join_type_(join_type),
          condition_(condition) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::JoinType join_type_;
    const node::ExprNode *condition_;
};

class PhysicalUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalUnionNode(PhysicalOpNode *left, PhysicalOpNode *right, bool is_all)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false, true),
          is_all_(is_all) {}
    const bool is_all_;
};

class PhysicalSortNode : public PhysicalUnaryNode {
 public:
    PhysicalSortNode(PhysicalOpNode *node, const node::OrderByNode *order)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true, false),
          order_(order) {}
    PhysicalSortNode(PhysicalOpNode *node,
                     const std::vector<std::string> orders)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true, false) {
        // TODO(chenjing): remove 临时适配, 有mem泄漏问题
        node::ExprListNode *expr = new node::ExprListNode();
        for (auto id : orders) {
            expr->AddChild(new node::ColumnRefNode(id, ""));
        }
        order_ = new node::OrderByNode(expr, true);
    }
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::OrderByNode *order_;
};

class PhysicalFliterNode : public PhysicalUnaryNode {
 public:
    PhysicalFliterNode(PhysicalOpNode *node, const node::ExprNode *condition)
        : PhysicalUnaryNode(node, kPhysicalOpFilter, false, false),
          condition_(condition) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprNode *condition_;
};

class PhysicalLimitNode : public PhysicalUnaryNode {
 public:
    PhysicalLimitNode(PhysicalOpNode *node, int32_t limit_cnt)
        : PhysicalUnaryNode(node, kPhysicalOpLimit, false, false),
          limit_cnt(limit_cnt) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const int32_t limit_cnt;
};

class PhysicalRenameNode : public PhysicalUnaryNode {
 public:
    PhysicalRenameNode(PhysicalOpNode *node, const std::string &name)
        : PhysicalUnaryNode(node, kPhysicalOpRename, false, false),
          name_(name) {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string &name_;
};

class PhysicalDistinctNode : public PhysicalUnaryNode {
 public:
    explicit PhysicalDistinctNode(PhysicalOpNode *node)
        : PhysicalUnaryNode(node, kPhysicalOpDistinct, false, false) {}
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_PHYSICAL_OP_H_
