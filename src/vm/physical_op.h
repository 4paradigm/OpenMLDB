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
    kPhysicalOpDataProvider,
    kPhysicalOpFilter,
    kPhysicalOpGroupBy,
    kPhysicalOpSortBy,
    kPhysicalOpGroupAndSort,
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
        case kPhysicalOpDataProvider:
            return "DATA_PROVIDER";
        case kPhysicalOpGroupBy:
            return "GROUP_BY";
        case kPhysicalOpSortBy:
            return "SORT_BY";
        case kPhysicalOpGroupAndSort:
            return "GROUP_AND_SORT_BY";
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
    virtual ~PhysicalOpNode() {}
    virtual bool consume() { return true; }
    virtual bool produce() { return true; }
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
    virtual bool InitSchema() = 0;
    virtual void PrintSchema();
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
        InitSchema();
    }
    virtual ~PhysicalUnaryNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
    bool InitSchema() override;
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
        InitSchema();
    }
    bool InitSchema() override;
    virtual ~PhysicalBinaryNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
};

enum DataProviderType {
    kProviderTypeTable,
    kProviderTypeIndexScan,
    kProviderTypeRequest
};

inline const std::string ScanTypeName(const DataProviderType &type) {
    switch (type) {
        case kProviderTypeTable:
            return "Table";
        case kProviderTypeIndexScan:
            return "IndexScan";
        case kProviderTypeRequest:
            return "Request";
        default:
            return "UNKNOW";
    }
}
class PhysicalDataProviderNode : public PhysicalOpNode {
 public:
    PhysicalDataProviderNode(const std::shared_ptr<TableHandler> &table_handler,
                             DataProviderType provider_type)
        : PhysicalOpNode(kPhysicalOpDataProvider, false, false),
          provider_type_(provider_type),
          table_handler_(table_handler) {
        InitSchema();
    }
    ~PhysicalDataProviderNode() {}
    bool InitSchema() override;
    const DataProviderType provider_type_;
    const std::shared_ptr<TableHandler> table_handler_;
};

class PhysicalTableProviderNode : public PhysicalDataProviderNode {
 public:
    explicit PhysicalTableProviderNode(
        const std::shared_ptr<TableHandler> &table_handler)
        : PhysicalDataProviderNode(table_handler, kProviderTypeTable) {}
    virtual ~PhysicalTableProviderNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
};

class PhysicalRequestProviderNode : public PhysicalDataProviderNode {
 public:
    explicit PhysicalRequestProviderNode(
        const std::shared_ptr<TableHandler> &table_handler)
        : PhysicalDataProviderNode(table_handler, kProviderTypeRequest) {}
    virtual ~PhysicalRequestProviderNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
};

class PhysicalScanIndexNode : public PhysicalDataProviderNode {
 public:
    PhysicalScanIndexNode(const std::shared_ptr<TableHandler> &table_handler,
                          const std::string &index_name)
        : PhysicalDataProviderNode(table_handler, kProviderTypeIndexScan),
          index_name_(index_name) {}
    virtual ~PhysicalScanIndexNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string index_name_;
};

class PhysicalGroupNode : public PhysicalUnaryNode {
 public:
    PhysicalGroupNode(PhysicalOpNode *node, const node::ExprListNode *groups)
        : PhysicalUnaryNode(node, kPhysicalOpGroupBy, true, false),
          groups_(groups) {}

    virtual ~PhysicalGroupNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
};

class PhysicalGroupAndSortNode : public PhysicalUnaryNode {
 public:
    PhysicalGroupAndSortNode(PhysicalOpNode *node,
                             const node::ExprListNode *groups,
                             const node::OrderByNode *orders)
        : PhysicalUnaryNode(node, kPhysicalOpGroupAndSort, true, false),
          groups_(groups),
          orders_(orders) {}

    virtual ~PhysicalGroupAndSortNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;

    const node::ExprListNode *groups_;
    const node::OrderByNode *orders_;
};

enum ProjectType {
    kRowProject,
    kTableProject,
    kAggregation,
    kGroupAggregation,
    kWindowAggregation,
};
inline const std::string ProjectTypeName(const ProjectType &type) {
    switch (type) {
        case kRowProject:
            return "RowProject";
        case kTableProject:
            return "TableProject";
        case kAggregation:
            return "Aggregation";
        case kGroupAggregation:
            return "GroupAggregation";
        case kWindowAggregation:
            return "WindowAggregation";
        default:
            return "UnKnown";
    }
}

class PhysicalProjectNode : public PhysicalUnaryNode {
 public:
    PhysicalProjectNode(PhysicalOpNode *node, const std::string &fn_name,
                        const Schema &schema, ProjectType project_type)
        : PhysicalUnaryNode(node, kPhysicalOpProject, false, false),
          project_type_(project_type),
          fn_name_(fn_name) {
        output_schema.CopyFrom(schema);
    }
    virtual ~PhysicalProjectNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    bool InitSchema() override;
    const ProjectType project_type_;
    const std::string fn_name_;
};

class PhysicalRowProjectNode : public PhysicalProjectNode {
 public:
    PhysicalRowProjectNode(PhysicalOpNode *node, const std::string fn_name,
                           const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kRowProject) {}
    virtual ~PhysicalRowProjectNode() {}
};

class PhysicalTableProjectNode : public PhysicalProjectNode {
 public:
    PhysicalTableProjectNode(PhysicalOpNode *node, const std::string fn_name,
                             const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kTableProject) {}
    virtual ~PhysicalTableProjectNode() {}
};
class PhysicalAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalAggrerationNode(PhysicalOpNode *node, const std::string &fn_name,
                            const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kAggregation) {}
    virtual ~PhysicalAggrerationNode() {}
};

class PhysicalGroupAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalGroupAggrerationNode(PhysicalOpNode *node,
                                 const node::ExprListNode *groups,
                                 const std::string &fn_name,
                                 const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kGroupAggregation),
          groups_(groups) {}
    virtual ~PhysicalGroupAggrerationNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
};

class PhysicalWindowAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalWindowAggrerationNode(PhysicalOpNode *node,
                                  const node::ExprListNode *groups,
                                  const node::OrderByNode *orders,
                                  const std::string &fn_name,
                                  const Schema &schema,
                                  const int64_t start_offset,
                                  const int64_t end_offset)
        : PhysicalProjectNode(node, fn_name, schema, kWindowAggregation),
          groups_(groups),
          orders_(orders),
          start_offset_(start_offset),
          end_offset_(end_offset) {}
    virtual ~PhysicalWindowAggrerationNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
    const node::OrderByNode *orders_;
    const int64_t start_offset_;
    const int64_t end_offset_;
};

class PhysicalBufferNode : public PhysicalUnaryNode {
 public:
    PhysicalBufferNode(PhysicalOpNode *node, const int64_t start,
                       const int64_t end)
        : PhysicalUnaryNode(node, kPhysicalOpBuffer, false, false),
          start_offset_(start),
          end_offset_(end) {}
    virtual ~PhysicalBufferNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const int64_t start_offset_;
    const int64_t end_offset_;
};
class PhysicalLoopsNode : public PhysicalUnaryNode {
 public:
    explicit PhysicalLoopsNode(PhysicalOpNode *node)
        : PhysicalUnaryNode(node, kPhysicalOpLoops, false, false) {}
    virtual ~PhysicalLoopsNode() {}
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
    virtual ~PhysicalJoinNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::JoinType join_type_;
    const node::ExprNode *condition_;
};

class PhysicalUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalUnionNode(PhysicalOpNode *left, PhysicalOpNode *right, bool is_all)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false, true),
          is_all_(is_all) {}
    virtual ~PhysicalUnionNode() {}
    bool InitSchema() override;
    const bool is_all_;
};

class PhysicalSortNode : public PhysicalUnaryNode {
 public:
    PhysicalSortNode(PhysicalOpNode *node, const node::OrderByNode *order)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true, false),
          order_(order) {}
    virtual ~PhysicalSortNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::OrderByNode *order_;
};

class PhysicalFliterNode : public PhysicalUnaryNode {
 public:
    PhysicalFliterNode(PhysicalOpNode *node, const node::ExprNode *condition)
        : PhysicalUnaryNode(node, kPhysicalOpFilter, false, false),
          condition_(condition) {}
    virtual ~PhysicalFliterNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprNode *condition_;
};

class PhysicalLimitNode : public PhysicalUnaryNode {
 public:
    PhysicalLimitNode(PhysicalOpNode *node, int32_t limit_cnt)
        : PhysicalUnaryNode(node, kPhysicalOpLimit, false, false),
          limit_cnt(limit_cnt) {}
    virtual ~PhysicalLimitNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const int32_t limit_cnt;
};

class PhysicalRenameNode : public PhysicalUnaryNode {
 public:
    PhysicalRenameNode(PhysicalOpNode *node, const std::string &name)
        : PhysicalUnaryNode(node, kPhysicalOpRename, false, false),
          name_(name) {}
    virtual ~PhysicalRenameNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string &name_;
};

class PhysicalDistinctNode : public PhysicalUnaryNode {
 public:
    explicit PhysicalDistinctNode(PhysicalOpNode *node)
        : PhysicalUnaryNode(node, kPhysicalOpDistinct, false, false) {}
    virtual ~PhysicalDistinctNode() {}
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_PHYSICAL_OP_H_
