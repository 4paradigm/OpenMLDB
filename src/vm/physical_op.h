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
#include <memory>
#include <string>
#include <vector>
#include "base/graph.h"
#include "node/plan_node.h"
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
    kPhysicalOpProject,
    kPhysicalOpLimit,
    kPhysicalOpRename,
    kPhysicalOpDistinct,
    kPhysicalOpJoin,
    kPhysicalOpUnoin,
    kPhysicalOpIndexSeek,
    kPhysicalOpRequestUnoin,
    kPhysicalOpRequestGroup,
    kPhysicalOpRequestGroupAndSort,
};

enum PhysicalSchemaType { kSchemaTypeTable, kSchemaTypeRow, kSchemaTypeGroup };
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
        case kPhysicalOpRequestUnoin:
            return "REQUEST_UNION";
        case kPhysicalOpIndexSeek:
            return "INDEX_SEEK";
        default:
            return "UNKNOW";
    }
}

class PhysicalOpNode {
 public:
    PhysicalOpNode(PhysicalOpType type, bool is_block, bool is_lazy)
        : type_(type),
          is_block_(is_block),
          is_lazy_(is_lazy),
          output_type(kSchemaTypeTable),
          fn_name_(""),
          fn_(nullptr),
          limit_cnt_(0) {}
    virtual ~PhysicalOpNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    void Print() const;

    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
    virtual bool InitSchema() = 0;
    virtual void PrintSchema();
    const std::vector<PhysicalOpNode *> &GetProducers() const {
        return producers_;
    }
    std::vector<PhysicalOpNode *> &GetProducers() { return producers_; }
    void UpdateProducer(int i, PhysicalOpNode *producer);

    void AddProducer(PhysicalOpNode *producer) {
        producers_.push_back(producer);
    }

    void SetFn(int8_t *fn) { fn_ = fn; }
    const int8_t *GetFn() const { return fn_; }

    void SetFnName(const std::string &fn_name) { fn_name_ = fn_name; }
    const std::string &GetFnName() const { return fn_name_; }

    void SetFnSchema(const Schema schema) { fn_schema_ = schema; }

    const vm::Schema &GetFnSchema() const { return fn_schema_; }

    void SetLimitCnt(int32_t limit_cnt) { limit_cnt_ = limit_cnt; }

    const int32_t GetLimitCnt() const { return limit_cnt_; }
    const PhysicalOpType type_;
    const bool is_block_;
    const bool is_lazy_;
    PhysicalSchemaType output_type;
    vm::Schema output_schema;

 protected:
    std::string fn_name_;
    int8_t *fn_;
    vm::Schema fn_schema_;
    int32_t limit_cnt_;
    std::vector<PhysicalOpNode *> producers_;
};

class PhysicalUnaryNode : public PhysicalOpNode {
 public:
    PhysicalUnaryNode(PhysicalOpNode *node, PhysicalOpType type, bool is_block,
                      bool is_lazy)
        : PhysicalOpNode(type, is_block, is_lazy) {
        AddProducer(node);
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
        InitSchema();
    }
    virtual bool InitSchema();
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

inline const std::string DataProviderTypeName(const DataProviderType &type) {
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
        : PhysicalOpNode(kPhysicalOpDataProvider, true, false),
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
        : PhysicalDataProviderNode(table_handler, kProviderTypeRequest) {
        output_type = kSchemaTypeRow;
    }
    virtual ~PhysicalRequestProviderNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
};

class PhysicalScanIndexNode : public PhysicalDataProviderNode {
 public:
    PhysicalScanIndexNode(const std::shared_ptr<TableHandler> table_handler,
                          const std::string &index_name)
        : PhysicalDataProviderNode(table_handler, kProviderTypeIndexScan),
          index_name_(index_name) {
        output_type = kSchemaTypeGroup;
    }
    virtual ~PhysicalScanIndexNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string index_name_;
};

class PhysicalGroupNode : public PhysicalUnaryNode {
 public:
    PhysicalGroupNode(PhysicalOpNode *node, const node::ExprListNode *groups)
        : PhysicalUnaryNode(node, kPhysicalOpGroupBy, true, false),
          groups_(groups) {
        output_type = kSchemaTypeGroup;
    }

    virtual ~PhysicalGroupNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
    void SetGroupsIdxs(const std::vector<int32_t> &idxs) {
        groups_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetGroupsIdxs() const { return groups_idxs_; }

 private:
    std::vector<int32_t> groups_idxs_;
};

class PhysicalGroupAndSortNode : public PhysicalUnaryNode {
 public:
    PhysicalGroupAndSortNode(PhysicalOpNode *node,
                             const node::ExprListNode *groups,
                             const node::OrderByNode *orders)
        : PhysicalUnaryNode(node, kPhysicalOpGroupAndSort, true, false),
          groups_(groups),
          orders_(orders) {
        output_type = kSchemaTypeGroup;
    }
    virtual ~PhysicalGroupAndSortNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
    const node::OrderByNode *orders_;
    void SetGroupsIdxs(const std::vector<int32_t> &idxs) {
        groups_idxs_ = idxs;
    }
    void SetOrdersIdxs(const std::vector<int32_t> &idxs) {
        orders_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetOrdersIdxs() const { return orders_idxs_; }
    const std::vector<int32_t> &GetGroupsIdxs() const { return groups_idxs_; }
    const bool GetIsAsc() const {
        return nullptr == orders_ ? true : orders_->is_asc_;
    }

 private:
    std::vector<int32_t> groups_idxs_;
    std::vector<int32_t> orders_idxs_;
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
                        const Schema &schema, ProjectType project_type,
                        const bool is_block, const bool is_lazy)
        : PhysicalUnaryNode(node, kPhysicalOpProject, is_block, is_lazy),
          project_type_(project_type) {
        output_schema.CopyFrom(schema);
        SetFnName(fn_name);
        SetFnSchema(schema);
    }
    virtual ~PhysicalProjectNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    bool InitSchema() override;
    const ProjectType project_type_;
};

class PhysicalRowProjectNode : public PhysicalProjectNode {
 public:
    PhysicalRowProjectNode(PhysicalOpNode *node, const std::string fn_name,
                           const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kRowProject, false,
                              false) {
        output_type = kSchemaTypeRow;
    }
    virtual ~PhysicalRowProjectNode() {}
};

class PhysicalTableProjectNode : public PhysicalProjectNode {
 public:
    PhysicalTableProjectNode(PhysicalOpNode *node, const std::string fn_name,
                             const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kTableProject, false,
                              false) {
        output_type = kSchemaTypeTable;
    }
    virtual ~PhysicalTableProjectNode() {}
};
class PhysicalAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalAggrerationNode(PhysicalOpNode *node, const std::string &fn_name,
                            const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kAggregation, true,
                              false) {
        output_type = kSchemaTypeRow;
    }
    virtual ~PhysicalAggrerationNode() {}
};

class PhysicalGroupAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalGroupAggrerationNode(PhysicalOpNode *node,
                                 const node::ExprListNode *groups,
                                 const std::string &fn_name,
                                 const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kGroupAggregation, true,
                              false),
          groups_(groups) {
        output_type = kSchemaTypeTable;
    }
    virtual ~PhysicalGroupAggrerationNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
    void SetGroupsIdxs(const std::vector<int32_t> &idxs) {
        groups_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetGroupsIdxs() const { return groups_idxs_; }

 private:
    std::vector<int32_t> groups_idxs_;
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
        : PhysicalProjectNode(node, fn_name, schema, kWindowAggregation, true,
                              false),
          groups_(groups),
          orders_(orders),
          start_offset_(start_offset),
          end_offset_(end_offset) {
        output_type = kSchemaTypeTable;
    }
    virtual ~PhysicalWindowAggrerationNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprListNode *groups_;
    const node::OrderByNode *orders_;
    const int64_t start_offset_;
    const int64_t end_offset_;

    void SetGroupsIdxs(const std::vector<int32_t> &idxs) {
        groups_idxs_ = idxs;
    }
    void SetOrdersIdxs(const std::vector<int32_t> &idxs) {
        orders_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetOrdersIdxs() const { return orders_idxs_; }
    const std::vector<int32_t> &GetGroupsIdxs() const { return groups_idxs_; }

 private:
    std::vector<int32_t> groups_idxs_;
    std::vector<int32_t> orders_idxs_;
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
          condition_(condition) {
        output_type = kSchemaTypeTable;
    }
    virtual ~PhysicalJoinNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::JoinType join_type_;
    const node::ExprNode *condition_;
    void SetConditionIdxs(const std::vector<int32_t> &idxs) {
        condition_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetConditionIdxs() const {
        return condition_idxs_;
    }

 private:
    std::vector<int32_t> condition_idxs_;
};

class PhysicalUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalUnionNode(PhysicalOpNode *left, PhysicalOpNode *right, bool is_all)
        : PhysicalBinaryNode(left, right, kPhysicalOpUnoin, true, true),
          is_all_(is_all) {
        output_type = kSchemaTypeTable;
    }
    virtual ~PhysicalUnionNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const bool is_all_;
};

class PhysicalSeekIndexNode : public PhysicalBinaryNode {
 public:
    PhysicalSeekIndexNode(PhysicalOpNode *left, PhysicalOpNode *right,
                          const node::ExprListNode *keys)
        : PhysicalBinaryNode(left, right, kPhysicalOpIndexSeek, true, true),
          keys_(keys) {
        output_type = kSchemaTypeGroup;
    }
    virtual ~PhysicalSeekIndexNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    void SetKeysIdxs(const std::vector<int32_t> &idxs) { keys_idxs_ = idxs; }

    const std::vector<int32_t> &GetKeysIdxs() const { return keys_idxs_; }
    const node::ExprListNode *keys_;

 private:
    std::vector<int32_t> keys_idxs_;
};

class PhysicalRequestUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalRequestUnionNode(PhysicalOpNode *left, PhysicalOpNode *right,
                             const node::ExprListNode *groups,
                             const node::OrderByNode *orders,
                             const node::OrderByNode *keys,
                             const int64_t start_offset,
                             const int64_t end_offset)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestUnoin, true, true),
          groups_(groups),
          orders_(orders),
          keys_(keys),
          start_offset_(start_offset),
          end_offset_(end_offset) {
        output_type = kSchemaTypeTable;
    }
    virtual ~PhysicalRequestUnionNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    void SetGroupsIdxs(const std::vector<int32_t> &idxs) {
        groups_idxs_ = idxs;
    }
    void SetOrdersIdxs(const std::vector<int32_t> &idxs) {
        orders_idxs_ = idxs;
    }
    void SetKeysIdxs(const std::vector<int32_t> &idxs) { keys_idxs_ = idxs; }

    const std::vector<int32_t> &GetKeysIdxs() const { return keys_idxs_; }
    const std::vector<int32_t> &GetOrdersIdxs() const { return orders_idxs_; }
    const std::vector<int32_t> &GetGroupsIdxs() const { return groups_idxs_; }
    const bool GetIsAsc() const {
        return nullptr == orders_ ? true : orders_->is_asc_;
    }

 private:
    std::vector<int32_t> groups_idxs_;
    std::vector<int32_t> orders_idxs_;
    std::vector<int32_t> keys_idxs_;

 public:
    const node::ExprListNode *groups_;
    const node::OrderByNode *orders_;
    const node::OrderByNode *keys_;

    const int64_t start_offset_;
    const int64_t end_offset_;
};

class PhysicalSortNode : public PhysicalUnaryNode {
 public:
    PhysicalSortNode(PhysicalOpNode *node, const node::OrderByNode *order)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true, false),
          order_(order) {}
    virtual ~PhysicalSortNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    void SetOrdersIdxs(const std::vector<int32_t> &idxs) {
        orders_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetOrdersIdxs() const { return orders_idxs_; }
    const node::OrderByNode *order_;

 private:
    std::vector<int32_t> orders_idxs_;
};

class PhysicalFliterNode : public PhysicalUnaryNode {
 public:
    PhysicalFliterNode(PhysicalOpNode *node, const node::ExprNode *condition)
        : PhysicalUnaryNode(node, kPhysicalOpFilter, true, false),
          condition_(condition) {}
    virtual ~PhysicalFliterNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::ExprNode *condition_;
    void SetConditionIdxs(const std::vector<int32_t> &idxs) {
        condition_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetConditionIdxs() const {
        return condition_idxs_;
    }

 private:
    std::vector<int32_t> condition_idxs_;
};

class PhysicalLimitNode : public PhysicalUnaryNode {
 public:
    PhysicalLimitNode(PhysicalOpNode *node, int32_t limit_cnt)
        : PhysicalUnaryNode(node, kPhysicalOpLimit, true, false) {
        limit_cnt_ = limit_cnt;
        limit_optimized_ = false;
    }
    virtual ~PhysicalLimitNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    void SetLimitOptimized(bool optimized) { limit_optimized_ = optimized; }
    const bool GetLimitOptimized() const { return limit_optimized_; }

 private:
    bool limit_optimized_;
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
        : PhysicalUnaryNode(node, kPhysicalOpDistinct, true, false) {}
    virtual ~PhysicalDistinctNode() {}
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_PHYSICAL_OP_H_
