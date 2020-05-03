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
#include <utility>
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
    kPhysicalOpAggrerate,
    kPhysicalOpWindowAgg,
    kPhysicalOpProject,
    kPhysicalOpLimit,
    kPhysicalOpRename,
    kPhysicalOpDistinct,
    kPhysicalOpJoin,
    kPhysicalOpUnoin,
    kPhysicalOpIndexSeek,
    kPhysicalOpRequestUnoin,
    kPhysicalOpRequestJoin,
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
        case kPhysicalOpFilter:
            return "FILTER_BY";
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
        case kPhysicalOpRequestJoin:
            return "REQUEST_JOIN";
        case kPhysicalOpIndexSeek:
            return "INDEX_SEEK";
        default:
            return "UNKNOW";
    }
}
struct FnInfo {
    std::string fn_name_ = "";
    int8_t *fn_ = nullptr;
    vm::Schema fn_schema_;
};

class Group {
 public:
    Group(const node::ExprListNode *groups) : groups_(groups) {}
    virtual ~Group() {}
    void SetGroupsIdxs(const std::vector<int32_t> &idxs) {
        groups_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetGroupsIdxs() const { return groups_idxs_; }
    void set_groups(const node::ExprListNode *groups) { groups_ = groups; }
    const node::ExprListNode *groups() const { return groups_; }
    const bool ValidGroup() { return !node::ExprListNullOrEmpty(groups_); }
    const node::ExprListNode *groups_;
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "groups=" << node::ExprString(groups_);
        return oss.str();
    }
    FnInfo fn_info_;

 protected:
    std::vector<int32_t> groups_idxs_;
};

class Sort {
 public:
    Sort(const node::OrderByNode *orders) : orders_(orders) {}
    virtual ~Sort() {}
    const node::OrderByNode *orders() const { return orders_; }
    void set_orders(const node::OrderByNode *orders) { orders_ = orders; }
    const std::vector<int32_t> &GetOrdersIdxs() const { return orders_idxs_; }
    const bool GetIsAsc() const {
        return nullptr == orders_ ? true : orders_->is_asc_;
    }
    void SetOrdersIdxs(const std::vector<int32_t> &idxs) {}
    const bool ValidSort() { return nullptr != orders_; }
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "orders=" << node::ExprString(orders_);
        return oss.str();
    }
    const node::OrderByNode *orders_;
    FnInfo fn_info_;

 protected:
    std::vector<int32_t> orders_idxs_;
};

class Range {
 public:
    Range(const node::OrderByNode *order, const int64_t start_offset,
          const int64_t end_offset)
        : range_key_(nullptr),
          start_offset_(start_offset),
          end_offset_(end_offset) {
        range_key_ = nullptr == order
                         ? nullptr
                         : node::ExprListNullOrEmpty(order->order_by_)
                               ? nullptr
                               : order->order_by_->children_[0];
    }
    virtual ~Range() {}
    const node::ExprNode *range_key() { return range_key_; }
    void set_range_key(const node::ExprNode *range_key) {
        range_key_ = range_key;
    }
    const bool Valid() const { return nullptr != range_key_; }
    const std::string ToString() const {
        std::ostringstream oss;
        if (nullptr != range_key_) {
            oss << "range=(" << node::ExprString(range_key_) << ", "
                << start_offset_ << ", " << end_offset_ << ")";
        }
        return oss.str();
    }
    FnInfo fn_info_;
    const node::ExprNode *range_key_;
    int64_t start_offset_;
    int64_t end_offset_;
};

class ConditionFilter {
 public:
    ConditionFilter(const node::ExprNode *condition) : condition_(condition) {}
    virtual ~ConditionFilter() {}
    void SetConditionIdxs(const std::vector<int32_t> &idxs) {
        condition_idxs_ = idxs;
    }
    const std::vector<int32_t> &GetConditionIdxs() const {
        return condition_idxs_;
    }
    const bool ValidCondition() { return nullptr != condition_; }
    void set_condition(const node::ExprNode *condition) {
        condition_ = condition;
    }
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "condition=" << node::ExprString(condition_);
        return oss.str();
    }
    const node::ExprNode *condition_;
    FnInfo fn_info_;

 protected:
    std::vector<int32_t> condition_idxs_;
};

class Hash {
 public:
    Hash() : keys_(nullptr) {}
    Hash(const node::ExprListNode *keys) : keys_(keys) {}
    virtual ~Hash() {}
    void SetKeysIdxs(const std::vector<int32_t> &idxs) { key_idxs_ = idxs; }
    const std::vector<int32_t> &GetKeysIdxs() const { return key_idxs_; }
    void set_keys(const node::ExprListNode *keys) { keys_ = keys; }
    const node::ExprListNode *keys() const { return keys_; }
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "keys=" << node::ExprString(keys_);
        return oss.str();
    }
    const bool Valid() const { return !node::ExprListNullOrEmpty(keys_); }
    const node::ExprListNode *keys_;
    FnInfo fn_info_;

 protected:
    std::vector<int32_t> key_idxs_;
};

class PhysicalOpNode {
 public:
    PhysicalOpNode(PhysicalOpType type, bool is_block, bool is_lazy)
        : type_(type),
          is_block_(is_block),
          is_lazy_(is_lazy),
          output_type_(kSchemaTypeTable),
          fn_info_({"", nullptr}),
          fn_infos_({&fn_info_}),
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
    std::vector<PhysicalOpNode *> &producers() { return producers_; }
    void UpdateProducer(int i, PhysicalOpNode *producer);

    void AddProducer(PhysicalOpNode *producer) {
        producers_.push_back(producer);
    }

    std::vector<FnInfo *> GetFnInfos() const { return fn_infos_; }
    const FnInfo &GetFnInfo() const { return fn_info_; }
    void SetFn(int8_t *fn) { fn_info_.fn_ = fn; }
    const int8_t *GetFn() const { return fn_info_.fn_; }
    PhysicalOpNode *GetProducer(size_t index) const {
        return producers_[index];
    }

    const vm::Schema *GetOutputSchema() const { return &output_schema_; }

    void SetProducer(size_t index, PhysicalOpNode *produce) {
        producers_[index] = produce;
    }
    size_t GetProducerCnt() const { return producers_.size(); }

    void SetFnName(const std::string &fn_name) { fn_info_.fn_name_ = fn_name; }
    const std::string &GetFnName() const { return fn_info_.fn_name_; }

    void SetFnSchema(const Schema schema) { fn_info_.fn_schema_ = schema; }

    const vm::Schema &GetFnSchema() const { return fn_info_.fn_schema_; }

    const std::vector<std::pair<const std::string, const vm::Schema *>>
        &GetOutputNameSchemaList() {
        return output_name_schema_list_;
    }

    void SetLimitCnt(int32_t limit_cnt) { limit_cnt_ = limit_cnt; }

    const int32_t GetLimitCnt() const { return limit_cnt_; }
    const PhysicalOpType type_;
    const bool is_block_;
    const bool is_lazy_;
    PhysicalSchemaType output_type_;
    vm::Schema output_schema_;

 protected:
    FnInfo fn_info_;
    std::vector<FnInfo *> fn_infos_;
    int32_t limit_cnt_;
    std::vector<PhysicalOpNode *> producers_;
    std::vector<std::pair<const std::string, const vm::Schema *>>
        output_name_schema_list_;
};
class PhysicalUnaryNode : public PhysicalOpNode {
 public:
    PhysicalUnaryNode(PhysicalOpNode *node, PhysicalOpType type, bool is_block,
                      bool is_lazy)
        : PhysicalOpNode(type, is_block, is_lazy) {
        AddProducer(node);
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
    }
    virtual ~PhysicalBinaryNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
};

enum DataProviderType {
    kProviderTypeTable,
    kProviderTypePartition,
    kProviderTypeRequest
};

inline const std::string DataProviderTypeName(const DataProviderType &type) {
    switch (type) {
        case kProviderTypeTable:
            return "Table";
        case kProviderTypePartition:
            return "Partition";
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
    static PhysicalDataProviderNode *CastFrom(PhysicalOpNode *node);
    const std::string &GetName() const;
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
        output_type_ = kSchemaTypeRow;
    }
    virtual ~PhysicalRequestProviderNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
};

class PhysicalPartitionProviderNode : public PhysicalDataProviderNode {
 public:
    PhysicalPartitionProviderNode(
        const std::shared_ptr<TableHandler> table_handler,
        const std::string &index_name)
        : PhysicalDataProviderNode(table_handler, kProviderTypePartition),
          index_name_(index_name) {
        output_type_ = kSchemaTypeGroup;
    }
    virtual ~PhysicalPartitionProviderNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string index_name_;
};

class PhysicalGroupNode : public PhysicalUnaryNode {
 public:
    PhysicalGroupNode(PhysicalOpNode *node, const node::ExprListNode *groups)
        : PhysicalUnaryNode(node, kPhysicalOpGroupBy, true, false),
          group_(groups) {
        output_type_ = kSchemaTypeGroup;
        InitSchema();
        fn_infos_.push_back(&group_.fn_info_);
    }
    virtual ~PhysicalGroupNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    bool Valid() { return group_.ValidGroup(); }
    Group group() const { return group_; }
    Group group_;
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
          project_type_(project_type),
          project_({fn_name, nullptr, schema}) {
        output_schema_ = schema;
        InitSchema();
        fn_infos_.push_back(&project_);
    }
    virtual ~PhysicalProjectNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    bool InitSchema() override;
    static PhysicalProjectNode *CastFrom(PhysicalOpNode *node);
    const ProjectType project_type_;
    FnInfo project_;
};

class PhysicalRowProjectNode : public PhysicalProjectNode {
 public:
    PhysicalRowProjectNode(PhysicalOpNode *node, const std::string fn_name,
                           const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kRowProject, false,
                              false) {
        output_type_ = kSchemaTypeRow;
    }
    virtual ~PhysicalRowProjectNode() {}
    static PhysicalRowProjectNode *CastFrom(PhysicalOpNode *node);
};

class PhysicalTableProjectNode : public PhysicalProjectNode {
 public:
    PhysicalTableProjectNode(PhysicalOpNode *node, const std::string fn_name,
                             const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kTableProject, false,
                              false) {
        output_type_ = kSchemaTypeTable;
    }
    virtual ~PhysicalTableProjectNode() {}
    static PhysicalTableProjectNode *CastFrom(PhysicalOpNode *node);
};
class PhysicalAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalAggrerationNode(PhysicalOpNode *node, const std::string &fn_name,
                            const Schema &schema)
        : PhysicalProjectNode(node, fn_name, schema, kAggregation, true,
                              false) {
        output_type_ = kSchemaTypeRow;
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
          group_(groups) {
        output_type_ = kSchemaTypeTable;
        fn_infos_.push_back(&group_.fn_info_);
    }
    virtual ~PhysicalGroupAggrerationNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    Group group_;
};

class PhysicalUnionNode;
class PhysicalJoinNode;
class Project {
 public:
    Project() {}
    virtual ~Project() {}
    FnInfo fn_info_;
};
class WindowOp {
 public:
    WindowOp(const node::ExprListNode *groups, const node::OrderByNode *orders,
             const int64_t start_offset, const int64_t end_offset)
        : group_(groups),
          sort_(orders),
          range_(orders, start_offset, end_offset) {}
    virtual ~WindowOp() {}
    const std::string ToString() const {
        std::ostringstream oss;
        oss << group_.ToString();
        oss << ", " << sort_.ToString();
        if (range_.Valid()) {
            oss << ", " << range_.ToString();
        }

        return oss.str();
    }
    Group group_;
    Sort sort_;
    Range range_;
};
class Join {
 public:
    Join() : filter_(nullptr), left_hash_(nullptr), right_partition_(nullptr) {}
    Join(const node::ExprNode *condition)
        : filter_(condition), left_hash_(nullptr), right_partition_(nullptr) {}
    Join(const node::ExprNode *condition, const node::ExprListNode *left_keys,
         const node::ExprListNode *right_keys)
        : filter_(condition),
          left_hash_(left_keys),
          right_partition_(right_keys) {}
    virtual ~Join() {}
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "condition=" << node::ExprString(filter_.condition_)
            << ", left_keys=" << node::ExprString(left_hash_.keys())
            << ", right_groups=" << node::ExprString(right_partition_.groups());
        return oss.str();
    }
    Hash left_hash_;
    Group right_partition_;
    ConditionFilter filter_;
};

class Union {
 public:
    Union() : need_union_(false) {}

    bool need_union_;
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
          window_(groups, orders, start_offset, end_offset),
          union_(),
          join_() {
        output_type_ = kSchemaTypeTable;
        InitSchema();
        fn_infos_.push_back(&window_.group_.fn_info_);
        fn_infos_.push_back(&window_.sort_.fn_info_);
        fn_infos_.push_back(&window_.range_.fn_info_);
    }
    PhysicalWindowAggrerationNode(
        PhysicalOpNode *node, PhysicalOpNode *union_with,
        PhysicalOpNode *join_with, const node::ExprListNode *groups,
        const node::OrderByNode *orders, const std::string &fn_name,
        const Schema &schema, const int64_t start_offset,
        const int64_t end_offset)
        : PhysicalProjectNode(node, fn_name, schema, kWindowAggregation, true,
                              false),
          window_(groups, orders, start_offset, end_offset),
          union_(),
          join_() {
        AddProducer(union_with);
        AddProducer(join_with);
        output_type_ = kSchemaTypeTable;
        InitSchema();
        fn_infos_.push_back(&window_.group_.fn_info_);
        fn_infos_.push_back(&window_.sort_.fn_info_);
        fn_infos_.push_back(&window_.range_.fn_info_);
    }
    virtual ~PhysicalWindowAggrerationNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    static PhysicalWindowAggrerationNode *CastFrom(PhysicalOpNode *node);
    const bool Valid() { return true; }
    WindowOp window_;
    Union union_;
    Join join_;
};

class PhysicalJoinNode : public PhysicalBinaryNode {
 public:
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false, true),
          join_type_(join_type),
          join_() {
        output_type_ = kSchemaTypeTable;
        InitSchema();
        fn_infos_.push_back(&join_.filter_.fn_info_);
        fn_infos_.push_back(&join_.left_hash_.fn_info_);
        fn_infos_.push_back(&join_.right_partition_.fn_info_);
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type,
                     const node::ExprNode *condition)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false, true),
          join_type_(join_type),
          join_(condition) {
        output_type_ = kSchemaTypeTable;
        InitSchema();
        fn_infos_.push_back(&join_.filter_.fn_info_);
        fn_infos_.push_back(&join_.left_hash_.fn_info_);
        fn_infos_.push_back(&join_.right_partition_.fn_info_);
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type,
                     const node::ExprNode *condition,
                     const node::ExprListNode *left_keys,
                     const node::ExprListNode *right_keys)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false, true),
          join_type_(join_type),
          join_(condition, left_keys, right_keys) {
        output_type_ = kSchemaTypeTable;
        InitSchema();
        fn_infos_.push_back(&join_.filter_.fn_info_);
        fn_infos_.push_back(&join_.left_hash_.fn_info_);
        fn_infos_.push_back(&join_.right_partition_.fn_info_);
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type, Join &join)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false, true),
          join_type_(join_type),
          join_(join) {
        output_type_ = kSchemaTypeTable;
        InitSchema();
        fn_infos_.push_back(&join_.filter_.fn_info_);
        fn_infos_.push_back(&join_.left_hash_.fn_info_);
        fn_infos_.push_back(&join_.right_partition_.fn_info_);
    }
    virtual ~PhysicalJoinNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const bool Valid() { return true; }
    const node::JoinType join_type_;
    Join join_;
};
class PhysicalRequestJoinNode : public PhysicalBinaryNode {
 public:
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const node::JoinType join_type)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false, true),
          join_type_(join_type),
          join_(),
          hash_() {
        output_type_ = kSchemaTypeRow;
        InitSchema();
        fn_infos_.push_back(&join_.filter_.fn_info_);
        fn_infos_.push_back(&join_.left_hash_.fn_info_);
        fn_infos_.push_back(&join_.right_partition_.fn_info_);
        fn_infos_.push_back(&hash_.fn_info_);
    }
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const node::JoinType join_type,
                            const node::ExprNode *condition)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false, true),
          join_type_(join_type),
          join_(condition),
          hash_() {
        output_type_ = kSchemaTypeRow;
        InitSchema();
        fn_infos_.push_back(&join_.filter_.fn_info_);
        fn_infos_.push_back(&join_.left_hash_.fn_info_);
        fn_infos_.push_back(&join_.right_partition_.fn_info_);
        fn_infos_.push_back(&hash_.fn_info_);
    }
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const node::JoinType join_type,
                            const node::ExprNode *condition,
                            const node::ExprListNode *left_keys,
                            const node::ExprListNode *right_keys)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false, true),
          join_type_(join_type),
          join_(condition, left_keys, right_keys),
          hash_() {
        output_type_ = kSchemaTypeRow;
        InitSchema();
        fn_infos_.push_back(&join_.filter_.fn_info_);
        fn_infos_.push_back(&join_.left_hash_.fn_info_);
        fn_infos_.push_back(&join_.right_partition_.fn_info_);
        fn_infos_.push_back(&hash_.fn_info_);
    }
    virtual ~PhysicalRequestJoinNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const node::JoinType join_type_;
    Join join_;
    Hash hash_;
};
class PhysicalUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalUnionNode(PhysicalOpNode *left, PhysicalOpNode *right, bool is_all)
        : PhysicalBinaryNode(left, right, kPhysicalOpUnoin, true, true),
          is_all_(is_all) {
        output_type_ = kSchemaTypeTable;
        InitSchema();
    }
    virtual ~PhysicalUnionNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const bool is_all_;
};

class PhysicalRequestUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalRequestUnionNode(PhysicalOpNode *left, PhysicalOpNode *right,
                             const node::ExprListNode *groups,
                             const node::OrderByNode *orders,
                             const int64_t start_offset,
                             const int64_t end_offset)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestUnoin, true, true),
          window_(groups, orders, start_offset, end_offset),
          hash_(nullptr) {
        output_type_ = kSchemaTypeTable;
        InitSchema();
        fn_infos_.push_back(&window_.group_.fn_info_);
        fn_infos_.push_back(&window_.sort_.fn_info_);
        fn_infos_.push_back(&window_.range_.fn_info_);
        fn_infos_.push_back(&hash_.fn_info_);
    }
    virtual ~PhysicalRequestUnionNode() {}
    bool InitSchema() override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const bool Valid() { return true; }
    WindowOp window_;
    Hash hash_;
};

class PhysicalSortNode : public PhysicalUnaryNode {
 public:
    PhysicalSortNode(PhysicalOpNode *node, const node::OrderByNode *order)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true, false),
          sort_(order) {
        output_type_ = node->output_type_;
        InitSchema();
        fn_infos_.push_back(&sort_.fn_info_);
    }
    PhysicalSortNode(PhysicalOpNode *node, Sort &sort)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true, false), sort_(sort) {
        output_type_ = node->output_type_;
        InitSchema();
        fn_infos_.push_back(&sort_.fn_info_);
    }
    virtual ~PhysicalSortNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;

    bool Valid() { return sort_.ValidSort(); }
    Sort sort_;
};

class PhysicalFliterNode : public PhysicalUnaryNode {
 public:
    PhysicalFliterNode(PhysicalOpNode *node, const node::ExprNode *condition)
        : PhysicalUnaryNode(node, kPhysicalOpFilter, true, false),
          filter_(condition) {
        output_type_ = node->output_type_;
        InitSchema();
        fn_infos_.push_back(&filter_.fn_info_);
    }
    virtual ~PhysicalFliterNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    bool Valid() { return filter_.ValidCondition(); }
    ConditionFilter filter_;
};

class PhysicalLimitNode : public PhysicalUnaryNode {
 public:
    PhysicalLimitNode(PhysicalOpNode *node, int32_t limit_cnt)
        : PhysicalUnaryNode(node, kPhysicalOpLimit, true, false) {
        limit_cnt_ = limit_cnt;
        limit_optimized_ = false;
        output_type_ = node->output_type_;
        InitSchema();
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
          name_(name) {
        output_type_ = node->output_type_;
        InitSchema();
    }
    virtual ~PhysicalRenameNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string &name_;
};

class PhysicalDistinctNode : public PhysicalUnaryNode {
 public:
    explicit PhysicalDistinctNode(PhysicalOpNode *node)
        : PhysicalUnaryNode(node, kPhysicalOpDistinct, true, false) {
        output_type_ = node->output_type_;
        InitSchema();
    }
    virtual ~PhysicalDistinctNode() {}
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_PHYSICAL_OP_H_
