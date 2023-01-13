/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_INCLUDE_VM_PHYSICAL_OP_H_
#define HYBRIDSE_INCLUDE_VM_PHYSICAL_OP_H_
#include <list>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "node/plan_node.h"
#include "passes/expression/expr_pass.h"
#include "vm/catalog.h"
#include "vm/schemas_context.h"
namespace hybridse {
namespace vm {

using hybridse::base::Status;
using hybridse::vm::SchemasContext;

// new and delete physical node managef
enum PhysicalOpType {
    kPhysicalOpDataProvider = 0,
    kPhysicalOpFilter,
    kPhysicalOpGroupBy,
    kPhysicalOpSortBy,
    kPhysicalOpAggregate,
    kPhysicalOpProject,
    kPhysicalOpSimpleProject,
    kPhysicalOpConstProject,
    kPhysicalOpLimit,
    kPhysicalOpRename,
    kPhysicalOpDistinct,
    kPhysicalOpJoin,
    kPhysicalOpUnion,
    kPhysicalOpWindow,
    kPhysicalOpIndexSeek,
    kPhysicalOpRequestUnion,
    kPhysicalOpRequestAggUnion,
    kPhysicalOpPostRequestUnion,
    kPhysicalOpRequestJoin,
    kPhysicalOpRequestGroup,
    kPhysicalOpRequestGroupAndSort,
    kPhysicalOpLoadData,
    kPhysicalOpDelete,
    kPhysicalOpSelectInto,
    kPhysicalOpInsert,
    kPhysicalCreateTable,
    kPhysicalOpFake,  // not a real type, for testing only
    kPhysicalOpLast = kPhysicalOpFake,
};

enum PhysicalSchemaType { kSchemaTypeTable, kSchemaTypeRow, kSchemaTypeGroup };
absl::string_view PhysicalOpTypeName(PhysicalOpType type);

/**
 * Function codegen information for physical node. It should
 * provide full information to generate execution code.
 * FnInfo should be never shared between different node.
 */
class FnInfo {
 public:
    const std::string &fn_name() const { return fn_name_; }
    const vm::Schema *fn_schema() const { return &fn_schema_; }
    const node::LambdaNode *fn_def() const { return fn_def_; }
    const SchemasContext *schemas_ctx() const { return schemas_ctx_; }

    bool IsValid() const {
        return fn_name_ != "" && !fn_schema_.empty() && fn_def_ != nullptr;
    }

    void SetFn(const std::string &fn_name, const node::LambdaNode *fn_def,
               const SchemasContext *schemas_ctx) {
        fn_name_ = fn_name;
        fn_def_ = fn_def;
        schemas_ctx_ = schemas_ctx;
    }

    void AddOutputColumn(const type::ColumnDef &column_def,
                         const node::FrameNode *frame = nullptr) {
        *fn_schema_.Add() = column_def;
        frames_.push_back(frame);
    }

    void SetPrimaryFrame(const node::FrameNode *frame) {
        primary_frame_ = frame;
    }

    void Clear() {
        fn_name_ = "";
        fn_schema_.Clear();
        fn_def_ = nullptr;
        primary_frame_ = nullptr;
        frames_.clear();
        schemas_ctx_ = nullptr;
        fn_ptr_ = nullptr;
    }

    const node::FrameNode *GetFrame(size_t idx) const {
        return idx < frames_.size() ? frames_[idx] : nullptr;
    }
    const std::vector<const node::FrameNode *> &GetFrames() const {
        return frames_;
    }
    const node::FrameNode *GetPrimaryFrame() const { return primary_frame_; }

    FnInfo() = default;

    const int8_t *fn_ptr() const { return fn_ptr_; }
    void SetFnPtr(const int8_t *fn) { fn_ptr_ = fn; }

 private:
    std::string fn_name_ = "";
    vm::Schema fn_schema_;

    // function definition
    const node::LambdaNode *fn_def_ = nullptr;

    // expression frames for agg projection
    const node::FrameNode *primary_frame_ = nullptr;
    std::vector<const node::FrameNode *> frames_;

    // codegen output schemas context
    const SchemasContext *schemas_ctx_ = nullptr;

    // function ptr
    const int8_t *fn_ptr_ = nullptr;
};

class FnComponent {
 public:
    const FnInfo &fn_info() const { return fn_info_; }
    FnInfo *mutable_fn_info() { return &fn_info_; }

 protected:
    FnInfo fn_info_;
};
// Sort Component can only handle single order expressions
class Sort : public FnComponent {
 public:
    explicit Sort(const node::OrderByNode *orders) : orders_(orders) {}
    virtual ~Sort() {}
    const node::OrderByNode *orders() const { return orders_; }
    void set_orders(const node::OrderByNode *orders) { orders_ = orders; }
    const bool is_asc() const {
        const node::OrderExpression *first_order_expression =
            nullptr == orders_ ? nullptr : orders_->GetOrderExpression(0);
        return nullptr == first_order_expression ? true : first_order_expression->is_asc();
    }
    const bool ValidSort() const { return nullptr != orders_; }
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "orders=" << node::ExprString(orders_);
        return oss.str();
    }
    const std::string FnDetail() const {
        return "sort = " + fn_info_.fn_name();
    }

    void ResolvedRelatedColumns(
        std::vector<const node::ExprNode *> *columns) const {
        if (nullptr == orders_) {
            return;
        }
        auto expr = orders_->GetOrderExpressionExpr(0);
        if (nullptr != expr) {
            node::ExprListNode exprs;
            exprs.AddChild(const_cast<node::ExprNode*>(expr));
            node::ColumnOfExpression(orders_->order_expressions_, columns);
        }
        return;
    }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, Sort *out) const;

    const node::OrderByNode *orders_;
};

class Range : public FnComponent {
 public:
    Range() : range_key_(nullptr), frame_(nullptr) {}
    Range(const node::OrderByNode *order, const node::FrameNode *frame) : range_key_(nullptr), frame_(frame) {
        range_key_ = nullptr == order ? nullptr
                     : node::ExprListNullOrEmpty(order->order_expressions_)
                         ? nullptr
                         : order->GetOrderExpressionExpr(0);
    }
    virtual ~Range() {}
    const bool Valid() const { return nullptr != range_key_; }
    const std::string ToString() const {
        std::ostringstream oss;
        if (nullptr != range_key_ && nullptr != frame_) {
            if (nullptr != frame_->frame_range()) {
                oss << "range=(" << range_key_->GetExprString() << ", "
                    << frame_->frame_range()->start()->GetExprString() << ", "
                    << frame_->frame_range()->end()->GetExprString();

                if (0 != frame_->frame_maxsize()) {
                    oss << ", maxsize=" << frame_->frame_maxsize();
                }
                oss << ")";
            }

            if (nullptr != frame_->frame_rows()) {
                if (nullptr != frame_->frame_range()) {
                    oss << ", ";
                }
                oss << "rows=(" << range_key_->GetExprString() << ", "
                    << frame_->frame_rows()->start()->GetExprString() << ", "
                    << frame_->frame_rows()->end()->GetExprString() << ")";
            }
        }
        return oss.str();
    }
    const node::ExprNode *range_key() const { return range_key_; }
    void set_range_key(const node::ExprNode *range_key) {
        range_key_ = range_key;
    }
    const node::FrameNode *frame() const { return frame_; }
    const std::string FnDetail() const { return "range=" + fn_info_.fn_name(); }

    void ResolvedRelatedColumns(std::vector<const node::ExprNode *> *) const;

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, Range *out) const;

    const node::ExprNode *range_key_;
    const node::FrameNode *frame_;
};

class ConditionFilter : public FnComponent {
 public:
    ConditionFilter()
        : condition_(nullptr) {}
    explicit ConditionFilter(const node::ExprNode *condition)
        : condition_(condition) {}
    virtual ~ConditionFilter() {}
    const bool ValidCondition() const { return nullptr != condition_; }
    void set_condition(const node::ExprNode *condition) {
        condition_ = condition;
    }
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "condition=" << node::ExprString(condition_);
        return oss.str();
    }
    const node::ExprNode *condition() const { return condition_; }
    const std::string FnDetail() const { return fn_info_.fn_name(); }
    virtual void ResolvedRelatedColumns(
        std::vector<const node::ExprNode *> *columns) const {
        node::ColumnOfExpression(condition_, columns);
        return;
    }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer, node::NodeManager *nm,
                             ConditionFilter *out) const;

 private:
    const node::ExprNode *condition_;
};

class Key : public FnComponent {
 public:
    Key() : keys_(nullptr) {}
    explicit Key(const node::ExprListNode *keys) : keys_(keys) {}
    virtual ~Key() {}
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "keys=" << node::ExprString(keys_);
        return oss.str();
    }
    const bool ValidKey() const { return !node::ExprListNullOrEmpty(keys_); }
    const node::ExprListNode *keys() const { return keys_; }
    void set_keys(const node::ExprListNode *keys) { keys_ = keys; }
    const node::ExprListNode *PhysicalProjectNode() const { return keys_; }
    const std::string FnDetail() const { return "keys=" + fn_info_.fn_name(); }

    void ResolvedRelatedColumns(
        std::vector<const node::ExprNode *> *columns) const {
        node::ColumnOfExpression(keys_, columns);
    }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, Key *out) const;

    const node::ExprListNode *keys_;
};

class ColumnProjects : public FnComponent {
 public:
    ColumnProjects() {}

    void Add(const std::string &name, const node::ExprNode *expr,
             const node::FrameNode *frame);

    void Clear();

    size_t size() const { return names_.size(); }

    const node::ExprNode *GetExpr(size_t idx) const {
        return idx < size() ? exprs_[idx] : nullptr;
    }

    const std::string GetName(size_t idx) const {
        return idx < size() ? names_[idx] : "";
    }

    const node::FrameNode *GetFrame(size_t idx) const {
        return idx < size() ? frames_[idx] : nullptr;
    }

    void SetPrimaryFrame(const node::FrameNode *frame) {
        primary_frame_ = frame;
    }

    const node::FrameNode *GetPrimaryFrame() const { return primary_frame_; }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, ColumnProjects *out) const;

 private:
    std::vector<std::string> names_;
    std::vector<const node::ExprNode *> exprs_;
    std::vector<const node::FrameNode *> frames_;
    const node::FrameNode *primary_frame_ = nullptr;
};

class PhysicalPlanContext;

class PhysicalOpNode : public node::NodeBase<PhysicalOpNode> {
 public:
    PhysicalOpNode(PhysicalOpType type, bool is_block)
        : type_(type),
          is_block_(is_block),
          output_type_(kSchemaTypeTable),
          limit_cnt_(std::nullopt),
          schemas_ctx_(this) {}

    const std::string GetTypeName() const override {
        return std::string(PhysicalOpTypeName(type_));
    }
    bool Equals(const PhysicalOpNode *other) const override {
        return this == other;
    }

    virtual ~PhysicalOpNode() {}
    void Print(std::ostream &output, const std::string &tab) const override;
    void Print() const;

    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;

    virtual base::Status WithNewChildren(
        node::NodeManager *nm, const std::vector<PhysicalOpNode *> &children,
        PhysicalOpNode **out) = 0;

    /**
     * Initialize physical node's output schema.
     */
    virtual base::Status InitSchema(PhysicalPlanContext *ctx) = 0;
    void ClearSchema() { schemas_ctx_.Clear(); }
    void FinishSchema() { schemas_ctx_.Build(); }

    virtual void PrintSchema() const;

    virtual std::string SchemaToString(const std::string &tab) const;

    const std::vector<PhysicalOpNode *> &GetProducers() const {
        return producers_;
    }
    std::vector<PhysicalOpNode *> &producers() { return producers_; }
    void UpdateProducer(int i, PhysicalOpNode *producer);

    void AddProducer(PhysicalOpNode *producer) {
        producers_.push_back(producer);
    }

    /**
     * Get all function infos bind to current physical node.
     */
    const std::vector<const FnInfo *>& GetFnInfos() const { return fn_infos_; }

    /**
     * Add component FnInfo to current physical node. The node fn list take
     * no ownership to this FnInfo instance.
     */
    void AddFnInfo(const FnInfo *fn_info) { fn_infos_.push_back(fn_info); }

    /**
     * Clear current function informations.
     */
    void ClearFnInfo() { fn_infos_.clear(); }

    PhysicalOpNode *GetProducer(size_t index) const {
        return producers_[index];
    }

    const hybridse::codec::Schema *GetOutputSchema() const {
        return schemas_ctx_.GetOutputSchema();
    }

    size_t GetOutputSchemaSize() const { return GetOutputSchema()->size(); }

    void SetProducer(size_t index, PhysicalOpNode *produce) {
        producers_[index] = produce;
    }
    size_t GetProducerCnt() const { return producers_.size(); }

    const size_t GetOutputSchemaSourceSize() const {
        return schemas_ctx_.GetSchemaSourceSize();
    }

    const hybridse::vm::SchemaSource *GetOutputSchemaSource(size_t idx) const {
        return idx < schemas_ctx_.GetSchemaSourceSize()
                   ? schemas_ctx_.GetSchemaSource(idx)
                   : nullptr;
    }

    void SetLimitCnt(std::optional<int32_t> limit_cnt) { limit_cnt_ = limit_cnt; }

    std::optional<int32_t> GetLimitCnt() const { return limit_cnt_; }

    // get the limit cnt value
    // if not set, -1 is returned
    //
    // limit always >= 0 so it is safe to do that
    int32_t GetLimitCntValue() const { return limit_cnt_.value_or(-1); }

    bool IsSameSchema(const vm::Schema &schema, const vm::Schema &exp_schema) const;

    // `lhs` schema contains `rhs` and is start with `rhs` schema
    //
    // return ok status if true
    //        error status with msg otherwise
    base::Status SchemaStartWith(const vm::Schema& lhs, const vm::Schema& rhs) const;

    PhysicalSchemaType GetOutputType() const { return output_type_; }

    PhysicalOpType GetOpType() const { return type_; }

    const SchemasContext *schemas_ctx() const { return &schemas_ctx_; }

    bool is_block() const { return is_block_; }

    /*
     * Add the duplicated function for swig-java which can not access
     * functions in template.
     */
    size_t GetNodeId() const { return node_id(); }

 protected:
    const PhysicalOpType type_;
    const bool is_block_;
    PhysicalSchemaType output_type_;

    std::vector<const FnInfo *> fn_infos_;

    // all physical node has limit property, default to empty (not set)
    std::optional<int32_t> limit_cnt_ = std::nullopt;
    std::vector<PhysicalOpNode *> producers_;

    SchemasContext schemas_ctx_;
};

class PhysicalUnaryNode : public PhysicalOpNode {
 public:
    PhysicalUnaryNode(PhysicalOpNode *node, PhysicalOpType type, bool is_block)
        : PhysicalOpNode(type, is_block) {
        AddProducer(node);
    }
    base::Status InitSchema(PhysicalPlanContext *) override;
    virtual ~PhysicalUnaryNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    virtual void PrintChildren(std::ostream &output,
                               const std::string &tab) const;
};

class PhysicalBinaryNode : public PhysicalOpNode {
 public:
    PhysicalBinaryNode(PhysicalOpNode *left, PhysicalOpNode *right,
                       PhysicalOpType type, bool is_block)
        : PhysicalOpNode(type, is_block) {
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
        : PhysicalOpNode(kPhysicalOpDataProvider, true),
          provider_type_(provider_type),
          table_handler_(table_handler) {}
    ~PhysicalDataProviderNode() {}

    base::Status InitSchema(PhysicalPlanContext *) override;

    static PhysicalDataProviderNode *CastFrom(PhysicalOpNode *node);
    const std::string &GetName() const;
    const std::string& GetDb() const {
        return table_handler_->GetDatabase();
    }
    const DataProviderType provider_type_;
    const std::shared_ptr<TableHandler> table_handler_;
};

class PhysicalTableProviderNode : public PhysicalDataProviderNode {
 public:
    explicit PhysicalTableProviderNode(
        const std::shared_ptr<TableHandler> &table_handler)
        : PhysicalDataProviderNode(table_handler, kProviderTypeTable) {}

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

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

    base::Status InitSchema(PhysicalPlanContext *) override;

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    virtual ~PhysicalRequestProviderNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
};

class PhysicalRequestProviderNodeWithCommonColumn
    : public PhysicalRequestProviderNode {
 public:
    explicit PhysicalRequestProviderNodeWithCommonColumn(
        const std::shared_ptr<TableHandler> &table_handler,
        const std::set<size_t> common_column_indices)
        : PhysicalRequestProviderNode(table_handler),
          common_column_indices_(common_column_indices) {}

    ~PhysicalRequestProviderNodeWithCommonColumn() {}

    base::Status InitSchema(PhysicalPlanContext *) override;
    void Print(std::ostream &output, const std::string &tab) const override;

 private:
    std::set<size_t> common_column_indices_;
    vm::Schema common_schema_;
    vm::Schema non_common_schema_;
};

class PhysicalPartitionProviderNode : public PhysicalDataProviderNode {
 public:
    PhysicalPartitionProviderNode(PhysicalDataProviderNode *depend,
                                  const std::string &index_name)
        : PhysicalDataProviderNode(depend->table_handler_,
                                   kProviderTypePartition),
          index_name_(index_name) {
        output_type_ = kSchemaTypeGroup;
    }
    virtual ~PhysicalPartitionProviderNode() {}

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    virtual void Print(std::ostream &output, const std::string &tab) const;
    const std::string index_name_;
};

class PhysicalGroupNode : public PhysicalUnaryNode {
 public:
    PhysicalGroupNode(PhysicalOpNode *node, const node::ExprListNode *groups)
        : PhysicalUnaryNode(node, kPhysicalOpGroupBy, true), group_(groups) {
        output_type_ = kSchemaTypeGroup;
        fn_infos_.push_back(&group_.fn_info());
    }
    virtual ~PhysicalGroupNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    static PhysicalGroupNode *CastFrom(PhysicalOpNode *node);

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    bool Valid() { return group_.ValidKey(); }
    Key group() const { return group_; }
    Key group_;
};

enum ProjectType {
    kRowProject,
    kTableProject,
    kAggregation,
    kGroupAggregation,
    kWindowAggregation,
    kReduceAggregation,
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
        case kReduceAggregation:
            return "ReduceAggregation";
        default:
            return "UnKnown";
    }
}

inline bool IsAggProjectType(const ProjectType &type) {
    switch (type) {
        case kAggregation:
        case kGroupAggregation:
        case kWindowAggregation:
        case kReduceAggregation:
            return true;
        default:
            return false;
    }
}

class PhysicalProjectNode : public PhysicalUnaryNode {
 public:
    PhysicalProjectNode(PhysicalOpNode *depend, ProjectType project_type,
                        const ColumnProjects &project, bool is_block)
        : PhysicalUnaryNode(depend, kPhysicalOpProject, is_block),
          project_type_(project_type),
          project_(project) {
        fn_infos_.push_back(&project_.fn_info());
    }
    virtual ~PhysicalProjectNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    static PhysicalProjectNode *CastFrom(PhysicalOpNode *node);

    base::Status InitSchema(PhysicalPlanContext *) override;

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    const ColumnProjects &project() const { return project_; }
    const ProjectType project_type_;

 protected:
    ColumnProjects project_;
};

class PhysicalRowProjectNode : public PhysicalProjectNode {
 public:
    PhysicalRowProjectNode(PhysicalOpNode *node, const ColumnProjects &project)
        : PhysicalProjectNode(node, kRowProject, project, false) {
        output_type_ = kSchemaTypeRow;
    }
    virtual ~PhysicalRowProjectNode() {}
    static PhysicalRowProjectNode *CastFrom(PhysicalOpNode *node);
};

class PhysicalTableProjectNode : public PhysicalProjectNode {
 public:
    PhysicalTableProjectNode(PhysicalOpNode *node,
                             const ColumnProjects &project)
        : PhysicalProjectNode(node, kTableProject, project, false) {
        output_type_ = kSchemaTypeTable;
    }
    virtual ~PhysicalTableProjectNode() {}
    static PhysicalTableProjectNode *CastFrom(PhysicalOpNode *node);
};

class PhysicalConstProjectNode : public PhysicalOpNode {
 public:
    explicit PhysicalConstProjectNode(const ColumnProjects &project)
        : PhysicalOpNode(kPhysicalOpConstProject, true), project_(project) {
        fn_infos_.push_back(&project_.fn_info());
    }
    virtual ~PhysicalConstProjectNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    static PhysicalConstProjectNode *CastFrom(PhysicalOpNode *node);
    const ColumnProjects &project() const { return project_; }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    base::Status InitSchema(PhysicalPlanContext *) override;

 private:
    ColumnProjects project_;
    // a empty SchemContext used by `InitSchema`, defined as class member to extend lifetime
    // because codegen later need access to it
    SchemasContext empty_schemas_ctx_;
};

class PhysicalSimpleProjectNode : public PhysicalUnaryNode {
 public:
    PhysicalSimpleProjectNode(PhysicalOpNode *node,
                              const ColumnProjects &project)
        : PhysicalUnaryNode(node, kPhysicalOpSimpleProject, true),
          project_(project) {
        output_type_ = node->GetOutputType();
        fn_infos_.push_back(&project_.fn_info());
    }

    virtual ~PhysicalSimpleProjectNode() {}
    void Print(std::ostream &output, const std::string &tab) const override;
    static PhysicalSimpleProjectNode *CastFrom(PhysicalOpNode *node);

    const ColumnProjects &project() const { return project_; }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    base::Status InitSchema(PhysicalPlanContext *) override;

    // return schema source index if target projects is just select all columns
    // from one input schema source with consistent order. return -1 otherwise.
    int GetSelectSourceIndex() const;

 private:
    ColumnProjects project_;
};

class PhysicalAggregationNode : public PhysicalProjectNode {
 public:
    PhysicalAggregationNode(PhysicalOpNode *node, const ColumnProjects &project, const node::ExprNode *condition)
        : PhysicalProjectNode(node, kAggregation, project, true), having_condition_(condition) {
        output_type_ = kSchemaTypeRow;
        fn_infos_.push_back(&having_condition_.fn_info());
    }
    virtual ~PhysicalAggregationNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    ConditionFilter having_condition_;
};

class PhysicalReduceAggregationNode : public PhysicalProjectNode {
 public:
    PhysicalReduceAggregationNode(PhysicalOpNode *node, const ColumnProjects &project,
                                  const node::ExprNode *condition, const PhysicalAggregationNode *orig_aggr)
        : PhysicalProjectNode(node, kReduceAggregation, project, true), having_condition_(condition) {
        output_type_ = kSchemaTypeRow;
        fn_infos_.push_back(&having_condition_.fn_info());
        orig_aggr_ = orig_aggr;
    }
    virtual ~PhysicalReduceAggregationNode() {}
    base::Status InitSchema(PhysicalPlanContext *) override;
    void Print(std::ostream &output, const std::string &tab) const override;
    ConditionFilter having_condition_;
    const PhysicalAggregationNode* orig_aggr_ = nullptr;
};

class PhysicalGroupAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalGroupAggrerationNode(PhysicalOpNode *node,
                                 const ColumnProjects &project,
                                 const node::ExprNode* having_condition,
                                 const node::ExprListNode *groups)
        : PhysicalProjectNode(node, kGroupAggregation, project, true),
          having_condition_(having_condition),
          group_(groups) {
        output_type_ = kSchemaTypeTable;
        fn_infos_.push_back(&having_condition_.fn_info());
        fn_infos_.push_back(&group_.fn_info());
    }
    virtual ~PhysicalGroupAggrerationNode() {}
    static PhysicalGroupAggrerationNode *CastFrom(PhysicalOpNode *node);
    virtual void Print(std::ostream &output, const std::string &tab) const;
    ConditionFilter having_condition_;
    Key group_;
};

class PhysicalUnionNode;
class PhysicalJoinNode;

class WindowOp {
 public:
    explicit WindowOp(const node::ExprListNode *partitions)
        : partition_(partitions), sort_(nullptr), range_() {}
    explicit WindowOp(const node::WindowPlanNode *w_ptr)
        : partition_(w_ptr->GetKeys()),
          sort_(w_ptr->GetOrders()),
          range_(w_ptr->GetOrders(), w_ptr->frame_node()),
          name_(w_ptr->GetName()) {}
    virtual ~WindowOp() {}
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "partition_" << partition_.ToString();
        oss << ", " << sort_.ToString();
        if (range_.Valid()) {
            oss << ", " << range_.ToString();
        }
        return oss.str();
    }
    const std::string FnDetail() const {
        std::ostringstream oss;
        oss << "partition_" << partition_.FnDetail();
        oss << ", " << sort_.FnDetail();
        if (range_.Valid()) {
            oss << ", " << range_.FnDetail();
        }
        return oss.str();
    }
    const Key &partition() const { return partition_; }
    const Sort &sort() const { return sort_; }
    const Range &range() const { return range_; }
    const std::string &name() const { return name_; }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, WindowOp *out) const;

    void ResolvedRelatedColumns(std::vector<const node::ExprNode *> *) const;

    Key partition_;
    Sort sort_;
    Range range_;
    std::string name_ = "";
};

class RequestWindowOp : public WindowOp {
 public:
    explicit RequestWindowOp(const node::ExprListNode *partitions)
        : WindowOp(partitions), index_key_() {}
    explicit RequestWindowOp(const node::WindowPlanNode *w_ptr)
        : WindowOp(w_ptr), index_key_() {}
    virtual ~RequestWindowOp() {}
    const std::string ToString() const {
        std::ostringstream oss;
        oss << WindowOp::ToString() << ", index_" << index_key_.ToString();
        return oss.str();
    }
    const std::string FnDetail() const {
        std::ostringstream oss;
        oss << WindowOp::FnDetail() << ", index_" << index_key_.FnDetail();
        return oss.str();
    }
    const Key &index_key() const { return index_key_; }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, RequestWindowOp *out) const;

    void ResolvedRelatedColumns(std::vector<const node::ExprNode *> *) const;

    Key index_key_;
};

class Filter {
 public:
    explicit Filter(const node::ExprNode *condition)
        : condition_(condition),
          left_key_(nullptr),
          right_key_(nullptr),
          index_key_(nullptr) {}
    Filter(const node::ExprNode *condition, const node::ExprListNode *left_keys,
           const node::ExprListNode *right_keys)
        : condition_(condition),
          left_key_(left_keys),
          right_key_(right_keys),
          index_key_(nullptr) {}
    virtual ~Filter() {}

    bool Valid() {
        return index_key_.ValidKey() || condition_.ValidCondition();
    }
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "condition=" << node::ExprString(condition_.condition())
            << ", left_keys=" << node::ExprString(left_key_.keys())
            << ", right_keys=" << node::ExprString(right_key_.keys())
            << ", index_keys=" << node::ExprString(index_key_.keys());
        return oss.str();
    }
    const std::string FnDetail() const {
        std::ostringstream oss;
        oss << ", condition=" << condition_.FnDetail();
        oss << ", left_keys=" << left_key_.FnDetail()
            << ", right_keys=" << right_key_.FnDetail()
            << ", index_keys=" << index_key_.FnDetail();
        return oss.str();
    }
    const Key &left_key() const { return left_key_; }
    const Key &right_key() const { return right_key_; }
    const Key &index_key() const { return index_key_; }
    const ConditionFilter &condition() const { return condition_; }
    virtual void ResolvedRelatedColumns(
        std::vector<const node::ExprNode *> *columns) const {
        left_key_.ResolvedRelatedColumns(columns);
        right_key_.ResolvedRelatedColumns(columns);
        index_key_.ResolvedRelatedColumns(columns);
        condition_.ResolvedRelatedColumns(columns);
    }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, Filter *out) const;

    ConditionFilter condition_;
    Key left_key_;
    Key right_key_;
    Key index_key_;
};

class Join : public Filter {
 public:
    explicit Join(const node::JoinType join_type)
        : Filter(nullptr), join_type_(join_type), right_sort_(nullptr) {}
    Join(const node::JoinType join_type, const node::ExprNode *condition)
        : Filter(condition), join_type_(join_type), right_sort_(nullptr) {}
    Join(const node::JoinType join_type, const node::OrderByNode *orders,
         const node::ExprNode *condition)
        : Filter(condition), join_type_(join_type), right_sort_(orders) {}
    Join(const node::JoinType join_type, const node::ExprNode *condition,
         const node::ExprListNode *left_keys,
         const node::ExprListNode *right_keys)
        : Filter(condition, left_keys, right_keys),
          join_type_(join_type),
          right_sort_(nullptr) {}
    Join(const node::JoinType join_type, const node::OrderByNode *orders,
         const node::ExprNode *condition, const node::ExprListNode *left_keys,
         const node::ExprListNode *right_keys)
        : Filter(condition, left_keys, right_keys),
          join_type_(join_type),
          right_sort_(orders) {}
    virtual ~Join() {}
    const std::string ToString() const {
        std::ostringstream oss;
        oss << "type=" << node::JoinTypeName(join_type_);
        if (right_sort_.ValidSort()) {
            oss << ", right_sort=" << node::ExprString(right_sort_.orders());
        }
        oss << ", " << Filter::ToString();
        return oss.str();
    }
    const std::string FnDetail() const {
        std::ostringstream oss;

        if (right_sort_.ValidSort()) {
            oss << "right_sort_=" << right_sort_.FnDetail();
        }
        oss << Filter::FnDetail();
        return oss.str();
    }
    const node::JoinType join_type() const { return join_type_; }
    const Sort &right_sort() const { return right_sort_; }
    void ResolvedRelatedColumns(
        std::vector<const node::ExprNode *> *columns) const {
        Filter::ResolvedRelatedColumns(columns);
        right_sort_.ResolvedRelatedColumns(columns);
    }

    base::Status ReplaceExpr(const passes::ExprReplacer &replacer,
                             node::NodeManager *nm, Join *out) const;

    node::JoinType join_type_;
    Sort right_sort_;
};

class WindowJoinList {
 public:
    WindowJoinList() : window_joins_() {}
    virtual ~WindowJoinList() {}
    void AddWindowJoin(PhysicalOpNode *node, const Join &join) {
        window_joins_.push_front(std::make_pair(node, join));
    }
    const bool Empty() const { return window_joins_.empty(); }
    const std::string FnDetail() const {
        std::ostringstream oss;
        for (auto &window_join : window_joins_) {
            oss << window_join.second.FnDetail() << "\n";
        }
        return oss.str();
    }
    std::list<std::pair<PhysicalOpNode *, Join>> &window_joins() {
        return window_joins_;
    }

    std::list<std::pair<PhysicalOpNode *, Join>> window_joins_;
};

class WindowUnionList {
 public:
    WindowUnionList() : window_unions_() {}
    virtual ~WindowUnionList() {}
    void AddWindowUnion(PhysicalOpNode *node, const WindowOp &window) {
        window_unions_.emplace_back(node, window);
    }
    const std::string FnDetail() const {
        std::ostringstream oss;
        for (auto &window_union : window_unions_) {
            oss << window_union.second.FnDetail() << "\n";
        }
        return oss.str();
    }
    const bool Empty() const { return window_unions_.empty(); }
    size_t GetSize() const { return window_unions_.size(); }
    PhysicalOpNode *GetUnionNode(size_t idx) const {
        auto iter = window_unions_.begin();
        for (size_t i = 0; i < idx; ++i) {
            ++iter;
        }
        return iter->first;
    }

    std::list<std::pair<PhysicalOpNode *, WindowOp>> window_unions_;
};

class RequestWindowUnionList {
 public:
    RequestWindowUnionList() : window_unions_() {}
    virtual ~RequestWindowUnionList() {}
    void AddWindowUnion(PhysicalOpNode *node, const RequestWindowOp &window) {
        window_unions_.push_back(std::make_pair(node, window));
    }
    const PhysicalOpNode *GetKey(uint32_t index) {
        auto iter = window_unions_.begin();
        for (uint32_t i = 0; i < index; ++i) {
            ++iter;
        }
        return iter->first;
        // return window_unions_[index].first;
    }

    const RequestWindowOp &GetValue(uint32_t index) {
        auto iter = window_unions_.begin();
        for (uint32_t i = 0; i < index; ++i) {
            ++iter;
        }
        return iter->second;
    }

    const uint32_t GetSize() { return window_unions_.size(); }

    const std::string FnDetail() const {
        std::ostringstream oss;
        for (auto &window_union : window_unions_) {
            oss << window_union.second.FnDetail() << "\n";
        }
        return oss.str();
    }
    const bool Empty() const { return window_unions_.empty(); }

    std::list<std::pair<PhysicalOpNode *, RequestWindowOp>> window_unions_;
};

class PhysicalWindowAggrerationNode : public PhysicalProjectNode {
 public:
    PhysicalWindowAggrerationNode(PhysicalOpNode *node, const ColumnProjects &project, const WindowOp &window_op,
                                  bool instance_not_in_window, bool need_append_input, bool exclude_current_time)
        : PhysicalProjectNode(node, kWindowAggregation, project, true),
          window_(window_op),
          window_unions_(),
          need_append_input_(need_append_input),
          instance_not_in_window_(instance_not_in_window),
          exclude_current_time_(exclude_current_time) {
        output_type_ = kSchemaTypeTable;
        fn_infos_.push_back(&window_.partition_.fn_info());
        fn_infos_.push_back(&window_.sort_.fn_info());
        fn_infos_.push_back(&window_.range_.fn_info());
    }

    virtual ~PhysicalWindowAggrerationNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    static PhysicalWindowAggrerationNode *CastFrom(PhysicalOpNode *node);
    const bool Valid() { return true; }
    void AddWindowJoin(PhysicalOpNode *node, const Join &join) {
        window_joins_.AddWindowJoin(node, join);
        Join &window_join = window_joins_.window_joins_.front().second;
        fn_infos_.push_back(&window_join.left_key_.fn_info());
        fn_infos_.push_back(&window_join.right_key_.fn_info());
        fn_infos_.push_back(&window_join.index_key_.fn_info());
        fn_infos_.push_back(&window_join.condition_.fn_info());
    }

    bool AddWindowUnion(PhysicalOpNode *node);

    const bool instance_not_in_window() const {
        return instance_not_in_window_;
    }

    const bool exclude_current_time() const { return exclude_current_time_; }
    const bool exclude_current_row() const { return exclude_current_row_; }
    void set_exclude_current_row(bool flag) { exclude_current_row_ = flag; }
    bool need_append_input() const { return need_append_input_; }

    WindowOp &window() { return window_; }
    WindowJoinList &window_joins() { return window_joins_; }
    WindowUnionList &window_unions() { return window_unions_; }

    base::Status InitSchema(PhysicalPlanContext *) override;

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;
    /**
     * Initialize inner state for window joins
     */
    base::Status InitJoinList(PhysicalPlanContext *plan_ctx);
    std::vector<PhysicalOpNode *> joined_op_list_;

    WindowOp window_;
    WindowUnionList window_unions_;
    WindowJoinList window_joins_;

    const bool need_append_input_;
    const bool instance_not_in_window_;
    const bool exclude_current_time_;
    bool exclude_current_row_ = false;
};

class PhysicalJoinNode : public PhysicalBinaryNode {
 public:
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false),
          join_(join_type),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = left->GetOutputType();
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type,
                     const node::OrderByNode *orders,
                     const node::ExprNode *condition)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false),
          join_(join_type, orders, condition),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = left->GetOutputType();

        RegisterFunctionInfo();
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type,
                     const node::ExprNode *condition,
                     const node::ExprListNode *left_keys,
                     const node::ExprListNode *right_keys)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false),
          join_(join_type, condition, left_keys, right_keys),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = left->GetOutputType();

        RegisterFunctionInfo();
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const node::JoinType join_type,
                     const node::OrderByNode *orders,
                     const node::ExprNode *condition,
                     const node::ExprListNode *left_keys,
                     const node::ExprListNode *right_keys)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false),
          join_(join_type, orders, condition, left_keys, right_keys),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = left->GetOutputType();

        RegisterFunctionInfo();
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const Join &join)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false),
          join_(join),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = left->GetOutputType();

        RegisterFunctionInfo();
    }
    PhysicalJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                     const Join &join, const bool output_right_only)
        : PhysicalBinaryNode(left, right, kPhysicalOpJoin, false),
          join_(join),
          joined_schemas_ctx_(this),
          output_right_only_(output_right_only) {
        output_type_ = left->GetOutputType();

        RegisterFunctionInfo();
    }
    virtual ~PhysicalJoinNode() {}
    base::Status InitSchema(PhysicalPlanContext *) override;
    void RegisterFunctionInfo() {
        fn_infos_.push_back(&join_.right_sort_.fn_info());
        fn_infos_.push_back(&join_.condition_.fn_info());
        fn_infos_.push_back(&join_.left_key_.fn_info());
        fn_infos_.push_back(&join_.right_key_.fn_info());
        fn_infos_.push_back(&join_.index_key_.fn_info());
    }
    virtual void Print(std::ostream &output, const std::string &tab) const;
    static PhysicalJoinNode *CastFrom(PhysicalOpNode *node);
    const bool Valid() { return true; }
    const Join &join() const { return join_; }
    const SchemasContext *joined_schemas_ctx() const {
        return &joined_schemas_ctx_;
    }
    const bool output_right_only() const { return output_right_only_; }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    Join join_;
    SchemasContext joined_schemas_ctx_;
    const bool output_right_only_;
};

class PhysicalRequestJoinNode : public PhysicalBinaryNode {
 public:
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const node::JoinType join_type)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false),
          join_(join_type),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = kSchemaTypeRow;
        RegisterFunctionInfo();
    }
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const node::JoinType join_type,
                            const node::OrderByNode *orders,
                            const node::ExprNode *condition)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false),
          join_(join_type, orders, condition),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = kSchemaTypeRow;
        RegisterFunctionInfo();
    }
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const Join &join, const bool output_right_only)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false),
          join_(join),
          joined_schemas_ctx_(this),
          output_right_only_(output_right_only) {
        output_type_ = kSchemaTypeRow;
        RegisterFunctionInfo();
    }

 private:
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const node::JoinType join_type,
                            const node::ExprNode *condition,
                            const node::ExprListNode *left_keys,
                            const node::ExprListNode *right_keys)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false),
          join_(join_type, condition, left_keys, right_keys),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = kSchemaTypeRow;
        RegisterFunctionInfo();
    }
    PhysicalRequestJoinNode(PhysicalOpNode *left, PhysicalOpNode *right,
                            const node::JoinType join_type,
                            const node::OrderByNode *orders,
                            const node::ExprNode *condition,
                            const node::ExprListNode *left_keys,
                            const node::ExprListNode *right_keys)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestJoin, false),
          join_(join_type, orders, condition, left_keys, right_keys),
          joined_schemas_ctx_(this),
          output_right_only_(false) {
        output_type_ = kSchemaTypeRow;
        RegisterFunctionInfo();
    }

 public:
    virtual ~PhysicalRequestJoinNode() {}

    base::Status InitSchema(PhysicalPlanContext *) override;
    static PhysicalRequestJoinNode *CastFrom(PhysicalOpNode *node);

    void RegisterFunctionInfo() {
        fn_infos_.push_back(&join_.right_sort_.fn_info());
        fn_infos_.push_back(&join_.condition_.fn_info());
        fn_infos_.push_back(&join_.left_key_.fn_info());
        fn_infos_.push_back(&join_.right_key_.fn_info());
        fn_infos_.push_back(&join_.index_key_.fn_info());
    }
    void Print(std::ostream &output, const std::string &tab) const override;
    const Join &join() const { return join_; }
    const bool output_right_only() const { return output_right_only_; }
    const SchemasContext *joined_schemas_ctx() const {
        return &joined_schemas_ctx_;
    }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    Join join_;
    SchemasContext joined_schemas_ctx_;
    const bool output_right_only_;
};

class PhysicalUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalUnionNode(PhysicalOpNode *left, PhysicalOpNode *right, bool is_all)
        : PhysicalBinaryNode(left, right, kPhysicalOpUnion, true),
          is_all_(is_all) {
        output_type_ = kSchemaTypeTable;
    }
    virtual ~PhysicalUnionNode() {}
    base::Status InitSchema(PhysicalPlanContext *) override;
    virtual void Print(std::ostream &output, const std::string &tab) const;

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    const bool is_all_;
    static PhysicalUnionNode *CastFrom(PhysicalOpNode *node);
};

class PhysicalPostRequestUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalPostRequestUnionNode(PhysicalOpNode *left, PhysicalOpNode *right,
                                 const Range &request_ts)
        : PhysicalBinaryNode(left, right, kPhysicalOpPostRequestUnion, true),
          request_ts_(request_ts) {
        output_type_ = kSchemaTypeTable;
        fn_infos_.push_back(&request_ts_.fn_info());
    }
    virtual ~PhysicalPostRequestUnionNode() {}
    base::Status InitSchema(PhysicalPlanContext *) override;
    virtual void Print(std::ostream &output, const std::string &tab) const;

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    static PhysicalPostRequestUnionNode *CastFrom(PhysicalOpNode *node);

    const Range &request_ts() const { return request_ts_; }
    Range *mutable_request_ts() { return &request_ts_; }

 private:
    Range request_ts_;
};

class PhysicalRequestUnionNode : public PhysicalBinaryNode {
 public:
    PhysicalRequestUnionNode(PhysicalOpNode *left, PhysicalOpNode *right,
                             const node::ExprListNode *partition)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestUnion, true),
          window_(partition),
          instance_not_in_window_(false),
          exclude_current_time_(false),
          output_request_row_(true) {
        output_type_ = kSchemaTypeTable;

        fn_infos_.push_back(&window_.partition_.fn_info());
        fn_infos_.push_back(&window_.index_key_.fn_info());
    }
    PhysicalRequestUnionNode(PhysicalOpNode *left, PhysicalOpNode *right,
                             const node::WindowPlanNode *w_ptr)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestUnion, true),
          window_(w_ptr),
          instance_not_in_window_(w_ptr->instance_not_in_window()),
          exclude_current_time_(w_ptr->exclude_current_time()),
          output_request_row_(true) {
        output_type_ = kSchemaTypeTable;

        fn_infos_.push_back(&window_.partition_.fn_info());
        fn_infos_.push_back(&window_.sort_.fn_info());
        fn_infos_.push_back(&window_.range_.fn_info());
        fn_infos_.push_back(&window_.index_key_.fn_info());
    }
    PhysicalRequestUnionNode(PhysicalOpNode *left, PhysicalOpNode *right,
                             const RequestWindowOp &window,
                             bool instance_not_in_window,
                             bool exclude_current_time, bool output_request_row)
        : PhysicalBinaryNode(left, right, kPhysicalOpRequestUnion, true),
          window_(window),
          instance_not_in_window_(instance_not_in_window),
          exclude_current_time_(exclude_current_time),
          output_request_row_(output_request_row) {
        output_type_ = kSchemaTypeTable;

        fn_infos_.push_back(&window_.partition_.fn_info());
        fn_infos_.push_back(&window_.sort_.fn_info());
        fn_infos_.push_back(&window_.range_.fn_info());
        fn_infos_.push_back(&window_.index_key_.fn_info());
    }
    virtual ~PhysicalRequestUnionNode() {}
    base::Status InitSchema(PhysicalPlanContext *) override;
    virtual void Print(std::ostream &output, const std::string &tab) const;
    const bool Valid() { return true; }
    static PhysicalRequestUnionNode *CastFrom(PhysicalOpNode *node);
    bool AddWindowUnion(PhysicalOpNode *node) {
        if (nullptr == node) {
            LOG(WARNING) << "Fail to add window union : table is null";
            return false;
        }
        if (producers_.empty() || nullptr == producers_[0]) {
            LOG(WARNING)
                << "Fail to add window union : producer is empty or null";
            return false;
        }
        if (output_request_row() &&
            !IsSameSchema(*node->GetOutputSchema(),
                          *producers_[0]->GetOutputSchema())) {
            LOG(WARNING)
                << "Union Table and window input schema aren't consistent";
            return false;
        }
        window_unions_.AddWindowUnion(node, window_);
        RequestWindowOp &window_union =
            window_unions_.window_unions_.back().second;
        fn_infos_.push_back(&window_union.partition_.fn_info());
        fn_infos_.push_back(&window_union.sort_.fn_info());
        fn_infos_.push_back(&window_union.range_.fn_info());
        fn_infos_.push_back(&window_union.index_key_.fn_info());
        return true;
    }
    const bool instance_not_in_window() const {
        return instance_not_in_window_;
    }
    const bool exclude_current_time() const { return exclude_current_time_; }
    const bool output_request_row() const { return output_request_row_; }
    const RequestWindowOp &window() const { return window_; }
    const RequestWindowUnionList &window_unions() const {
        return window_unions_;
    }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    RequestWindowOp window_;
    const bool instance_not_in_window_;
    const bool exclude_current_time_;
    const bool output_request_row_;
    RequestWindowUnionList window_unions_;

    bool exclude_current_row_ = false;
};

class PhysicalRequestAggUnionNode : public PhysicalOpNode {
 public:
    PhysicalRequestAggUnionNode(PhysicalOpNode *request, PhysicalOpNode *raw, PhysicalOpNode *aggr,
                                const RequestWindowOp &window, const RequestWindowOp &aggr_window,
                                bool instance_not_in_window, bool exclude_current_time, bool output_request_row,
                                const node::CallExprNode *project)
        : PhysicalOpNode(kPhysicalOpRequestAggUnion, true),
          window_(window),
          agg_window_(aggr_window),
          project_(project),
          instance_not_in_window_(instance_not_in_window),
          exclude_current_time_(exclude_current_time),
          output_request_row_(output_request_row) {
        output_type_ = kSchemaTypeTable;

        AddFnInfo(&window_.partition_.fn_info());
        AddFnInfo(&window_.sort_.fn_info());
        AddFnInfo(&window_.range_.fn_info());
        AddFnInfo(&window_.index_key_.fn_info());

        AddFnInfo(&agg_window_.partition_.fn_info());
        AddFnInfo(&agg_window_.sort_.fn_info());
        AddFnInfo(&agg_window_.range_.fn_info());
        AddFnInfo(&agg_window_.index_key_.fn_info());

        AddProducers(request, raw, aggr);
    }
    virtual ~PhysicalRequestAggUnionNode() {}
    base::Status InitSchema(PhysicalPlanContext *) override;
    void UpdateParentSchema(const SchemasContext* parent_schema_context) {
        parent_schema_context_ = parent_schema_context;
    }
    void Print(std::ostream &output, const std::string &tab) const override;
    void PrintChildren(std::ostream& output, const std::string& tab) const override;
    const bool Valid() { return true; }
    static PhysicalRequestAggUnionNode *CastFrom(PhysicalOpNode *node);

    const bool instance_not_in_window() const { return instance_not_in_window_; }
    const bool exclude_current_time() const { return exclude_current_time_; }
    const bool output_request_row() const { return output_request_row_; }
    void set_out_request_row(bool flag) { output_request_row_ = flag; }
    const RequestWindowOp &window() const { return window_; }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override {
        return base::Status(common::kUnSupport);
    }

    RequestWindowOp window_;
    RequestWindowOp agg_window_;

    // for long window, each node has only one projection node
    const node::CallExprNode* project_;
    const SchemasContext* parent_schema_context_ = nullptr;

 private:
    void AddProducers(PhysicalOpNode *request, PhysicalOpNode *raw, PhysicalOpNode *aggr) {
        AddProducer(request);
        AddProducer(raw);
        AddProducer(aggr);
    }

    const bool instance_not_in_window_;
    const bool exclude_current_time_;

    // Exclude the request row from request union results
    //
    // The option is different from `output_request_row_` in `PhysicalRequestUnionNode`.
    // Here it is only `false` when the SQL Window clause has attribute `EXCLUDE CURRENT_ROW`,
    // whereas in `PhysicalRequestUnionNode`, it is about common column optimized and not related to
    // `EXCLUDE CURRENT_ROW`
    bool output_request_row_;

    Schema agg_schema_;
};

class PhysicalSortNode : public PhysicalUnaryNode {
 public:
    PhysicalSortNode(PhysicalOpNode *node, const node::OrderByNode *order)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true), sort_(order) {
        output_type_ = node->GetOutputType();

        fn_infos_.push_back(&sort_.fn_info());
    }
    PhysicalSortNode(PhysicalOpNode *node, const Sort &sort)
        : PhysicalUnaryNode(node, kPhysicalOpSortBy, true), sort_(sort) {
        output_type_ = node->GetOutputType();

        fn_infos_.push_back(&sort_.fn_info());
    }
    virtual ~PhysicalSortNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    bool Valid() { return sort_.ValidSort(); }
    const Sort &sort() const { return sort_; }
    Sort sort_;
    static PhysicalSortNode *CastFrom(PhysicalOpNode *node);
};

class PhysicalFilterNode : public PhysicalUnaryNode {
 public:
    PhysicalFilterNode(PhysicalOpNode *node, const node::ExprNode *condition)
        : PhysicalUnaryNode(node, kPhysicalOpFilter, true), filter_(condition) {
        output_type_ = node->GetOutputType();

        fn_infos_.push_back(&filter_.condition_.fn_info());
        fn_infos_.push_back(&filter_.index_key_.fn_info());
    }
    virtual ~PhysicalFilterNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    bool Valid() { return filter_.Valid(); }
    const Filter &filter() const { return filter_; }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    Filter filter_;
    static PhysicalFilterNode *CastFrom(PhysicalOpNode *node);
};

class PhysicalLimitNode : public PhysicalUnaryNode {
 public:
    PhysicalLimitNode(PhysicalOpNode *node, int32_t limit_cnt)
        : PhysicalUnaryNode(node, kPhysicalOpLimit, true) {
        limit_cnt_ = limit_cnt;
        limit_optimized_ = false;
        output_type_ = node->GetOutputType();
    }
    virtual ~PhysicalLimitNode() {}
    virtual void Print(std::ostream &output, const std::string &tab) const;
    void SetLimitOptimized(bool optimized) { limit_optimized_ = optimized; }
    const bool GetLimitOptimized() const { return limit_optimized_; }
    static PhysicalLimitNode *CastFrom(PhysicalOpNode *node);

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

 private:
    bool limit_optimized_;
};

class PhysicalRenameNode : public PhysicalUnaryNode {
 public:
    PhysicalRenameNode(PhysicalOpNode *node, const std::string &name)
        : PhysicalUnaryNode(node, kPhysicalOpRename, false), name_(name) {
        output_type_ = node->GetOutputType();
    }
    base::Status InitSchema(PhysicalPlanContext *) override;
    virtual ~PhysicalRenameNode() {}
    static PhysicalRenameNode *CastFrom(PhysicalOpNode *node);
    virtual void Print(std::ostream &output, const std::string &tab) const;

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    const std::string &name_;
};

class PhysicalDistinctNode : public PhysicalUnaryNode {
 public:
    explicit PhysicalDistinctNode(PhysicalOpNode *node)
        : PhysicalUnaryNode(node, kPhysicalOpDistinct, true) {
        output_type_ = node->GetOutputType();
    }

    base::Status WithNewChildren(node::NodeManager *nm,
                                 const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;

    virtual ~PhysicalDistinctNode() {}
};

class PhysicalSelectIntoNode : public PhysicalUnaryNode {
 public:
    PhysicalSelectIntoNode(PhysicalOpNode *node, const std::string &query_str, const std::string &out_file,
                           std::shared_ptr<node::OptionsMap> options, std::shared_ptr<node::OptionsMap> config_options)
        : PhysicalUnaryNode(node, kPhysicalOpSelectInto, false),
          query_str_(query_str),
          out_file_(out_file),
          options_(std::move(options)),
          config_options_(std::move(config_options)) {}
    ~PhysicalSelectIntoNode() override = default;
    void Print(std::ostream &output, const std::string &tab) const override;
    base::Status InitSchema(PhysicalPlanContext *) override;
    base::Status WithNewChildren(node::NodeManager *nm, const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;
    static PhysicalSelectIntoNode *CastFrom(PhysicalOpNode *node);

    const std::string &QueryStr() const { return query_str_; }
    const std::string &OutFile() const { return out_file_; }

    // avoid to use map<A, B*>, the B* will result in python swig errors,
    //     e.g. no member named 'type_name' in 'swig::traits<B>'
    // vector<pair<>> is too complex, and will get a class in the package root dir.
    const hybridse::node::ConstNode *GetOption(const std::string &option) const {
        if (!options_) {
            return nullptr;
        }
        auto it = options_->find(option);
        return it == options_->end() ? nullptr : it->second;
    }
    const hybridse::node::ConstNode *GetConfigOption(const std::string &option) const {
        if (!config_options_) {
            return nullptr;
        }
        auto it = config_options_->find(option);
        return it == config_options_->end() ? nullptr : it->second;
    }

    std::string query_str_, out_file_;
    std::shared_ptr<node::OptionsMap> options_;
    std::shared_ptr<node::OptionsMap> config_options_;
};

class PhysicalLoadDataNode : public PhysicalOpNode {
 public:
    PhysicalLoadDataNode(const std::string &file, const std::string &db, const std::string &table,
                         std::shared_ptr<node::OptionsMap> options, std::shared_ptr<node::OptionsMap> config_options)
        : PhysicalOpNode(kPhysicalOpLoadData, false),
          file_(file),
          db_(db),
          table_(table),
          options_(std::move(options)),
          config_options_(std::move(config_options)) {}
    ~PhysicalLoadDataNode() override = default;
    void Print(std::ostream &output, const std::string &tab) const override;
    base::Status InitSchema(PhysicalPlanContext *) override;
    base::Status WithNewChildren(node::NodeManager *nm, const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override;
    static PhysicalLoadDataNode *CastFrom(PhysicalOpNode *node);

    const std::string &File() const { return file_; }
    const std::string &Db() const { return db_; }
    const std::string &Table() const { return table_; }
    // avoid to use map<A, B*>, the B* will result in python swig errors,
    //     e.g. no member named 'type_name' in 'swig::traits<B>'
    // vector<pair<>> is too complex, and will get a class in the package root dir.
    const hybridse::node::ConstNode *GetOption(const std::string &option) const {
        if (!options_) {
            return nullptr;
        }
        auto it = options_->find(option);
        return it == options_->end() ? nullptr : it->second;
    }

    std::string file_;
    std::string db_;
    std::string table_;
    std::shared_ptr<node::OptionsMap> options_;
    std::shared_ptr<node::OptionsMap> config_options_;
};

// there may more delete variants, don't mark it final
// the delete node support only 'delete job' statement currently
class PhysicalDeleteNode : public PhysicalOpNode {
 public:
    PhysicalDeleteNode(node::DeleteTarget target, const std::string& job_id)
        : PhysicalOpNode(kPhysicalOpDelete, false), target_(target), job_id_(job_id) {}
    ~PhysicalDeleteNode() {}

    const node::DeleteTarget GetTarget() const { return target_; }
    const std::string& GetJobId() const { return job_id_; }

    void Print(std::ostream &output, const std::string &tab) const override;
    base::Status InitSchema(PhysicalPlanContext *) override {
        return base::Status::OK();
    }
    base::Status WithNewChildren(node::NodeManager *nm, const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override {
        return base::Status::OK();
    }
 private:
    const node::DeleteTarget target_;
    const std::string job_id_;
};

class PhysicalCreateTableNode : public PhysicalOpNode {
 public:
    explicit PhysicalCreateTableNode(const node::CreatePlanNode *node)
        : PhysicalOpNode(kPhysicalCreateTable, false), data_(node) {}
    ~PhysicalCreateTableNode() override {}

    void Print(std::ostream &output, const std::string &tab) const override;
    base::Status InitSchema(PhysicalPlanContext *) override { return base::Status::OK(); }
    base::Status WithNewChildren(node::NodeManager *nm, const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override {
        return base::Status::OK();
    }

    const node::CreatePlanNode *data_;

    static PhysicalCreateTableNode *CastFrom(PhysicalOpNode *node);
};

class PhysicalInsertNode : public PhysicalOpNode {
 public:
    explicit PhysicalInsertNode(const node::InsertStmt *ins) :
        PhysicalOpNode(kPhysicalOpInsert, false), insert_stmt_(ins) {}
    ~PhysicalInsertNode() override {}

    void Print(std::ostream &output, const std::string &tab) const override;
    base::Status InitSchema(PhysicalPlanContext *) override { return base::Status::OK(); }
    base::Status WithNewChildren(node::NodeManager *nm, const std::vector<PhysicalOpNode *> &children,
                                 PhysicalOpNode **out) override {
        return base::Status::OK();
    }

    const node::InsertStmt* GetInsertStmt() const { return insert_stmt_; }

 private:
    const node::InsertStmt* insert_stmt_;
};
/**
 * Initialize expression replacer with schema change.
 */
Status BuildColumnReplacement(const node::ExprNode *expr,
                              const SchemasContext *origin_schema,
                              const SchemasContext *rebase_schema,
                              node::NodeManager *nm,
                              passes::ExprReplacer *replacer);

template <typename Component>
static Status ReplaceComponentExpr(const Component &component,
                                   const SchemasContext *origin_schema,
                                   const SchemasContext *rebase_schema,
                                   node::NodeManager *nm, Component *output) {
    *output = component;
    std::vector<const node::ExprNode *> depend_columns;
    component.ResolvedRelatedColumns(&depend_columns);
    passes::ExprReplacer replacer;
    for (auto col_expr : depend_columns) {
        CHECK_STATUS(BuildColumnReplacement(col_expr, origin_schema,
                                            rebase_schema, nm, &replacer));
    }
    return component.ReplaceExpr(replacer, nm, output);
}

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_VM_PHYSICAL_OP_H_
