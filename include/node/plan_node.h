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

#ifndef INCLUDE_NODE_PLAN_NODE_H_
#define INCLUDE_NODE_PLAN_NODE_H_

#include <glog/logging.h>
#include <list>
#include <string>
#include <utility>
#include <vector>
#include "node/node_enum.h"
#include "node/sql_node.h"
namespace hybridse {
namespace node {

std::string NameOfPlanNodeType(const PlanType &type);

class PlanNode : public NodeBase<PlanNode> {
 public:
    explicit PlanNode(PlanType type) : type_(type) {}

    virtual ~PlanNode() {}

    virtual bool AddChild(PlanNode *node) = 0;

    PlanType GetType() const { return type_; }

    const std::vector<PlanNode *> &GetChildren() const { return children_; }
    int GetChildrenSize() const { return children_.size(); }

    friend std::ostream &operator<<(std::ostream &output, const PlanNode &thiz);

    void Print(std::ostream &output, const std::string &tab) const override;
    virtual void PrintChildren(std::ostream &output, const std::string &tab) const;

    bool Equals(const PlanNode *that) const override;
    const PlanType type_;

    const std::string GetTypeName() const override { return NameOfPlanNodeType(type_); }

 protected:
    std::vector<PlanNode *> children_;
};

typedef std::vector<PlanNode *> PlanNodeList;

class LeafPlanNode : public PlanNode {
 public:
    explicit LeafPlanNode(PlanType type) : PlanNode(type) {}
    ~LeafPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void PrintChildren(std::ostream &output, const std::string &tab) const;
    virtual bool Equals(const PlanNode *that) const;
};

class UnaryPlanNode : public PlanNode {
 public:
    explicit UnaryPlanNode(PlanType type) : PlanNode(type) {}
    explicit UnaryPlanNode(PlanNode *node, PlanType type) : PlanNode(type) { AddChild(node); }
    ~UnaryPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
    virtual void PrintChildren(std::ostream &output, const std::string &tab) const;
    virtual bool Equals(const PlanNode *that) const;
    PlanNode *GetDepend() const { return children_[0]; }
};

class BinaryPlanNode : public PlanNode {
 public:
    explicit BinaryPlanNode(PlanType type) : PlanNode(type) {}
    explicit BinaryPlanNode(PlanType type, PlanNode *left, PlanNode *right) : PlanNode(type) {
        AddChild(left);
        AddChild(right);
    }
    ~BinaryPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
    virtual void PrintChildren(std::ostream &output, const std::string &tab) const;
    virtual bool Equals(const PlanNode *that) const;
    PlanNode *GetLeft() const { return children_[0]; }
    PlanNode *GetRight() const { return children_[1]; }
};

class MultiChildPlanNode : public PlanNode {
 public:
    explicit MultiChildPlanNode(PlanType type) : PlanNode(type) {}
    ~MultiChildPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
    virtual void PrintChildren(std::ostream &output, const std::string &tab) const;
    virtual bool Equals(const PlanNode *that) const;
};

class RenamePlanNode : public UnaryPlanNode {
 public:
    RenamePlanNode(PlanNode *node, const std::string table_name)
        : UnaryPlanNode(node, kPlanTypeRename), table_(table_name) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const PlanNode *that) const;

    const std::string table_;
};
class TablePlanNode : public LeafPlanNode {
 public:
    TablePlanNode(const std::string &db, const std::string &table)
        : LeafPlanNode(kPlanTypeTable), db_(db), table_(table), is_primary_(false) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *that) const;
    const bool IsPrimary() const { return is_primary_; }
    void SetIsPrimary(bool is_primary) { is_primary_ = is_primary; }

    const std::string db_;
    const std::string table_;

 private:
    bool is_primary_;
};

class DistinctPlanNode : public UnaryPlanNode {
 public:
    explicit DistinctPlanNode(PlanNode *node) : UnaryPlanNode(node, kPlanTypeDistinct) {}
};

class JoinPlanNode : public BinaryPlanNode {
 public:
    JoinPlanNode(PlanNode *left, PlanNode *right, JoinType join_type, const OrderByNode *orders,
                 const ExprNode *expression)
        : BinaryPlanNode(kPlanTypeJoin, left, right), join_type_(join_type), orders_(orders), condition_(expression) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *that) const;
    const JoinType join_type_;
    const OrderByNode *orders_;
    const ExprNode *condition_;
};

class UnionPlanNode : public BinaryPlanNode {
 public:
    UnionPlanNode(PlanNode *left, PlanNode *right, bool is_all)
        : BinaryPlanNode(kPlanTypeUnion, left, right), is_all(is_all) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *that) const;
    const bool is_all;
};

class CrossProductPlanNode : public BinaryPlanNode {
 public:
    CrossProductPlanNode(PlanNode *left, PlanNode *right) : BinaryPlanNode(kPlanTypeJoin, left, right) {}
};

class SortPlanNode : public UnaryPlanNode {
 public:
    SortPlanNode(PlanNode *node, const OrderByNode *order_list)
        : UnaryPlanNode(node, kPlanTypeSort), order_list_(order_list) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *that) const;
    const OrderByNode *order_list_;
};

class GroupPlanNode : public UnaryPlanNode {
 public:
    GroupPlanNode(PlanNode *node, const ExprListNode *by_list)
        : UnaryPlanNode(node, kPlanTypeGroup), by_list_(by_list) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *node) const;
    const ExprListNode *by_list_;
};

class QueryPlanNode : public UnaryPlanNode {
 public:
    explicit QueryPlanNode(PlanNode *node) : UnaryPlanNode(node, kPlanTypeQuery) {}
    ~QueryPlanNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *node) const;
};

class FilterPlanNode : public UnaryPlanNode {
 public:
    FilterPlanNode(PlanNode *node, const ExprNode *condition)
        : UnaryPlanNode(node, kPlanTypeFilter), condition_(condition) {}
    ~FilterPlanNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *node) const;
    const ExprNode *condition_;
};

class LimitPlanNode : public UnaryPlanNode {
 public:
    LimitPlanNode(PlanNode *node, int32_t limit_cnt) : UnaryPlanNode(node, kPlanTypeLimit), limit_cnt_(limit_cnt) {}

    ~LimitPlanNode() {}
    const int GetLimitCnt() const { return limit_cnt_; }
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const PlanNode *node) const;
    const int32_t limit_cnt_;
};

class ProjectNode : public LeafPlanNode {
 public:
    ProjectNode(int32_t pos, const std::string &name, const bool is_aggregation, node::ExprNode *expression,
                node::FrameNode *frame)
        : LeafPlanNode(kProjectNode),
          is_aggregation_(is_aggregation),
          pos_(pos),
          name_(name),
          expression_(expression),
          frame_(frame) {}

    ~ProjectNode() {}
    void Print(std::ostream &output, const std::string &orgTab) const;
    const uint32_t GetPos() const { return pos_; }
    std::string GetName() const { return name_; }
    node::ExprNode *GetExpression() const { return expression_; }
    void SetExpression(node::ExprNode *expr) { expression_ = expr; }
    node::FrameNode *frame() const { return frame_; }
    void set_frame(node::FrameNode *frame) { frame_ = frame; }
    virtual bool Equals(const PlanNode *node) const;

    const bool is_aggregation_;

 private:
    uint32_t pos_;
    std::string name_;
    node::ExprNode *expression_;
    node::FrameNode *frame_;
};

class WindowPlanNode : public LeafPlanNode {
 public:
    explicit WindowPlanNode(int id)
        : LeafPlanNode(kPlanTypeWindow),
          id(id),
          exclude_current_time_(false),
          instance_not_in_window_(false),
          name(""),
          keys_(nullptr),
          orders_(nullptr) {}
    ~WindowPlanNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    int64_t GetStartOffset() const { return frame_node_->GetHistoryRangeStart(); }
    int64_t GetEndOffset() const { return frame_node_->GetHistoryRangeEnd(); }
    const FrameNode *frame_node() const { return frame_node_; }
    void set_frame_node(FrameNode *frame_node) { frame_node_ = frame_node; }
    const ExprListNode *GetKeys() const { return keys_; }
    const OrderByNode *GetOrders() const { return orders_; }
    void SetKeys(ExprListNode *keys) { keys_ = keys; }
    void SetOrders(OrderByNode *orders) { orders_ = orders; }
    const std::string &GetName() const { return name; }
    void SetName(const std::string &name) { WindowPlanNode::name = name; }
    const int GetId() const { return id; }
    void AddUnionTable(PlanNode *node) { return union_tables_.push_back(node); }
    const PlanNodeList &union_tables() const { return union_tables_; }
    const bool instance_not_in_window() const { return instance_not_in_window_; }
    void set_instance_not_in_window(bool instance_not_in_window) { instance_not_in_window_ = instance_not_in_window; }
    const bool exclude_current_time() const { return exclude_current_time_; }
    void set_exclude_current_time(bool exclude_current_time) { exclude_current_time_ = exclude_current_time; }
    virtual bool Equals(const PlanNode *node) const;

 private:
    int id;
    bool exclude_current_time_;
    bool instance_not_in_window_;
    std::string name;
    FrameNode *frame_node_;
    ExprListNode *keys_;
    OrderByNode *orders_;
    PlanNodeList union_tables_;
};

class ProjectListNode : public LeafPlanNode {
 public:
    ProjectListNode() : LeafPlanNode(kProjectList), is_window_agg_(false), w_ptr_(nullptr), projects({}) {}
    ProjectListNode(const WindowPlanNode *w_ptr, const bool is_window_agg)
        : LeafPlanNode(kProjectList), is_window_agg_(is_window_agg), w_ptr_(w_ptr), projects({}) {}
    ~ProjectListNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const PlanNodeList &GetProjects() const { return projects; }
    void AddProject(ProjectNode *project) { projects.push_back(project); }

    const WindowPlanNode *GetW() const { return w_ptr_; }

    const bool IsWindowAgg() const { return is_window_agg_; }
    virtual bool Equals(const PlanNode *node) const;

    static bool MergeProjectList(node::ProjectListNode *project_list1, node::ProjectListNode *project_list2,
                                 node::ProjectListNode *merged_project);
    const bool is_window_agg_;
    const WindowPlanNode *w_ptr_;

    bool IsSimpleProjectList();

 private:
    PlanNodeList projects;
};

class ProjectPlanNode : public UnaryPlanNode {
 public:
    explicit ProjectPlanNode(PlanNode *node, const std::string &table, const PlanNodeList &project_list_vec,
                             const std::vector<std::pair<uint32_t, uint32_t>> &pos_mapping)
        : UnaryPlanNode(node, kPlanTypeProject),
          table_(table),
          project_list_vec_(project_list_vec),
          pos_mapping_(pos_mapping) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const PlanNode *node) const;

    const std::string table_;
    const PlanNodeList project_list_vec_;
    const std::vector<std::pair<uint32_t, uint32_t>> pos_mapping_;
    bool IsSimpleProjectPlan();
};

class CreatePlanNode : public LeafPlanNode {
 public:
    CreatePlanNode(const std::string &table_name, int replica_num, int partition_num, NodePointVector column_list,
                   NodePointVector distribution_list)
        : LeafPlanNode(kPlanTypeCreate),
          database_(""),
          table_name_(table_name),
          replica_num_(replica_num),
          partition_num_(partition_num),
          column_desc_list_(column_list),
          distribution_list_(distribution_list) {}
    ~CreatePlanNode() {}

    std::string GetDatabase() const { return database_; }

    void setDatabase(const std::string &database) { database_ = database; }

    std::string GetTableName() const { return table_name_; }

    void setTableName(const std::string &table_name) { table_name_ = table_name; }

    bool ExtractColumnsAndIndexs(std::vector<std::string> &columns,   // NOLINT
                                 std::vector<std::string> &indexs) {  // NOLINT
        if (column_desc_list_.empty()) {
            return false;
        }
        size_t index_id = 1;
        for (const auto item : column_desc_list_) {
            switch (item->GetType()) {
                case node::kColumnDesc: {
                    node::ColumnDefNode *column_def = dynamic_cast<node::ColumnDefNode *>(item);
                    columns.push_back(column_def->GetColumnName() + " " +
                                      node::DataTypeName(column_def->GetColumnType()));
                    break;
                }
                case node::kColumnIndex: {
                    node::ColumnIndexNode *column_index = dynamic_cast<node::ColumnIndexNode *>(item);
                    auto &keys = column_index->GetKey();
                    if (!keys.empty()) {
                        std::string index = "index" + std::to_string(index_id++) + ":" + keys[0];
                        for (size_t idx = 1; idx < keys.size(); idx++) {
                            index.append("|").append(keys[idx]);
                        }

                        if (!column_index->GetTs().empty()) {
                            index.append(":").append(column_index->GetTs());
                        }
                        indexs.push_back(index);
                    } else {
                        LOG(WARNING) << "Invalid column index node, empty keys";
                        return false;
                    }
                    break;
                }
                default: {
                    LOG(WARNING) << "Invalid column desc " << node::NameOfSqlNodeType(item->GetType());
                    return false;
                }
            }
        }
        return true;
    }

    NodePointVector &GetColumnDescList() { return column_desc_list_; }
    void SetColumnDescList(const NodePointVector &column_desc_list) { column_desc_list_ = column_desc_list; }

    int GetReplicaNum() const { return replica_num_; }

    void setReplicaNum(int replica_num) { replica_num_ = replica_num; }

    int GetPartitionNum() const { return partition_num_; }

    void setPartitionNum(int partition_num) { partition_num_ = partition_num; }

    NodePointVector &GetDistributionList() { return distribution_list_; }
    void SetDistributionList(const NodePointVector &distribution_list) { distribution_list_ = distribution_list; }
    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string database_;
    std::string table_name_;
    int replica_num_;
    int partition_num_;
    NodePointVector column_desc_list_;
    NodePointVector distribution_list_;
};

class CmdPlanNode : public LeafPlanNode {
 public:
    CmdPlanNode(const node::CmdType cmd_type, const std::vector<std::string> &args)
        : LeafPlanNode(kPlanTypeCmd), cmd_type_(cmd_type), args_(args) {}
    ~CmdPlanNode() {}

    const node::CmdType GetCmdType() const { return cmd_type_; }
    const std::vector<std::string> &GetArgs() const { return args_; }

 private:
    node::CmdType cmd_type_;
    std::vector<std::string> args_;
};

class InsertPlanNode : public LeafPlanNode {
 public:
    explicit InsertPlanNode(const InsertStmt *insert_node) : LeafPlanNode(kPlanTypeInsert), insert_node_(insert_node) {}
    ~InsertPlanNode() {}
    const InsertStmt *GetInsertNode() const { return insert_node_; }

 private:
    const InsertStmt *insert_node_;
};
class ExplainPlanNode : public LeafPlanNode {
 public:
    explicit ExplainPlanNode(const ExplainNode *explain_node)
        : LeafPlanNode(kPlanTypeExplain), explain_node_(explain_node) {}
    ~ExplainPlanNode() {}
    const ExplainNode *GetExplainNode() const { return explain_node_; }

 private:
    const ExplainNode *explain_node_;
};
class FuncDefPlanNode : public LeafPlanNode {
 public:
    explicit FuncDefPlanNode(FnNodeFnDef *fn_def) : LeafPlanNode(kPlanTypeFuncDef), fn_def_(fn_def) {}
    ~FuncDefPlanNode() {}
    void Print(std::ostream &output, const std::string &orgTab) const;
    FnNodeFnDef *fn_def_;
};
class CreateIndexPlanNode : public LeafPlanNode {
 public:
    explicit CreateIndexPlanNode(const CreateIndexNode *create_index_node)
        : LeafPlanNode(kPlanTypeCreateIndex), create_index_node_(create_index_node) {}
    ~CreateIndexPlanNode() {}
    void Print(std::ostream &output, const std::string &orgTab) const;
    const CreateIndexNode *create_index_node_;
};
class CreateProcedurePlanNode : public MultiChildPlanNode {
 public:
    CreateProcedurePlanNode(const std::string &sp_name, const NodePointVector &input_parameter_list,
                            const PlanNodeList &inner_plan_node_list)
        : MultiChildPlanNode(kPlanTypeCreateSp),
          database_(""),
          sp_name_(sp_name),
          input_parameter_list_(input_parameter_list),
          inner_plan_node_list_(inner_plan_node_list) {
        for (auto inner_plan_node : inner_plan_node_list) {
            AddChild(inner_plan_node);
        }
    }
    ~CreateProcedurePlanNode() {}

    const std::string &GetDatabase() const { return database_; }

    void setDatabase(const std::string &database) { database_ = database; }

    const std::string &GetSpName() const { return sp_name_; }

    void setSpName(const std::string &sp_name) { sp_name_ = sp_name; }

    NodePointVector &GetInputParameterList() { return input_parameter_list_; }
    void SetInputParameterList(const NodePointVector &input_parameter_list) {
        input_parameter_list_ = input_parameter_list;
    }

    const PlanNodeList &GetInnerPlanNodeList() const { return inner_plan_node_list_; }
    void SetInnerPlanNodeList(const PlanNodeList &inner_plan_node_list) {
        inner_plan_node_list_ = inner_plan_node_list;
    }

 private:
    std::string database_;
    std::string sp_name_;
    NodePointVector input_parameter_list_;
    PlanNodeList inner_plan_node_list_;
};

bool PlanEquals(const PlanNode *left, const PlanNode *right);
bool PlanListEquals(const std::vector<PlanNode *> &list1, const std::vector<PlanNode *> &list2);
void PrintPlanVector(std::ostream &output, const std::string &tab, PlanNodeList vec, const std::string vector_name,
                     bool last_item);

void PrintPlanNode(std::ostream &output, const std::string &org_tab, const PlanNode *node_ptr,
                   const std::string &item_name, bool last_child);

}  // namespace node
}  // namespace hybridse

#endif  // INCLUDE_NODE_PLAN_NODE_H_
