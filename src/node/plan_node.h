/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * plan_node.h
 *
 * Author: chenjing
 * Date: 2019/10/24
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_NODE_PLAN_NODE_H_
#define SRC_NODE_PLAN_NODE_H_

#include <glog/logging.h>
#include <node/sql_node.h>
#include <list>
#include <string>
#include <utility>
#include <vector>
#include "node/node_enum.h"
namespace fesql {
namespace node {

std::string NameOfPlanNodeType(const PlanType &type);

class PlanNode {
 public:
    explicit PlanNode(PlanType type) : type_(type) {}

    virtual ~PlanNode() {}

    virtual bool AddChild(PlanNode *node) = 0;

    PlanType GetType() const { return type_; }

    const std::vector<PlanNode *> &GetChildren() const { return children_; }

    int GetChildrenSize() const { return children_.size(); }
    friend std::ostream &operator<<(std::ostream &output, const PlanNode &thiz);

    virtual void Print(std::ostream &output, const std::string &tab) const;

 protected:
    PlanType type_;
    std::vector<PlanNode *> children_;
};

typedef std::vector<PlanNode *> PlanNodeList;

class LeafPlanNode : public PlanNode {
 public:
    explicit LeafPlanNode(PlanType type) : PlanNode(type) {}
    ~LeafPlanNode() {}
    virtual bool AddChild(PlanNode *node);
};

class UnaryPlanNode : public PlanNode {
 public:
    explicit UnaryPlanNode(PlanType type) : PlanNode(type) {}
    explicit UnaryPlanNode(PlanNode *node, PlanType type) : PlanNode(type) {
        AddChild(node);
    }
    ~UnaryPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
};

class BinaryPlanNode : public PlanNode {
 public:
    explicit BinaryPlanNode(PlanType type) : PlanNode(type) {}
    explicit BinaryPlanNode(PlanType type, PlanNode *left, PlanNode *right)
        : PlanNode(type) {
        AddChild(left);
        AddChild(right);
    }
    ~BinaryPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
};

class MultiChildPlanNode : public PlanNode {
 public:
    explicit MultiChildPlanNode(PlanType type) : PlanNode(type) {}
    ~MultiChildPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
};

class RelationNode : public LeafPlanNode {
 public:
    explicit RelationNode(const std::string &db, const std::string &table)
        : LeafPlanNode(kPlanTypeRelation), db_(db), table_(table) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string db_;
    const std::string table_;
};

class JoinPlanNode : public BinaryPlanNode {
 public:
    JoinPlanNode(PlanNode *left, PlanNode *right, ExprNode *expression)
        : BinaryPlanNode(kPlanTypeJoin, left, right), condition_(expression) {}
    const ExprNode *condition_;
};

class UnionPlanNode : public BinaryPlanNode {
 public:
    UnionPlanNode(PlanNode *left, PlanNode *right, bool is_all)
        : BinaryPlanNode(kPlanTypeJoin, left, right), is_all(true) {}
    const bool is_all;
};

class CrossProductPlanNode : public BinaryPlanNode {
 public:
    CrossProductPlanNode(PlanNode *left, PlanNode *right)
        : BinaryPlanNode(kPlanTypeJoin, left, right) {}
};

class SortPlanNode : public UnaryPlanNode {
 public:
    SortPlanNode(PlanNode *node, bool is_asc, PlanNodeList *order_list)
        : UnaryPlanNode(node, kPlanTypeSort),
          is_asc_(is_asc),
          order_list_(order_list) {}
    const bool is_asc_;
    PlanNodeList *order_list_;
};

class GroupPlanNode : public UnaryPlanNode {
 public:
    GroupPlanNode(PlanNode *node, const ExprListNode *by_list)
        : UnaryPlanNode(node, kPlanTypeSort), by_list_(by_list) {}
    const ExprListNode *by_list_;
};

class SelectPlanNode : public UnaryPlanNode {
 public:
    explicit SelectPlanNode(PlanNode *node)
        : UnaryPlanNode(node, kPlanTypeSelect) {}
    ~SelectPlanNode() {}
};

class FilterPlanNode : public UnaryPlanNode {
 public:
    FilterPlanNode(PlanNode *node, const ExprNode *condition)
        : UnaryPlanNode(node, kPlanTypeScan), condition_(condition) {}
    ~FilterPlanNode() {}
    const ExprNode *condition_;
};

class LimitPlanNode : public UnaryPlanNode {
 public:
    LimitPlanNode(PlanNode *node, int limit_cnt)
        : UnaryPlanNode(node, kPlanTypeLimit), limit_cnt_(limit_cnt) {}

    ~LimitPlanNode() {}
    const int GetLimitCnt() const { return limit_cnt_; }
    void SetLimitCnt(int limit_cnt) { limit_cnt_ = limit_cnt; }
    void Print(std::ostream &output, const std::string &org_tab) const override;

 private:
    int limit_cnt_;
};

class ProjectNode : public LeafPlanNode {
 public:
    ProjectNode(int32_t pos, const std::string &name,
                node::ExprNode *expression)
        : LeafPlanNode(kProjectNode),
          pos_(pos),
          name_(name),
          expression_(expression) {}

    ~ProjectNode() {}
    void Print(std::ostream &output, const std::string &orgTab) const;
    const uint32_t GetPos() const { return pos_; }
    std::string GetName() const { return name_; }
    node::ExprNode *GetExpression() const { return expression_; }

 private:
    uint32_t pos_;
    std::string name_;
    node::ExprNode *expression_;
};

class WindowPlanNode : public LeafPlanNode {
 public:
    explicit WindowPlanNode(int id)
        : LeafPlanNode(kPlanTypeWindow),
          id(id),
          name(""),
          start_offset_(0L),
          end_offset_(0L),
          is_range_between_(true),
          keys_(),
          orders_() {}
    ~WindowPlanNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    int64_t GetStartOffset() const { return start_offset_; }
    void SetStartOffset(int64_t startOffset) { start_offset_ = startOffset; }
    int64_t GetEndOffset() const { return end_offset_; }
    void SetEndOffset(int64_t endOffset) { end_offset_ = endOffset; }
    bool IsRangeBetween() const { return is_range_between_; }
    void SetIsRangeBetween(bool isRangeBetween) {
        is_range_between_ = isRangeBetween;
    }
    const std::vector<std::string> &GetKeys() const { return keys_; }
    const std::vector<std::string> &GetOrders() const { return orders_; }
    void SetKeys(const std::vector<std::string> &keys) { keys_ = keys; }
    void SetOrders(const std::vector<std::string> &orders) { orders_ = orders; }
    const std::string &GetName() const { return name; }
    void SetName(const std::string &name) { WindowPlanNode::name = name; }
    const int GetId() const { return id; }

 private:
    int id;
    std::string name;
    int64_t start_offset_;
    int64_t end_offset_;
    bool is_range_between_;
    std::vector<std::string> keys_;
    std::vector<std::string> orders_;
};

class ProjectListNode : public LeafPlanNode {
 public:
    ProjectListNode()
        : LeafPlanNode(kProjectList),
          w_ptr_(nullptr),
          is_window_agg_(false),
          scan_limit_(0L),
          projects({}) {}
    ProjectListNode(const std::string &table_name, const WindowPlanNode *w_ptr,
                    const bool is_window_agg)
        : LeafPlanNode(kProjectList),
          w_ptr_(w_ptr),
          table_name_(table_name),
          is_window_agg_(is_window_agg),
          scan_limit_(0L),
          projects({}) {}
    ~ProjectListNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const PlanNodeList &GetProjects() const { return projects; }
    void AddProject(ProjectNode *project) { projects.push_back(project); }

    const WindowPlanNode *GetW() const { return w_ptr_; }

    const bool IsWindowAgg() const { return is_window_agg_; }

    const std::string GetTable() const { return table_name_; }

    void SetScanLimit(int scan_limit) { scan_limit_ = scan_limit; }
    const uint64_t GetScanLimit() const { return scan_limit_; }

 private:
    const WindowPlanNode *w_ptr_;
    const std::string table_name_;
    bool is_window_agg_;
    uint64_t scan_limit_;
    PlanNodeList projects;
};

class ProjectPlanNode : public UnaryPlanNode {
 public:
    explicit ProjectPlanNode(
        PlanNode *node, const PlanNodeList &project_list_vec,
        const std::vector<std::pair<uint32_t, uint32_t>> &pos_mapping)
        : UnaryPlanNode(node, kPlanTypeProject),
          project_list_vec_(project_list_vec),
          pos_mapping_(pos_mapping) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const PlanNodeList project_list_vec_;
    const std::vector<std::pair<uint32_t, uint32_t>> pos_mapping_;
};

class CreatePlanNode : public LeafPlanNode {
 public:
    CreatePlanNode(const std::string &table_name, NodePointVector column_list)
        : LeafPlanNode(kPlanTypeCreate),
          database_(""),
          table_name_(table_name),
          column_desc_list_(column_list) {}
    ~CreatePlanNode() {}

    std::string GetDatabase() const { return database_; }

    void setDatabase(const std::string &database) { database_ = database; }

    std::string GetTableName() const { return table_name_; }

    void setTableName(const std::string &table_name) {
        table_name_ = table_name;
    }

    NodePointVector &GetColumnDescList() { return column_desc_list_; }
    void SetColumnDescList(const NodePointVector &column_desc_list) {
        column_desc_list_ = column_desc_list;
    }

 private:
    std::string database_;
    std::string table_name_;
    NodePointVector column_desc_list_;
};

class CmdPlanNode : public LeafPlanNode {
 public:
    CmdPlanNode(const node::CmdType cmd_type,
                const std::vector<std::string> &args)
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
    explicit InsertPlanNode(const InsertStmt *insert_node)
        : LeafPlanNode(kPlanTypeInsert), insert_node_(insert_node) {}
    ~InsertPlanNode() {}
    const InsertStmt *GetInsertNode() const { return insert_node_; }

 private:
    const InsertStmt *insert_node_;
};

class FuncDefPlanNode : public LeafPlanNode {
 public:
    explicit FuncDefPlanNode(const FnNodeFnDef *fn_def)
        : LeafPlanNode(kPlanTypeFuncDef), fn_def_(fn_def) {}
    ~FuncDefPlanNode() {}
    void Print(std::ostream &output, const std::string &orgTab) const;
    const FnNodeFnDef *fn_def_;
};

void PrintPlanVector(std::ostream &output, const std::string &tab,
                     PlanNodeList vec, const std::string vector_name,
                     bool last_item);

void PrintPlanNode(std::ostream &output, const std::string &org_tab,
                   const PlanNode *node_ptr, const std::string &item_name,
                   bool last_child);

}  // namespace node
}  // namespace fesql

#endif  // SRC_NODE_PLAN_NODE_H_
