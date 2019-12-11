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
    ~UnaryPlanNode() {}
    virtual bool AddChild(PlanNode *node);
    virtual void Print(std::ostream &output, const std::string &org_tab) const;
};

class BinaryPlanNode : public PlanNode {
 public:
    explicit BinaryPlanNode(PlanType type) : PlanNode(type) {}
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

class SelectPlanNode : public MultiChildPlanNode {
 public:
    SelectPlanNode() : MultiChildPlanNode(kPlanTypeSelect), limit_cnt_(-1) {}
    ~SelectPlanNode() {}
    int GetLimitCount() { return limit_cnt_; }

    void SetLimitCount(int count) { limit_cnt_ = count; }

 private:
    int limit_cnt_;
};

class ScanPlanNode : public UnaryPlanNode {
 public:
    ScanPlanNode(const std::string &table_name, PlanType scan_type)
        : UnaryPlanNode(kPlanTypeScan),
          scan_type_(scan_type),
          table_name_(table_name),
          condition_(nullptr),
          limit_cnt_(-1) {}
    ~ScanPlanNode() {}

    PlanType GetScanType() { return scan_type_; }

    const int GetLimit() const { return limit_cnt_; }

    const std::string &GetTable() const { return table_name_; }

    void SetLimit(int limit) { limit_cnt_ = limit; }
    const SQLNode *GetCondition() const { return condition_; }
    void SetCondition(SQLNode *condition) { condition_ = condition; }

 private:
    // TODO(chenjing): OP tid
    PlanType scan_type_;
    std::string table_name_;
    SQLNode *condition_;
    int limit_cnt_;
};

class LimitPlanNode : public MultiChildPlanNode {
 public:
    LimitPlanNode() : MultiChildPlanNode(kPlanTypeLimit) {}
    explicit LimitPlanNode(int limit_cnt)
        : MultiChildPlanNode(kPlanTypeLimit), limit_cnt_(limit_cnt) {}

    ~LimitPlanNode() {}

    const int GetLimitCnt() const { return limit_cnt_; }

    void SetLimitCnt(int limit_cnt) { limit_cnt_ = limit_cnt; }

 private:
    int limit_cnt_;
};

/**
 * TODO:
 * where 过滤: 暂时不用考虑
 * having: 对结果过滤
 *
 */
class FilterPlanNode : public UnaryPlanNode {
 public:
    FilterPlanNode() : UnaryPlanNode(kPlanTypeFilter), condition_(nullptr) {}
    ~FilterPlanNode();

 private:
    SQLNode *condition_;
};

class ProjectPlanNode : public LeafPlanNode {
 public:
    ProjectPlanNode()
        : LeafPlanNode(kProject),
          expression_(nullptr),
          name_(""),
          table_(""),
          w_("") {}

    ~ProjectPlanNode() {}
    void Print(std::ostream &output, const std::string &orgTab) const;

    void SetW(const std::string w) { w_ = w; }
    std::string GetW() const { return w_; }

    std::string GetTable() const { return table_; }

    void SetTable(const std::string table) { table_ = table; }

    std::string GetName() const { return name_; }

    void SetName(const std::string name) { name_ = name; }

    node::ExprNode *GetExpression() const { return expression_; }

    void SetExpression(node::ExprNode *expression) { expression_ = expression; }

    bool IsWindowProject() { return !w_.empty(); }

 private:
    node::ExprNode *expression_;
    std::string name_;
    std::string table_;
    std::string w_;
};

class MergePlanNode : public MultiChildPlanNode {
 public:
    MergePlanNode() : MultiChildPlanNode(kPlanTypeMerge) {}
    ~MergePlanNode() {}
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
class ProjectListPlanNode : public MultiChildPlanNode {
 public:
    ProjectListPlanNode()
        : MultiChildPlanNode(kProjectList),
          w_ptr_(nullptr),
          is_window_agg_(false) {}
    ProjectListPlanNode(const std::string &table, WindowPlanNode *w_ptr,
                        const bool is_window_agg)
        : MultiChildPlanNode(kProjectList),
          table_(table),
          w_ptr_(w_ptr),
          is_window_agg_(is_window_agg) {}
    ~ProjectListPlanNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const PlanNodeList &GetProjects() const { return projects; }
    void AddProject(ProjectPlanNode *project) { projects.push_back(project); }

    const std::string GetTable() const { return table_; }

    WindowPlanNode *GetW() const { return w_ptr_; }

    const bool IsWindowAgg() const { return is_window_agg_; }

 private:
    PlanNodeList projects;
    std::string table_;
    WindowPlanNode *w_ptr_;
    bool is_window_agg_;
};

class CreatePlanNode : public LeafPlanNode {
 public:
    CreatePlanNode()
        : LeafPlanNode(kPlanTypeCreate), database_(""), table_name_("") {}
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
    CmdPlanNode() : LeafPlanNode(kPlanTypeCmd) {}
    ~CmdPlanNode() {}

    void SetCmdNode(const CmdNode *node) {
        cmd_type_ = node->GetCmdType();
        args_ = node->GetArgs();
    }

    const node::CmdType GetCmdType() const { return cmd_type_; }
    const std::vector<std::string> &GetArgs() const { return args_; }

 private:
    node::CmdType cmd_type_;
    std::vector<std::string> args_;
};

class InsertPlanNode : public LeafPlanNode {
 public:
    InsertPlanNode() : LeafPlanNode(kPlanTypeInsert), insert_node_(nullptr) {}
    ~InsertPlanNode() {}
    void SetInsertNode(const InsertStmt *node) { insert_node_ = node; }

    const InsertStmt *GetInsertNode() const { return insert_node_; }

 private:
    const InsertStmt *insert_node_;
};

class FuncDefPlanNode : public LeafPlanNode {
 public:
    FuncDefPlanNode() : LeafPlanNode(kPlanTypeFuncDef) {}
    ~FuncDefPlanNode() {}

    void SetFuNodeList(const FnNodeList *fn_node_list) {
        fn_node_list_ = fn_node_list;
    }

    const FnNodeList *GetFnNodeList() const { return fn_node_list_; }

 private:
    const FnNodeList *fn_node_list_;
};
void PrintPlanVector(std::ostream &output, const std::string &tab,
                     PlanNodeList vec, const std::string vector_name,
                     bool last_item);

void PrintPlanNode(std::ostream &output, const std::string &org_tab,
                   PlanNode *node_ptr, const std::string &item_name,
                   bool last_child);

}  // namespace node
}  // namespace fesql

#endif  // SRC_NODE_PLAN_NODE_H_
