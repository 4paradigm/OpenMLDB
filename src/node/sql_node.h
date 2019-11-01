/*
 * parser/node.h
 * Copyright (C) 2019 chenjing <chenjing@4paradigm.com>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FESQL_NODE_SQL_NODE_H_
#define FESQL_NODE_SQL_NODE_H_

#include <string>
#include <vector>
#include <iostream>
#include "emun.h"
#include <glog/logging.h>
namespace fesql {
namespace node {

// Global methods
std::string NameOfSQLNodeType(const SQLNodeType &type);

class SQLNode {

public:
    SQLNode(const SQLNodeType &type, uint32_t line_num, uint32_t location)
        : type(type), line_num_(line_num), location_(location) {
    }

    virtual ~SQLNode() {
//        LOG(INFO) << "sql node: " << NameOfSQLNodeType(type_) << " distruction enter >> \n";
    }

    virtual void Print(std::ostream &output) const {
        Print(output, "");
    }

    virtual void Print(std::ostream &output, const std::string &tab) const {
        output << tab << SPACE_ST << "node[" << NameOfSQLNodeType(type) << "]";
    }

    SQLNodeType GetType() const {
        return type;
    }

    uint32_t GetLineNum() const {
        return line_num_;
    }

    uint32_t GetLocation() const {
        return location_;
    }

    friend std::ostream &operator<<(std::ostream &output, const SQLNode &thiz);

    SQLNodeType type;
private:
    uint32_t line_num_;
    uint32_t location_;
};

typedef std::vector<SQLNode *> NodePointVector;
struct SQLLinkedNode {
    SQLNode *node_ptr_;
    SQLLinkedNode *next_;
    SQLLinkedNode(SQLNode *node_ptr) {
        node_ptr_ = node_ptr;
        next_ = NULL;
    }
    /**
     * destruction: tobe optimized
     */
    ~SQLLinkedNode() {
    }
};

class SQLNodeList {

public:
    SQLNodeList() : size_(0), head_(NULL), tail_(NULL) {
    }

    SQLNodeList(SQLLinkedNode *head, SQLLinkedNode *tail, size_t size)
        : size_(size), head_(head), tail_(tail) {
    }

    /**
     * SQLNodeList 只负责存储指针，不释放指针管理的区域
     */
    ~SQLNodeList() {
    }

    const size_t Size() {
        return size_;
    }

    void Print(std::ostream &output) const {
        Print(output, "");
    }

    void Print(std::ostream &output, const std::string &tab) const;

    void PushFront(SQLLinkedNode *linked_node_ptr) {
        linked_node_ptr->next_ = head_;
        head_ = linked_node_ptr;
        size_ += 1;
        if (NULL == tail_) {
            tail_ = head_;
        }
    }

    void AppendNodeList(SQLNodeList *node_list_ptr) {
        if (NULL == node_list_ptr) {
            return;
        }

        if (NULL == tail_) {
            head_ = node_list_ptr->head_;
            tail_ = head_;
            size_ = node_list_ptr->size_;
            return;
        }

        tail_->next_ = node_list_ptr->head_;
        tail_ = node_list_ptr->tail_;
        size_ += node_list_ptr->size_;
    }

    SQLLinkedNode *GetHead() {
        return head_;
    }
    friend std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz);
private:
    size_t size_;
    SQLLinkedNode *head_;
    SQLLinkedNode *tail_;
};

/**
 * SQL Node for Select statement
 */
class SelectStmt : public SQLNode {
public:

    SelectStmt() : SQLNode(kSelectStmt, 0, 0), distinct_opt_(0) {
        limit_ptr_ = NULL;
        where_clause_ptr_ = NULL;
        group_clause_ptr_ = NULL;
        having_clause_ptr_ = NULL;
        order_clause_ptr_ = NULL;
    }

    ~SelectStmt() {
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

    NodePointVector &GetSelectList() {
        return select_list_ptr_;
    }

    SQLNode *GetLimit() const {
        return limit_ptr_;
    }

    NodePointVector &GetTableRefList() {
        return tableref_list_ptr_;
    }

    NodePointVector &GetWindowList() {
        return window_list_ptr_;
    }

    void SetLimit(SQLNode *limit) {
        limit_ptr_ = limit;
    }

private:
    int distinct_opt_;
    SQLNode *limit_ptr_;
    NodePointVector select_list_ptr_;
    NodePointVector tableref_list_ptr_;
    SQLNode *where_clause_ptr_;
    SQLNode *group_clause_ptr_;
    SQLNode *having_clause_ptr_;
    SQLNode *order_clause_ptr_;
    NodePointVector window_list_ptr_;
};

class ResTarget : public SQLNode {
public:
    ResTarget() : SQLNode(kResTarget, 0, 0) {}
    ResTarget(const std::string &name, SQLNode *val) : SQLNode(kResTarget, 0, 0), name_(name), val_(val) {}
    ResTarget(uint32_t line_num, uint32_t location) : SQLNode(kResTarget, line_num, location), indirection_(NULL) {}
    ~ResTarget() {
    }

    void Print(std::ostream &output, const std::string &org_tab) const;
    std::string GetName() const {
        return name_;
    }

    SQLNode *GetVal() const {
        return val_;
    }

private:
    std::string name_;            /* column name or NULL */
    SQLNode *val_;            /* the value expression to compute or assign */
    NodePointVector indirection_;    /* subscripts, field names, and '*', or NIL */
};

class WindowDefNode : public SQLNode {

public:
    WindowDefNode()
        : SQLNode(kWindowDef, 0, 0), window_name_(""), frame_ptr_(NULL) {};
    ~WindowDefNode() {
    }

    void SetName(const std::string &name) {
        window_name_ = name;
    }

    std::string GetName() const {
        return window_name_;
    }

    NodePointVector &GetPartitions() {
        return partition_list_ptr_;
    }

    NodePointVector &GetOrders() {
        return order_list_ptr_;
    }

    SQLNode *GetFrame() const {
        return frame_ptr_;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

    void SetFrame(SQLNode *frame) {
        frame_ptr_ = frame;
    }

private:
    std::string window_name_;            /* window's own name */
    NodePointVector partition_list_ptr_;    /* PARTITION BY expression list */
    NodePointVector order_list_ptr_;    /* ORDER BY (list of SortBy) */
    SQLNode *frame_ptr_;    /* expression for starting bound, if any */
};

class FrameBound : public SQLNode {
public:
    FrameBound() : SQLNode(kFrameBound, 0, 0), bound_type_(kPreceding), offset_(NULL) {};
    FrameBound(SQLNodeType bound_type) :
        SQLNode(kFrameBound, 0, 0), bound_type_(bound_type) {}
    FrameBound(SQLNodeType bound_type, SQLNode *offset) :
        SQLNode(kFrameBound, 0, 0), bound_type_(bound_type), offset_(offset) {}
    ~FrameBound() {
    }

    void Print(std::ostream &output, const std::string &org_tab) const {
        SQLNode::Print(output, org_tab);
        const std::string tab = org_tab + INDENT + SPACE_ED;
        std::string space = org_tab + INDENT + INDENT;
        output << "\n";
        output << tab << SPACE_ST << "bound: " << NameOfSQLNodeType(bound_type_) << "\n";
        if (NULL == offset_) {
            output << space << "UNBOUNDED";
        } else {
            offset_->Print(output, space);
        }
    }

    SQLNodeType GetBoundType() const {
        return bound_type_;
    }

    SQLNode *GetOffset() const {
        return offset_;
    }
private:
    SQLNodeType bound_type_;
    SQLNode *offset_;
};

class FrameNode : public SQLNode {
public:
    FrameNode() : SQLNode(kFrames, 0, 0), frame_type_(kFrameRange), start_(NULL), end_(NULL) {};

    ~FrameNode() {
    }

    void SetFrameType(SQLNodeType frame_type) {
        frame_type_ = frame_type;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

    SQLNodeType GetFrameType() const {
        return frame_type_;
    }

    SQLNode *GetStart() const {
        return start_;
    }

    SQLNode *GetEnd() const {
        return end_;
    }

    friend void FillFrameNode(FrameNode *thiz, SQLNodeType frame_type, FrameBound *start, FrameBound *end) {
        thiz->frame_type_ = frame_type;
        thiz->start_ = start;
        thiz->end_ = end;
    };

private:
    SQLNodeType frame_type_;
    SQLNode *start_;
    SQLNode *end_;
};

class LimitNode : public SQLNode {
public:
    LimitNode() : SQLNode(kLimit, 0, 0), limit_cnt_(0) {};
    LimitNode(int limit_cnt) : SQLNode(kLimit, 0, 0), limit_cnt_(limit_cnt) {};
    int GetLimitCount() const {
        return limit_cnt_;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;
private:
    int limit_cnt_;
};
class SQLExprNode : public SQLNode {
public:
    SQLExprNode() : SQLNode(kExpr, 0, 0) {}
    SQLExprNode(uint32_t line_num, uint32_t location) : SQLNode(kExpr, line_num, location) {
    }
    ~SQLExprNode() {
    }
};

class AllNode : public SQLNode {
public:
    AllNode() : SQLNode(kAll, 0, 0), relation_name_("") {
    }

    AllNode(const std::string &relation_name) : SQLNode(kAll, 0, 0), relation_name_(relation_name) {
    }

    void SetRelationName(const std::string &relation_name) {
        relation_name_ = relation_name;
    }

    std::string GetRelationName() const {
        return relation_name_;
    }

private:
    std::string relation_name_;
};
class ColumnRefNode : public SQLNode {

public:
    ColumnRefNode() : SQLNode(kColumn, 0, 0), column_name_(""), relation_name_("") {

    }
    ColumnRefNode(const std::string &column_name)
        : SQLNode(kColumn, 0, 0), column_name_(column_name), relation_name_("") {
    }

    ColumnRefNode(const std::string &column_name, const std::string &relation_name)
        : SQLNode(kColumn, 0, 0), column_name_(column_name), relation_name_(relation_name) {
    }

    std::string GetRelationName() const {
        return relation_name_;
    }

    std::string GetColumnName() const {
        return column_name_;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

private:
    std::string column_name_;
    std::string relation_name_;

};

class OrderByNode : public SQLNode {

public:
    OrderByNode(SQLNode *order) : SQLNode(kOrderBy, 0, 0), sort_type_(kDesc), order_by_(order) {}
    ~OrderByNode() {
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

    SQLNodeType GetSortType() const {
        return sort_type_;
    }
    SQLNode *GetOrderBy() const {
        return order_by_;
    }
    void SetOrderBy(SQLNode *order_by) {
        order_by_ = order_by;
    }
private:
    SQLNodeType sort_type_;
    SQLNode *order_by_;
};

class TableNode : SQLNode {

public:
    TableNode() : SQLNode(kTable, 0, 0), org_table_name_(""), alias_table_name_("") {};
    TableNode(const std::string &name, const std::string &alias)
        : SQLNode(kTable, 0, 0), org_table_name_(name), alias_table_name_(alias) {

    }

    void Print(std::ostream &output, const std::string &org_tab) const;

    std::string GetOrgTableName() const {
        return org_table_name_;
    }

    std::string GetAliasTableName() const {
        return alias_table_name_;
    }

private:
    std::string org_table_name_;
    std::string alias_table_name_;
};

class FuncNode : SQLNode {

public:
    FuncNode() : SQLNode(kFunc, 0, 0), function_name_(""), over_(nullptr) {};
    FuncNode(const std::string &function_name)
        : SQLNode(kFunc, 0, 0), function_name_(function_name), over_(nullptr) {};

    ~FuncNode() {
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

    std::string GetFunctionName() const {
        return function_name_;
    }

    NodePointVector &GetArgs() {
        return args_;
    }

    WindowDefNode *GetOver() const {
        return over_;
    }

    void SetOver(WindowDefNode *over) {
        over_ = over;
    }
private:
    std::string function_name_;
    NodePointVector args_;
    WindowDefNode *over_;
};


class OtherSqlNode : public SQLNode {
public:
    OtherSqlNode(SQLNodeType &type) : SQLNode(type, 0, 0) {}
    OtherSqlNode(SQLNodeType &type, uint32_t line_num, uint32_t location) : SQLNode(type, line_num, location) {}
    void AddChild(SQLNode *node) {};
};

class UnknowSqlNode : public SQLNode {
public:
    UnknowSqlNode() : SQLNode(kUnknow, 0, 0) {}
    UnknowSqlNode(uint32_t line_num, uint32_t location) : SQLNode(kUnknow, line_num, location) {}

    void AddChild(SQLNode *node) {};
};

inline const std::string FnNodeName(const SQLNodeType &type) {
    switch (type) {
        case kFnPrimaryBool:return "bool";
        case kFnPrimaryInt16:return "int16";
        case kFnPrimaryInt32:return "int32";
        case kFnPrimaryInt64:return "int64";
        case kFnPrimaryFloat:return "float";
        case kFnPrimaryDouble:return "double";
        case kFnDef:return "def";
        case kFnValue:return "value";
        case kFnId:return "id";
        case kFnAssignStmt:return "=";
        case kFnReturnStmt:return "return";
        case kFnExpr:return "expr";
        case kFnExprBinary:return "bexpr";
        case kFnExprUnary:return "uexpr";
        case kFnPara:return "para";
        case kFnParaList:return "plist";
        case kFnList:return "funlist";
        default: return "unknowFn";
    }

}

class FnNode : public SQLNode {
public:
    FnNode() : SQLNode(kFunc, 0, 0), indent(0) {};
    FnNode(SQLNodeType type) : SQLNode(type, 0, 0), indent(0) {};
    void AddChildren(FnNode *node) {
        children.push_back(node);
    }
public:
    std::vector<FnNode *> children;
    int32_t indent;
};

class FnNodeFnDef : public FnNode {
public:
    FnNodeFnDef() : FnNode(kFnDef) {};
public:
    char *name;
    SQLNodeType ret_type;
};

class FnAssignNode : public FnNode {
public:
    FnAssignNode() : FnNode(kFnAssignStmt) {};
public:
    std::string name;
};

class FnParaNode : public FnNode {
public:
    FnParaNode() : FnNode(kFnPara) {};
public:
    std::string name;
    SQLNodeType para_type;
};

class FnBinaryExpr : public FnNode {
public:
    FnBinaryExpr() : FnNode(kFnExprBinary) {};
public:
    FnOperator op;
};

class FnUnaryExpr : public FnNode {
public:
    FnUnaryExpr() : FnNode(kFnExprUnary) {};
public:
    FnOperator op;
};

class FnIdNode : public FnNode {
public:
    FnIdNode() : FnNode(kFnId) {};
public:
    std::string name;
};

class FnTypeNode : public FnNode {
public:
    FnTypeNode() : FnNode(kType) {};
public:
    DataType data_type_;
};


class ConstNode : public FnNode{

public:
    ConstNode() : FnNode(kPrimary), date_type_(kTypeNull) {
    }
    ConstNode(int val) : FnNode(kPrimary), date_type_(kTypeInt32) {
        val_.vint = val;
    }
    ConstNode(long val) : FnNode(kPrimary), date_type_(kTypeInt64) {
        val_.vlong = val;
    }
    ConstNode(float val) : FnNode(kPrimary), date_type_(kTypeFloat) {
        val_.vfloat = val;
    }

    ConstNode(double val) : FnNode(kPrimary), date_type_(kTypeDouble) {
        val_.vdouble = val;
    }

    ConstNode(const char *val) : FnNode(kPrimary), date_type_(kTypeString) {
        val_.vstr = val;
    }
    ConstNode(const std::string &val) : FnNode(kPrimary), date_type_(kTypeString) {
        val_.vstr = val.c_str();
    }

    ~ConstNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const {
        SQLNode::Print(output, org_tab);
        output << "\n";
        const std::string tab = org_tab + INDENT + SPACE_ED;
        output << tab << SPACE_ST;
        switch (date_type_) {
            case kTypeInt32:output << "value: " << val_.vint;
                break;
            case kTypeInt64:output << "value: " << val_.vlong;
                break;
            case kTypeString:output << "value: " << val_.vstr;
                break;
            case kTypeFloat:output << "value: " << val_.vfloat;
                break;
            case kTypeDouble:output << "value: " << val_.vdouble;
                break;
            case kTypeNull:output << "value: null";
                break;
            default:output << "value: unknow";
        }
    }

    int GetInt() const {
        return val_.vint;
    }

    long GetLong() const {
        return val_.vlong;
    }

    const char *GetStr() const {
        return val_.vstr;
    }

    float GetFloat() const {
        return val_.vfloat;
    }

    double GetDouble() const {
        return val_.vdouble;
    }

    DataType GetDataType() const {
        return date_type_;
    }

private:
    DataType date_type_;
    union {
        int vint;        /* machine integer */
        long vlong;        /* machine integer */
        const char *vstr;        /* string */
        float vfloat;
        double vdouble;
    } val_;
};

std::string WindowOfExpression(SQLNode *node_ptr);
void FillSQLNodeList2NodeVector(SQLNodeList *node_list_ptr, std::vector<SQLNode *> &node_list);
void PrintSQLNode(std::ostream &output,
                  const std::string &org_tab,
                  SQLNode *node_ptr,
                  const std::string &item_name,
                  bool last_child);
void PrintSQLVector(std::ostream &output,
                    const std::string &tab,
                    NodePointVector vec,
                    const std::string &vector_name,
                    bool last_item);
void PrintValue(std::ostream &output,
                const std::string &org_tab,
                const std::string &value,
                const std::string &item_name,
                bool last_child);
} // namespace of node
} // namespace of fesql
#endif /* !FESQL_NODE_SQL_NODE_H_ */
