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

#ifndef FEDB_SQL_NODE_H_
#define FEDB_SQL_NODE_H_

#include <string>
#include <list>
#include <iostream>
#include <forward_list>
namespace fedb {
namespace sql {

const std::string SPACE_ST = "+";
const std::string SPACE_ED = "";
enum SQLNodeType {
    kSelectStmt = 0,
    kExpr,
    kResTarget,
    kTable,
    kFunc,
    kWindowDef,
    kFrameBound,
    kFrames,
    kColumn,
    kConst,
    kAll,
    kList,
    kOrderBy,

    kNull,
    kInt,
    kBigInt,
    kFloat,
    kDouble,
    kString,

    kDesc,
    kAsc,

    kFrameRange,
    kFrameRows,

    kPreceding,
    kFollowing,
    kCurrent,
    kUnknow
};

// Global methods
std::string NameOfSQLNodeType(const SQLNodeType &type);

class SQLNode {
    uint32_t line_num_;
    uint32_t location_;

public:
    SQLNode(const SQLNodeType &type, uint32_t line_num, uint32_t location)
        : type_(type), line_num_(line_num), location_(location) {
    }

    virtual ~SQLNode() {}

    virtual void Print(std::ostream &output) const {
        Print(output, SPACE_ST);
    }

    virtual void Print(std::ostream &output, const std::string &tab) const {
        output << tab << SPACE_ED << NameOfSQLNodeType(type_);
    }

    SQLNodeType GetType() const {
        return type_;
    }

    uint32_t GetLineNum() const {
        return line_num_;
    }

    uint32_t GetLocation() const {
        return location_;
    }

    friend std::ostream &operator<<(std::ostream &output, const SQLNode &thiz);

protected:
    SQLNodeType type_;
};

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
        delete node_ptr_;
    }
};

class SQLNodeList {
    SQLLinkedNode *head_;
    SQLLinkedNode *tail_;
    size_t len_;
public:
    SQLNodeList() : len_(0), head_(NULL), tail_(NULL) {
    }

    SQLNodeList(SQLLinkedNode *head, SQLLinkedNode *tail, size_t len)
        : len_(len), head_(head), tail_(tail) {
    }

    ~SQLNodeList() {
        tail_ = NULL;
        if (NULL != head_) {
            SQLLinkedNode *pre = head_;
            SQLLinkedNode *p = pre->next_;
            while (NULL != p) {
                delete pre;
                pre = p;
                p = p->next_;
            }
            delete pre;
        }
    }

    const size_t Size() {
        return len_;
    }

    void Print(std::ostream &output) const {
        Print(output, "");
    }

    void Print(std::ostream &output, const std::string &tab) const {
        output << tab << "[\n";
        SQLLinkedNode *p = head_;
        const std::string space = tab + "\t";
        p->node_ptr_->Print(output, space);
        output << "\n";
        p = p->next_;
        while (NULL != p) {
            p->node_ptr_->Print(output, space);
            p = p->next_;
            output << "\n";
        }
        output << tab << "]";
    }

    void PushFront(SQLNode *node_ptr) {
        SQLLinkedNode *linked_node_ptr = new SQLLinkedNode(node_ptr);
        linked_node_ptr->next_ = head_;
        head_ = linked_node_ptr;
        len_ += 1;
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
            len_ = node_list_ptr->len_;
            return;
        }

        tail_->next_ = node_list_ptr->head_;
        tail_ = node_list_ptr->tail_;
        len_ += node_list_ptr->len_;
    }

    friend std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz);
};

/**
 * SQL Node for Select statement
 */
class SelectStmt : public SQLNode {
public:
    int distinct_opt_;
    SQLNode *limit_ptr_;
    SQLNodeList *select_list_ptr_;
    SQLNodeList *tableref_list_ptr_;
    SQLNode *where_clause_ptr_;
    SQLNode *group_clause_ptr_;
    SQLNode *having_clause_ptr_;
    SQLNode *order_clause_ptr_;
    SQLNodeList *window_clause_ptr_;

    SelectStmt() : SQLNode(kSelectStmt, 0, 0), distinct_opt_(0) {
        limit_ptr_ = NULL;
        select_list_ptr_ = NULL;
        tableref_list_ptr_ = NULL;
        where_clause_ptr_ = NULL;
        group_clause_ptr_ = NULL;
        having_clause_ptr_ = NULL;
        order_clause_ptr_ = NULL;
        window_clause_ptr_ = NULL;
    }

    ~SelectStmt() {
        delete limit_ptr_;
        delete tableref_list_ptr_;
        delete select_list_ptr_;
        delete where_clause_ptr_;
        delete group_clause_ptr_;
        delete having_clause_ptr_;
        delete order_clause_ptr_;
        delete window_clause_ptr_;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        const std::string tab = orgTab + "\t";
        const std::string space = tab + "\t";
        output << "\n";
        if (NULL == select_list_ptr_) {
            output << tab << "select_list_ptr_: NULL\n";
        } else {
            output << tab << "select_list: \n";
            select_list_ptr_->Print(output, space);
            output << "\n";
        }

        if (NULL == tableref_list_ptr_) {
            output << tab << "tableref_list_ptr_: NULL\n";
        } else {
            output << tab << "tableref_list_ptr_: \n";
            tableref_list_ptr_->Print(output, space);
            output << "\n";
        }
        if (NULL == where_clause_ptr_) {

            output << tab << "where_clause_: NULL\n";
        } else {
            output << tab << "where_clause_: \n";
            where_clause_ptr_->Print(output, tab);
            output << "\n";
        }

        if (NULL == group_clause_ptr_) {

            output << tab << "group_clause_: NULL\n";
        } else {
            output << tab << "group_clause_: \n";
            group_clause_ptr_->Print(output, tab);
            output << "\n";
        }

        if (NULL == having_clause_ptr_) {

            output << tab << "having_clause_: NULL\n";
        } else {
            output << tab << "having_clause_: " << *(having_clause_ptr_) << "\n";
        }

        if (NULL == order_clause_ptr_) {

            output << tab << "order_clause_: NULL\n";
        } else {
            output << tab << "order_clause_: " << *(order_clause_ptr_) << "\n";
        }

        if (NULL == window_clause_ptr_) {
            output << tab << "window_clause_ptr_: NULL\n";
        } else {
            output << tab << "window_clause_ptr_: \n";
            window_clause_ptr_->Print(output, space);
            output << "\n";
        }
    }
};

class ResTarget : public SQLNode {
    SQLNodeList *indirection_;    /* subscripts, field names, and '*', or NIL */
    SQLNode *val_;            /* the value expression to compute or assign */
    std::string name_;            /* column name or NULL */
public:
    ResTarget() : SQLNode(kResTarget, 0, 0) {}
    ResTarget(uint32_t line_num, uint32_t location) : SQLNode(kResTarget, line_num, location), indirection_(NULL) {
    }
    ~ResTarget() {
        delete val_;
        delete indirection_;
    }

    void setName(const std::string &name) {
        name_ = name;
    }
    void setVal(SQLNode *val) {
        val_ = val;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t" + SPACE_ED;
        const std::string space = orgTab + "\t\t";
        output << tab << "val: \n";
        val_->Print(output, space);
        output << "\n";
        output << tab << "name: \n";
        output << space << name_;
    }
};

class WindowDefNode : public SQLNode {
    std::string window_name_;            /* window's own name */
    SQLNodeList *partition_list_ptr_;    /* PARTITION BY expression list */
    SQLNodeList *order_list_ptr_;    /* ORDER BY (list of SortBy) */
    SQLNode *frame_ptr;    /* expression for starting bound, if any */
public:
    WindowDefNode()
        : SQLNode(kWindowDef, 0, 0), window_name_(""), partition_list_ptr_(NULL),
          order_list_ptr_(NULL), frame_ptr(NULL) {};
    ~WindowDefNode() {
        delete partition_list_ptr_;
        delete order_list_ptr_;
        delete frame_ptr;
    }

    void SetName(const std::string &name) {
        window_name_ = name;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        const std::string tab = orgTab + "\t";
        const std::string space = tab + "\t";
        output << "\n";

        output << tab << "window_name: " << window_name_ << "\n";
        if (NULL == partition_list_ptr_) {
            output << tab << "partition_list_ptr_: NULL\n";
        } else {
            output << tab << "partition_list_ptr_: \n";
            partition_list_ptr_->Print(output, space);
            output << "\n";
        }

        if (NULL == order_list_ptr_) {
            output << tab << "order_list_ptr_: NULL\n";
        } else {
            output << tab << "order_list_ptr_: \n";
            order_list_ptr_->Print(output, space);
            output << "\n";
        }
        if (NULL == frame_ptr) {

            output << tab << "frame_ptr: NULL";
        } else {
            output << tab << "frame_ptr: \n";
            frame_ptr->Print(output, space);
        }
    }

    friend void FillWindowSpection(WindowDefNode *node_ptr,
                                   SQLNodeList *partitions,
                                   SQLNodeList *orders,
                                   SQLNode *frame) {
        node_ptr->partition_list_ptr_ = partitions;
        node_ptr->order_list_ptr_ = orders;
        node_ptr->frame_ptr = frame;
    }
};

class FrameBound : public SQLNode {
    SQLNodeType bound_type_;
    SQLNode *offset_;
public:
    FrameBound() : SQLNode(kFrameBound, 0, 0), bound_type_(kPreceding), offset_(NULL) {};
    FrameBound(SQLNodeType bound_type) :
        SQLNode(kFrameBound, 0, 0), bound_type_(bound_type) {}
    FrameBound(SQLNodeType bound_type, SQLNode *offset) :
        SQLNode(kFrameBound, 0, 0), bound_type_(bound_type), offset_(offset) {}
    ~FrameBound() {
        delete offset_;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        const std::string tab = orgTab + "\t";
        const std::string space = tab + "\t";
        output << "\n";
        output << tab << "bound: " << NameOfSQLNodeType(bound_type_) << "\n";
        if (NULL == offset_) {
            output << space << "UNBOUNDED";
        } else {
            offset_->Print(output, space);
        }
    }
};

class FrameNode : public SQLNode {
    SQLNodeType frame_type_;
    FrameBound *start_;
    FrameBound *end_;
public:
    FrameNode() : SQLNode(kFrames, 0, 0), frame_type_(kFrameRange), start_(NULL), end_(NULL) {};
    FrameNode(SQLNodeType frame_type, FrameBound *start, FrameBound *end) : SQLNode(kFrames, 0, 0),
                                                                            frame_type_(frame_type),
                                                                            start_(start),
                                                                            end_(end) {};

    ~FrameNode() {
        delete start_;
        delete end_;
    }

    void SetFrameType(SQLNodeType frame_type) {
        frame_type_ = frame_type;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        const std::string tab = orgTab + "\t";
        const std::string space = tab + "\t";
        output << "\n";
        output << tab << "frames_type_ : " << NameOfSQLNodeType(frame_type_) << "\n";
        if (NULL == start_) {
            output << tab << "start: UNBOUNDED: \n";
        } else {
            output << tab << "start: \n";
            start_->Print(output, space);
            output << "\n";
        }

        if (NULL == end_) {
            output << tab << "end: UNBOUNDED";
        } else {
            output << tab << "end: \n";
            end_->Print(output, space);
        }
    }

};
class SQLExprNode : public SQLNode {
public:
    SQLExprNode() : SQLNode(kExpr, 0, 0) {}
    SQLExprNode(uint32_t line_num, uint32_t location) : SQLNode(kExpr, line_num, location) {
    }
    ~SQLExprNode() {
    }
};

class ColumnRefNode : public SQLNode {
    std::string column_name_;
    std::string relation_name_;

public:

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

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t" + SPACE_ED;
        output << tab << "column_ref: " << "{relation_name: "
               << relation_name_ << "," << " column_name: " << column_name_ << "}";
    }

};

class OrderByNode : public SQLNode {
    SQLNodeType sort_type_;
    SQLNode *order_by_;
public:
    OrderByNode(SQLNode *order) : SQLNode(kOrderBy, 0, 0), sort_type_(kDesc), order_by_(order) {}
    ~OrderByNode() {
        delete order_by_;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t" + SPACE_ED;
        const std::string space = orgTab + "\t" + "\t" + SPACE_ED;
        output << tab << "sort_type_: " << NameOfSQLNodeType(sort_type_) << "\n";
        if (NULL == order_by_) {
            output << tab << "order_by_: NULL\n";
        } else {
            output << tab << "order_by_: \n";
            order_by_->Print(output, space);
            output << "\n";
        }
    }
};

class TableNode : SQLNode {
    std::string org_table_name_;
    std::string alias_table_name_;
public:
    TableNode(const std::string &name, const std::string &alias)
        : SQLNode(kTable, 0, 0), org_table_name_(name), alias_table_name_(alias) {

    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t" + SPACE_ED;
        output << tab << "table: " << org_table_name_ << ", alias: " << alias_table_name_;
    }
};

class FuncNode : SQLNode {
    std::string function_name_;
    SQLNodeList *args_;
    WindowDefNode *over_;
public:
    FuncNode(const std::string &function_name)
        : SQLNode(kFunc, 0, 0), function_name_(function_name), args_(NULL), over_(NULL) {};
    FuncNode(const std::string &function_name, SQLNodeList *args, WindowDefNode *over)
        : SQLNode(kFunc, 0, 0), function_name_(function_name), args_(args), over_(over) {};

    ~FuncNode() {
        delete args_;
        delete over_;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t" + SPACE_ED;
        const std::string space = orgTab + "\t\t";
        output << tab << "function_name: " << function_name_;
        output << "\n";
        output << tab << "args: \n";
        if (NULL == args_ || 0 == args_->Size()) {
            output << space << "[]";
        } else {
            args_->Print(output, space);
        }
        output << "\n";
        if (NULL == over_) {
            output << tab << "over: NULL\n";
        } else {
            output << tab << "over: \n";
            over_->Print(output, space);
        }
    }
};

class ConstNode : public SQLNode {
    union {
        int vint;        /* machine integer */
        long vlong;        /* machine integer */
        const char *vstr;        /* string */
        float vfloat;
        double vdouble;
    } val_;

public:
    ConstNode() : SQLNode(kNull, 0, 0) {
    }
    ConstNode(int val) : SQLNode(kInt, 0, 0) {
        val_.vint = val;
    }
    ConstNode(long val) : SQLNode(kBigInt, 0, 0) {
        val_.vlong = val;
    }
    ConstNode(float val) : SQLNode(kFloat, 0, 0) {
        val_.vfloat = val;
    }

    ConstNode(double val) : SQLNode(kDouble, 0, 0) {
        val_.vdouble = val;
    }

    ConstNode(const char *val) : SQLNode(kString, 0, 0) {
        val_.vstr = val;
    }
    ConstNode(const std::string &val) : SQLNode(kString, 0, 0) {
        val_.vstr = val.c_str();
    }

    ~ConstNode() {}
    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t";
        output << tab;
        switch (type_) {
            case kInt:output << "value: " << val_.vint;
                break;
            case kBigInt:output << "value: " << val_.vlong;
                break;
            case kString:output << "value: " << val_.vstr;
                break;
            case kFloat:output << "value: " << val_.vfloat;
                break;
            case kDouble:output << "value: " << val_.vdouble;
                break;
        }
    }
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

SQLNode *MakeTableNode(const std::string &name, const std::string &alias);
SQLNode *MakeFuncNode(const std::string &name, SQLNodeList *args, SQLNode *over);
SQLNode *MakeWindowDefNode(const std::string &name);
SQLNode *MakeWindowDefNode(SQLNodeList *partitions, SQLNodeList *orders, SQLNode *frame);
SQLNode *MakeOrderByNode(SQLNode *node_ptr);
SQLNode *MakeFrameNode(SQLNode *start, SQLNode *end);
SQLNode *MakeRangeFrameNode(SQLNode *node_ptr);
SQLNode *MakeRowsFrameNode(SQLNode *node_ptr);

SQLNode *MakeConstNode(int value);
SQLNode *MakeConstNode(long value);
SQLNode *MakeConstNode(float value);
SQLNode *MakeConstNode(double value);
SQLNode *MakeConstNode(const std::string &value);
SQLNode *MakeColumnRefNode(const std::string &column_name, const std::string &relation_name);
SQLNode *MakeResTargetNode(SQLNode *node_ptr, const std::string &name);
SQLNode *MakeNode(const SQLNodeType &type, ...);
SQLNodeList *MakeNodeList(fedb::sql::SQLNode *node_ptr);

} // namespace of sql
} // namespace of fedb
#endif /* !FEDB_SQL_NODE_H_ */
