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
    kColumn,
    kConst,
    kList,

    kInt,
    kBigInt,
    kFloat,
    kDouble,
    kString,
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

class Value : public SQLNode {

};

struct SQLLinkedNode {
    SQLNode *node_;
    SQLLinkedNode *next_;
    SQLLinkedNode(SQLNode *node) {
        node_ = node;
        next_ = NULL;
    }
    /**
     * destruction: tobe optimized
     */
    ~SQLLinkedNode() {
        delete node_;
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
        p->node_->Print(output, space);
        output << "\n";
        p = p->next_;
        while (NULL != p) {
            p->node_->Print(output, space);
            p = p->next_;
            output << "\n";
        }
        output << tab << "]";
    }

    void PushFront(SQLNode *node) {
        SQLLinkedNode *linkedNode = new SQLLinkedNode(node);
        linkedNode->next_ = head_;
        head_ = linkedNode;
        len_ += 1;
        if (NULL == tail_) {
            tail_ = head_;
        }
    }

    void AppendNodeList(SQLNodeList *pList) {
        if (NULL == pList) {
            return;
        }

        if (NULL == tail_) {
            head_ = pList->head_;
            tail_ = head_;
            len_ = pList->len_;
            return;
        }

        tail_->next_ = pList->head_;
        tail_ = pList->tail_;
        len_ += pList->len_;
    }

    friend std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz);
};

/**
 * SQL Node for Select statement
 */
class SelectStmt : public SQLNode {
public:
    int distinct_opt_;
    SQLNode *limit_;
    SQLNodeList *select_list_;
    SQLNodeList *tableref_list_;
    SQLNode *where_clause_;
    SQLNode *group_clause_;
    SQLNode *having_clause_;
    SQLNode *order_clause_;

    SelectStmt() : SQLNode(kSelectStmt, 0, 0), distinct_opt_(0) {
        limit_ = NULL;
        select_list_ = NULL;
        tableref_list_ = NULL;
        where_clause_ = NULL;
        group_clause_ = NULL;
        having_clause_ = NULL;
        order_clause_ = NULL;
    }

    ~SelectStmt() {
        delete limit_;
        delete tableref_list_;
        delete select_list_;
        delete where_clause_;
        delete group_clause_;
        delete having_clause_;
        delete order_clause_;
    }

    void Print(std::ostream &output, const std::string &orgTab) const {
        SQLNode::Print(output, orgTab);
        const std::string tab = orgTab + "\t";
        const std::string space = tab + "\t";
        output << "\n";
        if (NULL == select_list_) {
            output << tab << "select_list_: NULL\n";
        } else {
            output << tab << "select_list: \n";
            select_list_->Print(output, space);
            output << "\n";
        }

        if (NULL == tableref_list_) {
            output << tab << "tableref_list_: NULL\n";
        } else {
            output << tab << "tableref_list_: \n";
            tableref_list_->Print(output, space);
            output << "\n";
        }
        if (NULL == where_clause_) {

            output << tab << "where_clause_: NULL\n";
        } else {
            output << tab << "where_clause_: \n";
            where_clause_->Print(output, tab);
            output << "\n";
        }

        if (NULL == group_clause_) {

            output << tab << "group_clause_: NULL\n";
        } else {
            output << tab << "group_clause_: \n";
            group_clause_->Print(output, tab);
            output << "\n";
        }

        if (NULL == having_clause_) {

            output << tab << "having_clause_: NULL\n";
        } else {
            output << tab << "having_clause_: " << *(having_clause_) << "\n";
        }

        if (NULL == order_clause_) {

            output << tab << "order_clause_: NULL\n";
        } else {
            output << tab << "order_clause_: " << *(order_clause_) << "\n";
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

    void setName(char *name) {
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

class ConstNode : public SQLNode {
    union {
        int vint;        /* machine integer */
        long vlong;        /* machine integer */
        const char * vstr;        /* string */
        float vfloat;
        double vdouble;
    } val_;

public:
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
SQLNode *MakeConstNode(int value);
SQLNode *MakeConstNode(long value);
SQLNode *MakeConstNode(float value);
SQLNode *MakeConstNode(double value);
SQLNode *MakeConstNode(const std::string &value);
SQLNode *MakeColumnRefNode(const std::string &column_name, const std::string &relation_name);
SQLNode *MakeNode(const SQLNodeType &type, ...);
SQLNodeList *MakeNodeList(fedb::sql::SQLNode *node);

} // namespace of sql
} // namespace of fedb
#endif /* !FEDB_SQL_NODE_H_ */
