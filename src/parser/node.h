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
    // SQL Node Type
        kSelectStmt = 0,
    kExpr,
    kConst,
    kTable,
    kColumn,
    kResTarget,

    kList,
    kName,
    // Value Type
        kInt,
    kBigInt,
    kFloat,
    kDouble,
    kString,

    kUnknow
};

typedef struct Value {
    SQLNodeType type;
    union ValUnion {
        int vint;        /* machine integer */
        long vlong;        /* machine integer */
        char *vstr;        /* string */
        float vflaot;
        double vdouble;
    } val;
};

// Global methods
std::string NameOfSQLNodeType(const SQLNodeType &type);

struct SQLNode {
    SQLNodeType type_;
    uint32_t line_num_;
    uint32_t location_;

public:
    SQLNode(const SQLNodeType &type,
            uint32_t line_num,
            uint32_t location) {
        type_ = type;
        line_num_ = line_num;
        location_ = location;
    }

    virtual ~SQLNode() {}

    virtual void AddChild(SQLNode *node) {}

    virtual void Print(std::ostream &output) const {
        Print(output, SPACE_ST);
    }

    virtual void Print(std::ostream &output, const std::string tab) const {
        output << tab << SPACE_ED << NameOfSQLNodeType(type_);
    }

    friend std::ostream &operator<<(std::ostream &output, const SQLNode &thiz);

    SQLNodeType &GetType() {
        return type_;
    }

    uint32_t GetLineNum() {
        return line_num_;
    }

    uint32_t GetLocation() {
        return location_;
    }
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
        delete (node_);
    }
};

class SQLNodeList : public SQLNode {
public:

    SQLLinkedNode *head_;

    SQLNodeList() : SQLNode(kList, 0, 0), len_(0), head_(NULL) {
    }

    SQLNodeList(SQLLinkedNode *head, size_t len) : SQLNode(kList, 0, 0), len_(len), head_(head) {

    }

    ~SQLNodeList() {
        if (NULL != head_) {
            SQLLinkedNode *pre = head_;
            SQLLinkedNode *p = pre->next_;
            while (NULL != p) {
                delete (pre);
                pre = p;
                p = p->next_;
            }
            delete (pre);
        }
    }

    void PushFront(SQLNode *node) {
        SQLLinkedNode *linkedNode = new SQLLinkedNode(node);
        linkedNode->next_ = head_;
        head_ = linkedNode;
        len_ += 1;
    }

    const size_t Size() {
        return len_;
    }

    void SetSize(size_t len) {
        len_ = len;
    }

    void Print(std::ostream &output, const std::string tab) const {
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

private:
    size_t len_;
};

typedef std::list<SQLNode *> SQLNodeLinkedList;

typedef std::list<SQLNode *> PNodeList;

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

    SelectStmt() : SQLNode(kSelectStmt, 0, 0) {}
    SelectStmt(uint32_t line_num, uint32_t location) : SQLNode(kSelectStmt, line_num, location) {
        limit_ = NULL;
        select_list_ = NULL;
        tableref_list_ = NULL;
        where_clause_ = NULL;
        group_clause_ = NULL;
        having_clause_ = NULL;
        order_clause_ = NULL;
    }
    ~SelectStmt() {
        delete (limit_);
        delete (tableref_list_);
        delete (select_list_);
        delete (where_clause_);
        delete (group_clause_);
        delete (having_clause_);
        delete (order_clause_);
    }
    void AddChild(SQLNode *node);

    // FIXME: this overloading does not work
    void Print(std::ostream &output, const std::string orgTab) const {
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
    PNodeList indirection_;    /* subscripts, field names, and '*', or NIL */
    SQLNode *val_;            /* the value expression to compute or assign */
    int location_;        /* token location, or -1 if unknown */
    std::string name_;            /* column name or NULL */
public:
    ResTarget() : SQLNode(kResTarget, 0, 0) {}
    ResTarget(uint32_t line_num, uint32_t location) : SQLNode(kResTarget, line_num, location) {
    }
    ~ResTarget() {

    }

    void setName(char *name) {
        name_ = name;
    }
    void setVal(SQLNode *val) {
        val_ = val;
    }

    void setLocation(int location) {
        location_ = location;
    }

    void Print(std::ostream &output, const std::string orgTab) const {
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

public:
    ColumnRefNode(const std::string column_name)
        : SQLNode(kColumn, 0, 0), column_name_(column_name), relation_name_("") {
    }

    ColumnRefNode(const std::string column_name, std::string relation_name)
        : SQLNode(kColumn, 0, 0), column_name_(column_name), relation_name_(relation_name) {
    }

    void Print(std::ostream &output, const std::string orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t" + SPACE_ED;
        output << tab << "column_ref: " << "{relation_name: "
               << relation_name_ << "," << " column_name: " << column_name_ << "}";
    }

    std::string GetRelationName() const {
        return relation_name_;
    }

    std::string GetColumnName() const {
        return column_name_;
    }

private:
    std::string column_name_;
    std::string relation_name_;
};
class NameNode : SQLNode {
public:
    NameNode(const std::string name) : SQLNode(kName, 0, 0), name_(name) {

    }
private:
    std::string name_;
};

class TableNode : SQLNode {
public:
    TableNode(const std::string name, const std::string alias)
        : SQLNode(kTable, 0, 0), org_table_name_(name), alias_table_name_(alias) {

    }

    void Print(std::ostream &output, const std::string orgTab) const {
        SQLNode::Print(output, orgTab);
        output << "\n";
        const std::string tab = orgTab + "\t" + SPACE_ED;
        output << tab << "table: " << org_table_name_ << ", alias: " << alias_table_name_;
    }
private:
    std::string org_table_name_;
    std::string alias_table_name_;
};

class ConstNode : public SQLNode {
    Value value_;
public:
    ConstNode() : SQLNode(kConst, 0, 0) {}
    ConstNode(uint32_t line_num, uint32_t location) : SQLNode(kConst, line_num, location) {
    }

    void setValue(int value) {
        value_.type = kInt;
        value_.val.vint = value;
    }

    void setValue(long value) {
        value_.type = kBigInt;
        value_.val.vlong = value;
    }

    void setValue(double value) {
        value_.type = kDouble;
        value_.val.vdouble = value;
    }

    void setValue(float value) {
        value_.type = kFloat;
        value_.val.vflaot = value;
    }

    void setValue(char *value) {
        value_.type = kString;
        value_.val.vstr = value;
    }

    void Print(std::ostream &output, const std::string orgTab) const {
        SQLNode::Print(output, orgTab);
        const std::string tab = orgTab + "\t" + orgTab;
        output << " {valtype: " << NameOfSQLNodeType(value_.type) << ", ";
        switch (value_.type) {
            case kInt:output << "value: " << value_.val.vint;
                break;
            case kBigInt:output << "value: " << value_.val.vlong;
                break;
            case kString:output << "value: " << value_.val.vstr;
                break;
            case kFloat:output << "value: " << value_.val.vflaot;
                break;
            case kDouble:output << "value: " << value_.val.vdouble;
                break;
        }
        output << "}";
    }
};

SQLNode *MakeTableNode(const std::string name, const std::string alias);
SQLNode *MakeColumnRefNode(const std::string column_name, const std::string relation_name);
SQLNode *MakeNode(const SQLNodeType &type, ...);
SQLNodeList *MakeNodeList(fedb::sql::SQLNode *node);
SQLNodeList *AppendNodeList(SQLNodeList *list, SQLNode *node);

} // namespace of ast
} // namespace of fedb
#endif /* !FEDB_SQL_NODE_H_ */
