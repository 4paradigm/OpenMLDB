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
#include "node_enum.h"
#include <glog/logging.h>
namespace fesql {
namespace node {

// Global methods
std::string NameOfSQLNodeType(const SQLNodeType &type);

inline const std::string DataTypeName(const DataType &type) {
    switch (type) {
        case kTypeBool:return "bool";
        case kTypeInt16:return "int16";
        case kTypeInt32:return "int32";
        case kTypeInt64:return "int64";
        case kTypeFloat:return "float";
        case kTypeDouble:return "double";
        case kTypeString:return "string";
        case kTypeTimestamp:return "timestamp";
        case kTypeNull:return "null";
        default: return "unknownType";
    }
}

inline const std::string FnNodeName(const SQLNodeType &type) {
    switch (type) {
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

class SQLNode {
public:
    SQLNode(const SQLNodeType &type, uint32_t line_num, uint32_t location)
        : type_(type), line_num_(line_num), location_(location) {
    }

    virtual ~SQLNode() {
    }

    virtual void Print(std::ostream &output, const std::string &tab) const;

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

private:
    SQLNodeType type_;
    uint32_t line_num_;
    uint32_t location_;
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
    }
};

typedef std::vector<SQLNode *> NodePointVector;

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

    const size_t GetSize() {
        return size_;
    }

    SQLLinkedNode *GetHead() {
        return head_;
    }

    void Print(std::ostream &output) const {
        Print(output, "");
    }

    void Print(std::ostream &output, const std::string &tab) const;
    void PushFront(SQLLinkedNode *linked_node_ptr);
    void AppendNodeList(SQLNodeList *node_list_ptr);

    friend std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz);

private:
    size_t size_;
    SQLLinkedNode *head_;
    SQLLinkedNode *tail_;
};

class NameNode : public SQLNode {
public:
    NameNode() : SQLNode(kName, 0, 0), name_("") {};
    NameNode(const std::string &name) : SQLNode(kName, 0, 0), name_(name) {};
    ~NameNode() {};

    std::string GetName() const {
        return name_;
    }
private:
    std::string name_;
};
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

class ConstNode : public FnNode {

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

    ConstNode(long val, DataType time_type) : FnNode(kPrimary), date_type_(time_type) {
        val_.vlong = val;
    }

    ~ConstNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;

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

class AllNode : public SQLNode {
public:
    AllNode() : SQLNode(kAll, 0, 0), relation_name_("") {
    }

    AllNode(const std::string &relation_name) : SQLNode(kAll, 0, 0), relation_name_(relation_name) {
    }

    std::string GetRelationName() const {
        return relation_name_;
    }

    void SetRelationName(const std::string &relation_name) {
        relation_name_ = relation_name;
    }

private:
    std::string relation_name_;
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

class TableNode : SQLNode {
public:
    TableNode() : SQLNode(kTable, 0, 0), org_table_name_(""), alias_table_name_("") {};

    TableNode(const std::string &name, const std::string &alias)
        : SQLNode(kTable, 0, 0), org_table_name_(name), alias_table_name_(alias) {}

    std::string GetOrgTableName() const {
        return org_table_name_;
    }

    std::string GetAliasTableName() const {
        return alias_table_name_;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

private:
    std::string org_table_name_;
    std::string alias_table_name_;
};

class ColumnRefNode : public SQLNode {

public:
    ColumnRefNode() : SQLNode(kColumnRef, 0, 0), column_name_(""), relation_name_("") {}

    ColumnRefNode(const std::string &column_name, const std::string &relation_name)
        : SQLNode(kColumnRef, 0, 0), column_name_(column_name), relation_name_(relation_name) {}

    std::string GetRelationName() const {
        return relation_name_;
    }

    void SetRelationName(const std::string &relation_name) {
        relation_name_ = relation_name;
    }

    std::string GetColumnName() const {
        return column_name_;
    }

    void SetColumnName(const std::string &column_name) {
        column_name_ = column_name;
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

class FrameBound : public SQLNode {
public:
    FrameBound() : SQLNode(kFrameBound, 0, 0), bound_type_(kPreceding), offset_(nullptr) {};

    FrameBound(SQLNodeType bound_type) :
        SQLNode(kFrameBound, 0, 0), bound_type_(bound_type), offset_(nullptr) {}

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
    FrameNode() : SQLNode(kFrames, 0, 0), frame_type_(kFrameRange), start_(nullptr), end_(nullptr) {};

    FrameNode(SQLNodeType frame_type, SQLNode *start, SQLNode *end) :
        SQLNode(kFrames, 0, 0), frame_type_(frame_type), start_(start), end_(end) {};

    ~FrameNode() {
    }

    SQLNodeType GetFrameType() const {
        return frame_type_;
    }

    void SetFrameType(SQLNodeType frame_type) {
        frame_type_ = frame_type;
    }

    SQLNode *GetStart() const {
        return start_;
    }

    SQLNode *GetEnd() const {
        return end_;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

private:
    SQLNodeType frame_type_;
    SQLNode *start_;
    SQLNode *end_;
};

class WindowDefNode : public SQLNode {
public:
    WindowDefNode()
        : SQLNode(kWindowDef, 0, 0), window_name_(""), frame_ptr_(NULL) {};

    ~WindowDefNode() {
    }

    std::string GetName() const {
        return window_name_;
    }

    void SetName(const std::string &name) {
        window_name_ = name;
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

    void SetFrame(SQLNode *frame) {
        frame_ptr_ = frame;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

private:
    std::string window_name_;            /* window's own name */
    SQLNode *frame_ptr_;    /* expression for starting bound, if any */
    NodePointVector partition_list_ptr_;    /* PARTITION BY expression list */
    NodePointVector order_list_ptr_;    /* ORDER BY (list of SortBy) */
};

class FuncNode : FnNode {

public:
    FuncNode() : FnNode(kFunc), is_agg_(true), function_name_(""), over_(nullptr) {};
    FuncNode(const std::string &function_name)
        : FnNode(kFunc), is_agg_(true), function_name_(function_name), over_(nullptr) {};

    ~FuncNode() {
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

    std::string GetFunctionName() const {
        return function_name_;
    }

    WindowDefNode *GetOver() const {
        return over_;
    }

    void SetOver(WindowDefNode *over) {
        over_ = over;
    }

    bool GetIsAgg() const {
        return is_agg_;
    }

    void SetAgg(bool is_agg) {
        is_agg_ = is_agg;
    }
    NodePointVector &GetArgs() {
        return args_;
    }

private:
    bool is_agg_;
    std::string function_name_;
    WindowDefNode *over_;
    NodePointVector args_;

};

class SQLExprNode : public SQLNode {
public:
    SQLExprNode() : SQLNode(kExpr, 0, 0) {}

    SQLExprNode(uint32_t line_num, uint32_t location) : SQLNode(kExpr, line_num, location) {}

    ~SQLExprNode() {}
};

class ResTarget : public SQLNode {
public:
    ResTarget() : SQLNode(kResTarget, 0, 0), name_(""), val_(nullptr) {}

    ResTarget(const std::string &name, SQLNode *val) : SQLNode(kResTarget, 0, 0), name_(name), val_(val) {}

    ~ResTarget() {
    }

    std::string GetName() const {
        return name_;
    }

    SQLNode *GetVal() const {
        return val_;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;

private:
    std::string name_;            /* column name or NULL */
    SQLNode *val_;            /* the value expression to compute or assign */
    NodePointVector indirection_;    /* subscripts, field names, and '*', or NIL */
};

class SelectStmt : public SQLNode {
public:
    SelectStmt() :
        SQLNode(kSelectStmt, 0, 0),
        distinct_opt_(0),
        where_clause_ptr_(nullptr),
        group_clause_ptr_(nullptr),
        having_clause_ptr_(
            nullptr),
        order_clause_ptr_(nullptr),
        limit_ptr_(nullptr) {
    }

    ~SelectStmt() {
    }

    // Getter and Setter
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

    int GetDistinctOpt() const {
        return distinct_opt_;
    }
    // Print
    void Print(std::ostream &output, const std::string &org_tab) const;

private:
    int distinct_opt_;
    SQLNode *where_clause_ptr_;
    SQLNode *group_clause_ptr_;
    SQLNode *having_clause_ptr_;
    SQLNode *order_clause_ptr_;
    SQLNode *limit_ptr_;
    NodePointVector select_list_ptr_;
    NodePointVector tableref_list_ptr_;
    NodePointVector window_list_ptr_;
};

class FnTypeNode : public FnNode {
public:
    FnTypeNode() : FnNode(kType) {};
public:
    DataType data_type_;
};

class FnParaNode : public FnNode {
public:
    FnParaNode() : FnNode(kFnPara) {};
    FnParaNode(const std::string &name, const DataType &para_type)
        : FnNode(kFnPara), name_(name), para_type_(para_type) {};
    std::string GetName() const {
        return name_;
    }

    DataType GetParaType() {
        return para_type_;
    }
private:
    std::string name_;
    DataType para_type_;
};

class FnNodeFnDef : public FnNode {
public:
    FnNodeFnDef() : FnNode(kFnDef) {};
    FnNodeFnDef(const std::string &name, const DataType ret_type) : FnNode(kFnDef), name_(name), ret_type_(ret_type) {};
    std::string GetName() const {
        return name_;
    }

    DataType GetRetType() const {
        return ret_type_;
    }

private:
    std::string name_;
    DataType ret_type_;
};

class FnBinaryExpr : public FnNode {
public:
    FnBinaryExpr() : FnNode(kFnExprBinary) {};
    FnBinaryExpr(FnOperator op) : FnNode(kFnExprBinary), op_(op) {};
    FnOperator GetOp() const {
        return op_;
    }
private:
    FnOperator op_;
};

class FnUnaryExpr : public FnNode {
public:
    FnUnaryExpr() : FnNode(kFnExprUnary) {};
    FnUnaryExpr(FnOperator op) : FnNode(kFnExprUnary), op_(op) {};
    FnOperator GetOp() const {
        return op_;
    }
private:
    FnOperator op_;
};

class FnIdNode : public FnNode {
public:
    FnIdNode() : FnNode(kFnId) {};
    FnIdNode(const std::string &name) : FnNode(kFnId), name_(name) {};
    std::string GetName() const {
        return name_;
    }
private:
    std::string name_;
};

class FnAssignNode : public FnNode {
public:
    FnAssignNode(const std::string &name) : FnNode(kFnAssignStmt), name_(name) {};
    std::string GetName() const {
        return name_;
    }
private:
    std::string name_;
};

class ColumnDefNode : public SQLNode {
public:
    ColumnDefNode() : SQLNode(kColumnDesc, 0, 0), column_name_(""), column_type_(kTypeNull) {}
    ColumnDefNode(const std::string &name, const DataType &data_type, bool op_not_null) : SQLNode(kColumnDesc, 0, 0),
                                                                                          column_name_(name),
                                                                                          column_type_(data_type),
                                                                                          op_not_null_(op_not_null) {};
    ~ColumnDefNode() {};

    std::string GetColumnName() const {
        return column_name_;
    }

    DataType GetColumnType() const {
        return column_type_;
    }

    bool GetIsNotNull() const {
        return op_not_null_;
    }
    void Print(std::ostream &output, const std::string &org_tab) const;

private:
    std::string column_name_;
    DataType column_type_;
    bool op_not_null_;

};

class CreateStmt : public SQLNode {
public:
    CreateStmt() : SQLNode(kCreateStmt, 0, 0), table_name_(""), op_if_not_exist_(false) {
    }

    CreateStmt(const std::string &table_name, bool op_if_not_exist) :
        SQLNode(kCreateStmt, 0, 0), table_name_(table_name), op_if_not_exist_(op_if_not_exist) {
    }

    ~CreateStmt() {};

    NodePointVector &GetColumnDefList() {
        return column_desc_list_;
    }

    std::string GetTableName() const {
        return table_name_;
    }

    bool GetOpIfNotExist() const {
        return op_if_not_exist_;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;
private:
    std::string table_name_;
    bool op_if_not_exist_;
    NodePointVector column_desc_list_;

};

class IndexKeyNode : public SQLNode {
public:
    IndexKeyNode() : SQLNode(kIndexKey, 0, 0) {}
    IndexKeyNode(const std::string &key) : SQLNode(kIndexKey, 0, 0) { key_.push_back(key); }
    ~IndexKeyNode() {};
    void AddKey(const std::string &key) {
        key_.push_back(key);
    }
    std::vector<std::string> &GetKey() {
        return key_;
    }
private:
    std::vector<std::string> key_;
};

class IndexVersionNode : public SQLNode {
public:
    IndexVersionNode() : SQLNode(kIndexVersion, 0, 0) {};
    IndexVersionNode(const std::string &column_name) : SQLNode(kIndexVersion, 0, 0),
                                                       column_name_(column_name),
                                                       count_(1) {};
    IndexVersionNode(const std::string &column_name, int count) : SQLNode(kIndexVersion, 0, 0),
                                                                  column_name_(column_name),
                                                                  count_(count) {};

    std::string &GetColumnName() {
        return column_name_;
    }

    int GetCount() const {
        return count_;
    }
private:
    std::string column_name_;
    int count_;
};

class IndexTsNode : public SQLNode {
public:
    IndexTsNode() : SQLNode(kIndexTs, 0, 0) {};
    IndexTsNode(const std::string &column_name) : SQLNode(kIndexTs, 0, 0), column_name_(column_name) {};

    std::string &GetColumnName() {
        return column_name_;
    }

private:
    std::string column_name_;
};

class ColumnIndexNode : public SQLNode {
public:
    ColumnIndexNode() : SQLNode(kColumnIndex, 0, 0), ts_(""), version_(""), ttl_(-1) {};

    std::vector<std::string> &GetKey() {
        return key_;
    }
    void SetKey(std::vector<std::string> &key) {
        key_ = key;
    }

    std::string GetTs() const {
        return ts_;
    }

    void SetTs(std::string &ts) {
        ts_ = ts;
    }

    std::string GetVersion() const {
        return version_;
    }

    void SetVersion(std::string &version) {
        version_ = version;
    }

    int GetVersionCount() const {
        return version_count_;
    }

    void SetVersionCount(int count) {
        version_count_ = count;
    }

    int64_t GetTTL() const {
        return ttl_;
    }
    void SetTTL(ConstNode *ttl) {
        if (nullptr == ttl) {
            ttl_ = -1l;
        } else {
            switch (ttl->GetDataType()) {
                case kTypeInt32:ttl_ = ttl->GetInt();
                    break;
                case kTypeInt64:ttl_ = ttl->GetLong();
                    break;
                case kTypeDay:ttl_ = ttl->GetLong() * 86400000L;
                    break;
                case kTypeHour:ttl_ = ttl->GetLong() * 3600000L;
                    break;
                case kTypeMinute:ttl_ = ttl->GetLong() * 60000;
                    break;
                case kTypeSecond:ttl_ = ttl->GetLong() * 1000;
                    break;
            }
        }
    }

    void Print(std::ostream &output, const std::string &org_tab) const;
private:
    std::vector<std::string> key_;
    std::string ts_;
    std::string version_;
    int version_count_;
    int64_t ttl_;

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
