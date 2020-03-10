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

#ifndef SRC_NODE_SQL_NODE_H_
#define SRC_NODE_SQL_NODE_H_

#include <glog/logging.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "node/node_enum.h"

namespace fesql {
namespace node {

// Global methods
std::string NameOfSQLNodeType(const SQLNodeType &type);

inline const std::string CmdTypeName(const CmdType &type) {
    switch (type) {
        case kCmdShowDatabases:
            return "show databases";
        case kCmdShowTables:
            return "show tables";
        case kCmdUseDatabase:
            return "use database";
        case kCmdCreateDatabase:
            return "create database";
        case kCmdSource:
            return "create table";
        case kCmdCreateGroup:
            return "create group";
        case kCmdDescTable:
            return "desc table";
        case kCmdDropTable:
            return "drop table";
        case kCmdExit:
            return "exit";
        default:
            return "unknown cmd type";
    }
}

inline const std::string JoinTypeName(const JoinType &type) {
    switch (type) {
        case kJoinTypeFull:
            return "FullJoin";
        case kJoinTypeLeft:
            return "LeftJoin";
        case kJoinTypeRight:
            return "RightJoin";
        case kJoinTypeInner:
            return "InnerJoin";
        default: {
            return "Unknow";
        }
    }
}
inline const std::string ExprOpTypeName(const FnOperator &op) {
    switch (op) {
        case kFnOpAdd:
            return "+";
        case kFnOpMinus:
            return "-";
        case kFnOpMulti:
            return "*";
        case kFnOpDiv:
            return "DIV";
        case kFnOpFDiv:
            return "/";
        case kFnOpMod:
            return "%";
        case kFnOpAnd:
            return "AND";
        case kFnOpOr:
            return "OR";
        case kFnOpNot:
            return "NOT";
        case kFnOpEq:
            return "=";
        case kFnOpNeq:
            return "!=";
        case kFnOpGt:
            return ">";
        case kFnOpGe:
            return ">=";
        case kFnOpLt:
            return "<";
        case kFnOpLe:
            return "<=";
        case kFnOpAt:
            return "[]";
        case kFnOpDot:
            return ".";
        case kFnOpLike:
            return "LIKE";
        case kFnOpBracket:
            return "()";
        case kFnOpNone:
            return "NONE";
        default:
            return "UNKNOWN";
    }
}

inline const std::string ExprTypeName(const ExprType &type) {
    switch (type) {
        case kExprPrimary:
            return "primary";
        case kExprId:
            return "id";
        case kExprBinary:
            return "binary";
        case kExprUnary:
            return "unary";
        case kExprCall:
            return "function";
        case kExprCase:
            return "case";
        case kExprIn:
            return "in";
        case kExprColumnRef:
            return "column ref";
        case kExprCast:
            return "cast";
        case kExprAll:
            return "all";
        case kExprStruct:
            return "struct";
        case kExprSubQuery:
            return "subquery";
        case kExprUnknow:
            return "unknow";
        default:
            return "unknown expr type";
    }
}

inline const std::string DataTypeName(const DataType &type) {
    switch (type) {
        case fesql::node::kBool:
            return "bool";
        case fesql::node::kInt16:
            return "int16";
        case fesql::node::kInt32:
            return "int32";
        case fesql::node::kInt64:
            return "int64";
        case fesql::node::kFloat:
            return "float";
        case fesql::node::kDouble:
            return "double";
        case fesql::node::kVarchar:
            return "string";
        case fesql::node::kTimestamp:
            return "timestamp";
        case fesql::node::kList:
            return "list";
        case fesql::node::kMap:
            return "map";
        case fesql::node::kIterator:
            return "iterator";
        case fesql::node::kRow:
            return "row";
        case fesql::node::kNull:
            return "null";
        case fesql::node::kVoid:
            return "void";
        default:
            return "unknown";
    }
}

inline const std::string FnNodeName(const SQLNodeType &type) {
    switch (type) {
        case kFnDef:
            return "def";
        case kFnValue:
            return "value";
        case kFnAssignStmt:
            return "=";
        case kFnReturnStmt:
            return "return";
        case kFnPara:
            return "para";
        case kFnParaList:
            return "plist";
        case kFnList:
            return "funlist";
        default:
            return "unknowFn";
    }
}

class SQLNode {
 public:
    SQLNode(const SQLNodeType &type, uint32_t line_num, uint32_t location)
        : type_(type), line_num_(line_num), location_(location) {}

    virtual ~SQLNode() {}

    virtual void Print(std::ostream &output, const std::string &tab) const;

    const SQLNodeType GetType() const { return type_; }

    uint32_t GetLineNum() const { return line_num_; }

    uint32_t GetLocation() const { return location_; }

    friend std::ostream &operator<<(std::ostream &output, const SQLNode &thiz);

 private:
    SQLNodeType type_;
    uint32_t line_num_;
    uint32_t location_;
};

typedef std::vector<SQLNode *> NodePointVector;

class SQLNodeList {
 public:
    SQLNodeList() {}
    ~SQLNodeList() {}
    void PushBack(SQLNode *node_ptr) { list_.push_back(node_ptr); }
    const bool IsEmpty() const { return list_.empty(); }
    const int GetSize() const { return list_.size(); }
    const std::vector<SQLNode *> &GetList() const { return list_; }
    void Print(std::ostream &output, const std::string &tab) const;

 private:
    std::vector<SQLNode *> list_;
};

class TypeNode : public SQLNode {
 public:
    TypeNode() : SQLNode(node::kType, 0, 0), base_(fesql::node::kNull) {}
    explicit TypeNode(fesql::node::DataType base)
        : SQLNode(node::kType, 0, 0), base_(base), generics_({}) {}
    explicit TypeNode(fesql::node::DataType base, DataType v1)
        : SQLNode(node::kType, 0, 0), base_(base), generics_({v1}) {}
    explicit TypeNode(fesql::node::DataType base, fesql::node::DataType v1,
                      fesql::node::DataType v2)
        : SQLNode(node::kType, 0, 0), base_(base), generics_({v1, v2}) {}
    ~TypeNode() {}
    const std::string GetName() const {
        std::string type_name = DataTypeName(base_);
        if (!generics_.empty()) {
            for (DataType type : generics_) {
                type_name.append("_");
                type_name.append(DataTypeName(type));
            }
        }
        return type_name;
    }
    fesql::node::DataType base_;
    std::vector<fesql::node::DataType> generics_;
    void Print(std::ostream &output, const std::string &org_tab) const override;
};
class ExprNode : public SQLNode {
 public:
    explicit ExprNode(ExprType expr_type)
        : SQLNode(kExpr, 0, 0), expr_type_(expr_type) {}
    ~ExprNode() {}
    void AddChild(ExprNode *expr) { children.push_back(expr); }
    const ExprType GetExprType() const { return expr_type_; }
    void PushBack(ExprNode *node_ptr) { children.push_back(node_ptr); }

    std::vector<ExprNode *> children;
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual const std::string GetExprString() const;

 private:
    ExprType expr_type_;
};
class ExprListNode : public ExprNode {
 public:
    ExprListNode() : ExprNode(kExprList) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const bool IsEmpty() const { return children.empty(); }
    const std::string GetExprString() const;
};
class FnNode : public SQLNode {
 public:
    FnNode() : SQLNode(kFn, 0, 0), indent(0) {}
    explicit FnNode(SQLNodeType type) : SQLNode(type, 0, 0), indent(0) {}

 public:
    int32_t indent;
};
class FnNodeList : public FnNode {
 public:
    FnNodeList() : FnNode(kFnList) {}

    const std::vector<FnNode *> &GetChildren() const { return children; }

    void AddChild(FnNode *child) { children.push_back(child); }
    void Print(std::ostream &output, const std::string &org_tab) const;
    std::vector<FnNode *> children;
};

class SelectStmt : public SQLNode {
 public:
    SelectStmt(bool is_distinct, SQLNodeList *select_list, SQLNodeList *tableref_list,
               ExprNode *where_expr, ExprListNode *group_expr_list,
               ExprNode *having_expr, ExprListNode *order_expr_list,
               SQLNodeList *window_list, SQLNode *limit_ptr)
        : SQLNode(kSelectStmt, 0, 0),
          distinct_opt_(is_distinct),
          where_clause_ptr_(where_expr),
          group_clause_ptr_(group_expr_list),
          having_clause_ptr_(having_expr),
          order_clause_ptr_(order_expr_list),
          limit_ptr_(limit_ptr),
          select_list_(select_list),
          tableref_list_(tableref_list),
          window_list_(window_list) {}

    ~SelectStmt() {}

    // Getter and Setter
    const SQLNodeList *GetSelectList() const { return select_list_; }

    SQLNodeList *GetSelectList() { return select_list_; }

    const SQLNode *GetLimit() const { return limit_ptr_; }

    const SQLNodeList *GetTableRefList() const { return tableref_list_; }

    SQLNodeList *GetTableRefList() { return tableref_list_; }

    const SQLNodeList *GetWindowList() const { return window_list_; }

    SQLNodeList *GetWindowList() { return window_list_; }

    void SetLimit(SQLNode *limit) { limit_ptr_ = limit; }

    int GetDistinctOpt() const { return distinct_opt_; }
    // Print
    void Print(std::ostream &output, const std::string &org_tab) const;

    const bool distinct_opt_;
    const ExprNode *where_clause_ptr_;
    const ExprListNode *group_clause_ptr_;
    const ExprNode *having_clause_ptr_;
    const SQLNode *order_clause_ptr_;
    const SQLNode *limit_ptr_;

 private:
    SQLNodeList *select_list_;
    SQLNodeList *tableref_list_;
    SQLNodeList *window_list_;
    void PrintSQLNodeList(std::ostream &output, const std::string &tab,
                          SQLNodeList *list, const std::string &name,
                          bool last_item) const;
};

class UnionStmt : public SQLNode {
 public:
    UnionStmt(const SQLNode *left, const SQLNode *right, bool is_all)
        : SQLNode(kUnionStmt, 0, 0),
          left_(left),
          right_(right),
          is_all_(is_all) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const SQLNode *left_;
    const SQLNode *right_;
    const bool is_all_;
};

class NameNode : public SQLNode {
 public:
    NameNode() : SQLNode(kName, 0, 0), name_("") {}
    explicit NameNode(const std::string &name)
        : SQLNode(kName, 0, 0), name_(name) {}
    ~NameNode() {}

    std::string GetName() const { return name_; }

 private:
    std::string name_;
};

class LimitNode : public SQLNode {
 public:
    LimitNode() : SQLNode(kLimit, 0, 0), limit_cnt_(0) {}

    explicit LimitNode(int limit_cnt)
        : SQLNode(kLimit, 0, 0), limit_cnt_(limit_cnt) {}

    int GetLimitCount() const { return limit_cnt_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    int limit_cnt_;
};
class TableRefNode : public SQLNode {
 public:
    explicit TableRefNode(SQLNodeType sql_type, std::string alias_table_name)
        : SQLNode(sql_type, 0, 0), alias_table_name_(alias_table_name) {}
    const std::string alias_table_name_;
};
class TableNode : public TableRefNode {
 public:
    TableNode() : TableRefNode(kTable, ""), org_table_name_("") {}

    TableNode(const std::string &name, const std::string &alias)
        : TableRefNode(kTable, alias), org_table_name_(name) {}

    const std::string& GetOrgTableName() const { return org_table_name_; }

    const std::string& GetAliasTableName() const { return alias_table_name_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string org_table_name_;
};



class JoinNode : public TableRefNode {
 public:
    JoinNode(const TableRefNode *left, const TableRefNode *right,
             const JoinType join_type, const ExprNode *condition,
             const std::string &alias)
        : TableRefNode(kJoin, alias),
          left_(left),
          right_(right),
          join_type_(join_type),
          condition_(condition) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const TableRefNode *left_;
    const TableRefNode *right_;
    const JoinType join_type_;
    const ExprNode *condition_;
};

class OrderByNode : public SQLNode {
 public:
    explicit OrderByNode(SQLNode *order)
        : SQLNode(kOrderBy, 0, 0), sort_type_(kDesc), order_by_(order) {}
    ~OrderByNode() {}

    void Print(std::ostream &output, const std::string &org_tab) const;

    SQLNodeType GetSortType() const { return sort_type_; }
    SQLNode *GetOrderBy() const { return order_by_; }
    void SetOrderBy(SQLNode *order_by) { order_by_ = order_by; }

 private:
    SQLNodeType sort_type_;
    SQLNode *order_by_;
};

class FrameBound : public SQLNode {
 public:
    FrameBound()
        : SQLNode(kFrameBound, 0, 0),
          bound_type_(kPreceding),
          offset_(nullptr) {}

    explicit FrameBound(SQLNodeType bound_type)
        : SQLNode(kFrameBound, 0, 0),
          bound_type_(bound_type),
          offset_(nullptr) {}

    FrameBound(SQLNodeType bound_type, ExprNode *offset)
        : SQLNode(kFrameBound, 0, 0),
          bound_type_(bound_type),
          offset_(offset) {}

    ~FrameBound() {}

    void Print(std::ostream &output, const std::string &org_tab) const {
        SQLNode::Print(output, org_tab);
        const std::string tab = org_tab + INDENT + SPACE_ED;
        std::string space = org_tab + INDENT + INDENT;
        output << "\n";
        output << tab << SPACE_ST << "bound: " << NameOfSQLNodeType(bound_type_)
               << "\n";
        if (NULL == offset_) {
            output << space << "UNBOUNDED";
        } else {
            offset_->Print(output, space);
        }
    }

    SQLNodeType GetBoundType() const { return bound_type_; }

    ExprNode *GetOffset() const { return offset_; }

 private:
    SQLNodeType bound_type_;
    ExprNode *offset_;
};
class FrameNode : public SQLNode {
 public:
    FrameNode()
        : SQLNode(kFrames, 0, 0),
          frame_type_(kFrameRange),
          start_(nullptr),
          end_(nullptr) {}

    FrameNode(SQLNodeType frame_type, FrameBound *start, FrameBound *end)
        : SQLNode(kFrames, 0, 0),
          frame_type_(frame_type),
          start_(start),
          end_(end) {}

    ~FrameNode() {}

    SQLNodeType GetFrameType() const { return frame_type_; }

    void SetFrameType(SQLNodeType frame_type) { frame_type_ = frame_type; }

    FrameBound *GetStart() const { return start_; }

    FrameBound *GetEnd() const { return end_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    SQLNodeType frame_type_;
    FrameBound *start_;
    FrameBound *end_;
};
class WindowDefNode : public SQLNode {
 public:
    WindowDefNode()
        : SQLNode(kWindowDef, 0, 0), window_name_(""), frame_ptr_(NULL) {}

    ~WindowDefNode() {}

    const std::string &GetName() const { return window_name_; }

    void SetName(const std::string &name) { window_name_ = name; }

    std::vector<std::string> &GetPartitions() { return partitions_; }

    std::vector<std::string> &GetOrders() { return orders_; }

    SQLNode *GetFrame() const { return frame_ptr_; }

    void SetFrame(FrameNode *frame) { frame_ptr_ = frame; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string window_name_; /* window's own name */
    FrameNode *frame_ptr_;    /* expression for starting bound, if any */
    std::vector<std::string> partitions_; /* PARTITION BY expression list */
    std::vector<std::string> orders_;     /* ORDER BY (list of SortBy) */
};

class AllNode : public ExprNode {
 public:
    AllNode() : ExprNode(kExprAll), relation_name_("") {}

    explicit AllNode(const std::string &relation_name)
        : ExprNode(kExprAll), relation_name_(relation_name) {}

    std::string GetRelationName() const { return relation_name_; }

    void SetRelationName(const std::string &relation_name) {
        relation_name_ = relation_name;
    }
    const std::string GetExprString() const;

 private:
    std::string relation_name_;
};
class CallExprNode : public ExprNode {
 public:
    explicit CallExprNode(const std::string &function_name,
                          const ExprListNode *args, const WindowDefNode *over)
        : ExprNode(kExprCall),
          is_agg_(true),
          function_name_(function_name),
          over_(over),
          args_(args) {}

    ~CallExprNode() {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;

    std::string GetFunctionName() const { return function_name_; }

    const WindowDefNode *GetOver() const { return over_; }

    void SetOver(WindowDefNode *over) { over_ = over; }

    bool GetIsAgg() const { return is_agg_; }

    void SetAgg(bool is_agg) { is_agg_ = is_agg; }
    const ExprListNode *GetArgs() const { return args_; }

    const int GetArgsSize() const {
        return nullptr == args_ ? 0 : args_->children.size();
    }

 private:
    bool is_agg_;
    const std::string function_name_;
    const WindowDefNode *over_;
    const ExprListNode *args_;
};

class SubQueryExpr : public ExprNode {
 public:
    explicit SubQueryExpr(const SelectStmt *query)
        : ExprNode(kExprSubQuery), sub_query(query) {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const SelectStmt *sub_query;
};

class SubQueryTableNode : public TableRefNode {
 public:
    SubQueryTableNode(const SubQueryExpr *sub_query,
                      const std::string &alias)
        : TableRefNode(kSubQuery, alias), sub_query_(sub_query) {}
    const SubQueryExpr *sub_query_;
};

class BinaryExpr : public ExprNode {
 public:
    BinaryExpr() : ExprNode(kExprBinary) {}
    explicit BinaryExpr(FnOperator op) : ExprNode(kExprBinary), op_(op) {}
    FnOperator GetOp() const { return op_; }
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;

 private:
    FnOperator op_;
};
class UnaryExpr : public ExprNode {
 public:
    UnaryExpr() : ExprNode(kExprUnary) {}
    explicit UnaryExpr(FnOperator op) : ExprNode(kExprUnary), op_(op) {}
    FnOperator GetOp() const { return op_; }
    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string GetExprString() const;

 private:
    FnOperator op_;
};
class ExprIdNode : public ExprNode {
 public:
    ExprIdNode() : ExprNode(kExprId) {}
    explicit ExprIdNode(const std::string &name)
        : ExprNode(kExprId), name_(name) {}
    const std::string GetName() const { return name_; }
    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string GetExprString() const;

 private:
    std::string name_;
};

class ConstNode : public ExprNode {
 public:
    ConstNode() : ExprNode(kExprPrimary), date_type_(fesql::node::kNull) {}
    explicit ConstNode(int16_t val)
        : ExprNode(kExprPrimary), date_type_(fesql::node::kInt16) {
        val_.vsmallint = val;
    }
    explicit ConstNode(int val)
        : ExprNode(kExprPrimary), date_type_(fesql::node::kInt32) {
        val_.vint = val;
    }
    explicit ConstNode(int64_t val)
        : ExprNode(kExprPrimary), date_type_(fesql::node::kInt64) {
        val_.vlong = val;
    }
    explicit ConstNode(float val)
        : ExprNode(kExprPrimary), date_type_(fesql::node::kFloat) {
        val_.vfloat = val;
    }

    explicit ConstNode(double val)
        : ExprNode(kExprPrimary), date_type_(fesql::node::kDouble) {
        val_.vdouble = val;
    }

    explicit ConstNode(const char *val)
        : ExprNode(kExprPrimary), date_type_(fesql::node::kVarchar) {
        val_.vstr = strdup(val);
    }

    explicit ConstNode(const std::string &val)
        : ExprNode(kExprPrimary), date_type_(fesql::node::kVarchar) {
        val_.vstr = val.c_str();
    }

    ConstNode(int64_t val, DataType time_type)
        : ExprNode(kExprPrimary), date_type_(time_type) {
        val_.vlong = val;
    }

    ~ConstNode() {
        if (date_type_ == fesql::node::kVarchar) {
            delete val_.vstr;
        }
    }
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;

    int16_t GetSmallInt() const { return val_.vsmallint; }

    int GetInt() const { return val_.vint; }

    int64_t GetLong() const { return val_.vlong; }

    const char *GetStr() const { return val_.vstr; }

    float GetFloat() const { return val_.vfloat; }

    double GetDouble() const { return val_.vdouble; }

    DataType GetDataType() const { return date_type_; }

    int64_t GetMillis() const {
        switch (date_type_) {
            case fesql::node::kDay:
                return 86400000 * val_.vlong;
            case fesql::node::kHour:
                return 3600000 * val_.vlong;
            case fesql::node::kMinute:
                return 60000 * val_.vlong;
            case fesql::node::kSecond:
                return 1000 * val_.vlong;
            default: {
                LOG(WARNING)
                    << "error occur when get milli second from wrong type "
                    << DataTypeName(date_type_);
                return -1;
            }
        }
    }

 private:
    DataType date_type_;
    union {
        int16_t vsmallint;
        int vint;         /* machine integer */
        int64_t vlong;    /* machine integer */
        const char *vstr; /* string */
        float vfloat;
        double vdouble;
    } val_;
};
class ColumnRefNode : public ExprNode {
 public:
    ColumnRefNode()
        : ExprNode(kExprColumnRef), column_name_(""), relation_name_("") {}

    ColumnRefNode(const std::string &column_name,
                  const std::string &relation_name)
        : ExprNode(kExprColumnRef),
          column_name_(column_name),
          relation_name_(relation_name) {}

    std::string GetRelationName() const { return relation_name_; }

    void SetRelationName(const std::string &relation_name) {
        relation_name_ = relation_name;
    }

    std::string GetColumnName() const { return column_name_; }

    void SetColumnName(const std::string &column_name) {
        column_name_ = column_name;
    }

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;

 private:
    std::string column_name_;
    std::string relation_name_;
};

class ResTarget : public SQLNode {
 public:
    ResTarget() : SQLNode(kResTarget, 0, 0), name_(""), val_(nullptr) {}

    ResTarget(const std::string &name, ExprNode *val)
        : SQLNode(kResTarget, 0, 0), name_(name), val_(val) {}

    ~ResTarget() {}

    std::string GetName() const { return name_; }

    ExprNode *GetVal() const { return val_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string name_; /* column name or NULL */
    ExprNode *val_;    /* the value expression to compute or assign */
    NodePointVector indirection_; /* subscripts, field names, and '*', or NIL */
};

class ColumnDefNode : public SQLNode {
 public:
    ColumnDefNode()
        : SQLNode(kColumnDesc, 0, 0), column_name_(""), column_type_() {}
    ColumnDefNode(const std::string &name, const DataType &data_type,
                  bool op_not_null)
        : SQLNode(kColumnDesc, 0, 0),
          column_name_(name),
          column_type_(data_type),
          op_not_null_(op_not_null) {}
    ~ColumnDefNode() {}

    std::string GetColumnName() const { return column_name_; }

    DataType GetColumnType() const { return column_type_; }

    bool GetIsNotNull() const { return op_not_null_; }
    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string column_name_;
    DataType column_type_;
    bool op_not_null_;
};

class InsertStmt : public SQLNode {
 public:
    InsertStmt(const std::string &table_name,
               const std::vector<std::string> &columns,
               const std::vector<ExprNode *> &values)
        : SQLNode(kInsertStmt, 0, 0),
          table_name_(table_name),
          columns_(columns),
          values_(values),
          is_all_(false) {}

    InsertStmt(const std::string &table_name,
               const std::vector<ExprNode *> &values)
        : SQLNode(kInsertStmt, 0, 0),
          table_name_(table_name),
          values_(values),
          is_all_(true) {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const std::string table_name_;
    const std::vector<std::string> columns_;
    const std::vector<ExprNode *> values_;
    const bool is_all_;
};
class CreateStmt : public SQLNode {
 public:
    CreateStmt()
        : SQLNode(kCreateStmt, 0, 0),
          table_name_(""),
          op_if_not_exist_(false) {}

    CreateStmt(const std::string &table_name, bool op_if_not_exist)
        : SQLNode(kCreateStmt, 0, 0),
          table_name_(table_name),
          op_if_not_exist_(op_if_not_exist) {}

    ~CreateStmt() {}

    NodePointVector &GetColumnDefList() { return column_desc_list_; }
    const NodePointVector &GetColumnDefList() const {
        return column_desc_list_;
    }

    std::string GetTableName() const { return table_name_; }

    bool GetOpIfNotExist() const { return op_if_not_exist_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string table_name_;
    bool op_if_not_exist_;
    NodePointVector column_desc_list_;
};
class IndexKeyNode : public SQLNode {
 public:
    IndexKeyNode() : SQLNode(kIndexKey, 0, 0) {}
    explicit IndexKeyNode(const std::string &key) : SQLNode(kIndexKey, 0, 0) {
        key_.push_back(key);
    }
    ~IndexKeyNode() {}
    void AddKey(const std::string &key) { key_.push_back(key); }
    std::vector<std::string> &GetKey() { return key_; }

 private:
    std::vector<std::string> key_;
};
class IndexVersionNode : public SQLNode {
 public:
    IndexVersionNode() : SQLNode(kIndexVersion, 0, 0) {}
    explicit IndexVersionNode(const std::string &column_name)
        : SQLNode(kIndexVersion, 0, 0), column_name_(column_name), count_(1) {}
    IndexVersionNode(const std::string &column_name, int count)
        : SQLNode(kIndexVersion, 0, 0),
          column_name_(column_name),
          count_(count) {}

    std::string &GetColumnName() { return column_name_; }

    int GetCount() const { return count_; }

 private:
    std::string column_name_;
    int count_;
};
class IndexTsNode : public SQLNode {
 public:
    IndexTsNode() : SQLNode(kIndexTs, 0, 0) {}
    explicit IndexTsNode(const std::string &column_name)
        : SQLNode(kIndexTs, 0, 0), column_name_(column_name) {}

    std::string &GetColumnName() { return column_name_; }

 private:
    std::string column_name_;
};
class IndexTTLNode : public SQLNode {
 public:
    IndexTTLNode() : SQLNode(kIndexTTL, 0, 0) {}
    explicit IndexTTLNode(ExprNode *expr)
        : SQLNode(kIndexTTL, 0, 0), ttl_expr_(expr) {}

    ExprNode *GetTTLExpr() const { return ttl_expr_; }

 private:
    ExprNode *ttl_expr_;
};
class ColumnIndexNode : public SQLNode {
 public:
    ColumnIndexNode()
        : SQLNode(kColumnIndex, 0, 0),
          ts_(""),
          version_(""),
          version_count_(0),
          ttl_(-1L),
          name_("") {}

    std::vector<std::string> &GetKey() { return key_; }
    void SetKey(const std::vector<std::string> &key) { key_ = key; }

    std::string GetTs() const { return ts_; }

    void SetTs(const std::string &ts) { ts_ = ts; }

    std::string GetVersion() const { return version_; }

    void SetVersion(const std::string &version) { version_ = version; }

    std::string GetName() const { return name_; }

    void SetName(const std::string &name) { name_ = name; }
    int GetVersionCount() const { return version_count_; }

    void SetVersionCount(int count) { version_count_ = count; }

    int64_t GetTTL() const { return ttl_; }
    void SetTTL(ExprNode *ttl_node) {
        if (nullptr == ttl_node) {
            ttl_ = -1l;
        } else {
            switch (ttl_node->GetExprType()) {
                case kExprPrimary: {
                    const ConstNode *ttl = dynamic_cast<ConstNode *>(ttl_node);
                    switch (ttl->GetDataType()) {
                        case fesql::node::kInt32:
                            ttl_ = ttl->GetInt();
                            break;
                        case fesql::node::kInt64:
                            ttl_ = ttl->GetLong();
                            break;
                        case fesql::node::kDay:
                        case fesql::node::kHour:
                        case fesql::node::kMinute:
                        case fesql::node::kSecond:
                            ttl_ = ttl->GetMillis();
                            break;
                        default: {
                            ttl_ = -1;
                        }
                    }
                    break;
                }
                default: {
                    LOG(WARNING) << "can't set ttl with expr type "
                                 << ExprTypeName(ttl_node->GetExprType());
                }
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
    std::string name_;
};
class CmdNode : public SQLNode {
 public:
    explicit CmdNode(node::CmdType cmd_type)
        : SQLNode(kCmdStmt, 0, 0), cmd_type_(cmd_type) {}

    ~CmdNode() {}

    void AddArg(const std::string &arg) { args_.push_back(arg); }
    const std::vector<std::string> &GetArgs() const { return args_; }
    void Print(std::ostream &output, const std::string &org_tab) const;

    const node::CmdType GetCmdType() const { return cmd_type_; }

 private:
    node::CmdType cmd_type_;
    std::vector<std::string> args_;
};

class FnParaNode : public FnNode {
 public:
    FnParaNode(const std::string &name, const TypeNode *para_type)
        : FnNode(kFnPara), name_(name), para_type_(para_type) {}
    const std::string &GetName() const { return name_; }

    const TypeNode *GetParaType() const { return para_type_; }
    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string name_;
    const TypeNode *para_type_;
};
class FnNodeFnHeander : public FnNode {
 public:
    FnNodeFnHeander(const std::string &name, FnNodeList *parameters,
                    const TypeNode *ret_type)
        : FnNode(kFnHeader),
          name_(name),
          parameters_(parameters),
          ret_type_(ret_type) {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetCodegenFunctionName() const;
    const std::string name_;
    const FnNodeList *parameters_;
    const TypeNode *ret_type_;
};
class FnNodeFnDef : public FnNode {
 public:
    FnNodeFnDef(const FnNodeFnHeander *header, const FnNodeList *block)
        : FnNode(kFnDef), header_(header), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const FnNodeFnHeander *header_;
    const FnNodeList *block_;
};

class FnAssignNode : public FnNode {
 public:
    explicit FnAssignNode(const std::string &name, ExprNode *expression)
        : FnNode(kFnAssignStmt),
          name_(name),
          expression_(expression),
          is_ssa_(false) {}
    std::string GetName() const { return name_; }
    const bool IsSSA() const { return is_ssa_; }
    void EnableSSA() { is_ssa_ = true; }
    void DisableSSA() { is_ssa_ = false; }
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string name_;
    const ExprNode *expression_;

 private:
    bool is_ssa_;
};

class FnIfNode : public FnNode {
 public:
    explicit FnIfNode(const ExprNode *expression)
        : FnNode(kFnIfStmt), expression_(expression) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const ExprNode *expression_;
};
class FnElifNode : public FnNode {
 public:
    explicit FnElifNode(ExprNode *expression)
        : FnNode(kFnElifStmt), expression_(expression) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const ExprNode *expression_;
};
class FnElseNode : public FnNode {
 public:
    FnElseNode() : FnNode(kFnElseStmt) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
};

class FnForInNode : public FnNode {
 public:
    FnForInNode(const std::string &var_name, const ExprNode *in_expression)
        : FnNode(kFnForInStmt),
          var_name_(var_name),
          in_expression_(in_expression) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string var_name_;
    const ExprNode *in_expression_;
};

class FnIfBlock : public FnNode {
 public:
    FnIfBlock(const FnIfNode *node, const FnNodeList *block)
        : FnNode(kFnIfBlock), if_node(node), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const FnIfNode *if_node;
    const FnNodeList *block_;
};

class FnElifBlock : public FnNode {
 public:
    FnElifBlock(const FnElifNode *node, const FnNodeList *block)
        : FnNode(kFnElifBlock), elif_node_(node), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const FnElifNode *elif_node_;
    const FnNodeList *block_;
};
class FnElseBlock : public FnNode {
 public:
    explicit FnElseBlock(const FnNodeList *block)
        : FnNode(kFnElseBlock), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const FnNodeList *block_;
};
class FnIfElseBlock : public FnNode {
 public:
    FnIfElseBlock(const FnIfBlock *if_block, const FnElseBlock *else_block)
        : FnNode(kFnIfElseBlock),
          if_block_(if_block),
          else_block_(else_block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const FnIfBlock *if_block_;
    std::vector<FnNode *> elif_blocks_;
    const FnElseBlock *else_block_;
};

class FnForInBlock : public FnNode {
 public:
    FnForInBlock(const FnForInNode *for_in_node, const FnNodeList *block)
        : FnNode(kFnForInBlock), for_in_node_(for_in_node), block_(block) {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    const FnForInNode *for_in_node_;
    const FnNodeList *block_;
};
class FnReturnStmt : public FnNode {
 public:
    explicit FnReturnStmt(ExprNode *return_expr)
        : FnNode(kFnReturnStmt), return_expr_(return_expr) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    const ExprNode *return_expr_;
};
class StructExpr : public ExprNode {
 public:
    explicit StructExpr(const std::string &name)
        : ExprNode(kExprStruct), class_name_(name) {}
    void SetFileds(FnNodeList *fileds) { fileds_ = fileds; }
    void SetMethod(FnNodeList *methods) { methods_ = methods; }

    const FnNodeList *GetMethods() const { return methods_; }

    const FnNodeList *GetFileds() const { return fileds_; }

    const std::string &GetName() const { return class_name_; }
    void Print(std::ostream &output, const std::string &org_tab) const override;

 private:
    const std::string class_name_;
    FnNodeList *fileds_;
    FnNodeList *methods_;
};

WindowDefNode *WindowOfExpression(
    std::map<std::string, WindowDefNode *> windows, ExprNode *node_ptr);
void FillSQLNodeList2NodeVector(
    SQLNodeList *node_list_ptr,
    std::vector<SQLNode *> &node_list);  // NOLINT (runtime/references)
void PrintSQLNode(std::ostream &output, const std::string &org_tab,
                  const SQLNode *node_ptr, const std::string &item_name,
                  bool last_child);
void PrintSQLVector(std::ostream &output, const std::string &tab,
                    const NodePointVector &vec, const std::string &vector_name,
                    bool last_item);
void PrintSQLVector(std::ostream &output, const std::string &tab,
                    const std::vector<ExprNode *> &vec,
                    const std::string &vector_name, bool last_item);
void PrintValue(std::ostream &output, const std::string &org_tab,
                const std::string &value, const std::string &item_name,
                bool last_child);
}  // namespace node
}  // namespace fesql
#endif  // SRC_NODE_SQL_NODE_H_
