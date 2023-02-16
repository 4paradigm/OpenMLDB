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

#ifndef HYBRIDSE_INCLUDE_NODE_SQL_NODE_H_
#define HYBRIDSE_INCLUDE_NODE_SQL_NODE_H_

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/predicate.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "node/expr_node.h"
#include "node/node_base.h"
#include "node/node_enum.h"

// fwd
namespace hybridse::udf {
class LlvmUdfGenBase;
}

namespace hybridse {
namespace node {

class ConstNode;
class WithClauseEntry;

typedef std::unordered_map<std::string, const ConstNode*> OptionsMap;

// Global methods
std::string NameOfSqlNodeType(const SqlNodeType &type);

absl::string_view CmdTypeName(const CmdType type);

inline const std::string ExplainTypeName(const ExplainType &explain_type) {
    switch (explain_type) {
        case kExplainLogical:
            return "logical";
        case kExplainPhysical:
            return "physical";
        default: {
            return "Unknow";
        }
    }
}

inline const std::string JoinTypeName(const JoinType &type) {
    switch (type) {
        case kJoinTypeFull:
            return "FullJoin";
        case kJoinTypeLast:
            return "LastJoin";
        case kJoinTypeLeft:
            return "LeftJoin";
        case kJoinTypeRight:
            return "RightJoin";
        case kJoinTypeInner:
            return "InnerJoin";
        case kJoinTypeConcat:
            return "kJoinTypeConcat";
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
        case kFnOpXor:
            return "XOR";
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
        case kFnOpILike:
            return "ILIKE";
        case kFnOpRLike:
            return "RLIKE";
        case kFnOpIn:
            return "IN";
        case kFnOpBracket:
            return "()";
        case kFnOpIsNull:
            return "IS_NULL";
        case kFnOpNone:
            return "NONE";
        case kFnOpNonNull:
            return "NONNULL";
        case kFnOpBitwiseAnd:
            return "&";
        case kFnOpBitwiseOr:
            return "|";
        case kFnOpBitwiseXor:
            return "^";
        case kFnOpBitwiseNot:
            return "~";
        default:
            return "UNKNOWN";
    }
}
inline const std::string TableRefTypeName(const TableRefType &type) {
    switch (type) {
        case kRefQuery:
            return "kQuery";
        case kRefJoin:
            return "kJoin";
        case kRefTable:
            return "kTable";
        default: {
            return "unknow";
        }
    }
}
inline const std::string QueryTypeName(const QueryType &type) {
    switch (type) {
        case kQuerySelect:
            return "kQuerySelect";
        case kQueryUnion:
            return "kQueryUnion";
        case kQuerySub:
            return "kQuerySub";
        default: {
            return "unknow";
        }
    }
}

std::string ExprTypeName(ExprType type);

inline const std::string FrameTypeName(const FrameType &type) {
    switch (type) {
        case hybridse::node::kFrameRange:
            return "RANGE";
        case hybridse::node::kFrameRows:
            return "ROWS";
        case hybridse::node::kFrameRowsRange:
            return "ROWS_RANGE";
        case hybridse::node::kFrameRowsMergeRowsRange:
            return "ROWS_MERGE_ROWS_RANGE";
    }
    return "";
}

inline const std::string BoundTypeName(const BoundType &type) {
    switch (type) {
        case hybridse::node::kPrecedingUnbound:
            return "PRECEDING UNBOUND";
        case hybridse::node::kOpenPreceding:
            return "OPEN PRECEDING";
        case hybridse::node::kPreceding:
            return "PRECEDING";
        case hybridse::node::kCurrent:
            return "CURRENT";
        case hybridse::node::kOpenFollowing:
            return "OPEN FOLLOWING";
        case hybridse::node::kFollowing:
            return "FOLLOWING";
        case hybridse::node::kFollowingUnbound:
            return "FOLLOWING UNBOUND";
        default:
            return "UNKNOW";
    }
    return "";
}

std::string DataTypeName(DataType type);

inline const std::string TypeName(type::Type type) {
    switch (type) {
        case type::kInt16:
            return "smallint";
        case type::kInt32:
            return "int";
        case type::kInt64:
            return "bigint";
        case type::kFloat:
            return "float";
        case type::kDouble:
            return "double";
        case type::kVarchar:
            return "string";
        case type::kTimestamp:
            return "timestamp";
        case type::kDate:
            return "date";
        case type::kBool:
            return "bool";
        case type::kBlob:
            return "blob";
        case type::kNull:
            return "null";
    }
    return "unknown";
}

inline const std::string VariableScopeName(const node::VariableScope scope) {
    switch (scope) {
        case kGlobalSystemVariable: {
            return "GlobalSystemVariable";
        }
        case kSessionSystemVariable: {
            return "SessionSystemVariable";
        }
    }
    return "unknow";
}
/**
 * Convert string to corresponding DataType. e.g. "i32" => kInt32
 *
 * \param identifier: string to match
 * \param type: output type
 * \return: Status
 */
Status StringToDataType(const std::string identifier, DataType *type);

inline const std::string FnNodeName(const SqlNodeType &type) {
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

inline const std::string StorageModeName(StorageMode mode) {
    switch (mode) {
        case kMemory:
            return "memory";
        case kHDD:
            return "hdd";
        case kSSD:
            return "ssd";
        default:
            return "unknown";
    }
}

inline const StorageMode NameToStorageMode(const std::string& name) {
    if (boost::iequals(name, "memory")) {
        return kMemory;
    } else if (boost::iequals(name, "hdd")) {
        return kHDD;
    } else if (boost::iequals(name, "ssd")) {
        return kSSD;
    } else {
        return kUnknown;
    }
}

inline const std::string RoleTypeName(RoleType type) {
    switch (type) {
        case kLeader:
            return "leader";
        case kFollower:
            return "follower";
        default:
            return "unknow";
    }
}

class SqlNode : public NodeBase<SqlNode> {
 public:
    SqlNode(const SqlNodeType &type, uint32_t line_num, uint32_t location)
        : type_(type), line_num_(line_num), location_(location) {}

    virtual ~SqlNode() {}

    const SqlNodeType GetType() const { return type_; }
    uint32_t GetLineNum() const override { return line_num_; }
    uint32_t GetLocation() const override { return location_; }

    const std::string GetTypeName() const override { return NameOfSqlNodeType(type_); }

    bool Equals(const SqlNode *node) const override;

    SqlNodeType type_;

 private:
    uint32_t line_num_;
    uint32_t location_;
};

typedef std::vector<SqlNode *> NodePointVector;

class SqlNodeList : public SqlNode {
 public:
    SqlNodeList() : SqlNode(kNodeList, 0, 0) {}
    virtual ~SqlNodeList() {}
    void PushBack(SqlNode *node_ptr) { list_.push_back(node_ptr); }
    const bool IsEmpty() const { return list_.empty(); }
    const int GetSize() const { return list_.size(); }
    const NodePointVector &GetList() const { return list_; }
    void Print(std::ostream &output, const std::string &tab) const override;
    bool Equals(const SqlNodeList *that) const;

 private:
    NodePointVector list_;
};

class TypeNode;

class ExprNode : public SqlNode {
 public:
    explicit ExprNode(ExprType expr_type) : SqlNode(kExpr, 0, 0), expr_type_(expr_type) {}
    ~ExprNode() {}
    void AddChild(ExprNode *expr) { children_.push_back(expr); }
    void SetChild(size_t idx, ExprNode *expr) { children_[idx] = expr; }
    ExprNode *GetChild(size_t idx) const { return children_[idx]; }
    ExprNode *GetChildOrNull(size_t idx) const {
        return idx < GetChildNum() ? GetChild(idx) : nullptr;
    }
    uint32_t GetChildNum() const { return children_.size(); }

    const ExprType GetExprType() const { return expr_type_; }
    void PushBack(ExprNode *node_ptr) { children_.push_back(node_ptr); }

    std::vector<ExprNode *> children_;
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual const std::string GetExprString() const;
    virtual const std::string GenerateExpressionName() const;
    virtual bool Equals(const ExprNode *that) const;

    const ExprType expr_type_;

    const TypeNode *GetOutputType() const { return output_type_; }
    void SetOutputType(const TypeNode *dtype) { output_type_ = dtype; }

    bool nullable() const { return nullable_; }
    void SetNullable(bool flag) { nullable_ = flag; }

    /**
     * Infer static attributes of expression; Including
     *
     * - output type: abstract type node of expression output
     * - nullable: whether output can be null
     */
    virtual Status InferAttr(ExprAnalysisContext *ctx) {
        return Status(common::kUnSupport, "Not implemented: " + GetExprString());
    }

    /**
     * Determinie whether expression require iterative input
     * at specified position. This will affect lambdafy
     * process on the expression.
     */
    virtual bool RequireListAt(ExprAnalysisContext *ctx, size_t index) const { return false; }
    /**
     * Determinie whether expression will return list type.
     */
    virtual bool IsListReturn(ExprAnalysisContext *ctx) const { return false; }

    /**
     * Default expression node deep copy implementation
     */
    virtual ExprNode *ShadowCopy(NodeManager *) const = 0;
    ExprNode *DeepCopy(NodeManager *) const override;

    // Get the compatible type that lhs and rhs can both casted into
    static absl::StatusOr<const TypeNode *> CompatibleType(NodeManager *, const TypeNode *, const TypeNode *);

    static bool IsSafeCast(const TypeNode *from_type, const TypeNode *target_type);

    static bool IsIntFloat2PointerCast(const TypeNode *left_type, const TypeNode *right_type);
    static Status InferNumberCastTypes(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                                       const TypeNode **output_type);

    static Status AndTypeAccept(node::NodeManager *nm, const TypeNode *lhs, const TypeNode *rhs,
                                const TypeNode **output_type);
    static Status LShiftTypeAccept(node::NodeManager *nm, const TypeNode *lhs, const TypeNode *rhs,
                                   const TypeNode **output_type);
    static Status AddTypeAccept(node::NodeManager *nm, const TypeNode *lhs, const TypeNode *rhs,
                                const TypeNode **output_type);
    static Status SubTypeAccept(node::NodeManager *nm, const TypeNode *lhs, const TypeNode *rhs,
                                const TypeNode **output_type);
    static Status IsCastAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                               const TypeNode **output_type);

    static Status MultiTypeAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                                  const TypeNode **output_type);
    static Status FDivTypeAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                                 const TypeNode **output_type);
    static Status SDivTypeAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                                 const TypeNode **output_type);
    static Status ModTypeAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                                const TypeNode **output_type);
    static Status NotTypeAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode **output_type);
    static Status CompareTypeAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                                    const TypeNode **output_type);
    static Status LogicalOpTypeAccept(node::NodeManager *nm, const TypeNode *left_type, const TypeNode *right_type,
                                      const TypeNode **output_type);

    static Status BitwiseLogicalTypeAccept(node::NodeManager *nm, const TypeNode *lhs, const TypeNode *rhs,
                                      const TypeNode **output_type);

    static Status BitwiseNotTypeAccept(node::NodeManager* nm, const TypeNode* rhs, const TypeNode** output_type);

    static Status BetweenTypeAccept(node::NodeManager* nm, const TypeNode* lhs, const TypeNode* low,
                                    const TypeNode* high, const TypeNode** output_type);

    static Status LikeTypeAccept(node::NodeManager* nm, const TypeNode* lhs, const TypeNode* rhs,
                                 const TypeNode** output);

    static Status RlikeTypeAccept(node::NodeManager* nm, const TypeNode* lhs, const TypeNode* rhs,
                                 const TypeNode** output);

 private:
    const TypeNode *output_type_ = nullptr;
    bool nullable_ = true;
};

class ExprListNode : public ExprNode {
 public:
    ExprListNode() : ExprNode(kExprList) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const bool IsEmpty() const { return children_.empty(); }
    const std::string GetExprString() const;
    ExprListNode *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;
};

// fixed size array
class ArrayExpr : public ExprNode {
 public:
    ArrayExpr() : ExprNode(kExprArray) {}
    ~ArrayExpr() override {}

    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string GetExprString() const override;
    ArrayExpr *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;

    // array type may specific already in SQL, e.g. ARRAY<FLOAT>[1,2,3]
    TypeNode* specific_type_ = nullptr;
};

class FnNode : public SqlNode {
 public:
    FnNode() : SqlNode(kFn, 0, 0), indent(0) {}
    explicit FnNode(SqlNodeType type) : SqlNode(type, 0, 0), indent(0) {}

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
class OrderExpression : public ExprNode {
 public:
    OrderExpression(const ExprNode *expr, const bool is_asc)
        : ExprNode(kExprOrderExpression), expr_(expr), is_asc_(is_asc) {}
    ~OrderExpression() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *that) const;
    OrderExpression *ShadowCopy(NodeManager *) const override;
    const ExprNode *expr() const { return expr_; }
    const bool is_asc() const { return is_asc_; }

 private:
    const ExprNode *expr_;
    const bool is_asc_;
};
class OrderByNode : public ExprNode {
 public:
    explicit OrderByNode(const ExprListNode *order_expressions)
        : ExprNode(kExprOrder), order_expressions_(order_expressions) {}
    ~OrderByNode() {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *that) const;
    OrderByNode *ShadowCopy(NodeManager *) const override;
    const ExprListNode *order_expressions() const { return order_expressions_; }
    const OrderExpression *GetOrderExpression(size_t idx) const {
        if (nullptr == order_expressions_) {
            return nullptr;
        }
        if (idx < 0 || idx >= order_expressions_->GetChildNum()) {
            return nullptr;
        }
        return dynamic_cast<const OrderExpression *>(order_expressions_->GetChild(idx));
    }
    const ExprNode *GetOrderExpressionExpr(int idx) const {
        auto order_expression = GetOrderExpression(idx);
        if (nullptr == order_expression) {
            return nullptr;
        }
        return order_expression->expr();
    }
    bool is_asc() const { return false; }
    const ExprListNode *order_expressions_;
};
class TableRefNode : public SqlNode {
 public:
    explicit TableRefNode(TableRefType ref_type, std::string alias_table_name)
        : SqlNode(kTableRef, 0, 0), ref_type_(ref_type), alias_table_name_(alias_table_name) {}
    ~TableRefNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;

    const TableRefType ref_type_;
    const std::string alias_table_name_;
};

class QueryNode : public SqlNode {
 public:
    explicit QueryNode(QueryType query_type) : SqlNode(node::kQuery, 0, 0), query_type_(query_type) {}
    ~QueryNode() {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;

    void SetWithClauses(absl::Span<WithClauseEntry *> withes) { with_clauses_ = withes; }

    const QueryType query_type_;
    std::shared_ptr<OptionsMap> config_options_;
    absl::Span<WithClauseEntry *> with_clauses_;
};

class WithClauseEntry : public SqlNode {
 public:
    WithClauseEntry(const std::string& alias, QueryNode *query)
        : SqlNode(node::kWithClauseEntry, 0, 0), alias_(alias), query_(query) {}
    ~WithClauseEntry() override {}

    void Print(std::ostream &, const std::string &) const override;
    bool Equals(const SqlNode *node) const override;

    std::string alias_;
    QueryNode *query_;
};

class TableNode : public TableRefNode {
 public:
    TableNode() : TableRefNode(kRefTable, ""), db_(""), org_table_name_("") {}

    TableNode(const std::string &name, const std::string &alias)
        : TableRefNode(kRefTable, alias), db_(""), org_table_name_(name) {}
    TableNode(const std::string &db, const std::string &name, const std::string &alias)
        : TableRefNode(kRefTable, alias), db_(db), org_table_name_(name) {}
    ~TableNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;
    const std::string db_;
    const std::string org_table_name_;
};

class QueryRefNode : public TableRefNode {
 public:
    QueryRefNode(const QueryNode *query, const std::string &alias) : TableRefNode(kRefQuery, alias), query_(query) {}
    ~QueryRefNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;
    const QueryNode *query_;
};

class JoinNode : public TableRefNode {
 public:
    JoinNode(const TableRefNode *left, const TableRefNode *right, const JoinType join_type, const OrderByNode *orders,
             const ExprNode *condition, const std::string &alias_name)
        : TableRefNode(kRefJoin, alias_name),
          left_(left),
          right_(right),
          join_type_(join_type),
          orders_(orders),
          condition_(condition) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;
    const TableRefNode *left_;
    const TableRefNode *right_;
    const JoinType join_type_;
    const node::OrderByNode *orders_;
    const ExprNode *condition_;
};
class SelectQueryNode : public QueryNode {
 public:
    SelectQueryNode(bool is_distinct, SqlNodeList *select_list, SqlNodeList *tableref_list, ExprNode *where_expr,
                    ExprListNode *group_expr_list, ExprNode *having_expr, OrderByNode *order_expr_list,
                    SqlNodeList *window_list, SqlNode *limit_ptr)
        : QueryNode(kQuerySelect),
          distinct_opt_(is_distinct),
          where_clause_ptr_(where_expr),
          group_clause_ptr_(group_expr_list),
          having_clause_ptr_(having_expr),
          order_clause_ptr_(order_expr_list),
          limit_ptr_(limit_ptr),
          select_list_(select_list),
          tableref_list_(tableref_list),
          window_list_(window_list) {}

    ~SelectQueryNode() {}

    // Getter and Setter
    const SqlNodeList *GetSelectList() const { return select_list_; }

    SqlNodeList *GetSelectList() { return select_list_; }

    const SqlNode *GetLimit() const { return limit_ptr_; }

    const SqlNodeList *GetTableRefList() const { return tableref_list_; }

    SqlNodeList *GetTableRefList() { return tableref_list_; }

    const SqlNodeList *GetWindowList() const { return window_list_; }

    SqlNodeList *GetWindowList() { return window_list_; }

    void SetLimit(SqlNode *limit) { limit_ptr_ = limit; }

    void SetOrder(OrderByNode *order) { order_clause_ptr_ = order; }

    int GetDistinctOpt() const { return distinct_opt_; }
    // Print
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;

    const bool distinct_opt_;
    const ExprNode *where_clause_ptr_;
    const ExprListNode *group_clause_ptr_;
    const ExprNode *having_clause_ptr_;
    const OrderByNode *order_clause_ptr_;
    const SqlNode *limit_ptr_;

 private:
    SqlNodeList *select_list_;
    SqlNodeList *tableref_list_;
    SqlNodeList *window_list_;
    void PrintSqlNodeList(std::ostream &output, const std::string &tab, SqlNodeList *list, const std::string &name,
                          bool last_item) const;
};

class UnionQueryNode : public QueryNode {
 public:
    UnionQueryNode(const QueryNode *left, const QueryNode *right, bool is_all)
        : QueryNode(kQueryUnion), left_(left), right_(right), is_all_(is_all) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;
    const QueryNode *left_;
    const QueryNode *right_;
    const bool is_all_;
};

class ParameterExpr : public ExprNode {
 public:
    explicit ParameterExpr(int position) : ExprNode(kExprParameter), position_(position) {}
    ~ParameterExpr() {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    virtual bool Equals(const ExprNode *node) const;
    ParameterExpr *ShadowCopy(NodeManager *) const override;
    const std::string GetExprString() const;
    const int position() const { return position_; }
    Status InferAttr(ExprAnalysisContext *ctx) override;

 private:
    int position_;
};
class ConstNode : public ExprNode {
    struct FeDate {
        int32_t year = -1;
        int32_t month = -1;
        int32_t day = -1;
    };

 public:
    ConstNode() : ExprNode(kExprPrimary), data_type_(hybridse::node::kNull) {}
    explicit ConstNode(DataType data_type) : ExprNode(kExprPrimary), data_type_(data_type) {}
    explicit ConstNode(int16_t val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kInt16) {
        val_.vsmallint = val;
    }
    explicit ConstNode(bool val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kBool) {
        val_.vint = val ? 1 : 0;
    }
    explicit ConstNode(int val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kInt32) { val_.vint = val; }
    explicit ConstNode(int64_t val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kInt64) { val_.vlong = val; }
    explicit ConstNode(int val, TTLType ttl_type)
        : ExprNode(kExprPrimary), data_type_(hybridse::node::kInt32), ttl_type_(ttl_type) {
        val_.vint = val;
    }
    explicit ConstNode(int64_t val, TTLType ttl_type)
        : ExprNode(kExprPrimary), data_type_(hybridse::node::kInt64), ttl_type_(ttl_type) {
        val_.vlong = val;
    }
    explicit ConstNode(float val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kFloat) { val_.vfloat = val; }

    explicit ConstNode(double val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kDouble) { val_.vdouble = val; }

    explicit ConstNode(const char *val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kVarchar) {
        val_.vstr = strdup(val);
    }

    explicit ConstNode(const std::string &val) : ExprNode(kExprPrimary), data_type_(hybridse::node::kVarchar) {
        val_.vstr = strdup(val.c_str());
    }

    ConstNode(const ConstNode &that) : ExprNode(kExprPrimary), data_type_(that.data_type_) {
        if (kVarchar == that.data_type_) {
            val_.vstr = strdup(that.val_.vstr);
        } else {
            val_ = that.val_;
        }
    }

    explicit ConstNode(int64_t val, DataType time_type) : ExprNode(kExprPrimary), data_type_(time_type) {
        val_.vlong = val;
    }

    ~ConstNode() {
        if (data_type_ == hybridse::node::kVarchar) {
            free(const_cast<char *>(val_.vstr));
        }
    }
    void Print(std::ostream &output, const std::string &org_tab) const;

    virtual bool Equals(const ExprNode *node) const;
    ConstNode *ShadowCopy(NodeManager *) const override;

    const bool IsNull() const { return kNull == data_type_; }
    const bool IsNumber() const {
        switch (GetDataType()) {
            case kBool:
            case kInt16:
            case kInt32:
            case kInt64:
            case kFloat:
            case kDouble: {
                return true;
            }
            default: {
                return false;
            }
        }
    }
    bool ConvertNegative() {
        switch (GetDataType()) {
            case kNull:
            case kBool: {
                return true;
            }
            case kInt16: {
                val_.vsmallint = -val_.vsmallint;
                return true;
            }
            case kInt32: {
                val_.vint = -val_.vint;
                return true;
            }
            case kInt64: {
                val_.vlong = -val_.vlong;
                return true;
            }
            case kFloat: {
                val_.vfloat = -val_.vfloat;
                return true;
            }
            case kDouble: {
                val_.vdouble = -val_.vdouble;
                return true;
            }
            case kSecond:
            case kMinute:
            case kHour:
            case kDay: {
                val_.vlong = -val_.vlong;
                return true;
            }

            default: {
                LOG(WARNING) << "Can't convert negative with const " + node::DataTypeName(data_type_);
                return false;
            }
        }
    }
    const bool IsPlaceholder() const { return kPlaceholder == data_type_; }
    const std::string GetExprString() const;

    bool GetBool() const { return val_.vint > 0; }

    int16_t GetSmallInt() const { return val_.vsmallint; }

    int GetInt() const { return val_.vint; }

    int64_t GetLong() const { return val_.vlong; }

    const char *GetStr() const { return val_.vstr; }

    float GetFloat() const { return val_.vfloat; }

    double GetDouble() const { return val_.vdouble; }

    DataType GetDataType() const { return data_type_; }

    TTLType GetTTLType() const { return ttl_type_; }

    int64_t GetMillis() const {
        switch (data_type_) {
            case hybridse::node::kDay:
                return 86400000 * val_.vlong;
            case hybridse::node::kHour:
                return 3600000 * val_.vlong;
            case hybridse::node::kMinute:
                return 60000 * val_.vlong;
            case hybridse::node::kSecond:
                return 1000 * val_.vlong;
            default: {
                LOG(WARNING) << "error occur when get milli second from wrong type " << DataTypeName(data_type_);
                return -1;
            }
        }
    }

    const int32_t GetAsInt32() const {
        switch (data_type_) {
            case kInt32:
                return static_cast<int32_t>(val_.vint);
            case kInt16:
                return static_cast<int32_t>(val_.vsmallint);
            case kInt64:
                return static_cast<int64_t>(val_.vlong);
            case kFloat:
                return static_cast<int64_t>(val_.vfloat);
            case kDouble:
                return static_cast<int64_t>(val_.vdouble);
            default: {
                return 0;
            }
        }
    }

    const int16_t GetAsInt16() const {
        switch (data_type_) {
            case kInt32:
                return static_cast<int16_t>(val_.vint);
            case kInt16:
                return static_cast<int16_t>(val_.vsmallint);
            case kInt64:
                return static_cast<int16_t>(val_.vlong);
            case kFloat:
                return static_cast<int16_t>(val_.vfloat);
            case kDouble:
                return static_cast<int16_t>(val_.vdouble);
            default: {
                return 0;
            }
        }
    }

    const int64_t GetAsInt64() const {
        switch (data_type_) {
            case kInt32:
                return static_cast<int64_t>(val_.vint);
            case kInt16:
                return static_cast<int64_t>(val_.vsmallint);
            case kInt64:
                return static_cast<int64_t>(val_.vlong);
            case kFloat:
                return static_cast<int64_t>(val_.vfloat);
            case kDouble:
                return static_cast<int64_t>(val_.vdouble);
            default: {
                return 0;
            }
        }
    }

    const float GetAsFloat() const {
        switch (data_type_) {
            case kInt32:
                return static_cast<float>(val_.vint);
            case kInt16:
                return static_cast<float>(val_.vsmallint);
            case kInt64:
                return static_cast<float>(val_.vlong);
            case kFloat:
                return static_cast<float>(val_.vfloat);
            case kDouble:
                return static_cast<float>(val_.vdouble);
            default: {
                return 0.0;
            }
        }
    }

    const bool GetAsDate(int32_t *year, int32_t *month, int32_t *day) const {
        switch (data_type_) {
            case kVarchar: {
                std::string date_str(val_.vstr);
                std::vector<std::string> date_vec;
                boost::split(date_vec, date_str, boost::is_any_of("-"), boost::token_compress_on);
                if (date_vec.size() < 3) {
                    LOG(WARNING) << "Invalid Date Format";
                    return false;
                }
                *year = boost::lexical_cast<int32_t>(date_vec[0]);
                *month = boost::lexical_cast<int32_t>(date_vec[1]);
                *day = boost::lexical_cast<int32_t>(date_vec[2]);
                return true;
            }
            default: {
                LOG(WARNING) << "Invalid data type for date";
                return false;
            }
        }
    }
    const double GetAsDouble() const {
        switch (data_type_) {
            case kInt32:
                return static_cast<double>(val_.vint);
            case kInt16:
                return static_cast<double>(val_.vsmallint);
            case kInt64:
                return static_cast<double>(val_.vlong);
            case kFloat:
                return static_cast<double>(val_.vfloat);
            case kDouble:
                return static_cast<double>(val_.vdouble);
            default: {
                return 0.0;
            }
        }
    }

    const std::string GetAsString() const {
        switch (data_type_) {
            case kInt32:
                return std::to_string(val_.vint);
            case kInt16:
                return std::to_string(val_.vsmallint);
            case kInt64:
                return std::to_string(val_.vlong);
            case kFloat:
                return std::to_string(val_.vfloat);
            case kDouble:
                return std::to_string(val_.vdouble);
            case kVarchar:
                return std::string(val_.vstr);
            case kBool:
                return val_.vint == 1 ? "true" : "false";
            default: {
                return "";
            }
        }
    }

    // include 'udf/literal_traits.h' for Nullable lead to recursive include
    // so `optional` is used for nullable info
    template <typename T>
    absl::StatusOr<std::optional<T>> GetAs() const {
        if (IsNull()) {
            return std::nullopt;
        }

        if constexpr (std::is_same_v<T, bool>) {
            return GetBool();
        } else if constexpr(std::is_same_v<T, int16_t>) {
            return GetAsInt16();
        } else if constexpr (std::is_same_v<T, int32_t>) {
            return GetAsInt32();
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return GetAsInt64();
        } else if constexpr (std::is_same_v<T, float>) {
            return GetAsFloat();
        } else if constexpr (std::is_same_v<T, double>) {
            return GetAsDouble();
        } else if constexpr (std::is_same_v<T, std::string>) {
            return GetAsString();
        } else {
            return absl::InvalidArgumentError("can't cast as T");
        }
    }

    Status InferAttr(ExprAnalysisContext *ctx) override;
    static ConstNode *CastFrom(ExprNode *node);

 private:
    DataType data_type_;
    union {
        int16_t vsmallint;
        int vint;         /* machine integer */
        int64_t vlong;    /* machine integer */
        const char *vstr; /* string */
        float vfloat;
        double vdouble;
    } val_;
    TTLType ttl_type_ = hybridse::node::kAbsolute;
};
class LimitNode : public SqlNode {
 public:
    LimitNode() : SqlNode(kLimit, 0, 0), limit_cnt_(0) {}

    explicit LimitNode(int limit_cnt) : SqlNode(kLimit, 0, 0), limit_cnt_(limit_cnt) {}

    int GetLimitCount() const { return limit_cnt_; }

    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;

 private:
    int limit_cnt_;
};

class FrameBound : public SqlNode {
 public:
    FrameBound() : SqlNode(kFrameBound, 0, 0), bound_type_(kPreceding), is_time_offset_(false), offset_(0) {}

    explicit FrameBound(BoundType bound_type)
        : SqlNode(kFrameBound, 0, 0), bound_type_(bound_type), is_time_offset_(false), offset_(0) {}

    FrameBound(BoundType bound_type, int64_t offset, bool is_time_offet)
        : SqlNode(kFrameBound, 0, 0), bound_type_(bound_type), is_time_offset_(is_time_offet), offset_(offset) {}

    ~FrameBound() {}

    void Print(std::ostream &output, const std::string &org_tab) const override;

    const std::string GetExprString() const {
        return absl::StrCat(offset_, " ", BoundTypeName(bound_type_));
    }

    BoundType bound_type() const { return bound_type_; }
    void set_bound_type(BoundType type) { bound_type_ = type; }
    const bool is_time_offset() const { return is_time_offset_; }
    int64_t GetOffset() const { return offset_; }
    void SetOffset(int64_t v) { offset_ = v; }


    /// \brief get the inclusive frame bound offset value that has signed symbol
    ///
    /// usually, `offset_` is non-negative (think about the offset expr in SQL window frame:
    ///         'ROWS BETWEEN 2 PRECEDING AND 0 OPEN PRECEDING')
    /// * for 'PRECEDING' bound, output is negative
    /// * for 'FOLLOWING' bound, output is positive
    /// * for 'OPEN [ PRECEDING | FOLLOWING ]', output is one increasing or decreasing, based on
    ///   whether it is start bound or end bound
    ///
    /// \param is_start_frame whether this bound is start or end of window frame
    ///
    /// by convention, start frame is the one has the lower signed offset value
    /// and should be the first frame in SQL string
    ///
    /// e.g
    /// 2s PRECEDING AND 0s PRECEDING -> (true, false)
    /// 2s following and 1s following -> (true, false)
    int64_t GetSignedOffset(bool is_start_frame) const {
        switch (bound_type_) {
            case node::kCurrent:
                return 0;
            case node::kFollowing:
                return offset_;
            case node::kOpenFollowing:
                return is_start_frame ? offset_ + 1 : offset_ - 1;
            case node::kPreceding:
                return -1 * offset_;
            case node::kOpenPreceding:
                return is_start_frame ? -1 * ( offset_  - 1 ) : -1 * (offset_ + 1);
            case node::kPrecedingUnbound:
                return INT64_MIN;
            case node::kFollowingUnbound:
                return INT64_MAX;
        }
        return 0;
    }
    virtual bool Equals(const SqlNode *node) const;
    static int Compare(const FrameBound *bound1, const FrameBound *bound2);

 private:
    BoundType bound_type_;
    bool is_time_offset_;
    int64_t offset_;
};

class FrameExtent : public SqlNode {
 public:
    explicit FrameExtent(FrameBound *start) : SqlNode(kFrameExtent, 0, 0), start_(start) {}

    FrameExtent(FrameBound *start, FrameBound *end) : SqlNode(kFrameExtent, 0, 0), start_(start), end_(end) {}

    ~FrameExtent() override {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;

    FrameBound *start() const { return start_; }
    FrameBound *end() const { return end_; }
    void SetStart(FrameBound* start) { start_ = start; }
    void SetEnd(FrameBound* end) { end_ = end; }

    std::string GetExprString() const;

    // get the inclusive frome bound offset value for start and end
    inline int64_t GetStartOffset() const {
        return start_->GetSignedOffset(true);
    }

    inline int64_t GetEndOffset() const {
        return end_->GetSignedOffset(false);
    }

    /// \brief check if current FrameExtent is valid
    ///
    /// rules:
    /// * start offset <= end offset
    bool Valid() const;

    FrameExtent* ShadowCopy(NodeManager* nm) const override;

 private:
    FrameBound *start_;
    FrameBound *end_;
};
class FrameNode : public SqlNode {
 public:
    FrameNode(FrameType frame_type, FrameExtent *frame_range, FrameExtent *frame_rows, int64_t frame_maxsize)
        : SqlNode(kFrames, 0, 0),
          frame_type_(frame_type),
          frame_range_(frame_range),
          frame_rows_(frame_rows),
          frame_maxsize_(frame_maxsize) {}
    ~FrameNode() {}
    FrameType frame_type() const { return frame_type_; }
    void set_frame_type(FrameType frame_type) { frame_type_ = frame_type; }
    FrameExtent *frame_range() const { return frame_range_; }
    FrameExtent *frame_rows() const { return frame_rows_; }
    void SetFrameRange(FrameExtent* ext) { frame_range_ = ext; }
    void SetFrameRows(FrameExtent* ext) { frame_rows_ = ext; }

    int64_t frame_maxsize() const { return frame_maxsize_; }
    void set_frame_maxsize(int64_t s) { frame_maxsize_ = s; }
    int64_t GetHistoryRangeStart() const {
        if (nullptr == frame_rows_ && nullptr == frame_range_) {
            return INT64_MIN;
        }
        if (nullptr == frame_rows_) {
            return nullptr == frame_range_ || nullptr == frame_range_->start() ? INT64_MIN
                   : frame_range_->GetStartOffset() > 0                        ? 0
                                                                               : frame_range_->GetStartOffset();
        } else {
            return nullptr == frame_range_ || nullptr == frame_range_->start() ? 0
                   : frame_range_->GetStartOffset() > 0                        ? 0
                                                                               : frame_range_->GetStartOffset();
        }
    }
    int64_t GetHistoryRangeEnd() const {
        return nullptr == frame_range_ || nullptr == frame_range_->end() ? 0
               : frame_range_->GetEndOffset() > 0                        ? 0
                                                                         : frame_range_->GetEndOffset();
    }

    int64_t GetHistoryRowsStartPreceding() const { return -1 * GetHistoryRowsStart(); }
    int64_t GetHistoryRowsStart() const {
        if (nullptr == frame_rows_ && nullptr == frame_range_) {
            return INT64_MIN;
        }
        if (nullptr == frame_range_) {
            return nullptr == frame_rows_ || nullptr == frame_rows_->start() ? INT64_MIN
                   : frame_rows_->GetStartOffset() > 0                       ? 0
                                                                             : frame_rows_->GetStartOffset();
        } else {
            return nullptr == frame_rows_ || nullptr == frame_rows_->start() ? 0
                   : frame_rows_->GetStartOffset() > 0                       ? 0
                                                                             : frame_rows_->GetStartOffset();
        }
    }
    int64_t GetHistoryRowsEnd() const {
        if (nullptr == frame_rows_ && nullptr == frame_range_) {
            return INT64_MIN;
        }
        if (nullptr == frame_range_) {
            return nullptr == frame_rows_ || nullptr == frame_rows_->start() ? INT64_MIN
                                                                             : frame_rows_->GetEndOffset();
        } else {
            return nullptr == frame_rows_ || nullptr == frame_rows_->start() ? 0
                   : frame_rows_->GetEndOffset() > 0                         ? 0
                                                                             : frame_rows_->GetEndOffset();
        }
    }

    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;
    const std::string GetExprString() const;
    bool CanMergeWith(const FrameNode *that, const bool enbale_merge_with_maxsize = true) const;
    inline bool IsRowsRangeLikeFrame() const {
        return kFrameRowsRange == frame_type_ || kFrameRowsMergeRowsRange == frame_type_;
    }
    inline bool IsRowsRangeLikeMaxSizeFrame() const { return IsRowsRangeLikeFrame() && frame_maxsize_ > 0; }
    bool IsPureHistoryFrame() const {
        switch (frame_type_) {
            case kFrameRows: {
                return GetHistoryRowsEnd() < 0;
            }
            case kFrameRowsRange: {
                return GetHistoryRangeEnd() < 0;
            }
            case kFrameRowsMergeRowsRange: {
                return GetHistoryRangeEnd() < 0 && GetHistoryRowsEnd() < 0;
            }
            case kFrameRange: {
                return false;
            }
        }
        return false;
    }

    FrameNode* ShadowCopy(node::NodeManager* nm) const override;

    mutable bool exclude_current_row_ = false;

 private:
    FrameType frame_type_;
    FrameExtent *frame_range_;
    FrameExtent *frame_rows_;
    int64_t frame_maxsize_;
};

class WindowDefNode : public SqlNode {
 public:
    WindowDefNode()
        : SqlNode(kWindowDef, 0, 0),
          window_name_(""),
          frame_ptr_(NULL),
          union_tables_(nullptr),
          partitions_(nullptr),
          orders_(nullptr) {}

    ~WindowDefNode() {}

    const std::string &GetName() const { return window_name_; }

    void SetName(const std::string &name) { window_name_ = name; }

    ExprListNode *GetPartitions() const { return partitions_; }

    OrderByNode *GetOrders() const { return orders_; }

    FrameNode *GetFrame() const { return frame_ptr_; }

    void SetPartitions(ExprListNode *partitions) { partitions_ = partitions; }
    void SetOrders(OrderByNode *orders) { orders_ = orders; }
    void SetFrame(FrameNode *frame) { frame_ptr_ = frame; }

    SqlNodeList *union_tables() const { return union_tables_; }
    void set_union_tables(SqlNodeList *union_table) { union_tables_ = union_table; }

    const bool instance_not_in_window() const { return instance_not_in_window_; }
    void set_instance_not_in_window(bool instance_not_in_window) { instance_not_in_window_ = instance_not_in_window; }
    const bool exclude_current_time() const { return exclude_current_time_; }

    void set_exclude_current_time(bool exclude_current_time) { exclude_current_time_ = exclude_current_time; }
    bool exclude_current_row() const { return frame_ptr_ ? frame_ptr_->exclude_current_row_ : false; }

    void Print(std::ostream &output, const std::string &org_tab) const;
    bool Equals(const SqlNode *that) const override;
    bool CanMergeWith(const WindowDefNode *that, const bool enable_window_maxsize_merged = true) const;

    // shadow copy all fields except window_name_
    WindowDefNode* ShadowCopy(NodeManager* nm) const override;

 private:
    std::string window_name_;   /* window's own name */
    FrameNode *frame_ptr_;      /* expression for starting bound, if any */
    SqlNodeList *union_tables_; /* union other table in window */
    ExprListNode *partitions_;  /* PARTITION BY expression list */
    OrderByNode *orders_;       /* ORDER BY (list of SortBy) */

    bool exclude_current_time_ = false;
    bool instance_not_in_window_ = false;
};

class AllNode : public ExprNode {
 public:
    AllNode() : ExprNode(kExprAll), relation_name_("") {}

    explicit AllNode(const std::string &relation_name) : ExprNode(kExprAll), relation_name_(relation_name) {}
    explicit AllNode(const std::string &relation_name, const std::string &db_name)
        : ExprNode(kExprAll), relation_name_(relation_name), db_name_(db_name) {}

    ~AllNode() {}
    const std::string& GetRelationName() const { return relation_name_; }
    const std::string& GetDBName() const { return db_name_; }

    void SetRelationName(const std::string &relation_name) { relation_name_ = relation_name; }
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *that) const;
    AllNode *ShadowCopy(NodeManager *) const override;

 private:
    std::string relation_name_;
    std::string db_name_;
};

class FnDefNode : public SqlNode {
 public:
    explicit FnDefNode(const SqlNodeType &type) : SqlNode(type, 0, 0) {}
    virtual const TypeNode *GetReturnType() const = 0;
    virtual bool IsReturnNullable() const = 0;
    virtual size_t GetArgSize() const = 0;
    virtual const TypeNode *GetArgType(size_t i) const = 0;
    virtual bool IsArgNullable(size_t i) const = 0;
    virtual const std::string GetName() const = 0;
    virtual base::Status Validate(const std::vector<const TypeNode *> &arg_types) const = 0;

    std::string GetFlatString() const override;

    virtual FnDefNode *ShadowCopy(NodeManager *) const = 0;
    virtual FnDefNode *DeepCopy(NodeManager *) const = 0;

    virtual bool RequireListAt(ExprAnalysisContext *ctx, size_t index) const;
    virtual bool IsListReturn(ExprAnalysisContext *ctx) const;
};

class CastExprNode : public ExprNode {
 public:
    explicit CastExprNode(const node::DataType cast_type, node::ExprNode *expr)
        : ExprNode(kExprCast), cast_type_(cast_type) {
        this->AddChild(expr);
    }

    ~CastExprNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *that) const;
    CastExprNode *ShadowCopy(NodeManager *) const override;
    static CastExprNode *CastFrom(ExprNode *node);

    ExprNode *expr() const { return GetChild(0); }
    const DataType cast_type_;

    Status InferAttr(ExprAnalysisContext *ctx) override;
};

class WhenExprNode : public ExprNode {
 public:
    WhenExprNode(node::ExprNode *when_expr, node::ExprNode *then_expr) : ExprNode(kExprWhen) {
        this->AddChild(when_expr);
        this->AddChild(then_expr);
    }
    ~WhenExprNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *that) const;
    WhenExprNode *ShadowCopy(NodeManager *) const override;

    ExprNode *when_expr() const { return GetChild(0); }
    ExprNode *then_expr() const { return GetChild(1); }
    Status InferAttr(ExprAnalysisContext *ctx) override;
};

class CaseWhenExprNode : public ExprNode {
 public:
    CaseWhenExprNode(node::ExprListNode *when_expr_list, node::ExprNode *else_expr) : ExprNode(kExprCase) {
        this->AddChild(when_expr_list);
        this->AddChild(else_expr);
    }
    ~CaseWhenExprNode() {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *that) const;
    CaseWhenExprNode *ShadowCopy(NodeManager *) const override;

    ExprListNode *when_expr_list() const { return dynamic_cast<ExprListNode *>(GetChild(0)); }
    ExprNode *else_expr() const { return GetChild(1); }
    Status InferAttr(ExprAnalysisContext *ctx) override;
};

class CallExprNode : public ExprNode {
 public:
    explicit CallExprNode(FnDefNode *fn_def, ExprListNode *args, const WindowDefNode *over)
        : ExprNode(kExprCall), fn_def_(fn_def), over_(over) {
        if (args != nullptr) {
            for (size_t i = 0; i < args->GetChildNum(); ++i) {
                this->AddChild(args->GetChild(i));
            }
        }
    }

    ~CallExprNode() {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *that) const;

    CallExprNode *ShadowCopy(NodeManager *) const override;
    CallExprNode *DeepCopy(NodeManager *) const override;

    const WindowDefNode *GetOver() const { return over_; }

    void SetOver(WindowDefNode *over) { over_ = over; }

    FnDefNode *GetFnDef() const { return fn_def_; }
    void SetFnDef(FnDefNode *fn_def) { fn_def_ = fn_def; }

    Status InferAttr(ExprAnalysisContext *ctx) override;
    bool RequireListAt(ExprAnalysisContext *ctx, size_t index) const override;
    bool IsListReturn(ExprAnalysisContext *ctx) const override;

 private:
    FnDefNode *fn_def_;
    const WindowDefNode *over_;
};

class QueryExpr : public ExprNode {
 public:
    explicit QueryExpr(const QueryNode *query) : ExprNode(kExprQuery), query_(query) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const ExprNode *node) const;
    const std::string GetExprString() const;
    QueryExpr *ShadowCopy(NodeManager *) const override;

    const QueryNode *query_;
};

class BinaryExpr : public ExprNode {
 public:
    BinaryExpr() : ExprNode(kExprBinary) {}
    explicit BinaryExpr(FnOperator op) : ExprNode(kExprBinary), op_(op) {}
    FnOperator GetOp() const { return op_; }

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *node) const;
    BinaryExpr *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;

    static BinaryExpr *CastFrom(ExprNode *node) {
        return dynamic_cast<BinaryExpr *>(node);
    }

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
    virtual bool Equals(const ExprNode *node) const;
    UnaryExpr *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;

 private:
    FnOperator op_;
};

class CondExpr : public ExprNode {
 public:
    CondExpr(ExprNode *condition, ExprNode *left, ExprNode *right) : ExprNode(kExprCond) {
        AddChild(condition);
        AddChild(left);
        AddChild(right);
    }
    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *node) const;
    CondExpr *ShadowCopy(NodeManager *) const override;

    ExprNode *GetCondition() const;
    ExprNode *GetLeft() const;
    ExprNode *GetRight() const;

    Status InferAttr(ExprAnalysisContext *ctx) override;
};

class ExprIdNode : public ExprNode {
 public:
    ExprIdNode() : ExprNode(kExprId) {}
    explicit ExprIdNode(const std::string &name, size_t id) : ExprNode(kExprId), name_(name), id_(id) {}
    const std::string &GetName() const { return name_; }
    int64_t GetId() const { return id_; }
    void SetId(int64_t id) { id_ = id; }
    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string GetExprString() const override;
    bool Equals(const ExprNode *node) const override;

    ExprIdNode *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;

    bool IsResolved() const { return id_ >= 0; }

    bool IsListReturn(ExprAnalysisContext *ctx) const;

 private:
    std::string name_;
    int64_t id_;
};

class ColumnRefNode : public ExprNode {
 public:
    ColumnRefNode() : ExprNode(kExprColumnRef), column_name_(""), relation_name_("") {}

    ColumnRefNode(const std::string &column_name, const std::string &relation_name)
        : ExprNode(kExprColumnRef), column_name_(column_name), relation_name_(relation_name), db_name_("") {}

    ColumnRefNode(const std::string &column_name, const std::string &relation_name, const std::string &db_name)
        : ExprNode(kExprColumnRef), column_name_(column_name), relation_name_(relation_name), db_name_(db_name) {}

    std::string GetDBName() const { return db_name_; }

    void SetDBName(const std::string &db_name) { db_name_ = db_name; }

    std::string GetRelationName() const { return relation_name_; }

    void SetRelationName(const std::string &relation_name) { relation_name_ = relation_name; }

    const std::string &GetColumnName() const { return column_name_; }

    void SetColumnName(const std::string &column_name) { column_name_ = column_name; }

    static ColumnRefNode *CastFrom(ExprNode *node);
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    const std::string GenerateExpressionName() const;
    virtual bool Equals(const ExprNode *node) const;
    ColumnRefNode *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;

    bool IsListReturn(ExprAnalysisContext *ctx) const override { return true; }

 private:
    std::string column_name_;
    std::string relation_name_;
    std::string db_name_;
};

class ColumnIdNode : public ExprNode {
 public:
    explicit ColumnIdNode(size_t column_id) : ExprNode(kExprColumnId), column_id_(column_id) {}

    size_t GetColumnID() const { return column_id_; }

    static ColumnIdNode *CastFrom(ExprNode *node);
    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    const std::string GenerateExpressionName() const;
    virtual bool Equals(const ExprNode *node) const;
    ColumnIdNode *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;

 private:
    size_t column_id_;
};

class GetFieldExpr : public ExprNode {
 public:
    GetFieldExpr(ExprNode *input, const std::string &column_name, size_t column_id)
        : ExprNode(kExprGetField), column_name_(column_name), column_id_(column_id) {
        this->AddChild(input);
    }

    std::string GetColumnName() const { return column_name_; }
    size_t GetColumnID() const { return column_id_; }
    ExprNode *GetRow() const { return GetChild(0); }

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    const std::string GenerateExpressionName() const;
    virtual bool Equals(const ExprNode *node) const;
    GetFieldExpr *ShadowCopy(NodeManager *) const override;

    Status InferAttr(ExprAnalysisContext *ctx) override;

 private:
    std::string column_name_;
    size_t column_id_;
};

class BetweenExpr : public ExprNode {
 public:
    BetweenExpr(ExprNode *expr, ExprNode *left, ExprNode *right):
        BetweenExpr(expr, left, right, false) {}
    BetweenExpr(ExprNode* lhs, ExprNode* low, ExprNode* high, bool is_not_between)
        : ExprNode(kExprBetween), is_not_between_(is_not_between) {
        AddChild(lhs);
        AddChild(low);
        AddChild(high);
    }
    ~BetweenExpr() {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GetExprString() const;
    virtual bool Equals(const ExprNode *node) const;
    BetweenExpr *ShadowCopy(NodeManager *) const override;

    void set_is_not_between(const bool flag) { is_not_between_ = flag; }
    const bool is_not_between() const { return is_not_between_; }

    ExprNode* GetLhs() const { return GetChildNum() > 0 ? GetChild(0) : nullptr; }
    ExprNode* GetLow() const { return GetChildNum() > 1 ? GetChild(1) : nullptr; }
    ExprNode* GetHigh() const { return GetChildNum() > 2 ? GetChild(2) : nullptr; }

    Status InferAttr(ExprAnalysisContext *ctx) override;

 private:
    bool is_not_between_;
};

class InExpr : public ExprNode {
 public:
    explicit InExpr(ExprNode* lhs, ExprNode* in_list, bool is_not)
        : ExprNode(kExprIn), is_not_(is_not) {
        AddChild(lhs);
        AddChild(in_list);
    }
    ~InExpr() {}

    const bool IsNot() const { return is_not_; }
    ExprNode* GetLhs() const { return GetChildOrNull(0); }
    ExprNode* GetInList() const { return GetChildOrNull(1); }

    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string GetExprString() const override;
    bool Equals(const ExprNode *node) const override;
    InExpr *ShadowCopy(NodeManager *) const override;
    Status InferAttr(ExprAnalysisContext *ctx) override;

    std::string GetInTypeString() const;

 private:
    const bool is_not_ = false;
};

class EscapedExpr : public ExprNode {
 public:
    EscapedExpr(ExprNode* pattern, ExprNode* escape)
        : ExprNode(kExprEscaped) {
        AddChild(pattern);
        AddChild(escape);
    }
    ~EscapedExpr() {}

    void Print(std::ostream &output, const std::string &org_tab) const override;
    const std::string GetExprString() const override;
    EscapedExpr *ShadowCopy(NodeManager *) const override;
    Status InferAttr(ExprAnalysisContext *ctx) override;

    ExprNode* GetPattern() const {
        return GetChildOrNull(0);
    }

    ExprNode* GetEscape() const {
        return GetChildOrNull(1);
    }
};

class ResTarget : public SqlNode {
 public:
    ResTarget() : SqlNode(kResTarget, 0, 0), name_(""), val_(nullptr) {}

    ResTarget(const std::string &name, ExprNode *val) : SqlNode(kResTarget, 0, 0), name_(name), val_(val) {}

    ~ResTarget() {}

    std::string GetName() const { return name_; }

    ExprNode *GetVal() const { return val_; }

    void Print(std::ostream &output, const std::string &org_tab) const;
    virtual bool Equals(const SqlNode *node) const;

 private:
    std::string name_;            /* column name or NULL */
    ExprNode *val_;               /* the value expression to compute or assign */
    NodePointVector indirection_; /* subscripts, field names, and '*', or NIL */
};

class ColumnDefNode : public SqlNode {
 public:
    ColumnDefNode() : SqlNode(kColumnDesc, 0, 0), column_name_(""), column_type_() {}
    ColumnDefNode(const std::string &name, const DataType &data_type, bool op_not_null, ExprNode *default_value)
        : SqlNode(kColumnDesc, 0, 0),
          column_name_(name),
          column_type_(data_type),
          op_not_null_(op_not_null),
          default_value_(default_value) {}
    ~ColumnDefNode() {}

    std::string GetColumnName() const { return column_name_; }

    DataType GetColumnType() const { return column_type_; }

    ExprNode* GetDefaultValue() const { return default_value_; }

    bool GetIsNotNull() const { return op_not_null_; }
    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string column_name_;
    DataType column_type_;
    bool op_not_null_;
    ExprNode* default_value_ = nullptr;
};

class InsertStmt : public SqlNode {
 public:
    InsertStmt(const std::string &db_name,
               const std::string &table_name,
               const std::vector<std::string> &columns,
               const std::vector<ExprNode *> &values)
        : SqlNode(kInsertStmt, 0, 0),
          db_name_(db_name),
          table_name_(table_name),
          columns_(columns),
          values_(values),
          is_all_(columns.empty()) {}

    InsertStmt(const std::string &db_name, const std::string &table_name, const std::vector<ExprNode *> &values)
        : SqlNode(kInsertStmt, 0, 0), db_name_(db_name), table_name_(table_name), values_(values), is_all_(true) {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const std::string db_name_;
    const std::string table_name_;
    const std::vector<std::string> columns_;
    const std::vector<ExprNode *> values_;
    const bool is_all_;
};

class StorageModeNode : public SqlNode {
 public:
    StorageModeNode() : SqlNode(kStorageMode, 0, 0), storage_mode_(kMemory) {}

    explicit StorageModeNode(StorageMode storage_mode)
        : SqlNode(kStorageMode, 0, 0), storage_mode_(storage_mode) {}

    ~StorageModeNode() {}

    StorageMode GetStorageMode() const { return storage_mode_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    StorageMode storage_mode_;
};

class CreateTableLikeClause {
 public:
    CreateTableLikeClause() = default;
    enum LikeKind { PARQUET = 0, HIVE = 1 };

    static std::string ToKindString(LikeKind kind) {
        switch (kind) {
            case PARQUET:
                return "PARQUET";
            case HIVE:
                return "HIVE";
            default:
                return "UNKNOWN";
        }
    }

    void Print(std::ostream &, const std::string &) const;

    LikeKind kind_;
    std::string path_;
};

class CreateStmt : public SqlNode {
 public:
    CreateStmt()
        : SqlNode(kCreateStmt, 0, 0),
          table_name_(""),
          op_if_not_exist_(false) {}

    CreateStmt(const std::string &db_name, const std::string &table_name, bool op_if_not_exist)
        : SqlNode(kCreateStmt, 0, 0), db_name_(db_name), table_name_(table_name), op_if_not_exist_(op_if_not_exist) {}

    ~CreateStmt() {}

    NodePointVector* MutableColumnDefList() { return &column_desc_list_; }
    const NodePointVector &GetColumnDefList() const { return column_desc_list_; }

    std::string GetTableName() const { return table_name_; }
    std::string GetDbName() const { return db_name_; }

    bool GetOpIfNotExist() const { return op_if_not_exist_; }

    NodePointVector* MutableTableOptionList() { return &table_option_list_; }
    const NodePointVector &GetTableOptionList() const { return table_option_list_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

    // refactor later, I'd keep it simple currently
    std::shared_ptr<CreateTableLikeClause> like_clause_ = nullptr;

 private:
    std::string db_name_;
    std::string table_name_;
    NodePointVector column_desc_list_;
    NodePointVector table_option_list_;
    bool op_if_not_exist_;
};
class IndexKeyNode : public SqlNode {
 public:
    IndexKeyNode() : SqlNode(kIndexKey, 0, 0) {}
    explicit IndexKeyNode(const std::string &key) : SqlNode(kIndexKey, 0, 0), key_({key}) {}
    explicit IndexKeyNode(const std::vector<std::string> &keys) : SqlNode(kIndexKey, 0, 0), key_(keys) {}
    ~IndexKeyNode() {}
    void AddKey(const std::string &key) { key_.push_back(key); }
    std::vector<std::string> &GetKey() { return key_; }

 private:
    std::vector<std::string> key_;
};
class IndexVersionNode : public SqlNode {
 public:
    IndexVersionNode() : SqlNode(kIndexVersion, 0, 0) {}
    explicit IndexVersionNode(const std::string &column_name)
        : SqlNode(kIndexVersion, 0, 0), column_name_(column_name), count_(1) {}
    IndexVersionNode(const std::string &column_name, int count)
        : SqlNode(kIndexVersion, 0, 0), column_name_(column_name), count_(count) {}

    std::string &GetColumnName() { return column_name_; }

    int GetCount() const { return count_; }

 private:
    std::string column_name_;
    int count_;
};
class IndexTsNode : public SqlNode {
 public:
    IndexTsNode() : SqlNode(kIndexTs, 0, 0) {}
    explicit IndexTsNode(const std::string &column_name) : SqlNode(kIndexTs, 0, 0), column_name_(column_name) {}

    std::string &GetColumnName() { return column_name_; }

 private:
    std::string column_name_;
};
class IndexTTLNode : public SqlNode {
 public:
    IndexTTLNode() : SqlNode(kIndexTTL, 0, 0) {}
    explicit IndexTTLNode(ExprListNode *expr) : SqlNode(kIndexTTL, 0, 0), ttl_expr_(expr) {}

    ExprListNode *GetTTLExpr() const { return ttl_expr_; }

 private:
    ExprListNode *ttl_expr_;
};
class IndexTTLTypeNode : public SqlNode {
 public:
    IndexTTLTypeNode() : SqlNode(kIndexTTLType, 0, 0) {}
    explicit IndexTTLTypeNode(const std::string &ttl_type) : SqlNode(kIndexTTLType, 0, 0), ttl_type_(ttl_type) {}

    void set_ttl_type(const std::string &ttl_type) { this->ttl_type_ = ttl_type; }
    const std::string &ttl_type() const { return ttl_type_; }

 private:
    std::string ttl_type_;
};

class ColumnIndexNode : public SqlNode {
 public:
    ColumnIndexNode()
        : SqlNode(kColumnIndex, 0, 0),
          ts_(""),
          version_(""),
          version_count_(0),
          abs_ttl_(-2),
          lat_ttl_(-2),
          ttl_type_(""),
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

    const std::string &ttl_type() const { return ttl_type_; }
    void set_ttl_type(const std::string &ttl_type) { ttl_type_ = ttl_type; }

    int64_t GetAbsTTL() const { return abs_ttl_; }
    int64_t GetLatTTL() const { return lat_ttl_; }

    void SetTTL(ExprListNode *ttl_node_list);

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::vector<std::string> key_;
    std::string ts_;
    std::string version_;
    int version_count_;
    int64_t abs_ttl_;
    int64_t lat_ttl_;
    std::string ttl_type_;
    std::string name_;
};
class CmdNode : public SqlNode {
 public:
    explicit CmdNode(node::CmdType cmd_type) : SqlNode(kCmdStmt, 0, 0), cmd_type_(cmd_type) {}

    ~CmdNode() {}

    void AddArg(const std::string &arg) { args_.push_back(arg); }
    const std::vector<std::string> &GetArgs() const { return args_; }

    void Print(std::ostream &output, const std::string &org_tab) const;
    bool Equals(const SqlNode *node) const override;

    const node::CmdType GetCmdType() const { return cmd_type_; }

    bool IsIfNotExists() const {
        return if_not_exist_;
    }

    void SetIfNotExists(bool b) {
        if_not_exist_ = b;
    }

    bool IsIfExists() const {
        return if_exist_;
    }

    void SetIfExists(bool b) {
        if_exist_ = b;
    }

 private:
    node::CmdType cmd_type_;
    std::vector<std::string> args_;
    bool if_not_exist_ = false;
    bool if_exist_ = false;
};

enum class DeleteTarget {
    JOB = 1,
    TABLE = 2,
};
std::string DeleteTargetString(DeleteTarget target);

class DeleteNode : public SqlNode {
 public:
    DeleteNode(DeleteTarget t, std::string job_id,
            const std::string& db_name, const std::string& table_name, const node::ExprNode* where_expr)
        : SqlNode(kDeleteStmt, 0, 0), target_(t), job_id_(job_id),
         db_name_(db_name), table_name_(table_name), condition_(where_expr) {}
    ~DeleteNode() = default;

    void Print(std::ostream &output, const std::string &org_tab) const override;
    std::string GetTargetString() const;

    const DeleteTarget GetTarget() const { return target_; }
    const std::string& GetJobId() const { return job_id_; }
    const std::string& GetTableName() const { return table_name_; }
    const std::string& GetDbName() const { return db_name_; }
    const ExprNode* GetCondition() const { return condition_; }

 private:
    const DeleteTarget target_;
    const std::string job_id_;
    const std::string db_name_;
    const std::string table_name_;
    const ExprNode *condition_;
};

class SelectIntoNode : public SqlNode {
 public:
    SelectIntoNode(const QueryNode *query, const std::string &query_str, const std::string &out,
                   const std::shared_ptr<OptionsMap>&& options, const std::shared_ptr<OptionsMap>&& op2)
        : SqlNode(kSelectIntoStmt, 0, 0),
          query_(query),
          query_str_(query_str),
          out_file_(out),
          options_(options),
          config_options_(op2) {}
    ~SelectIntoNode() {}

    const QueryNode* Query() const { return query_; }
    const std::string& QueryStr() const { return query_str_; }
    const std::string& OutFile() const { return out_file_; }
    const std::shared_ptr<OptionsMap> Options() const { return options_; }
    const std::shared_ptr<OptionsMap> ConfigOptions() const { return config_options_; }

    void Print(std::ostream& output, const std::string& org_tab) const override;

 private:
    const QueryNode* query_;
    const std::string query_str_;
    const std::string out_file_;
    // optional options for load data, e.g csv related options
    const std::shared_ptr<OptionsMap> options_;
    // optinal config option for load data, to config offline job parameters
    const std::shared_ptr<OptionsMap> config_options_ = nullptr;
};

class CreateFunctionNode : public SqlNode {
 public:
     CreateFunctionNode(const std::string& function_name, const TypeNode* return_type,
             const NodePointVector& args_type, bool is_aggregate, std::shared_ptr<OptionsMap> options)
        : SqlNode(kCreateFunctionStmt, 0, 0),
          function_name_(function_name),
          return_type_(return_type),
          args_type_(args_type),
          is_aggregate_(is_aggregate),
          options_(options) {}
    const std::string& Name() const { return function_name_; }
    const TypeNode* GetReturnType() const { return return_type_; }
    const NodePointVector& GetArgsType() const { return args_type_; }
    bool IsAggregate() const { return is_aggregate_; }
    const std::shared_ptr<OptionsMap> Options() const { return options_; }
    void Print(std::ostream& output, const std::string& org_tab) const override;
 private:
    const std::string function_name_;
    const TypeNode* return_type_;
    const NodePointVector args_type_;
    const bool is_aggregate_;
    const std::shared_ptr<OptionsMap> options_;
};

class LoadDataNode : public SqlNode {
 public:
    LoadDataNode(const std::string &f, const std::string &db, const std::string &table,
                          const std::shared_ptr<OptionsMap>&& op, const std::shared_ptr<OptionsMap>&& op2)
        : SqlNode(kLoadDataStmt, 0, 0), file_(f), db_(db), table_(table), options_(op), config_options_(op2) {}
    ~LoadDataNode() {}

    const std::string& File() const { return file_; }
    const std::string& Db() const { return db_; }
    const std::string& Table() const { return table_; }
    const std::shared_ptr<OptionsMap> Options() const { return options_; }
    const std::shared_ptr<OptionsMap> ConfigOptions() const { return config_options_; }

    void Print(std::ostream &output, const std::string &org_tab) const override;

 private:
    const std::string file_;
    const std::string db_;
    const std::string table_;
    // TODO(aceforeverd): extend value to other type like list
    //    replace map with optimized class
    // optional options for load data, e.g csv related options
    const std::shared_ptr<OptionsMap> options_ = nullptr;
    // optinal config option for load data, to config offline job parameters
    const std::shared_ptr<OptionsMap> config_options_ = nullptr;
};

class SetNode : public SqlNode {
 public:
    explicit SetNode(const node::VariableScope scope, const std::string& key, const ConstNode* value)
        : SqlNode(kSetStmt, 0, 0), scope_(scope), key_(key), value_(value) {}
    ~SetNode() {}

    const node::VariableScope Scope() const { return scope_; }
    const std::string& Key() const { return key_; }
    const ConstNode* Value() const { return value_; }

    void Print(std::ostream& output, const std::string& org_tab) const override;

 private:
    const node::VariableScope scope_;
    const std::string key_;
    const ConstNode* value_;
};

class CreateIndexNode : public SqlNode {
 public:
    explicit CreateIndexNode(const std::string &index_name, const std::string &db_name,
                             const std::string &table_name, ColumnIndexNode *index)
        : SqlNode(kCreateIndexStmt, 0, 0), index_name_(index_name),
          db_name_(db_name), table_name_(table_name), index_(index) {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const std::string index_name_;
    const std::string db_name_;
    const std::string table_name_;
    node::ColumnIndexNode *index_;
};

class ExplainNode : public SqlNode {
 public:
    explicit ExplainNode(const QueryNode *query, node::ExplainType explain_type)
        : SqlNode(kExplainStmt, 0, 0), explain_type_(explain_type), query_(query) {}
    void Print(std::ostream &output, const std::string &org_tab) const;

    const node::ExplainType explain_type_;
    const node::QueryNode *query_;
};

class DeployNode : public SqlNode {
 public:
    explicit DeployNode(const std::string &name, const SqlNode *stmt, const std::string &stmt_str,
                        const std::shared_ptr<OptionsMap> options, bool if_not_exists)
        : SqlNode(kDeployStmt, 0, 0),
          name_(name),
          stmt_(stmt),
          stmt_str_(stmt_str),
          if_not_exists_(if_not_exists),
          options_(options) {}
    ~DeployNode() {}

    const std::string& Name() const { return name_; }
    const SqlNode* Stmt() const { return stmt_; }
    const bool IsIfNotExists() const { return if_not_exists_; }
    const std::string& StmtStr() const { return stmt_str_; }
    const std::shared_ptr<OptionsMap> Options() const { return options_; }

    void Print(std::ostream& output, const std::string& tab) const override;

 private:
    const std::string name_;
    const SqlNode* stmt_ = nullptr;
    const std::string stmt_str_;
    const bool if_not_exists_ = false;
    const std::shared_ptr<OptionsMap> options_;
};

class FnParaNode : public FnNode {
 public:
    explicit FnParaNode(ExprIdNode *para_id) : FnNode(kFnPara), para_id_(para_id) {}
    const std::string &GetName() const { return para_id_->GetName(); }

    const TypeNode *GetParaType() const { return para_id_->GetOutputType(); }

    ExprIdNode *GetExprId() const { return para_id_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    ExprIdNode *para_id_;
};
class FnNodeFnHeander : public FnNode {
 public:
    FnNodeFnHeander(const std::string &name, FnNodeList *parameters, const TypeNode *ret_type)
        : FnNode(kFnHeader), name_(name), parameters_(parameters), ret_type_(ret_type) {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    const std::string GeIRFunctionName() const;
    const std::string name_;
    const FnNodeList *parameters_;
    const TypeNode *ret_type_;
};
class FnNodeFnDef : public FnNode {
 public:
    FnNodeFnDef(const FnNodeFnHeander *header, FnNodeList *block) : FnNode(kFnDef), header_(header), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    const FnNodeFnHeander *header_;
    FnNodeList *block_;
};

class FnAssignNode : public FnNode {
 public:
    explicit FnAssignNode(ExprIdNode *var, ExprNode *expression)
        : FnNode(kFnAssignStmt), var_(var), expression_(expression), is_ssa_(false) {}
    std::string GetName() const { return var_->GetName(); }
    const bool IsSSA() const { return is_ssa_; }
    void EnableSSA() { is_ssa_ = true; }
    void DisableSSA() { is_ssa_ = false; }
    void Print(std::ostream &output, const std::string &org_tab) const;
    node::ExprIdNode *var_;
    ExprNode *expression_;

 private:
    bool is_ssa_;
};

class FnIfNode : public FnNode {
 public:
    explicit FnIfNode(ExprNode *expression) : FnNode(kFnIfStmt), expression_(expression) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    ExprNode *expression_;
};
class FnElifNode : public FnNode {
 public:
    explicit FnElifNode(ExprNode *expression) : FnNode(kFnElifStmt), expression_(expression) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    ExprNode *expression_;
};
class FnElseNode : public FnNode {
 public:
    FnElseNode() : FnNode(kFnElseStmt) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
};

class FnForInNode : public FnNode {
 public:
    FnForInNode(ExprIdNode *var, ExprNode *in_expression)
        : FnNode(kFnForInStmt), var_(var), in_expression_(in_expression) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    ExprIdNode *var_;
    ExprNode *in_expression_;
};

class FnIfBlock : public FnNode {
 public:
    FnIfBlock(FnIfNode *node, FnNodeList *block) : FnNode(kFnIfBlock), if_node(node), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    FnIfNode *if_node;
    FnNodeList *block_;
};

class FnElifBlock : public FnNode {
 public:
    FnElifBlock(FnElifNode *node, FnNodeList *block) : FnNode(kFnElifBlock), elif_node_(node), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    FnElifNode *elif_node_;
    FnNodeList *block_;
};
class FnElseBlock : public FnNode {
 public:
    explicit FnElseBlock(FnNodeList *block) : FnNode(kFnElseBlock), block_(block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    FnNodeList *block_;
};
class FnIfElseBlock : public FnNode {
 public:
    FnIfElseBlock(FnIfBlock *if_block, const std::vector<FnNode *> &elif_blocks, FnElseBlock *else_block)
        : FnNode(kFnIfElseBlock), if_block_(if_block), elif_blocks_(elif_blocks), else_block_(else_block) {}
    void Print(std::ostream &output, const std::string &org_tab) const;
    FnIfBlock *if_block_;
    std::vector<FnNode *> elif_blocks_;
    FnElseBlock *else_block_;
};

class FnForInBlock : public FnNode {
 public:
    FnForInBlock(FnForInNode *for_in_node, FnNodeList *block)
        : FnNode(kFnForInBlock), for_in_node_(for_in_node), block_(block) {}

    void Print(std::ostream &output, const std::string &org_tab) const;
    FnForInNode *for_in_node_;
    FnNodeList *block_;
};
class FnReturnStmt : public FnNode {
 public:
    explicit FnReturnStmt(ExprNode *return_expr) : FnNode(kFnReturnStmt), return_expr_(return_expr) {}
    void Print(std::ostream &output, const std::string &org_tab) const override;
    ExprNode *return_expr_;
};

class StructExpr : public ExprNode {
 public:
    explicit StructExpr(const std::string &name) : ExprNode(kExprStruct), class_name_(name) {}
    void SetFileds(FnNodeList *fileds) { fileds_ = fileds; }
    void SetMethod(FnNodeList *methods) { methods_ = methods; }

    const FnNodeList *GetMethods() const { return methods_; }

    const FnNodeList *GetFileds() const { return fileds_; }

    const std::string &GetName() const { return class_name_; }
    void Print(std::ostream &output, const std::string &org_tab) const override;
    StructExpr *ShadowCopy(NodeManager *) const override;

 private:
    const std::string class_name_;
    FnNodeList *fileds_;
    FnNodeList *methods_;
};

class ExternalFnDefNode : public FnDefNode {
 public:
    ExternalFnDefNode(const std::string &name, void *fn_ptr, const node::TypeNode *ret_type, bool ret_nullable,
                      const std::vector<const node::TypeNode *> &arg_types, const std::vector<int> &arg_nullable,
                      int variadic_pos = -1, bool return_by_arg = false)
        : FnDefNode(kExternalFnDef),
          function_name_(name),
          function_ptr_(fn_ptr),
          ret_type_(ret_type),
          ret_nullable_(ret_nullable),
          arg_types_(arg_types),
          arg_nullable_(arg_nullable),
          variadic_pos_(variadic_pos),
          return_by_arg_(return_by_arg) {}

    const std::string function_name() const { return function_name_; }

    const std::string GetName() const override { return function_name_; }

    void *function_ptr() const { return function_ptr_; }
    const node::TypeNode *ret_type() const { return ret_type_; }
    const std::vector<const node::TypeNode *> &arg_types() const { return arg_types_; }
    int variadic_pos() const { return variadic_pos_; }
    bool return_by_arg() const { return return_by_arg_; }

    void Print(std::ostream &output, const std::string &tab) const override;
    bool Equals(const SqlNode *node) const override;

    void SetReturnType(const node::TypeNode *dtype) { this->ret_type_ = dtype; }
    void SetReturnNullable(bool flag) { this->ret_nullable_ = flag; }

    void SetReturnByArg(bool flag) { this->return_by_arg_ = flag; }

    bool IsResolved() const { return ret_type_ != nullptr; }

    base::Status Validate(const std::vector<const TypeNode *> &arg_types) const override;

    const TypeNode *GetReturnType() const override { return ret_type_; }
    size_t GetArgSize() const override { return arg_types_.size(); }
    const TypeNode *GetArgType(size_t i) const override { return arg_types_[i]; }
    bool IsArgNullable(size_t i) const override { return arg_nullable_[i] > 0; }
    bool IsReturnNullable() const override { return ret_nullable_; }

    bool RequireListAt(ExprAnalysisContext *ctx, size_t index) const override;
    bool IsListReturn(ExprAnalysisContext *ctx) const override;

    ExternalFnDefNode *ShadowCopy(NodeManager *) const override;
    ExternalFnDefNode *DeepCopy(NodeManager *) const override;

 private:
    std::string function_name_;
    void *function_ptr_;

    const node::TypeNode *ret_type_;
    bool ret_nullable_;
    std::vector<const node::TypeNode *> arg_types_;
    std::vector<int> arg_nullable_;

    // eg, variadic_pos_=1 for fn(x, ...);
    // -1 denotes non-variadic
    int variadic_pos_;

    bool return_by_arg_;
};

class DynamicUdfFnDefNode : public FnDefNode {
 public:
    DynamicUdfFnDefNode(const std::string &name, void *fn_ptr, const node::TypeNode *ret_type, bool ret_nullable,
                      const std::vector<const node::TypeNode *> &arg_types, const std::vector<int> &arg_nullable,
                      bool return_by_arg, ExternalFnDefNode *init_node)
        : FnDefNode(kDynamicUdfFnDef),
          function_name_(name),
          function_ptr_(fn_ptr),
          ret_type_(ret_type),
          ret_nullable_(ret_nullable),
          arg_types_(arg_types),
          arg_nullable_(arg_nullable),
          return_by_arg_(return_by_arg),
          init_context_node_(init_node) {}

    const std::string GetName() const override { return function_name_; }

    void *function_ptr() const { return function_ptr_; }
    const node::TypeNode *ret_type() const { return ret_type_; }
    const std::vector<const node::TypeNode *> &arg_types() const { return arg_types_; }
    bool return_by_arg() const { return return_by_arg_; }

    void Print(std::ostream &output, const std::string &tab) const override;
    bool Equals(const SqlNode *node) const override;

    void SetReturnType(const node::TypeNode *dtype) { this->ret_type_ = dtype; }
    void SetReturnNullable(bool flag) { this->ret_nullable_ = flag; }

    void SetReturnByArg(bool flag) { this->return_by_arg_ = flag; }

    bool IsResolved() const { return ret_type_ != nullptr; }

    base::Status Validate(const std::vector<const TypeNode *> &arg_types) const override;

    const TypeNode *GetReturnType() const override { return ret_type_; }
    size_t GetArgSize() const override { return arg_types_.size(); }
    const TypeNode *GetArgType(size_t i) const override { return arg_types_[i]; }
    bool IsArgNullable(size_t i) const override { return arg_nullable_[i] > 0; }
    bool IsReturnNullable() const override { return ret_nullable_; }

    // bool RequireListAt(ExprAnalysisContext *ctx, size_t index) const override;
    bool IsListReturn(ExprAnalysisContext *ctx) const override { return false; };

    DynamicUdfFnDefNode *ShadowCopy(NodeManager *) const override;
    DynamicUdfFnDefNode *DeepCopy(NodeManager *) const override;

    const ExternalFnDefNode *GetInitContextNode() const { return init_context_node_; }

 private:
    std::string function_name_;
    void *function_ptr_;
    const node::TypeNode *ret_type_;
    bool ret_nullable_;
    std::vector<const node::TypeNode *> arg_types_;
    std::vector<int> arg_nullable_;
    bool return_by_arg_;
    ExternalFnDefNode *init_context_node_;
};

class UdfDefNode : public FnDefNode {
 public:
    explicit UdfDefNode(FnNodeFnDef *def) : FnDefNode(kUdfDef), def_(def) {}
    FnNodeFnDef *def() const { return def_; }

    const std::string GetName() const override { return "Udf"; }

    void Print(std::ostream &output, const std::string &tab) const override;
    bool Equals(const SqlNode *node) const override;

    const TypeNode *GetReturnType() const override { return def()->header_->ret_type_; }
    size_t GetArgSize() const override { return def()->header_->parameters_->GetChildren().size(); }
    const TypeNode *GetArgType(size_t i) const {
        auto node = def()->header_->parameters_->GetChildren()[i];
        return dynamic_cast<FnParaNode *>(node)->GetParaType();
    }

    bool IsArgNullable(size_t i) const override { return false; }
    bool IsReturnNullable() const override { return false; }

    base::Status Validate(const std::vector<const TypeNode *> &arg_types) const override { return Status::OK(); }

    UdfDefNode *ShadowCopy(NodeManager *) const override;
    UdfDefNode *DeepCopy(NodeManager *) const override;

 private:
    FnNodeFnDef *def_;
};

class UdfByCodeGenDefNode : public FnDefNode {
 public:
    UdfByCodeGenDefNode(const std::string &name, const std::vector<const node::TypeNode *> &arg_types,
                        const std::vector<int> &arg_nullable, const node::TypeNode *ret_type, bool ret_nullable)
        : FnDefNode(kUdfByCodeGenDef),
          name_(name),
          arg_types_(arg_types),
          arg_nullable_(arg_nullable),
          ret_type_(ret_type),
          ret_nullable_(ret_nullable) {}

    const std::string GetName() const override { return name_; }

    void SetGenImpl(std::shared_ptr<udf::LlvmUdfGenBase> gen_impl) { this->gen_impl_ = gen_impl; }

    std::shared_ptr<udf::LlvmUdfGenBase> GetGenImpl() const { return this->gen_impl_; }

    const TypeNode *GetReturnType() const override { return ret_type_; }
    size_t GetArgSize() const override { return arg_types_.size(); }
    const TypeNode *GetArgType(size_t i) const { return arg_types_[i]; }
    bool IsArgNullable(size_t i) const override { return arg_nullable_[i]; }
    bool IsReturnNullable() const override { return ret_nullable_; }

    base::Status Validate(const std::vector<const TypeNode *> &arg_types) const override { return Status::OK(); }

    void Print(std::ostream &output, const std::string &tab) const override;
    bool Equals(const SqlNode *node) const override;

    UdfByCodeGenDefNode *ShadowCopy(NodeManager *) const override;
    UdfByCodeGenDefNode *DeepCopy(NodeManager *) const override;

 private:
    const std::string name_;
    std::shared_ptr<udf::LlvmUdfGenBase> gen_impl_;
    std::vector<const node::TypeNode *> arg_types_;
    std::vector<int> arg_nullable_;
    const node::TypeNode *ret_type_;
    bool ret_nullable_;
};

class LambdaNode : public FnDefNode {
 public:
    LambdaNode(const std::vector<node::ExprIdNode *> &args, node::ExprNode *body)
        : FnDefNode(kLambdaDef), args_(args), body_(body) {}

    const std::string GetName() const override { return "Lambda"; }

    const TypeNode *GetReturnType() const override { return body_->GetOutputType(); }
    size_t GetArgSize() const override { return args_.size(); }
    const TypeNode *GetArgType(size_t i) const override { return args_[i]->GetOutputType(); }

    bool IsArgNullable(size_t i) const override { return args_[i]->nullable(); }
    bool IsReturnNullable() const override { return body_->nullable(); }

    node::ExprIdNode *GetArg(size_t i) const { return args_[i]; }
    node::ExprNode *body() const { return body_; }

    void SetBody(node::ExprNode *body) { body_ = body; }

    void Print(std::ostream &output, const std::string &tab) const override;
    bool Equals(const SqlNode *node) const override;

    base::Status Validate(const std::vector<const TypeNode *> &arg_types) const override;

    LambdaNode *ShadowCopy(NodeManager *) const override;
    LambdaNode *DeepCopy(NodeManager *) const override;

 private:
    std::vector<node::ExprIdNode *> args_;
    node::ExprNode *body_;
};

class UdafDefNode : public FnDefNode {
 public:
    UdafDefNode(const std::string &name, const std::vector<const TypeNode *> &arg_types, ExprNode *init_expr,
                FnDefNode *update_func, FnDefNode *merge_func, FnDefNode *output_func)
        : FnDefNode(kUdafDef),
          name_(name),
          arg_types_(arg_types),
          init_expr_(init_expr),
          update_(update_func),
          merge_(merge_func),
          output_(output_func) {}

    const std::string GetName() const override { return name_; }

    bool Equals(const SqlNode *node) const override;
    void Print(std::ostream &output, const std::string &tab) const override;

    ExprNode *init_expr() const { return init_expr_; }
    FnDefNode *update_func() const { return update_; }
    FnDefNode *merge_func() const { return merge_; }
    FnDefNode *output_func() const { return output_; }

    bool AllowMerge() const { return merge_ != nullptr; }

    base::Status Validate(const std::vector<const TypeNode *> &arg_types) const override;

    const TypeNode *GetElementType(size_t i) const;
    bool IsElementNullable(size_t i) const;

    const TypeNode *GetStateType() const {
        if (init_expr_ != nullptr) {
            return init_expr_->GetOutputType();
        } else {
            return GetElementType(0);
        }
    }

    const TypeNode *GetReturnType() const override {
        if (output_ != nullptr) {
            return output_->GetReturnType();
        } else {
            return GetStateType();
        }
    }

    bool IsReturnNullable() const override {
        if (output_ != nullptr) {
            return output_->IsReturnNullable();
        } else if (init_expr_ != nullptr) {
            return init_expr_->nullable() || update_->IsReturnNullable();
        } else {
            return true;
        }
    }

    bool IsArgNullable(size_t i) const override { return false; }

    size_t GetArgSize() const override { return arg_types_.size(); }

    const TypeNode *GetArgType(size_t i) const { return arg_types_[i]; }
    const std::vector<const TypeNode *> &GetArgTypeList() const { return arg_types_; }

    UdafDefNode *ShadowCopy(NodeManager *) const override;
    UdafDefNode *DeepCopy(NodeManager *) const override;

 private:
    std::string name_;
    std::vector<const TypeNode *> arg_types_;
    ExprNode *init_expr_;
    FnDefNode *update_;
    FnDefNode *merge_;
    FnDefNode *output_;
};

class PartitionMetaNode : public SqlNode {
 public:
    PartitionMetaNode() : SqlNode(kPartitionMeta, 0, 0), endpoint_(""), role_type_() {}

    PartitionMetaNode(const std::string &endpoint, RoleType role_type)
        : SqlNode(kPartitionMeta, 0, 0), endpoint_(endpoint), role_type_(role_type) {}

    ~PartitionMetaNode() {}

    std::string GetEndpoint() const { return endpoint_; }

    RoleType GetRoleType() const { return role_type_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string endpoint_;
    RoleType role_type_;
};

class ReplicaNumNode : public SqlNode {
 public:
    ReplicaNumNode() : SqlNode(kReplicaNum, 0, 0), replica_num_(1) {}

    explicit ReplicaNumNode(int num) : SqlNode(kReplicaNum, 0, 0), replica_num_(num) {}

    ~ReplicaNumNode() {}

    int GetReplicaNum() const { return replica_num_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    int replica_num_;
};

class PartitionNumNode : public SqlNode {
 public:
    PartitionNumNode() : SqlNode(kPartitionNum, 0, 0), partition_num_(1) {}

    explicit PartitionNumNode(int num) : SqlNode(kPartitionNum, 0, 0), partition_num_(num) {}

    ~PartitionNumNode() {}

    int GetPartitionNum() const { return partition_num_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    int partition_num_;
};

class DistributionsNode : public SqlNode {
 public:
    explicit DistributionsNode(const NodePointVector& distribution_list)
        : SqlNode(kDistributions, 0, 0), distribution_list_(distribution_list) {}

    ~DistributionsNode() {}

    const NodePointVector& GetDistributionList() const { return distribution_list_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    NodePointVector distribution_list_;
};

class CreateSpStmt : public SqlNode {
 public:
    explicit CreateSpStmt(const std::string &sp_name) : SqlNode(kCreateSpStmt, 0, 0), sp_name_(sp_name) {}

    ~CreateSpStmt() {}

    NodePointVector &GetInputParameterList() { return input_parameter_list_; }
    const NodePointVector &GetInputParameterList() const { return input_parameter_list_; }

    NodePointVector &GetInnerNodeList() { return inner_node_list_; }
    const NodePointVector &GetInnerNodeList() const { return inner_node_list_; }

    const std::string &GetSpName() const { return sp_name_; }

    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string sp_name_;
    NodePointVector input_parameter_list_;
    NodePointVector inner_node_list_;
};

class InputParameterNode : public SqlNode {
 public:
    InputParameterNode(const std::string &name, DataType data_type, bool is_constant)
        : SqlNode(kInputParameter, 0, 0), column_name_(name), column_type_(data_type), is_constant_(is_constant) {}
    ~InputParameterNode() {}

    std::string GetColumnName() const { return column_name_; }

    DataType GetColumnType() const { return column_type_; }

    bool GetIsConstant() const { return is_constant_; }
    void Print(std::ostream &output, const std::string &org_tab) const;

 private:
    std::string column_name_;
    DataType column_type_;
    bool is_constant_;
};

std::string ExprString(const ExprNode *expr);
std::string MakeExprWithTable(const ExprNode *expr, const std::string db);
const bool IsNullPrimary(const ExprNode *expr);
bool ExprListNullOrEmpty(const ExprListNode *expr);
bool ExprIsConst(const ExprNode *expr);
bool ExprIsSimple(const ExprNode *expr);
bool ExprListIsConst(const std::vector<node::ExprNode *> &exprs);
bool SqlEquals(const SqlNode *left, const SqlNode *right);
bool SqlListEquals(const SqlNodeList *left, const SqlNodeList *right);
bool ExprEquals(const ExprNode *left, const ExprNode *right);
bool FnDefEquals(const FnDefNode *left, const FnDefNode *right);
bool TypeEquals(const TypeNode *left, const TypeNode *right);

// retrieve the `WindowDefNode` for the `ExprNode`, which is either from
//  `ExprNode` itself inside if it is an anonymous window e.g `fn() over (window)`
//  or find in `windows` map by window name
bool WindowOfExpression(const std::map<std::string, const WindowDefNode *>& windows, ExprNode *node_ptr,
                        const WindowDefNode **output);

bool IsAggregationExpression(const udf::UdfLibrary* lib, const node::ExprNode* node_ptr);
void ColumnOfExpression(const ExprNode *node_ptr,
                        std::vector<const node::ExprNode *> *columns);  // NOLINT
void FillSqlNodeList2NodeVector(SqlNodeList *node_list_ptr,
                                std::vector<SqlNode *> &node_list);  // NOLINT (runtime/references)
void PrintSqlNode(std::ostream &output, const std::string &org_tab, const SqlNode *node_ptr,
                  const std::string &item_name, bool last_child);
void PrintSqlVector(std::ostream &output, const std::string &tab, const NodePointVector &vec,
                    const std::string &vector_name, bool last_item);
void PrintSqlVector(std::ostream &output, const std::string &tab, const std::vector<ExprNode *> &vec,
                    const std::string &vector_name, bool last_item);
void PrintValue(std::ostream &output, const std::string &org_tab, const std::string &value,
                const std::string &item_name, bool last_child);
void PrintValue(std::ostream &output, const std::string &org_tab, const std::vector<std::string> &vec,
                const std::string &item_name, bool last_child);

void PrintValue(std::ostream &output, const std::string &org_tab, const OptionsMap* value,
                const std::string &item_name, bool last_child);
}  // namespace node
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_NODE_SQL_NODE_H_
