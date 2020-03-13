/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * sql_node.cc
 *
 * Author: chenjing
 * Date: 2019/10/11
 *--------------------------------------------------------------------------
 **/

#include "node/sql_node.h"
#include <numeric>
#include <utility>
#include "glog/logging.h"
#include "node/node_manager.h"
namespace fesql {
namespace node {

bool SQLEquals(const SQLNode *left, const SQLNode *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}

bool SQLListEquals(const SQLNodeList *left, const SQLNodeList *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}
bool ExprEquals(const ExprNode *left, const ExprNode *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}
void PrintSQLNode(std::ostream &output, const std::string &org_tab,
                  const SQLNode *node_ptr, const std::string &item_name,
                  bool last_child) {
    output << org_tab << SPACE_ST << item_name << ":";

    if (nullptr == node_ptr) {
        output << " null";
    } else if (last_child) {
        output << "\n";
        node_ptr->Print(output, org_tab + INDENT);
    } else {
        output << "\n";
        node_ptr->Print(output, org_tab + OR_INDENT);
    }
}
void PrintSQLVector(std::ostream &output, const std::string &tab,
                    const std::vector<FnNode *> &vec,
                    const std::string &vector_name, bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]: \n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintSQLNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintSQLNode(output, space, vec[i], "" + std::to_string(i), true);
}
void PrintSQLVector(std::ostream &output, const std::string &tab,
                    const std::vector<ExprNode *> &vec,
                    const std::string &vector_name, bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]: \n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintSQLNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintSQLNode(output, space, vec[i], "" + std::to_string(i), true);
}

void PrintSQLVector(std::ostream &output, const std::string &tab,
                    const std::vector<std::pair<std::string, DataType>> &vec,
                    const std::string &vector_name, bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]: \n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintValue(output, space, DataTypeName(vec[i].second),
                   "" + vec[i].first, false);
        output << "\n";
    }
    PrintValue(output, space, DataTypeName(vec[i].second), "" + vec[i].first,
               true);
}

void PrintSQLVector(std::ostream &output, const std::string &tab,
                    const NodePointVector &vec, const std::string &vector_name,
                    bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]: \n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintSQLNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintSQLNode(output, space, vec[i], "" + std::to_string(i), true);
}

void SelectQueryNode::PrintSQLNodeList(std::ostream &output,
                                       const std::string &tab,
                                       SQLNodeList *list,
                                       const std::string &name,
                                       bool last_item) const {
    if (nullptr == list) {
        output << tab << SPACE_ST << name << ": []";
        return;
    }
    PrintSQLVector(output, tab, list->GetList(), name, last_item);
}

void PrintValue(std::ostream &output, const std::string &org_tab,
                const std::string &value, const std::string &item_name,
                bool last_child) {
    output << org_tab << SPACE_ST << item_name << ": " << value;
}

void PrintValue(std::ostream &output, const std::string &org_tab,
                const std::vector<std::string> &vec,
                const std::string &item_name, bool last_child) {
    std::string value = "";
    for (auto item : vec) {
        value.append(item).append(",");
    }
    if (vec.size() > 0) {
        value.pop_back();
    }
    output << org_tab << SPACE_ST << item_name << ": " << value;
}

void SQLNode::Print(std::ostream &output, const std::string &tab) const {
    output << tab << SPACE_ST << "node[" << NameOfSQLNodeType(type_) << "]";
}

bool SQLNode::Equals(const SQLNode *that) const {
    if (this == that) {
        return true;
    }
    if (nullptr == that || type_ != that->type_) {
        return false;
    }
    return true;
}

void SQLNodeList::Print(std::ostream &output, const std::string &tab) const {
    PrintSQLVector(output, tab, list_, "list", true);
}
bool SQLNodeList::Equals(const SQLNodeList *that) const {
    if (this == that) {
        return true;
    }
    if (nullptr == that || this->list_.size() != that->list_.size()) {
        return false;
    }

    auto iter1 = list_.cbegin();
    auto iter2 = that->list_.cbegin();
    while (iter1 != list_.cend()) {
        if (!(*iter1)->Equals(*iter2)) {
            return false;
        }
        iter1++;
        iter2++;
    }
    return true;
}

void QueryNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    output << ": " << QueryTypeName(query_type_);
}
bool QueryNode::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }

    const QueryNode *that = dynamic_cast<const QueryNode *>(node);
    return this->query_type_ == that->query_type_;
}

const std::string AllNode::GetExprString() const {
    if (relation_name_.empty()) {
        return "*";
    } else {
        return relation_name_ + ".*";
    }
}
bool AllNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const AllNode *that = dynamic_cast<const AllNode *>(node);
    return this->relation_name_ == that->relation_name_ &&
           ExprNode::Equals(node);
}

void ConstNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    output << "\n";
    output << org_tab << SPACE_ST;
    output << "value: " << GetExprString();
}
const std::string ConstNode::GetExprString() const {
    switch (date_type_) {
        case fesql::node::kInt16:
            return std::to_string(val_.vsmallint);
        case fesql::node::kInt32:
            return std::to_string(val_.vint);
        case fesql::node::kInt64:
            return std::to_string(val_.vlong);
        case fesql::node::kVarchar:
            return val_.vstr;
        case fesql::node::kFloat:
            return std::to_string(val_.vfloat);
        case fesql::node::kDouble:
            return std::to_string(val_.vdouble);
        case fesql::node::kNull:
            return "null";
            break;
        case fesql::node::kVoid:
            return "void";
        default:
            return "unknow";
    }
}
bool ConstNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const ConstNode *that = dynamic_cast<const ConstNode *>(node);
    return this->date_type_ == that->date_type_ &&
           GetExprString() == that->GetExprString() && ExprNode::Equals(node);
}

void LimitNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::to_string(limit_cnt_), "limit_cnt", true);
}
bool LimitNode::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }
    const LimitNode *that = dynamic_cast<const LimitNode *>(node);
    return this->limit_cnt_ == that->limit_cnt_;
}

void TableNode::Print(std::ostream &output, const std::string &org_tab) const {
    TableRefNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, org_table_name_, "table", false);
    output << "\n";
    PrintValue(output, tab, alias_table_name_, "alias", true);
}
bool TableNode::Equals(const SQLNode *node) const {
    if (!TableRefNode::Equals(node)) {
        return false;
    }

    const TableNode *that = dynamic_cast<const TableNode *>(node);
    return this->org_table_name_ == that->org_table_name_;
}

void ColumnRefNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, relation_name_, "relation_name", false);
    output << "\n";
    PrintValue(output, tab, column_name_, "column_name", true);
}
const std::string ColumnRefNode::GetExprString() const {
    std::string str = "";
    if (!relation_name_.empty()) {
        str.append(relation_name_).append(".");
    }
    str.append(column_name_);
    return str;
}
bool ColumnRefNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const ColumnRefNode *that = dynamic_cast<const ColumnRefNode *>(node);
    return this->relation_name_ == that->relation_name_ &&
           this->column_name_ == that->column_name_ && ExprNode::Equals(node);
}

void OrderByNode::Print(std::ostream &output,
                        const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;

    output << "\n";
    PrintValue(output, tab, is_asc_ ? "ASC" : "DESC", "sort_type", false);

    output << "\n";
    PrintSQLNode(output, tab, order_by_, "order_by", true);
}
const std::string OrderByNode::GetExprString() const {
    std::string str = "";
    str.append(nullptr == order_by_ ? "()" : order_by_->GetExprString());
    str.append(is_asc_ ? " ASC" : " DESC");
    return str;
}
bool OrderByNode::Equals(const ExprNode *node) const {
    if (!ExprNode::Equals(node)) {
        return false;
    }
    const OrderByNode *that = dynamic_cast<const OrderByNode *>(node);
    return is_asc_ == that->is_asc_ && ExprEquals(order_by_, that->order_by_);
}

void FrameNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;

    output << "\n";
    PrintValue(output, tab, NameOfSQLNodeType(frame_type_), "type", false);

    output << "\n";
    if (NULL == start_) {
        PrintValue(output, tab, "UNBOUNDED", "start", false);
    } else {
        PrintSQLNode(output, tab, start_, "start", false);
    }

    output << "\n";
    if (NULL == end_) {
        PrintValue(output, tab, "UNBOUNDED", "end", true);
    } else {
        PrintSQLNode(output, tab, end_, "end", true);
    }
}
bool FrameNode::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }
    const FrameNode *that = dynamic_cast<const FrameNode *>(node);
    return this->frame_type_ == that->frame_type_;
    SQLEquals(this->start_, that->start_) && SQLEquals(this->end_, that->end_);
}

void CallExprNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << tab << "function_name: " << function_name_;
    output << "\n";
    PrintSQLNode(output, tab, args_, "args", false);
    output << "\n";
    PrintSQLNode(output, tab, over_, "over", true);
}
const std::string CallExprNode::GetExprString() const {
    std::string str = function_name_;
    str.append(nullptr == args_ ? "()" : args_->GetExprString());

    if (nullptr != over_) {
        str.append("over ").append(over_->GetName());
    }
    return str;
}
bool CallExprNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const CallExprNode *that = dynamic_cast<const CallExprNode *>(node);
    return this->is_agg_ == that->is_agg_ &&
           this->function_name_ == that->function_name_ &&
           ExprEquals(this->args_, that->args_) &&
           SQLEquals(this->over_, that->over_) && ExprNode::Equals(node);
}

void WindowDefNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab;
    output << "\n";
    PrintValue(output, tab, window_name_, "window_name", false);

    output << "\n";
    PrintValue(output, tab, partitions_, "partitions", false);

    output << "\n";
    PrintValue(output, tab, orders_, "orders", false);

    output << "\n";
    PrintSQLNode(output, tab, frame_ptr_, "frame", true);
}
bool WindowDefNode::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }
    const WindowDefNode *that = dynamic_cast<const WindowDefNode *>(node);
    return this->window_name_ == that->window_name_ &&
           this->orders_ == that->orders_ &&
           this->partitions_ == that->partitions_ &&
           SQLEquals(this->frame_ptr_, that->frame_ptr_);
}

void ResTarget::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintSQLNode(output, tab, val_, "val", false);
    output << "\n";
    PrintValue(output, tab, name_, "name", true);
}
bool ResTarget::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }
    const ResTarget *that = dynamic_cast<const ResTarget *>(node);
    return this->name_ == that->name_ && ExprEquals(this->val_, that->val_);
}

void SelectQueryNode::Print(std::ostream &output,
                            const std::string &org_tab) const {
    QueryNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    bool last_child = false;
    PrintValue(output, tab, distinct_opt_ ? "true" : "false", "distinct_opt",
               last_child);
    output << "\n";
    PrintSQLNode(output, tab, where_clause_ptr_, "where_expr", last_child);
    output << "\n";
    PrintSQLNode(output, tab, group_clause_ptr_, "group_expr_list", last_child);
    output << "\n";
    PrintSQLNode(output, tab, having_clause_ptr_, "having_expr", last_child);
    output << "\n";
    PrintSQLNode(output, tab, order_clause_ptr_, "order_expr_list", last_child);
    output << "\n";
    PrintSQLNode(output, tab, limit_ptr_, "limit", last_child);
    output << "\n";
    PrintSQLNodeList(output, tab, select_list_, "select_list", last_child);
    output << "\n";
    PrintSQLNodeList(output, tab, tableref_list_, "tableref_list", last_child);
    output << "\n";
    last_child = true;
    PrintSQLNodeList(output, tab, window_list_, "window_list", last_child);
}
bool SelectQueryNode::Equals(const SQLNode *node) const {
    if (!QueryNode::Equals(node)) {
        return false;
    }
    const SelectQueryNode *that = dynamic_cast<const SelectQueryNode *>(node);
    return this->distinct_opt_ == that->distinct_opt_ &&
           SQLListEquals(this->select_list_, that->select_list_) &&
           SQLListEquals(this->tableref_list_, that->tableref_list_) &&
           SQLListEquals(this->window_list_, that->window_list_) &&
           SQLEquals(this->where_clause_ptr_, that->where_clause_ptr_) &&
           SQLEquals(this->group_clause_ptr_, that->group_clause_ptr_) &&
           SQLEquals(this->having_clause_ptr_, that->having_clause_ptr_) &&
           ExprEquals(this->order_clause_ptr_, that->order_clause_ptr_) &&
           SQLEquals(this->limit_ptr_, that->limit_ptr_);
}

// Return the node type name
// param type
// return
std::string NameOfSQLNodeType(const SQLNodeType &type) {
    std::string output;
    switch (type) {
        case kCreateStmt:
            output = "CREATE";
            break;
        case kCmdStmt:
            output = "CMD";
            break;
        case kExplainSmt:
            output = "EXPLAIN";
        case kName:
            output = "kName";
            break;
        case kType:
            output = "kType";
            break;
        case kResTarget:
            output = "kResTarget";
            break;
        case kTableRef:
            output = "kTableRef";
            break;
        case kQuery:
            output = "kQuery";
            break;
        case kColumnDesc:
            output = "kColumnDesc";
            break;
        case kColumnIndex:
            output = "kColumnIndex";
            break;
        case kExpr:
            output = "kExpr";
            break;
        case kWindowDef:
            output = "kWindowDef";
            break;
        case kFrames:
            output = "kFrame";
            break;
        case kFrameBound:
            output = "kBound";
            break;
        case kPreceding:
            output = "kPreceding";
            break;
        case kFollowing:
            output = "kFollowing";
            break;
        case kCurrent:
            output = "kCurrent";
            break;
        case kFrameRange:
            output = "kFrameRange";
            break;
        case kFrameRows:
            output = "kFrameRows";
            break;
        case kConst:
            output = "kConst";
            break;
        case kLimit:
            output = "kLimit";
            break;
        case kFnList:
            output = "kFnList";
            break;
        case kFnDef:
            output = "kFnDef";
            break;
        case kFnHeader:
            output = "kFnHeader";
            break;
        case kFnPara:
            output = "kFnPara";
            break;
        case kFnReturnStmt:
            output = "kFnReturnStmt";
            break;
        case kFnAssignStmt:
            output = "kFnAssignStmt";
            break;
        case kFnIfStmt:
            output = "kFnIfStmt";
            break;
        case kFnElifStmt:
            output = "kFnElseifStmt";
            break;
        case kFnElseStmt:
            output = "kFnElseStmt";
            break;
        case kFnIfBlock:
            output = "kFnIfBlock";
            break;
        case kFnElseBlock:
            output = "kFnElseBlock";
            break;
        case kFnIfElseBlock:
            output = "kFnIfElseBlock";
            break;
        case kFnElifBlock:
            output = "kFnElIfBlock";
            break;
        case kFnValue:
            output = "kFnValue";
            break;
        case kFnForInStmt:
            output = "kFnForInStmt";
            break;
        case kFnForInBlock:
            output = "kFnForInBlock";
            break;
        default:
            output = "unknown";
    }
    return output;
}

std::ostream &operator<<(std::ostream &output, const SQLNode &thiz) {
    thiz.Print(output, "");
    return output;
}
std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz) {
    thiz.Print(output, "");
    return output;
}

void FillSQLNodeList2NodeVector(
    SQLNodeList *node_list_ptr,
    std::vector<SQLNode *> &node_list  // NOLINT (runtime/references)
) {
    if (nullptr != node_list_ptr) {
        for (auto item : node_list_ptr->GetList()) {
            node_list.push_back(item);
        }
    }
}

WindowDefNode *WindowOfExpression(
    std::map<std::string, WindowDefNode *> windows, ExprNode *node_ptr) {
    WindowDefNode *w_ptr = nullptr;
    switch (node_ptr->GetExprType()) {
        case kExprCall: {
            CallExprNode *func_node_ptr =
                dynamic_cast<CallExprNode *>(node_ptr);
            if (nullptr != func_node_ptr->GetOver()) {
                return windows.at(func_node_ptr->GetOver()->GetName());
            }
            if (nullptr == func_node_ptr->GetArgs() ||
                func_node_ptr->GetArgs()->IsEmpty()) {
                return nullptr;
            }
            for (auto arg : func_node_ptr->GetArgs()->children_) {
                WindowDefNode *ptr =
                    WindowOfExpression(windows, dynamic_cast<ExprNode *>(arg));
                if (nullptr != ptr && nullptr != w_ptr) {
                    LOG(WARNING)
                        << "Cannot handle more than 1 windows in an expression";
                    return nullptr;
                }
            }

            return w_ptr;
        }
        default:
            return w_ptr;
    }
}

void CreateStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, table_name_, "table", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(op_if_not_exist_), "IF NOT EXIST",
               false);
    output << "\n";
    PrintSQLVector(output, tab, column_desc_list_, "column_desc_list_", true);
    output << "\n";
}

void ColumnDefNode::Print(std::ostream &output,
                          const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, column_name_, "column_name", false);
    output << "\n";
    PrintValue(output, tab, DataTypeName(column_type_), "column_type", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(op_not_null_), "NOT NULL", false);
}

void ColumnIndexNode::Print(std::ostream &output,
                            const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    std::string lastdata;
    lastdata = accumulate(key_.begin(), key_.end(), lastdata);
    PrintValue(output, tab, lastdata, "keys", false);
    output << "\n";
    PrintValue(output, tab, ts_, "ts_col", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(ttl_), "ttl", false);
    output << "\n";
    PrintValue(output, tab, version_, "version_column", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(version_count_), "version_count",
               true);
}
void CmdNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, CmdTypeName(cmd_type_), "cmd_type", false);
    output << "\n";
    PrintValue(output, tab, args_, "args", true);
}

void ExplainNode::Print(std::ostream &output,
                        const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, ExplainTypeName(type_), "explain_type", false);
    output << "\n";
    PrintSQLNode(output, tab, query_, "query", true);
}
void InsertStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, table_name_, "table_name", false);
    output << "\n";
    if (is_all_) {
        PrintValue(output, tab, "all", "columns", false);
    } else {
        PrintValue(output, tab, columns_, "columns", false);
    }

    PrintSQLVector(output, tab, values_, "values", false);
}
void BinaryExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLVector(output, tab, children_, ExprOpTypeName(op_), true);
}
const std::string BinaryExpr::GetExprString() const {
    std::string str = "";
    str.append("")
        .append(children_[0]->GetExprString())
        .append(" ")
        .append(ExprOpTypeName(op_))
        .append(" ")
        .append(children_[1]->GetExprString())
        .append("");
    return str;
}
bool BinaryExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const BinaryExpr *that = dynamic_cast<const BinaryExpr *>(node);
    return this->op_ == that->op_ && ExprNode::Equals(node);
}

void UnaryExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLVector(output, tab, children_, ExprOpTypeName(op_), true);
}
const std::string UnaryExpr::GetExprString() const {
    std::string str = "";
    if (op_ == kFnOpBracket) {
        str.append("(").append(children_[0]->GetExprString()).append(")");
        return str;
    }
    str.append(ExprOpTypeName(op_))
        .append(" ")
        .append(children_[0]->GetExprString());
    return str;
}
bool UnaryExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const UnaryExpr *that = dynamic_cast<const UnaryExpr *>(node);
    return this->op_ == that->op_ && ExprNode::Equals(node);
}
void ExprIdNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, name_, "var", true);
}
const std::string ExprIdNode::GetExprString() const { return name_; }
bool ExprIdNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const ExprIdNode *that = dynamic_cast<const ExprIdNode *>(node);
    return this->name_ == that->name_;
}

void ExprNode::Print(std::ostream &output, const std::string &org_tab) const {
    output << org_tab << SPACE_ST << "expr[" << ExprTypeName(expr_type_) << "]";
}
const std::string ExprNode::GetExprString() const { return ""; }
bool ExprNode::Equals(const ExprNode *that) const {
    if (this == that) {
        return true;
    }
    if (nullptr == that || expr_type_ != that->expr_type_ ||
        children_.size() != that->children_.size()) {
        return false;
    }

    auto iter1 = children_.cbegin();
    auto iter2 = that->children_.cbegin();
    while (iter1 != children_.cend()) {
        if (!(*iter1)->Equals(*iter2)) {
            return false;
        }
        iter1++;
        iter2++;
    }
    return true;
}

void ExprListNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    if (children_.empty()) {
        return;
    }
    const std::string tab = org_tab + INDENT + SPACE_ED;
    auto iter = children_.cbegin();
    (*iter)->Print(output, org_tab);
    iter++;
    for (; iter != children_.cend(); iter++) {
        output << "\n";
        (*iter)->Print(output, org_tab);
    }
}
const std::string ExprListNode::GetExprString() const {
    if (children_.empty()) {
        return "()";
    } else {
        std::string str = "(";
        auto iter = children_.cbegin();
        str.append((*iter)->GetExprString());
        iter++;
        for (; iter != children_.cend(); iter++) {
            str.append(",");
            str.append((*iter)->GetExprString());
        }
        str.append(")");
        return str;
    }
}
void FnParaNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";

    PrintSQLNode(output, tab, para_type_, name_, true);
}
void FnNodeFnHeander::Print(std::ostream &output,
                            const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, this->name_, "func_name", true);
    output << "\n";
    PrintSQLNode(output, tab, ret_type_, "return_type", true);
    output << "\n";
    PrintSQLNode(output, tab, reinterpret_cast<const SQLNode *>(parameters_),
                 "parameters", true);
}
const std::string FnNodeFnHeander::GetCodegenFunctionName() const {
    std::string fn_name = name_;
    if (!parameters_->children.empty()) {
        for (node::SQLNode *node : parameters_->children) {
            node::FnParaNode *para_node =
                dynamic_cast<node::FnParaNode *>(node);
            switch (para_node->GetParaType()->base_) {
                case fesql::node::kList:
                case fesql::node::kIterator:
                case fesql::node::kMap:
                    fn_name.append("_").append(
                        para_node->GetParaType()->GetName());
                default: {
                }
            }
        }
    }
    return fn_name;
}
void FnNodeFnDef::Print(std::ostream &output,
                        const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, header_, "header", false);
    output << "\n";
    PrintSQLNode(output, tab, block_, "block", true);
}
void FnNodeList::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLVector(output, tab, children, "list", true);
}
void FnAssignNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, is_ssa_ ? "true" : "false", "ssa", false);
    output << "\n";
    PrintSQLNode(output, tab, reinterpret_cast<const SQLNode *>(expression_),
                 name_, true);
}
void FnReturnStmt::Print(std::ostream &output,
                         const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, return_expr_, "return", true);
}

void FnIfNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, expression_, "if", true);
}
void FnElifNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, expression_, "elif", true);
}
void FnElseNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    output << "\n";
}
void FnForInNode::Print(std::ostream &output,
                        const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, var_name_, "var", false);
    output << "\n";
    PrintSQLNode(output, tab, in_expression_, "in", true);
}

void FnIfBlock::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, if_node, "if", false);
    output << "\n";
    PrintSQLNode(output, tab, block_, "block", true);
}

void FnElifBlock::Print(std::ostream &output,
                        const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, elif_node_, "elif", false);
    output << "\n";
    PrintSQLNode(output, tab, block_, "block", true);
}
void FnElseBlock::Print(std::ostream &output,
                        const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, block_, "block", true);
}
void FnIfElseBlock::Print(std::ostream &output,
                          const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, if_block_, "if", false);
    output << "\n";
    PrintSQLVector(output, tab, elif_blocks_, "elif_list", false);
    output << "\n";
    PrintSQLNode(output, tab, else_block_, "else", true);
}

void FnForInBlock::Print(std::ostream &output,
                         const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, for_in_node_, "for", false);
    output << "\n";
    PrintSQLNode(output, tab, block_, "body", true);
}
void StructExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintValue(output, tab, class_name_, "name", false);
    output << "\n";
    PrintSQLNode(output, tab, fileds_, "fileds", false);
    output << "\n";
    PrintSQLNode(output, tab, methods_, "methods", true);
}

void TypeNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;

    output << "\n";
    PrintValue(output, tab, GetName(), "type", true);
}
bool TypeNode::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }

    const TypeNode *that = dynamic_cast<const TypeNode *>(node);
    return this->base_ == that->base_ && this->generics_ == that->generics_;
}

void JoinNode::Print(std::ostream &output, const std::string &org_tab) const {
    TableRefNode::Print(output, org_tab);

    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, JoinTypeName(join_type_), "join_type", false);
    output << "\n";
    PrintSQLNode(output, tab, left_, "left", false);
    output << "\n";
    PrintSQLNode(output, tab, right_, "right", true);
}
bool JoinNode::Equals(const SQLNode *node) const {
    if (!TableRefNode::Equals(node)) {
        return false;
    }
    const JoinNode *that = dynamic_cast<const JoinNode *>(node);
    return join_type_ == that->join_type_ &&
           ExprEquals(condition_, that->condition_) &&
           SQLEquals(this->left_, that->right_);
}
void UnionQueryNode::Print(std::ostream &output,
                           const std::string &org_tab) const {
    QueryNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, is_all_ ? "ALL UNION" : "DISTINCT UNION",
               "union_type", false);
    output << "\n";
    PrintSQLNode(output, tab, left_, "left", false);
    output << "\n";
    PrintSQLNode(output, tab, right_, "right", true);
}
bool UnionQueryNode::Equals(const SQLNode *node) const {
    if (!QueryNode::Equals(node)) {
        return false;
    }
    const UnionQueryNode *that = dynamic_cast<const UnionQueryNode *>(node);
    return this->is_all_ && that->is_all_ &&
           SQLEquals(this->left_, that->right_);
}
void QueryExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, query_, "query", true);
}
bool QueryExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const QueryExpr *that = dynamic_cast<const QueryExpr *>(node);
    return SQLEquals(this->query_, that->query_) && ExprNode::Equals(node);
}
const std::string QueryExpr::GetExprString() const {
    return "query expr";
}

void TableRefNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    output << ": " << TableRefTypeName(ref_type_);
}
bool TableRefNode::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }

    const TableRefNode *that = dynamic_cast<const TableRefNode *>(node);
    return this->ref_type_ == that->ref_type_ &&
           this->alias_table_name_ == that->alias_table_name_;
}

void QueryRefNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    TableRefNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLNode(output, tab, query_, "query", true);
}
bool QueryRefNode::Equals(const SQLNode *node) const {
    if (!TableRefNode::Equals(node)) {
        return false;
    }
    const QueryRefNode *that = dynamic_cast<const QueryRefNode *>(node);
    return SQLEquals(this->query_, that->query_);
}
bool FrameBound::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }
    const FrameBound *that = dynamic_cast<const FrameBound *>(node);
    return this->bound_type_ == that->bound_type_ &&
           ExprEquals(this->offset_, that->offset_);
}
bool NameNode::Equals(const SQLNode *node) const {
    if (!SQLNode::Equals(node)) {
        return false;
    }
    const NameNode *that = dynamic_cast<const NameNode *>(node);
    return this->name_ == that->name_;
}
}  // namespace node
}  // namespace fesql
