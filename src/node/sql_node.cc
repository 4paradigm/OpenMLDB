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

void SelectStmt::PrintSQLNodeList(std::ostream &output, const std::string &tab,
                                  SQLNodeList *list, const std::string &name,
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

void SQLNodeList::Print(std::ostream &output, const std::string &tab) const {
    PrintSQLVector(output, tab, list_, "list", true);
}

const std::string AllNode::GetExprString() const {
    if (relation_name_.empty()) {
        return "*";
    } else {
        return relation_name_ + ".*";
    }
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

void LimitNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::to_string(limit_cnt_), "limit_cnt", true);
}

void TableNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, org_table_name_, "table", false);
    output << "\n";
    PrintValue(output, tab, alias_table_name_, "alias", true);
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

void OrderByNode::Print(std::ostream &output,
                        const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;

    output << "\n";
    PrintValue(output, tab, NameOfSQLNodeType(sort_type_), "sort_type", false);

    output << "\n";
    PrintSQLNode(output, tab, order_by_, "order_by", true);
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
    str.append(args_->GetExprString());

    if (nullptr != over_) {
        str.append("over ").append(over_->GetName());
    }
    return str;
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

void ResTarget::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintSQLNode(output, tab, val_, "val", false);
    output << "\n";
    PrintValue(output, tab, name_, "name", true);
}

void SelectStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    bool last_child = false;
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

// Return the node type name
// param type
// return
std::string NameOfSQLNodeType(const SQLNodeType &type) {
    std::string output;
    switch (type) {
        case kSelectStmt:
            output = "SELECT";
            break;
        case kCreateStmt:
            output = "CREATE";
            break;
        case kCmdStmt:
            output = "CMD";
            break;
        case kName:
            output = "kName";
            break;
        case kType:
            output = "kType";
            break;
        case kResTarget:
            output = "kResTarget";
            break;
        case kTable:
            output = "kTable";
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
        case kOrderBy:
            output = "kOrderBy";
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
            for (auto arg : func_node_ptr->GetArgs()->children) {
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
    //    if (nullptr != table_def_) {
    //        PrintValue(output, tab, table_def_->DebugString() , "table def",
    //        true);
    //    }
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
    PrintSQLVector(output, tab, children, ExprOpTypeName(op_), true);
}
const std::string BinaryExpr::GetExprString() const {
    std::string str = "";
    str.append(children[0]->GetExprString())
        .append(ExprOpTypeName(op_))
        .append(children[1]->GetExprString());
    return str;
}
void UnaryExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSQLVector(output, tab, children, ExprOpTypeName(op_), true);
}
const std::string UnaryExpr::GetExprString() const {
    std::string str = ExprOpTypeName(op_);
    str.append(children[0]->GetExprString());
    return str;
}
void ExprIdNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, name_, "var", true);
}
const std::string ExprIdNode::GetExprString() const { return name_; }

void ExprNode::Print(std::ostream &output, const std::string &org_tab) const {
    output << org_tab << SPACE_ST << "expr[" << ExprTypeName(expr_type_) << "]";
}
const std::string ExprNode::GetExprString() const { return ""; }

void ExprListNode::Print(std::ostream &output,
                         const std::string &org_tab) const {
    if (children.empty()) {
        return;
    }
    const std::string tab = org_tab + INDENT + SPACE_ED;
    auto iter = children.cbegin();
    (*iter)->Print(output, org_tab);
    iter++;
    for (; iter != children.cend(); iter++) {
        output << "\n";
        (*iter)->Print(output, org_tab);
    }
}
const std::string ExprListNode::GetExprString() const {
    if (children.empty()) {
        return "()";
    } else {
        std::string str = "(";
        auto iter = children.cbegin();
        str.append((*iter)->GetExprString());
        for (; iter != children.cend(); iter++) {
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

void JoinNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);

    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, JoinTypeName(join_type_), "join_type", false);
    output << "\n";
    PrintSQLNode(output, tab, left_, "left", false);
    output << "\n";
    PrintSQLNode(output, tab, right_, "right", true);
}
}  // namespace node
}  // namespace fesql
