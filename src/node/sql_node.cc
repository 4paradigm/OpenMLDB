//
// Created by chenjing on 2019/10/11.
//

#include <node/node_memory.h>
#include "glog/logging.h"
#include "sql_node.h"

namespace fesql {
namespace node {

////////////////// Global ///////////////////////////////////

/**
 * get the node type name
 * @param type
 * @param output
 */
std::string NameOfSQLNodeType(const SQLNodeType &type) {
    std::string output;
    switch (type) {
        case kSelectStmt:output = "kSelectStmt";
            break;
        case kResTarget:output = "kResTarget";
            break;
        case kTable: output = "kTable";
            break;
        case kColumn: output = "kColumn";
            break;
        case kExpr: output = "kExpr";
            break;
        case kFunc: output = "kFunc";
            break;
        case kWindowDef: output = "kWindowDef";
            break;
        case kFrames: output = "kFrame";
            break;
        case kFrameBound: output = "kBound";
            break;
        case kPreceding: output = "kPreceding";
            break;
        case kFollowing: output = "kFollowing";
            break;
        case kCurrent: output = "kCurrent";
            break;
        case kFrameRange: output = "kFrameRange";
            break;
        case kFrameRows: output = "kFrameRows";
            break;
        case kConst: output = "kConst";
            break;
        case kOrderBy: output = "kOrderBy";
            break;
        case kLimit: output = "kLimit";
            break;
        case kAll: output = "kAll";
            break;
        case kNull: output = "kNull";
            break;
        case kInt: output = "kInt";
            break;
        case kBigInt: output = "kBigInt";
            break;
        case kFloat: output = "kFloat";
            break;
        case kDouble: output = "kDouble";
            break;
        case kString: output = "kString";
            break;
        default: output = "unknown";
    }
    return output;
}

std::ostream &operator<<(std::ostream &output, const SQLNode &thiz) {
    thiz.Print(output);
    return output;
}

std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz) {
    thiz.Print(output, "");
    return output;
}
void SQLNodeList::Print(std::ostream &output, const std::string &tab) const {
    if (0 == size_ || NULL == head_) {
        output << tab << "[]";
        return;
    }
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
void FillSQLNodeList2NodeVector(SQLNodeList *node_list_ptr, std::vector<SQLNode *> &node_list) {
    if (nullptr != node_list_ptr) {
        SQLLinkedNode *ptr = node_list_ptr->GetHead();
        while (nullptr != ptr && nullptr != ptr->node_ptr_) {
            node_list.push_back(ptr->node_ptr_);
            ptr = ptr->next_;
        }
    }
}

std::string WindowOfExpression(SQLNode *node_ptr) {
    switch (node_ptr->GetType()) {
        case kFunc: {
            FuncNode *func_node_ptr = (FuncNode *) node_ptr;
            if (nullptr != func_node_ptr->GetOver()) {
                return func_node_ptr->GetOver()->GetName();
            }

            if (func_node_ptr->GetArgs().empty()) {
                return "";
            }

            for (auto arg: func_node_ptr->GetArgs()) {
                std::string arg_w = WindowOfExpression(arg);
                if (false == arg_w.empty()) {
                    return arg_w;
                }
            }

            return "";
        }
        default:return "";
    }
}

void PrintSQLNode(std::ostream &output,
                  const std::string &org_tab,
                  SQLNode *node_ptr,
                  const std::string &item_name,
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

void PrintSQLVector(std::ostream &output,
                    const std::string &tab,
                    NodePointVector vec,
                    const std::string &vector_name,
                    bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]: \n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int i = 0;
    for (i = 0; i < vec.size() - 1; ++i) {
        PrintSQLNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintSQLNode(output, space, vec[i], "" + std::to_string(i), true);
}

void PrintValue(std::ostream &output,
                const std::string &org_tab,
                const std::string &value,
                const std::string &item_name,
                bool last_child) {

    output << org_tab << SPACE_ST << item_name << ": " << value;
}

void SelectStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    bool last_child = false;
    PrintSQLNode(output, tab, where_clause_ptr_, "where_clause", last_child);
    output << "\n";
    PrintSQLNode(output, tab, group_clause_ptr_, "group_clause", last_child);
    output << "\n";
    PrintSQLNode(output, tab, having_clause_ptr_, "haveing_clause", last_child);
    output << "\n";
    PrintSQLNode(output, tab, order_clause_ptr_, "order_clause", last_child);
    output << "\n";
    PrintSQLNode(output, tab, limit_ptr_, "limit", last_child);
    output << "\n";
    PrintSQLVector(output, tab, select_list_ptr_, "select_list", last_child);
    output << "\n";
    PrintSQLVector(output, tab, tableref_list_ptr_, "tableref_list", last_child);
    output << "\n";
    last_child = true;
    PrintSQLVector(output, tab, window_list_ptr_, "window_list", last_child);
}
void ResTarget::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintSQLNode(output, tab, val_, "val", false);
    output << "\n";
    PrintValue(output, tab, name_, "name", true);

}
void WindowDefNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab;
    output << "\n";
    PrintValue(output, tab, window_name_, "window_name", false);

    output << "\n";
    PrintSQLVector(output, tab, partition_list_ptr_, "partitions", false);

    output << "\n";
    PrintSQLVector(output, tab, order_list_ptr_, "orders", false);

    output << "\n";
    PrintSQLNode(output, tab, frame_ptr_, "frame", true);
}
void TableNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, org_table_name_, "table", false);
    output << "\n";
    PrintValue(output, tab, alias_table_name_, "alias", true);
}
void FuncNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << tab << "function_name: " << function_name_;
    output << "\n";
    PrintSQLVector(output, tab, args_, "args", false);
    output << "\n";
    PrintSQLNode(output, tab, over_, "over", true);

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
void LimitNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::to_string(limit_cnt_), "limit_cnt", true);
}
void ColumnRefNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, relation_name_, "relation_name", false);
    output << "\n";
    PrintValue(output, tab, column_name_, "column_name", true);
}
void OrderByNode::Print(std::ostream &output, const std::string &org_tab) const {
    SQLNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;

    output << "\n";
    PrintValue(output, tab, NameOfSQLNodeType(sort_type_), "sort_type", false);

    output << "\n";
    PrintSQLNode(output, tab, order_by_, "order_by", true);
}

}
}
