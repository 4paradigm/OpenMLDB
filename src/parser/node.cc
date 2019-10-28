//
// Created by chenjing on 2019/10/11.
//

#include "glog/logging.h"
#include "parser/node.h"

namespace fesql {
namespace parser {

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
        case kLimit: output = "kLimit";
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

SQLNode *MakeNode(const SQLNodeType &type, ...) {
    switch (type) {
        case kSelectStmt:return new SelectStmt();
        case kResTarget:return new ResTarget();
        case kAll:return new SQLNode(type, 0, 0);
        default:return new UnknowSqlNode();
    }
}
void ConverSQLNodeList2NodeList(SQLNodeList *node_list_ptr, std::vector<SQLNode *> &node_list) {
    if (nullptr != node_list_ptr) {
        SQLLinkedNode *ptr = node_list_ptr->GetHead();
        while (nullptr != ptr && nullptr != ptr->node_ptr_) {
            node_list.push_back(ptr->node_ptr_);
            ptr = ptr->next_;
        }
    }
}

////////////////// Make Table Node///////////////////////////////////
SQLNode *MakeTableNode(const std::string &name, const std::string &alias) {
    TableNode *node_ptr = new TableNode(name, alias);
    return (SQLNode *) node_ptr;
}

////////////////// Make Function Node///////////////////////////////////
SQLNode *MakeFuncNode(const std::string &name, SQLNodeList *list_ptr, SQLNode *over) {
    std::vector<SQLNode*> list;
    ConverSQLNodeList2NodeList(list_ptr, list);
    // 释放SQLNodeList
    delete list_ptr;
    FuncNode *node_ptr = new FuncNode(name, list, (WindowDefNode *) (over));
    return (SQLNode *) node_ptr;
}

////////////////// Make ResTarget Node///////////////////////////////////
SQLNode *MakeResTargetNode(SQLNode *node, const std::string &name) {
    ResTarget *node_ptr = new ResTarget(name, node);
    return node_ptr;
}

////////////////// Make Column Reference Node///////////////////////////////////
SQLNode *MakeColumnRefNode(const std::string &column_name, const std::string &relation_name) {
    ColumnRefNode *node_ptr = new ColumnRefNode(column_name, relation_name);
    return (SQLNode *) node_ptr;
}

///////////////// Make SQL Node List with single Node/////////////////////////
SQLNodeList *MakeNodeList(SQLNode *node_ptr) {
    SQLLinkedNode *head = new SQLLinkedNode(node_ptr);
    SQLNodeList *new_list_ptr = new SQLNodeList(head, head, 1);
    return new_list_ptr;
}


SQLNode *MakeSelectStmtNode(SQLNodeList *select_list_ptr,
                            SQLNodeList *tableref_list_ptr,
                            SQLNodeList *window_clause_ptr,
                            SQLNode *limit_ptr) {
    SelectStmt *node_ptr = new SelectStmt();

    std::vector<SQLNode*> select_node_list;
    ConverSQLNodeList2NodeList(select_list_ptr, select_node_list);
    // 释放SQLNodeList
    delete select_list_ptr;

    std::vector<SQLNode*> tableref_list;
    ConverSQLNodeList2NodeList(tableref_list_ptr, tableref_list);
    // 释放SQLNodeList
    delete tableref_list_ptr;


    std::vector<SQLNode*> window_list;
    ConverSQLNodeList2NodeList(window_clause_ptr, window_list);
    // 释放SQLNodeList
    delete window_clause_ptr;
    FillSelectAttributions(node_ptr, select_node_list, tableref_list, window_list, limit_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make Function Node///////////////////////////////////
SQLNode *MakeWindowDefNode(SQLNodeList *partitions, SQLNodeList *orders, SQLNode *frame) {
    WindowDefNode *node_ptr = new WindowDefNode();
    std::vector<SQLNode*> partition_list;
    ConverSQLNodeList2NodeList(partitions, partition_list);
    delete partitions;

    std::vector<SQLNode*> order_list;
    ConverSQLNodeList2NodeList(orders, order_list);
    delete orders;
    FillWindowSpection(node_ptr, partition_list, order_list, frame);
    return (SQLNode *) node_ptr;
}

SQLNode *MakeWindowDefNode(const std::string &name) {
    WindowDefNode *node_ptr = new WindowDefNode();
    node_ptr->SetName(name);
    return (SQLNode *) node_ptr;
}

SQLNode *MakeFrameNode(SQLNode *start, SQLNode *end) {
    FrameNode *node_ptr = new FrameNode(kUnknow, (FrameBound *) (start), (FrameBound *) end);
    return (SQLNode *) node_ptr;
}
SQLNode *MakeRangeFrameNode(SQLNode *node_ptr) {
    ((FrameNode *) node_ptr)->SetFrameType(kFrameRange);
    return node_ptr;
}

SQLNode *MakeRowsFrameNode(SQLNode *node_ptr) {
    ((FrameNode *) node_ptr)->SetFrameType(kFrameRows);
    return node_ptr;
}

SQLNode *MakeLimitNode(int count) {
    LimitNode *node_ptr = new LimitNode(count);
    return (SQLNode *) node_ptr;
}

////////////////// Make Function Node///////////////////////////////////
SQLNode *MakeOrderByNode(SQLNode *order) {
    OrderByNode *node_ptr = new OrderByNode(order);
    return (SQLNode *) node_ptr;
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

            for(auto arg: func_node_ptr->GetArgs()) {
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

std::ostream &operator<<(std::ostream &output, const SQLNode &thiz) {
    thiz.Print(output);
    return output;
}

std::ostream &operator<<(std::ostream &output, const SQLNodeList &thiz) {
    thiz.Print(output, "");
    return output;
}

}
}
