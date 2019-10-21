//
// Created by chenjing on 2019/10/11.
//

#include "glog/logging.h"
#include "parser/node.h"

namespace fedb {
namespace sql {

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

////////////////// Make Table Node///////////////////////////////////
SQLNode *MakeTableNode(const std::string &name, const std::string &alias) {
    TableNode *node_ptr = new TableNode(name, alias);
    return (SQLNode *) node_ptr;
}

////////////////// Make Function Node///////////////////////////////////
SQLNode *MakeFuncNode(const std::string &name, SQLNodeList *list_ptr, SQLNode *over) {
    FuncNode *node_ptr = new FuncNode(name, list_ptr, (WindowDefNode *) (over));
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

SQLNode * MakeSelectStmtNode(SQLNodeList *select_list_ptr_, SQLNodeList *tableref_list_ptr, SQLNodeList *window_clause_ptr) {
    SelectStmt * node_ptr = new SelectStmt();
    FillSelectAttributions(node_ptr, select_list_ptr_, tableref_list_ptr, window_clause_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make Function Node///////////////////////////////////
SQLNode *MakeWindowDefNode(SQLNodeList *partitions, SQLNodeList *orders, SQLNode *frame) {
    WindowDefNode *node_ptr = new WindowDefNode();
    FillWindowSpection(node_ptr, partitions, orders, frame);
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

////////////////// Make Function Node///////////////////////////////////
SQLNode *MakeOrderByNode(SQLNode *order) {
    OrderByNode *node_ptr = new OrderByNode(order);
    return (SQLNode *) node_ptr;
}

// FIXME: this overloading does not work
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
