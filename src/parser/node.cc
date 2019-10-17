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
        case kConst: output = "kConst";
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
        default:return new UnknowSqlNode();
    }
}

////////////////// Make Table Node///////////////////////////////////
SQLNode *MakeTableNode(const std::string &name, const std::string &alias) {
    TableNode *node = new TableNode(name, alias);
    return (SQLNode *) node;
}


////////////////// Make Column Reference Node///////////////////////////////////
SQLNode *MakeColumnRefNode(const std::string &column_name, const std::string &relation_name) {
    ColumnRefNode *node = new ColumnRefNode(column_name, relation_name);
    return (SQLNode *) node;
}

///////////////// Make SQL Node List with single Node/////////////////////////
SQLNodeList *MakeNodeList(SQLNode *node) {
    SQLLinkedNode *head = new SQLLinkedNode(node);
    SQLNodeList *new_list = new SQLNodeList(head, head, 1);
    return new_list;
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
