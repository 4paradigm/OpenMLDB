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

}
}
