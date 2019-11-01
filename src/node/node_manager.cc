/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * node_manager.cc
 *      
 * Author: chenjing
 * Date: 2019/10/28 
 *--------------------------------------------------------------------------
**/

#include "node_manager.h"
namespace fesql {
namespace node {

/**
 * TODO: 全局内存管理；
 *  1. new SQLNode
 *  2. delete SQLNode
 *  3. maintain SQLNode
 *  4. Object pool maybe ...
 * @param type
 * @param ...
 * @return
 */



////////////////// Make SQL Parser Node///////////////////////////////////

SQLNode *NodeManager::MakeSQLNode(const SQLNodeType &type) {
    switch (type) {
        case kSelectStmt:return RegisterNode((SQLNode *) new SelectStmt());
        case kExpr: return RegisterNode((SQLNode *) new SQLExprNode());
        case kResTarget: return RegisterNode((SQLNode *) new SQLExprNode());
        case kTable: return RegisterNode((SQLNode *) new TableNode());
        case kFunc: return RegisterNode((SQLNode *) new FuncNode());
        case kWindowFunc: return RegisterNode((SQLNode *) new FuncNode());
        case kWindowDef: return RegisterNode((SQLNode *) new WindowDefNode());
        case kFrameBound: return RegisterNode((SQLNode *) new FrameBound());
        case kFrames: return RegisterNode((SQLNode *) new FrameNode());
        case kColumnRef: return RegisterNode((SQLNode *) new ColumnRefNode());
        case kConst: return RegisterNode((SQLNode *) new ConstNode());
        case kOrderBy: return RegisterNode((SQLNode *) new OrderByNode(nullptr));
        case kLimit: return RegisterNode((SQLNode *) new LimitNode(0));
        case kAll: return RegisterNode((SQLNode *) new AllNode());
        case kFnDef: return RegisterNode((SQLNode *) new FnNodeFnDef());
        default:LOG(WARNING) << "can not make sql node with type " << NameOfSQLNodeType(type);
            return nullptr;
    }
}

////////////////// Make Select Node///////////////////////////////////
SQLNode *NodeManager::MakeSelectStmtNode(SQLNodeList *select_list_ptr,
                                         SQLNodeList *tableref_list_ptr,
                                         SQLNodeList *window_clause_ptr,
                                         SQLNode *limit_ptr) {
    SelectStmt *node_ptr = new SelectStmt();

    FillSQLNodeList2NodeVector(select_list_ptr, node_ptr->GetSelectList());
    // 释放SQLNodeList

    FillSQLNodeList2NodeVector(tableref_list_ptr, node_ptr->GetTableRefList());
    // 释放SQLNodeList

    FillSQLNodeList2NodeVector(window_clause_ptr, node_ptr->GetWindowList());
    // 释放SQLNodeList
    node_ptr->SetLimit(limit_ptr);

    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make Table Node///////////////////////////////////
SQLNode *NodeManager::MakeTableNode(const std::string &name, const std::string &alias) {
    TableNode *node_ptr = new TableNode(name, alias);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make ResTarget Node///////////////////////////////////
SQLNode *NodeManager::MakeResTargetNode(SQLNode *node, const std::string &name) {
    ResTarget *node_ptr = new ResTarget(name, node);
    RegisterNode((SQLNode *) node_ptr);
    return node_ptr;
}

////////////////// Make Column Reference Node///////////////////////////////////
SQLNode *NodeManager::MakeColumnRefNode(const std::string &column_name, const std::string &relation_name) {
    ColumnRefNode *node_ptr = new ColumnRefNode(column_name, relation_name);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make Function Node///////////////////////////////////
SQLNode *NodeManager::MakeFuncNode(const std::string &name, SQLNodeList *list_ptr, SQLNode *over) {
    FuncNode *node_ptr = new FuncNode(name);
    FillSQLNodeList2NodeVector(list_ptr, node_ptr->GetArgs());
    node_ptr->SetOver((WindowDefNode *) over);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make Limit Node///////////////////////////////////
SQLNode *NodeManager::MakeLimitNode(int count) {
    LimitNode *node_ptr = new LimitNode(count);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make Function Node///////////////////////////////////
SQLNode *NodeManager::MakeWindowDefNode(SQLNodeList *partitions, SQLNodeList *orders, SQLNode *frame) {
    WindowDefNode *node_ptr = new WindowDefNode();
    FillSQLNodeList2NodeVector(partitions, node_ptr->GetPartitions());
    FillSQLNodeList2NodeVector(orders, node_ptr->GetOrders());
    node_ptr->SetFrame(frame);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeWindowDefNode(const std::string &name) {
    WindowDefNode *node_ptr = new WindowDefNode();
    node_ptr->SetName(name);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

////////////////// Make Frame and Bound Node///////////////////////////////////
SQLNode *NodeManager::MakeFrameBound(SQLNodeType bound_type) {
    FrameBound *node_ptr = new FrameBound(bound_type);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeFrameBound(SQLNodeType bound_type, SQLNode *offset) {
    FrameBound *node_ptr = new FrameBound(bound_type, offset);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}
SQLNode *NodeManager::MakeFrameNode(SQLNode *start, SQLNode *end) {
    FrameNode *node_ptr = new FrameNode(kFrameRange, start, end);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}
SQLNode *NodeManager::MakeRangeFrameNode(SQLNode *node_ptr) {
    ((FrameNode *) node_ptr)->SetFrameType(kFrameRange);
    return node_ptr;
}

SQLNode *NodeManager::MakeRowsFrameNode(SQLNode *node_ptr) {
    ((FrameNode *) node_ptr)->SetFrameType(kFrameRows);
    return node_ptr;
}

SQLNode *NodeManager::MakeOrderByNode(SQLNode *order) {
    OrderByNode *node_ptr = new OrderByNode(order);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

///////////////// Make Const Node /////////////////////////
SQLNode *NodeManager::MakeConstNode(int value) {
    SQLNode *node_ptr = new ConstNode(value);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeConstNode(long value) {
    SQLNode *node_ptr = new ConstNode(value);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeConstNode(float value) {
    SQLNode *node_ptr = new ConstNode(value);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeConstNode(double value) {
    SQLNode *node_ptr = new ConstNode(value);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeConstNode(const std::string &value) {
    SQLNode *node_ptr = new ConstNode(value);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}
SQLNode *NodeManager::MakeConstNode() {
    SQLNode *node_ptr = new ConstNode();
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

///////////////// Make SQL Node List/////////////////////////
SQLNodeList *NodeManager::MakeNodeList() {
    SQLNodeList *new_list_ptr = new SQLNodeList();
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}

SQLNodeList *NodeManager::MakeNodeList(SQLNode *node_ptr) {
    SQLLinkedNode *linked_node_ptr = MakeLinkedNode(node_ptr);
    SQLNodeList *new_list_ptr = new SQLNodeList(linked_node_ptr, linked_node_ptr, 1);
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}

SQLLinkedNode *NodeManager::MakeLinkedNode(SQLNode *node_ptr) {
    SQLLinkedNode *linked_node_ptr = new SQLLinkedNode(node_ptr);
    RegisterNode(linked_node_ptr);
    return linked_node_ptr;
}

////////////////// Make Plan Node///////////////////////////////////
PlanNode *NodeManager::MakeLeafPlanNode(const PlanType &type) {
    PlanNode *node_ptr = (PlanNode *) new LeafPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakeUnaryPlanNode(const PlanType &type) {
    PlanNode *node_ptr = (PlanNode *) new UnaryPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakeBinaryPlanNode(const PlanType &type) {
    PlanNode *node_ptr = (PlanNode *) new BinaryPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakeMultiPlanNode(const PlanType &type) {
    PlanNode *node_ptr = (PlanNode *) new MultiChildPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}
ProjectListPlanNode *NodeManager::MakeProjectListPlanNode(const std::string &table, const std::string &w) {
    ProjectListPlanNode *node_ptr = new ProjectListPlanNode(table, w);
    RegisterNode((PlanNode *) node_ptr);
    return node_ptr;
}

ProjectPlanNode *NodeManager::MakeProjectPlanNode(node::SQLNode *expression,
                                                  const std::string &name,
                                                  const std::string &table,
                                                  const std::string &w) {
    ProjectPlanNode *node_ptr = new ProjectPlanNode(expression, name, table, w);
    RegisterNode((PlanNode *) node_ptr);

    return node_ptr;
}

PlanNode *NodeManager::MakePlanNode(const PlanType &type) {
    PlanNode *node_ptr;
    switch (type) {
        case kSelect:node_ptr = (PlanNode *) new SelectPlanNode();
            break;
        case kProjectList:node_ptr = (PlanNode *) new ProjectListPlanNode();
            break;
        case kProject:node_ptr = (PlanNode *) new ProjectPlanNode();
            break;
        default: node_ptr = new PlanNode(kUnknowPlan);
    }
    RegisterNode(node_ptr);
    return node_ptr;
}

FnNode *NodeManager::MakeFnDefNode(const std::string &name, FnNode *plist, SQLNodeType return_type) {
    ::fesql::node::FnNodeFnDef *fn_def = new FnNodeFnDef();
    fn_def->name = const_cast<char *>(name.c_str());
    fn_def->AddChildren(plist);
    fn_def->ret_type = return_type;
    return RegisterNode((FnNode *) fn_def);
}

FnNode *NodeManager::MakeAssignNode(const std::string &name, FnNode *expression) {
    ::fesql::node::FnAssignNode *fn_assign = new ::fesql::node::FnAssignNode();
    fn_assign->name = name;
    fn_assign->AddChildren(expression);
    return RegisterNode((FnNode *) fn_assign);
}

FnNode *NodeManager::MakeReturnStmtNode(FnNode *value) {
    FnNode *fn_node = new FnNode(kFnReturnStmt);
    fn_node->AddChildren(value);
    return RegisterNode((FnNode *) fn_node);
}

FnNode *NodeManager::MakeFnNode(const SQLNodeType &type) {
    return RegisterNode((FnNode *) new FnNode(type));
}

FnNode *NodeManager::MakeFnParaNode(const std::string &name, SQLNodeType para_type) {
    ::fesql::node::FnParaNode* para_node = new ::fesql::node::FnParaNode();
    para_node->name = name;
    para_node->para_type = para_type;
    return RegisterNode((FnNode *) para_node);
}

FnNode *NodeManager::MakeTypeNode(const DataType &type) {
    FnTypeNode* type_node = new FnTypeNode();
    type_node->data_type_ = type;
    return RegisterNode((FnNode *) type_node);
}

FnNode *NodeManager::MakeFnIdNode(const std::string &name) {
    ::fesql::node::FnIdNode* id_node = new ::fesql::node::FnIdNode();
    id_node->name = name;
    return RegisterNode((FnNode *) id_node);
}

FnNode *NodeManager::MakeBinaryExprNode(FnNode *left, FnNode *right, FnOperator op) {
    ::fesql::node::FnBinaryExpr* bexpr = new ::fesql::node::FnBinaryExpr();
    bexpr->AddChildren(left);
    bexpr->AddChildren(right);
    bexpr->op = op;
    return RegisterNode((FnNode *) bexpr);
}

FnNode *NodeManager::MakeUnaryExprNode(FnNode *left, FnOperator op) {
    ::fesql::node::FnUnaryExpr* uexpr = new ::fesql::node::FnUnaryExpr();
    uexpr->AddChildren(left);
    uexpr->op = op;
    return RegisterNode((FnNode *) uexpr);
}

}
}

