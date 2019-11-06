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
        default:LOG(WARNING) << "can not make sql node with type "
                             << NameOfSQLNodeType(type);
            return nullptr;
    }
}

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

SQLNode *NodeManager::MakeTableNode(const std::string &name,
                                    const std::string &alias) {
    TableNode *node_ptr = new TableNode(name, alias);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeResTargetNode(SQLNode *node,
                                        const std::string &name) {
    ResTarget *node_ptr = new ResTarget(name, node);
    RegisterNode((SQLNode *) node_ptr);
    return node_ptr;
}

SQLNode *NodeManager::MakeColumnRefNode(const std::string &column_name,
                                        const std::string &relation_name) {
    ColumnRefNode *node_ptr = new ColumnRefNode(column_name, relation_name);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeFuncNode(const std::string &name,
                                   SQLNodeList *list_ptr,
                                   SQLNode *over) {
    FuncNode *node_ptr = new FuncNode(name);
    FillSQLNodeList2NodeVector(list_ptr, node_ptr->GetArgs());
    node_ptr->SetOver((WindowDefNode *) over);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeLimitNode(int count) {
    LimitNode *node_ptr = new LimitNode(count);
    RegisterNode((SQLNode *) node_ptr);
    return (SQLNode *) node_ptr;
}

SQLNode *NodeManager::MakeWindowDefNode(SQLNodeList *partitions,
                                        SQLNodeList *orders,
                                        SQLNode *frame) {
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

SQLNode *NodeManager::MakeConstNode(long value, DataType time_type) {
    SQLNode *node_ptr = new ConstNode(value, time_type);
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

SQLNode *NodeManager::MakeConstNode(const char *value) {
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

SQLNode *NodeManager::MakeNameNode(const std::string &name) {
    SQLNode *node_ptr = new NameNode(name);
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeCreateTableNode(bool op_if_not_exist,
                                          const std::string &table_name,
                                          SQLNodeList *column_desc_list) {
    CreateStmt *node_ptr = new CreateStmt(table_name, op_if_not_exist);
    FillSQLNodeList2NodeVector(column_desc_list, node_ptr->GetColumnDefList());
    return RegisterNode((SQLNode *) node_ptr);
}

SQLNode *NodeManager::MakeColumnIndexNode(SQLNodeList *index_item_list) {
    ColumnIndexNode *node_ptr = new ColumnIndexNode();
    if (nullptr != index_item_list && 0 != index_item_list->GetSize()) {
        SQLLinkedNode *cur = index_item_list->GetHead();
        while (nullptr != cur && nullptr != cur->node_ptr_) {
            switch (cur->node_ptr_->GetType()) {
                case kIndexKey:node_ptr->SetKey(((IndexKeyNode *) cur->node_ptr_)->GetKey());
                    break;
                case kIndexTs:node_ptr->SetTs(((IndexTsNode *) cur->node_ptr_)->GetColumnName());
                    break;
                case kIndexVersion:node_ptr->SetVersion(((IndexVersionNode *) cur->node_ptr_)->GetColumnName());
                    node_ptr->SetVersionCount(((IndexVersionNode *) cur->node_ptr_)->GetCount());

                    break;
                case kPrimary:node_ptr->SetTTL((ConstNode *) cur->node_ptr_);
                    break;
                default: {
                    LOG(WARNING) << "can not handle type "
                                 << NameOfSQLNodeType(cur->node_ptr_->GetType())
                                 << " for column index";
                }
            }
            cur = cur->next_;
        }

    }
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeColumnIndexNode(SQLNodeList *keys,
                                          SQLNode *ts,
                                          SQLNode *ttl,
                                          SQLNode *version) {
    SQLNode *node_ptr = new SQLNode(kColumnIndex, 0, 0);
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeColumnDescNode(const std::string &column_name,
                                         const DataType data_type,
                                         bool op_not_null) {
    SQLNode *node_ptr = new ColumnDefNode(column_name, data_type, op_not_null);
    return RegisterNode(node_ptr);
}

SQLNodeList *NodeManager::MakeNodeList() {
    SQLNodeList *new_list_ptr = new SQLNodeList();
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}

SQLNodeList *NodeManager::MakeNodeList(SQLNode *node_ptr) {
    SQLLinkedNode *linked_node_ptr = MakeLinkedNode(node_ptr);
    SQLNodeList
        *new_list_ptr = new SQLNodeList(linked_node_ptr, linked_node_ptr, 1);
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}

SQLLinkedNode *NodeManager::MakeLinkedNode(SQLNode *node_ptr) {
    SQLLinkedNode *linked_node_ptr = new SQLLinkedNode(node_ptr);
    RegisterNode(linked_node_ptr);
    return linked_node_ptr;
}

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

ScanPlanNode *NodeManager::MakeSeqScanPlanNode(const std::string &table) {
    node::ScanPlanNode *node_ptr = new ScanPlanNode(table, kScanTypeSeqScan);
    RegisterNode((PlanNode *) node_ptr);
    return node_ptr;
}

ScanPlanNode *NodeManager::MakeIndexScanPlanNode(const std::string &table) {
    node::ScanPlanNode *node_ptr = new ScanPlanNode(table, kScanTypeIndexScan);
    RegisterNode((PlanNode *) node_ptr);
    return node_ptr;
}

ProjectListPlanNode *NodeManager::MakeProjectListPlanNode(const std::string &table,
                                                          const std::string &w) {
    ProjectListPlanNode *node_ptr = new ProjectListPlanNode(table, w);
    RegisterNode((PlanNode *) node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakePlanNode(const PlanType &type) {
    PlanNode *node_ptr;
    switch (type) {
        case kPlanTypeSelect:node_ptr = (PlanNode *) new SelectPlanNode();
            break;
        case kProjectList:node_ptr = (PlanNode *) new ProjectListPlanNode();
            break;
        case kProject:node_ptr = (PlanNode *) new ProjectPlanNode();
            break;
        case kPlanTypeLimit:node_ptr = (PlanNode *) new LimitPlanNode();
            break;
        case kPlanTypeCreate:node_ptr = (PlanNode *) new CreatePlanNode();
            break;
        default: node_ptr = (PlanNode *) new LeafPlanNode(kUnknowPlan);
    }
    RegisterNode(node_ptr);
    return node_ptr;
}

FnNode *NodeManager::MakeFnDefNode(const std::string &name,
                                   FnNode *plist,
                                   DataType return_type) {
    ::fesql::node::FnNodeFnDef *fn_def = new FnNodeFnDef(name, return_type);
    fn_def->AddChildren(plist);
    return RegisterNode((FnNode *) fn_def);
}

FnNode *NodeManager::MakeAssignNode(const std::string &name,
                                    FnNode *expression) {
    ::fesql::node::FnAssignNode
        *fn_assign = new ::fesql::node::FnAssignNode(name);
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

FnNode *NodeManager::MakeFnParaNode(const std::string &name,
                                    const DataType &para_type) {
    ::fesql::node::FnParaNode
        *para_node = new ::fesql::node::FnParaNode(name, para_type);
    return RegisterNode((FnNode *) para_node);
}

FnNode *NodeManager::MakeTypeNode(const DataType &type) {
    FnTypeNode *type_node = new FnTypeNode();
    type_node->data_type_ = type;
    return RegisterNode((FnNode *) type_node);
}

FnNode *NodeManager::MakeFnIdNode(const std::string &name) {
    ::fesql::node::FnIdNode *id_node = new ::fesql::node::FnIdNode(name);
    return RegisterNode((FnNode *) id_node);
}

FnNode *NodeManager::MakeBinaryExprNode(FnNode *left,
                                        FnNode *right,
                                        FnOperator op) {
    ::fesql::node::FnBinaryExpr *bexpr = new ::fesql::node::FnBinaryExpr(op);
    bexpr->AddChildren(left);
    bexpr->AddChildren(right);
    return RegisterNode((FnNode *) bexpr);
}

FnNode *NodeManager::MakeUnaryExprNode(FnNode *left, FnOperator op) {
    ::fesql::node::FnUnaryExpr *uexpr = new ::fesql::node::FnUnaryExpr(op);
    uexpr->AddChildren(left);
    return RegisterNode((FnNode *) uexpr);
}

SQLNode *NodeManager::MakeKeyNode(SQLNodeList *key_list) {
    SQLNode *node_ptr = new SQLNode(kIndexKey, 0, 0);

    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeKeyNode(const std::string &key) {
    SQLNode *node_ptr = new SQLNode(kIndexKey, 0, 0);

    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeIndexKeyNode(const std::string &key) {
    SQLNode *node_ptr = new IndexKeyNode(key);
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeIndexTsNode(const std::string &ts) {
    SQLNode *node_ptr = new IndexTsNode(ts);
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeIndexVersionNode(const std::string &version) {
    SQLNode *node_ptr = new IndexVersionNode(version);
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeIndexVersionNode(const std::string &version,
                                           int count) {
    SQLNode *node_ptr = new IndexVersionNode(version, count);
    return RegisterNode(node_ptr);
}

}
}

