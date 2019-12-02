/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * node_manager.cc
 *
 * Author: chenjing
 * Date: 2019/10/28
 *--------------------------------------------------------------------------
 **/

#include "node/node_manager.h"
#include <string>
#include <vector>

namespace fesql {
namespace node {

SQLNode *NodeManager::MakeSQLNode(const SQLNodeType &type) {
    switch (type) {
        case kSelectStmt:
            return RegisterNode(new SelectStmt());
        case kExpr:
            return RegisterNode(new ExprNode(kExprUnknow));
        case kResTarget:
            return RegisterNode(new ResTarget());
        case kTable:
            return RegisterNode(new TableNode());
        case kWindowFunc:
            return RegisterNode(new CallExprNode());
        case kWindowDef:
            return RegisterNode(new WindowDefNode());
        case kFrameBound:
            return RegisterNode(new FrameBound());
        case kFrames:
            return RegisterNode(new FrameNode());
        case kConst:
            return RegisterNode(new ConstNode());
        case kOrderBy:
            return RegisterNode(new OrderByNode(nullptr));
        case kLimit:
            return RegisterNode(new LimitNode(0));
        default:
            LOG(WARNING) << "can not make sql node with type "
                         << NameOfSQLNodeType(type);
            return RegisterNode(new SQLNode(kUnknow, 0, 0));
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

    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeTableNode(const std::string &name,
                                    const std::string &alias) {
    TableNode *node_ptr = new TableNode(name, alias);

    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeResTargetNode(ExprNode *node,
                                        const std::string &name) {
    ResTarget *node_ptr = new ResTarget(name, node);
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeLimitNode(int count) {
    LimitNode *node_ptr = new LimitNode(count);
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeWindowDefNode(ExprListNode *partitions,
                                        ExprListNode *orders, SQLNode *frame) {
    WindowDefNode *node_ptr = new WindowDefNode();
    for (auto expr : orders->children) {
        switch (expr->GetExprType()) {
            case kExprColumnRef:
                node_ptr->GetOrders().push_back(
                    dynamic_cast<ColumnRefNode *>(expr)->GetColumnName());
                break;
            default: {
                LOG(WARNING) << "fail to create window node with invalid expr";
                delete node_ptr;
                return nullptr;
            }
        }
    }
    for (auto expr : partitions->children) {
        switch (expr->GetExprType()) {
            case kExprColumnRef:
                node_ptr->GetPartitions().push_back(
                    dynamic_cast<ColumnRefNode *>(expr)->GetColumnName());
                break;
            default: {
                LOG(WARNING) << "fail to create window node with invalid expr";
                delete node_ptr;
                return nullptr;
            }
        }
    }
    node_ptr->SetFrame(dynamic_cast<FrameNode *>(frame));
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeWindowDefNode(const std::string &name) {
    WindowDefNode *node_ptr = new WindowDefNode();
    node_ptr->SetName(name);
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeFrameBound(SQLNodeType bound_type) {
    FrameBound *node_ptr = new FrameBound(bound_type);
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeFrameBound(SQLNodeType bound_type, ExprNode *offset) {
    FrameBound *node_ptr = new FrameBound(bound_type, offset);
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeFrameNode(SQLNode *start, SQLNode *end) {
    FrameNode *node_ptr =
        new FrameNode(kFrameRange, dynamic_cast<FrameBound *>(start),
                      dynamic_cast<FrameBound *>(end));
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeRangeFrameNode(SQLNode *node_ptr) {
    dynamic_cast<FrameNode *>(node_ptr)->SetFrameType(kFrameRange);
    return node_ptr;
}

SQLNode *NodeManager::MakeRowsFrameNode(SQLNode *node_ptr) {
    dynamic_cast<FrameNode *>(node_ptr)->SetFrameType(kFrameRows);
    return node_ptr;
}

SQLNode *NodeManager::MakeOrderByNode(SQLNode *order) {
    OrderByNode *node_ptr = new OrderByNode(order);
    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeColumnRefNode(const std::string &column_name,
                                         const std::string &relation_name) {
    ColumnRefNode *node_ptr = new ColumnRefNode(column_name, relation_name);

    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeFuncNode(const std::string &name,
                                    SQLNodeList *list_ptr, SQLNode *over) {
    CallExprNode *node_ptr = new CallExprNode(name);
    FillSQLNodeList2NodeVector(list_ptr, node_ptr->GetArgs());
    node_ptr->SetOver(dynamic_cast<WindowDefNode *>(over));
    return RegisterNode(node_ptr);
}
ExprNode *NodeManager::MakeConstNode(int value) {
    ExprNode *node_ptr = new ConstNode(value);
    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeConstNode(int64_t value) {
    ExprNode *node_ptr = new ConstNode(value);
    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeConstNode(int64_t value, DataType time_type) {
    ExprNode *node_ptr = new ConstNode(value, time_type);
    return RegisterNode(node_ptr);
}
ExprNode *NodeManager::MakeConstNode(float value) {
    ExprNode *node_ptr = new ConstNode(value);
    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeConstNode(double value) {
    ExprNode *node_ptr = new ConstNode(value);
    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeConstNode(const char *value) {
    ExprNode *node_ptr = new ConstNode(value);
    return RegisterNode(node_ptr);
}
ExprNode *NodeManager::MakeConstNode(const std::string &value) {
    ExprNode *node_ptr = new ConstNode(value);
    return RegisterNode(node_ptr);
}
ExprNode *NodeManager::MakeConstNode() {
    ExprNode *node_ptr = new ConstNode();
    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeFnIdNode(const std::string &name) {
    ::fesql::node::ExprNode *id_node = new ::fesql::node::ExprIdNode(name);
    return RegisterNode(id_node);
}

ExprNode *NodeManager::MakeBinaryExprNode(ExprNode *left, ExprNode *right,
                                          FnOperator op) {
    ::fesql::node::BinaryExpr *bexpr = new ::fesql::node::BinaryExpr(op);
    bexpr->AddChild(left);
    bexpr->AddChild(right);
    return RegisterNode(bexpr);
}

ExprNode *NodeManager::MakeUnaryExprNode(ExprNode *left, FnOperator op) {
    ::fesql::node::UnaryExpr *uexpr = new ::fesql::node::UnaryExpr(op);
    uexpr->AddChild(left);
    return RegisterNode(uexpr);
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
    return RegisterNode(node_ptr);
}

SQLNode *NodeManager::MakeColumnIndexNode(SQLNodeList *index_item_list) {
    ColumnIndexNode *index_ptr = new ColumnIndexNode();
    if (nullptr != index_item_list && 0 != index_item_list->GetSize()) {
        for (auto node_ptr : index_item_list->GetList()) {
            switch (node_ptr->GetType()) {
                case kIndexKey:
                    index_ptr->SetKey(
                        dynamic_cast<IndexKeyNode *>(node_ptr)->GetKey());
                    break;
                case kIndexTs:
                    index_ptr->SetTs(
                        dynamic_cast<IndexTsNode *>(node_ptr)->GetColumnName());
                    break;
                case kIndexVersion:
                    index_ptr->SetVersion(
                        dynamic_cast<IndexVersionNode *>(node_ptr)
                            ->GetColumnName());

                    index_ptr->SetVersionCount(
                        dynamic_cast<IndexVersionNode *>(node_ptr)->GetCount());
                    break;
                case kIndexTTL: {
                    IndexTTLNode *ttl_node =
                        dynamic_cast<IndexTTLNode *>(node_ptr);
                    index_ptr->SetTTL(ttl_node->GetTTLExpr());
                    break;
                }
                default: {
                    LOG(WARNING) << "can not handle type "
                                 << NameOfSQLNodeType(node_ptr->GetType())
                                 << " for column index";
                }
            }
        }
    }
    return RegisterNode(index_ptr);
}
SQLNode *NodeManager::MakeColumnIndexNode(SQLNodeList *keys, SQLNode *ts,
                                          SQLNode *ttl, SQLNode *version) {
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

SQLNodeList *NodeManager::MakeNodeList(SQLNode *node) {
    SQLNodeList *new_list_ptr = new SQLNodeList();
    new_list_ptr->PushBack(node);
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}

ExprListNode *NodeManager::MakeExprList() {
    ExprListNode *new_list_ptr = new ExprListNode();
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}
ExprListNode *NodeManager::MakeExprList(ExprNode *expr_node) {
    ExprListNode *new_list_ptr = new ExprListNode();
    new_list_ptr->AddChild(expr_node);
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}

PlanNode *NodeManager::MakeLeafPlanNode(const PlanType &type) {
    PlanNode *node_ptr = new LeafPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakeUnaryPlanNode(const PlanType &type) {
    PlanNode *node_ptr = new UnaryPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakeBinaryPlanNode(const PlanType &type) {
    PlanNode *node_ptr = new BinaryPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakeMultiPlanNode(const PlanType &type) {
    PlanNode *node_ptr = new MultiChildPlanNode(type);
    RegisterNode(node_ptr);
    return node_ptr;
}

ScanPlanNode *NodeManager::MakeSeqScanPlanNode(const std::string &table) {
    node::ScanPlanNode *node_ptr = new ScanPlanNode(table, kScanTypeSeqScan);
    RegisterNode(node_ptr);
    return node_ptr;
}

ScanPlanNode *NodeManager::MakeIndexScanPlanNode(const std::string &table) {
    node::ScanPlanNode *node_ptr = new ScanPlanNode(table, kScanTypeIndexScan);
    RegisterNode(node_ptr);
    return node_ptr;
}

ProjectListPlanNode *NodeManager::MakeProjectListPlanNode(
    const std::string &table, WindowPlanNode *w_ptr) {

    ProjectListPlanNode *node_ptr =
        new ProjectListPlanNode(table, w_ptr, w_ptr != nullptr);
    RegisterNode(node_ptr);
    return node_ptr;
}

PlanNode *NodeManager::MakePlanNode(const PlanType &type) {
    PlanNode *node_ptr;
    switch (type) {
        case kPlanTypeSelect:
            node_ptr = new SelectPlanNode();
            break;
        case kProjectList:
            node_ptr = new ProjectListPlanNode();
            break;
        case kProject:
            node_ptr = new ProjectPlanNode();
            break;
        case kPlanTypeLimit:
            node_ptr = new LimitPlanNode();
            break;
        case kPlanTypeCreate:
            node_ptr = new CreatePlanNode();
            break;
        case kPlanTypeCmd:
            node_ptr = new CmdPlanNode();
            break;
        case kPlanTypeInsert:
            node_ptr = new InsertPlanNode();
            break;
        case kPlanTypeFuncDef:
            node_ptr = new FuncDefPlanNode();
            break;
        case kPlanTypeWindow:
            node_ptr = new WindowPlanNode();
            break;
        default:
            node_ptr = new LeafPlanNode(kUnknowPlan);
    }
    RegisterNode(node_ptr);
    return node_ptr;
}

FnNode *NodeManager::MakeFnDefNode(const std::string &name, FnNodeList *plist,
                                   DataType return_type) {
    ::fesql::node::FnNodeFnDef *fn_def =
        new FnNodeFnDef(name, plist, return_type);
    return RegisterNode(fn_def);
}

FnNode *NodeManager::MakeAssignNode(const std::string &name,
                                    ExprNode *expression) {
    ::fesql::node::FnAssignNode *fn_assign =
        new fesql::node::FnAssignNode(name, expression);
    return RegisterNode(fn_assign);
}

FnNode *NodeManager::MakeReturnStmtNode(ExprNode *value) {
    FnNode *fn_node = new FnReturnStmt(value);
    return RegisterNode(fn_node);
}

FnNode *NodeManager::MakeFnNode(const SQLNodeType &type) {
    return RegisterNode(new FnNode(type));
}

FnNodeList *NodeManager::MakeFnListNode() {
    FnNodeList *fn_list = new FnNodeList();
    RegisterNode(fn_list);
    return fn_list;
}
FnNode *NodeManager::MakeFnParaNode(const std::string &name,
                                    const DataType &para_type) {
    ::fesql::node::FnParaNode *para_node =
        new ::fesql::node::FnParaNode(name, para_type);
    return RegisterNode(para_node);
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

SQLNode *NodeManager::MakeIndexTTLNode(ExprNode *ttl_expr) {
    SQLNode *node_ptr = new IndexTTLNode(ttl_expr);
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
SQLNode *NodeManager::MakeCmdNode(node::CmdType cmd_type) {
    SQLNode *node_ptr = new CmdNode(cmd_type);
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeCmdNode(node::CmdType cmd_type,
                                  const std::string &arg) {
    CmdNode *node_ptr = new CmdNode(cmd_type);
    node_ptr->AddArg(arg);
    return RegisterNode(node_ptr);
}
ExprNode *NodeManager::MakeAllNode(const std::string &relation_name) {
    ExprNode *node_ptr = new AllNode(relation_name);
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeInsertTableNode(const std::string &table_name,
                                          const ExprListNode *columns_expr,
                                          const ExprListNode *values) {
    if (nullptr == columns_expr) {
        InsertStmt *node_ptr = new InsertStmt(table_name, values->children);
        return RegisterNode(node_ptr);
    } else {
        std::vector<std::string> column_names;
        for (auto expr : columns_expr->children) {
            switch (expr->GetExprType()) {
                case kExprColumnRef: {
                    ColumnRefNode *column_ref =
                        dynamic_cast<ColumnRefNode *>(expr);
                    column_names.push_back(column_ref->GetColumnName());
                    break;
                }
                default: {
                    LOG(WARNING)
                        << "Can't not handle insert column name with type"
                        << ExprTypeName(expr->GetExprType());
                }
            }
        }
        InsertStmt *node_ptr =
            new InsertStmt(table_name, column_names, values->children);
        return RegisterNode(node_ptr);
    }
}


}  // namespace node
}  // namespace fesql
