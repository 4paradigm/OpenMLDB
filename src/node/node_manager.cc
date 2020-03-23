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
#include <utility>
#include <vector>

namespace fesql {
namespace node {

QueryNode *NodeManager::MakeSelectQueryNode(
    bool is_distinct, SQLNodeList *select_list_ptr,
    SQLNodeList *tableref_list_ptr, ExprNode *where_expr,
    ExprListNode *group_expr_list, ExprNode *having_expr,
    ExprNode *order_expr_list, SQLNodeList *window_list_ptr,
    SQLNode *limit_ptr) {
    SelectQueryNode *node_ptr =
        new SelectQueryNode(is_distinct, select_list_ptr, tableref_list_ptr,
                            where_expr, group_expr_list, having_expr,
                            dynamic_cast<OrderByNode *>(order_expr_list),
                            window_list_ptr, limit_ptr);
    RegisterNode(node_ptr);
    return node_ptr;
}

QueryNode *NodeManager::MakeUnionQueryNode(QueryNode *left, QueryNode *right,
                                           bool is_all) {
    UnionQueryNode *node_ptr = new UnionQueryNode(left, right, is_all);
    RegisterNode(node_ptr);
    return node_ptr;
}

TableRefNode *NodeManager::MakeTableNode(const std::string &name,
                                         const std::string &alias) {
    TableRefNode *node_ptr = new TableNode(name, alias);
    RegisterNode(node_ptr);
    return node_ptr;
}

TableRefNode *NodeManager::MakeJoinNode(const TableRefNode *left,
                                        const TableRefNode *right,
                                        const JoinType type,
                                        const ExprNode *condition,
                                        const std::string alias) {
    TableRefNode *node_ptr = new JoinNode(left, right, type, condition, alias);
    RegisterNode(node_ptr);
    return node_ptr;
}

TableRefNode *NodeManager::MakeQueryRefNode(const QueryNode *sub_query,
                                            const std::string &alias) {
    TableRefNode *node_ptr = new QueryRefNode(sub_query, alias);
    RegisterNode(node_ptr);
    return node_ptr;
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
                                        ExprNode *orders, SQLNode *frame) {
    WindowDefNode *node_ptr = new WindowDefNode();
    if (nullptr != orders) {
        if (node::kExprOrder != orders->GetExprType()) {
            LOG(WARNING)
                << "fail to create window node with invalid order type " +
                       NameOfSQLNodeType(orders->GetType());
            delete node_ptr;
            return nullptr;
        }
        auto expr_list = dynamic_cast<OrderByNode *>(orders)->order_by_;
        for (auto expr : expr_list->children_) {
            switch (expr->GetExprType()) {
                case kExprColumnRef:
                    // TODO(chenjing): window 支持未来窗口
                    node_ptr->GetOrders().push_back(
                        dynamic_cast<const ColumnRefNode *>(expr)
                            ->GetColumnName());
                    break;
                    // TODO(chenjing): 支持复杂表达式作为order列
                default: {
                    LOG(WARNING)
                        << "fail to create window node with invalid expr";
                    delete node_ptr;
                    return nullptr;
                }
            }
        }
    }

    for (auto expr : partitions->children_) {
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

ExprNode *NodeManager::MakeOrderByNode(ExprListNode *order, const bool is_asc) {
    OrderByNode *node_ptr = new OrderByNode(order, is_asc);
    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeColumnRefNode(const std::string &column_name,
                                         const std::string &relation_name) {
    ColumnRefNode *node_ptr = new ColumnRefNode(column_name, relation_name);

    return RegisterNode(node_ptr);
}

ExprNode *NodeManager::MakeFuncNode(const std::string &name,
                                    const ExprListNode *list_ptr,
                                    const SQLNode *over) {
    CallExprNode *node_ptr = new CallExprNode(
        name, list_ptr, dynamic_cast<const WindowDefNode *>(over));
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

ExprNode *NodeManager::MakeExprIdNode(const std::string &name) {
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

PlanNode *NodeManager::MakeTablePlanNode(const std::string &table_name) {
    PlanNode *node_ptr = new TablePlanNode("", table_name);
    return RegisterNode(node_ptr);
}

PlanNode *NodeManager::MakeRenamePlanNode(PlanNode *node,
                                          std::string alias_name) {
    PlanNode *node_ptr = new RenamePlanNode(node, alias_name);
    return RegisterNode(node_ptr);
}

FilterPlanNode *NodeManager::MakeFilterPlanNode(PlanNode *node,
                                                const ExprNode *condition) {
    node::FilterPlanNode *node_ptr = new FilterPlanNode(node, condition);
    RegisterNode(node_ptr);
    return node_ptr;
}

WindowPlanNode *NodeManager::MakeWindowPlanNode(int w_id) {
    WindowPlanNode *node_ptr = new WindowPlanNode(w_id);
    RegisterNode(node_ptr);
    return node_ptr;
}

ProjectListNode *NodeManager::MakeProjectListPlanNode(const WindowPlanNode *w_ptr) {
    ProjectListNode *node_ptr = new ProjectListNode(w_ptr, w_ptr != nullptr);
    RegisterNode(node_ptr);
    return node_ptr;
}

FnNode *NodeManager::MakeFnHeaderNode(const std::string &name,
                                      FnNodeList *plist,
                                      const TypeNode *return_type) {
    ::fesql::node::FnNodeFnHeander *fn_header =
        new FnNodeFnHeander(name, plist, return_type);
    return RegisterNode(fn_header);
}

FnNode *NodeManager::MakeFnDefNode(const FnNode *header,
                                   const FnNodeList *block) {
    ::fesql::node::FnNodeFnDef *fn_def =
        new FnNodeFnDef(dynamic_cast<const FnNodeFnHeander *>(header), block);
    return RegisterNode(fn_def);
}
FnNode *NodeManager::MakeAssignNode(const std::string &name,
                                    ExprNode *expression) {
    ::fesql::node::FnAssignNode *fn_assign =
        new fesql::node::FnAssignNode(name, expression);
    return RegisterNode(fn_assign);
}

FnNode *NodeManager::MakeAssignNode(const std::string &name,
                                    ExprNode *expression, const FnOperator op) {
    ::fesql::node::FnAssignNode *fn_assign = new fesql::node::FnAssignNode(
        name, MakeBinaryExprNode(MakeExprIdNode(name), expression, op));

    return RegisterNode(fn_assign);
}
FnNode *NodeManager::MakeReturnStmtNode(ExprNode *value) {
    FnNode *fn_node = new FnReturnStmt(value);
    return RegisterNode(fn_node);
}

FnNode *NodeManager::MakeIfStmtNode(const ExprNode *value) {
    FnNode *fn_node = new FnIfNode(value);
    return RegisterNode(fn_node);
}
FnNode *NodeManager::MakeElseStmtNode() {
    FnNode *fn_node = new FnElseNode();
    return RegisterNode(fn_node);
}
FnNode *NodeManager::MakeElifStmtNode(ExprNode *value) {
    FnNode *fn_node = new FnElifNode(value);
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

FnIfBlock *NodeManager::MakeFnIfBlock(const FnIfNode *if_node,
                                      const FnNodeList *block) {
    ::fesql::node::FnIfBlock *if_block =
        new ::fesql::node::FnIfBlock(if_node, block);
    RegisterNode(if_block);
    return if_block;
}

FnElifBlock *NodeManager::MakeFnElifBlock(const FnElifNode *elif_node,
                                          const FnNodeList *block) {
    ::fesql::node::FnElifBlock *elif_block =
        new ::fesql::node::FnElifBlock(elif_node, block);
    RegisterNode(elif_block);
    return elif_block;
}
FnIfElseBlock *NodeManager::MakeFnIfElseBlock(const FnIfBlock *if_block,
                                              const FnElseBlock *else_block) {
    ::fesql::node::FnIfElseBlock *if_else_block =
        new ::fesql::node::FnIfElseBlock(if_block, else_block);
    RegisterNode(if_else_block);
    return if_else_block;
}
FnElseBlock *NodeManager::MakeFnElseBlock(const FnNodeList *block) {
    ::fesql::node::FnElseBlock *else_block =
        new ::fesql::node::FnElseBlock(block);
    RegisterNode(else_block);
    return else_block;
}

FnNode *NodeManager::MakeFnParaNode(const std::string &name,
                                    const TypeNode *para_type) {
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
        InsertStmt *node_ptr = new InsertStmt(table_name, values->children_);
        return RegisterNode(node_ptr);
    } else {
        std::vector<std::string> column_names;
        for (auto expr : columns_expr->children_) {
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
            new InsertStmt(table_name, column_names, values->children_);
        return RegisterNode(node_ptr);
    }
}

DatasetNode *NodeManager::MakeDataset(const std::string &table) {
    DatasetNode *db = new DatasetNode(table);
    batch_plan_node_list_.push_back(db);
    return db;
}

MapNode *NodeManager::MakeMapNode(const NodePointVector &nodes) {
    MapNode *mn = new MapNode(nodes);
    batch_plan_node_list_.push_back(mn);
    return mn;
}

TypeNode *NodeManager::MakeTypeNode(fesql::node::DataType base) {
    TypeNode *node_ptr = new TypeNode(base);
    RegisterNode(node_ptr);
    return node_ptr;
}

TypeNode *NodeManager::MakeTypeNode(fesql::node::DataType base,
                                    fesql::node::DataType v1) {
    TypeNode *node_ptr = new TypeNode(base, v1);
    RegisterNode(node_ptr);
    return node_ptr;
}
TypeNode *NodeManager::MakeTypeNode(fesql::node::DataType base,
                                    fesql::node::DataType v1,
                                    fesql::node::DataType v2) {
    TypeNode *node_ptr = new TypeNode(base, v1, v2);
    RegisterNode(node_ptr);
    return node_ptr;
}

FnNode *NodeManager::MakeForInStmtNode(const std::string &var_name,
                                       const ExprNode *expression) {
    FnForInNode *node_ptr = new FnForInNode(var_name, expression);
    return RegisterNode(node_ptr);
}

FnForInBlock *NodeManager::MakeForInBlock(FnForInNode *for_in_node,
                                          FnNodeList *block) {
    FnForInBlock *node_ptr = new FnForInBlock(for_in_node, block);
    RegisterNode(node_ptr);
    return node_ptr;
}
PlanNode *NodeManager::MakeJoinNode(PlanNode *left, PlanNode *right,
                                    JoinType join_type,
                                    const ExprNode *condition) {
    node::JoinPlanNode *node_ptr =
        new JoinPlanNode(left, right, join_type, condition);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeSelectPlanNode(PlanNode *node) {
    node::QueryPlanNode *select_plan_ptr = new QueryPlanNode(node);
    return RegisterNode(select_plan_ptr);
}
PlanNode *NodeManager::MakeGroupPlanNode(PlanNode *node,
                                         const ExprListNode *by_list) {
    node::GroupPlanNode *node_ptr = new GroupPlanNode(node, by_list);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeProjectPlanNode(
    PlanNode *node, const std::string &table,
    const PlanNodeList &projection_list,
    const std::vector<std::pair<uint32_t, uint32_t>> &pos_mapping) {
    node::ProjectPlanNode *node_ptr =
        new ProjectPlanNode(node, table, projection_list, pos_mapping);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeLimitPlanNode(PlanNode *node, int limit_cnt) {
    node::LimitPlanNode *node_ptr = new LimitPlanNode(node, limit_cnt);
    return RegisterNode(node_ptr);
}
ProjectNode *NodeManager::MakeProjectNode(const int32_t pos,
                                          const std::string &name,
                                          const bool is_aggregation,
                                          node::ExprNode *expression) {
    node::ProjectNode *node_ptr =
        new ProjectNode(pos, name, is_aggregation, expression);
    RegisterNode(node_ptr);
    return node_ptr;
}
CreatePlanNode *NodeManager::MakeCreateTablePlanNode(
    std::string table_name, const NodePointVector &column_list) {
    node::CreatePlanNode *node_ptr =
        new CreatePlanNode(table_name, column_list);
    RegisterNode(node_ptr);
    return node_ptr;
}
CmdPlanNode *NodeManager::MakeCmdPlanNode(const CmdNode *node) {
    node::CmdPlanNode *node_ptr =
        new CmdPlanNode(node->GetCmdType(), node->GetArgs());
    RegisterNode(node_ptr);
    return node_ptr;
}
InsertPlanNode *NodeManager::MakeInsertPlanNode(const InsertStmt *node) {
    node::InsertPlanNode *node_ptr = new InsertPlanNode(node);
    RegisterNode(node_ptr);
    return node_ptr;
}
FuncDefPlanNode *NodeManager::MakeFuncPlanNode(const FnNodeFnDef *node) {
    node::FuncDefPlanNode *node_ptr = new FuncDefPlanNode(node);
    RegisterNode(node_ptr);
    return node_ptr;
}
ExprNode *NodeManager::MakeQueryExprNode(const QueryNode *query) {
    node::ExprNode *node_ptr = new QueryExpr(query);
    RegisterNode(node_ptr);
    return node_ptr;
}
PlanNode *NodeManager::MakeSortPlanNode(PlanNode *node,
                                        const OrderByNode *order_list) {
    node::SortPlanNode *node_ptr = new SortPlanNode(node, order_list);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeUnionPlanNode(PlanNode *left, PlanNode *right,
                                         const bool is_all) {
    node::UnionPlanNode *node_ptr = new UnionPlanNode(left, right, is_all);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeDistinctPlanNode(PlanNode *node) {
    node::DistinctPlanNode *node_ptr = new DistinctPlanNode(node);
    return RegisterNode(node_ptr);
}
SQLNode *NodeManager::MakeExplainNode(const QueryNode *query,
                                      ExplainType explain_type) {
    node::ExplainNode *node_ptr = new ExplainNode(query, explain_type);
    return RegisterNode(node_ptr);
}
ProjectNode *NodeManager::MakeAggProjectNode(const int32_t pos,
                                             const std::string &name,
                                             node::ExprNode *expression) {
    return MakeProjectNode(pos, name, true, expression);
}
ProjectNode *NodeManager::MakeRowProjectNode(const int32_t pos,
                                             const std::string &name,
                                             node::ExprNode *expression) {
    return MakeProjectNode(pos, name, false, expression);
}

}  // namespace node
}  // namespace fesql
