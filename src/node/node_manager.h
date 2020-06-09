// Copyright (C) 2019, 4paradigm
// memory_manager.h
//     负责FeSQL的基础元件（SQLNode, PlanNode)的创建和销毁
//     SQL的语法解析树、查询计划里面维护的只是这些节点的指针或者引用
// Author: chenjing
// Date: 2019/10/28

#ifndef SRC_NODE_NODE_MANAGER_H_
#define SRC_NODE_NODE_MANAGER_H_

#include <ctype.h>
#include <list>
#include <string>
#include <utility>
#include <vector>
#include "node/batch_plan_node.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "vm/physical_op.h"

namespace fesql {
namespace node {
class NodeManager {
 public:
    NodeManager()
        : parser_node_list_(),
          sql_node_list_list_(),
          plan_node_list_(),
          batch_plan_node_list_(),
          physical_plan_node_list_() {}

    ~NodeManager() {
        for (auto sql_node_ite = parser_node_list_.begin();
             sql_node_ite != parser_node_list_.end(); ++sql_node_ite) {
            delete (*sql_node_ite);
            sql_node_ite = parser_node_list_.erase(sql_node_ite);
        }

        for (auto sql_node_list_iter = sql_node_list_list_.begin();
             sql_node_list_iter != sql_node_list_list_.end();
             ++sql_node_list_iter) {
            delete (*sql_node_list_iter);
            sql_node_list_iter = sql_node_list_list_.erase(sql_node_list_iter);
        }

        for (auto plan_node_ite = plan_node_list_.begin();
             plan_node_ite != plan_node_list_.end(); ++plan_node_ite) {
            delete (*plan_node_ite);
            plan_node_ite = plan_node_list_.erase(plan_node_ite);
        }

        for (auto physical_node_list_iter = physical_plan_node_list_.begin();
             physical_node_list_iter != physical_plan_node_list_.end();
             ++physical_node_list_iter) {
            delete (*physical_node_list_iter);
            physical_node_list_iter =
                physical_plan_node_list_.erase(physical_node_list_iter);
        }
    }

    int GetParserNodeListSize() { return parser_node_list_.size(); }

    int GetPlanNodeListSize() { return plan_node_list_.size(); }

    // Make xxxPlanNode
    //    PlanNode *MakePlanNode(const PlanType &type);
    PlanNode *MakeLeafPlanNode(const PlanType &type);
    PlanNode *MakeUnaryPlanNode(const PlanType &type);
    PlanNode *MakeBinaryPlanNode(const PlanType &type);
    PlanNode *MakeMultiPlanNode(const PlanType &type);
    PlanNode *MakeMergeNode(int column_size);
    WindowPlanNode *MakeWindowPlanNode(int w_id);
    ProjectListNode *MakeProjectListPlanNode(const WindowPlanNode *w,
                                             const bool need_agg);
    FilterPlanNode *MakeFilterPlanNode(PlanNode *node,
                                       const ExprNode *condition);

    ProjectNode *MakeRowProjectNode(const int32_t pos, const std::string &name,
                                    node::ExprNode *expression);
    ProjectNode *MakeAggProjectNode(const int32_t pos, const std::string &name,
                                    node::ExprNode *expression,
                                    node::FrameNode *frame);
    PlanNode *MakeTablePlanNode(const std::string &node);
    PlanNode *MakeJoinNode(PlanNode *left, PlanNode *right, JoinType join_type,
                           const OrderByNode *order_by,
                           const ExprNode *condition);
    // Make SQLxxx Node
    QueryNode *MakeSelectQueryNode(
        bool is_distinct, SQLNodeList *select_list_ptr,
        SQLNodeList *tableref_list_ptr, ExprNode *where_expr,
        ExprListNode *group_expr_list, ExprNode *having_expr,
        ExprNode *order_expr_list, SQLNodeList *window_list_ptr,
        SQLNode *limit_ptr);
    QueryNode *MakeUnionQueryNode(QueryNode *left, QueryNode *right,
                                  bool is_all);
    TableRefNode *MakeTableNode(const std::string &name,
                                const std::string &alias);
    TableRefNode *MakeJoinNode(const TableRefNode *left,
                               const TableRefNode *right, const JoinType type,
                               const ExprNode *condition,
                               const std::string alias);
    TableRefNode *MakeLastJoinNode(const TableRefNode *left,
                                   const TableRefNode *right,
                                   const ExprNode *order,
                                   const ExprNode *condition,
                                   const std::string alias);
    TableRefNode *MakeQueryRefNode(const QueryNode *sub_query,
                                   const std::string &alias);
    ExprNode *MakeFuncNode(const std::string &name, const ExprListNode *args,
                           const SQLNode *over);
    ExprNode *MakeQueryExprNode(const QueryNode *query);
    SQLNode *MakeWindowDefNode(const std::string &name);
    SQLNode *MakeWindowDefNode(ExprListNode *partitions, ExprNode *orders,
                               SQLNode *frame);
    SQLNode *MakeWindowDefNode(SQLNodeList *union_tables,
                               ExprListNode *partitions, ExprNode *orders,
                               SQLNode *frame, bool instance_not_in_window);
    WindowDefNode *MergeWindow(const WindowDefNode *w1,
                               const WindowDefNode *w2);
    ExprNode *MakeOrderByNode(const ExprListNode *node_ptr, const bool is_asc);
    SQLNode *MakeFrameExtent(SQLNode *start, SQLNode *end);
    SQLNode *MakeFrameBound(BoundType bound_type);
    SQLNode *MakeFrameBound(BoundType bound_type, ExprNode *offset);
    SQLNode *MakeFrameBound(BoundType bound_type, int64_t offset);
    SQLNode *MakeFrameNode(FrameType frame_type, SQLNode *node_ptr,
                           ExprNode *frame_size);
    SQLNode *MakeFrameNode(FrameType frame_type, SQLNode *node_ptr);
    SQLNode *MakeFrameNode(FrameType frame_type, SQLNode *node_ptr,
                           int64_t maxsize);
    SQLNode *MakeFrameNode(FrameType frame_type, FrameExtent *frame_range,
                           FrameExtent *frame_rows, int64_t maxsize);
    FrameNode *MergeFrameNode(const FrameNode *frame1, const FrameNode *frame2);
    SQLNode *MakeLimitNode(int count);

    SQLNode *MakeNameNode(const std::string &name);
    SQLNode *MakeInsertTableNode(const std::string &table_name,
                                 const ExprListNode *column_names,
                                 const ExprListNode *values);
    SQLNode *MakeCreateTableNode(bool op_if_not_exist,
                                 const std::string &table_name,
                                 SQLNodeList *column_desc_list);
    SQLNode *MakeColumnDescNode(const std::string &column_name,
                                const DataType data_type, bool op_not_null);
    SQLNode *MakeColumnIndexNode(SQLNodeList *keys, SQLNode *ts, SQLNode *ttl,
                                 SQLNode *version);
    SQLNode *MakeColumnIndexNode(SQLNodeList *index_item_list);
    SQLNode *MakeKeyNode(SQLNodeList *key_list);
    SQLNode *MakeKeyNode(const std::string &key);
    SQLNode *MakeIndexKeyNode(const std::string &key);
    SQLNode *MakeIndexTsNode(const std::string &ts);
    SQLNode *MakeIndexTTLNode(ExprNode *ttl_expr);
    SQLNode *MakeIndexTTLTypeNode(const std::string &ttl_type);
    SQLNode *MakeIndexVersionNode(const std::string &version);
    SQLNode *MakeIndexVersionNode(const std::string &version, int count);

    SQLNode *MakeResTargetNode(ExprNode *node_ptr, const std::string &name);

    TypeNode *MakeTypeNode(fesql::node::DataType base);
    TypeNode *MakeTypeNode(fesql::node::DataType base,
                           fesql::node::DataType v1);
    TypeNode *MakeTypeNode(fesql::node::DataType base, fesql::node::DataType v1,
                           fesql::node::DataType v2);

    ExprNode *MakeColumnRefNode(const std::string &column_name,
                                const std::string &relation_name,
                                const std::string &db_name);
    ExprNode *MakeColumnRefNode(const std::string &column_name,
                                const std::string &relation_name);
    ExprNode *MakeBetweenExpr(ExprNode *expr, ExprNode *left, ExprNode *right);
    ExprNode *MakeBinaryExprNode(ExprNode *left, ExprNode *right,
                                 FnOperator op);
    ExprNode *MakeUnaryExprNode(ExprNode *left, FnOperator op);
    ExprNode *MakeExprFrom(const ExprNode *node,
                           const std::string relation_name,
                           const std::string db_name);
    ExprNode *MakeExprIdNode(const std::string &name);
    // Make Fn Node
    ExprNode *MakeConstNode(int value);
    ExprNode *MakeConstNode(int64_t value, DataType unit);
    ExprNode *MakeConstNode(int64_t value);
    ExprNode *MakeConstNode(float value);
    ExprNode *MakeConstNode(double value);
    ExprNode *MakeConstNode(const std::string &value);
    ExprNode *MakeConstNode(const char *value);
    ExprNode *MakeConstNode();

    ExprNode *MakeAllNode(const std::string &relation_name);
    ExprNode *MakeAllNode(const std::string &relation_name,
                          const std::string &db_name);

    FnNode *MakeFnNode(const SQLNodeType &type);
    FnNodeList *MakeFnListNode();
    FnNode *MakeFnDefNode(const FnNode *header, const FnNodeList *block);
    FnNode *MakeFnHeaderNode(const std::string &name, FnNodeList *plist,
                             const TypeNode *return_type);

    FnNode *MakeFnParaNode(const std::string &name, const TypeNode *para_type);
    FnNode *MakeAssignNode(const std::string &name, ExprNode *expression);
    FnNode *MakeAssignNode(const std::string &name, ExprNode *expression,
                           const FnOperator op);
    FnNode *MakeReturnStmtNode(ExprNode *value);
    FnIfBlock *MakeFnIfBlock(const FnIfNode *if_node, const FnNodeList *block);
    FnElifBlock *MakeFnElifBlock(const FnElifNode *elif_node,
                                 const FnNodeList *block);
    FnIfElseBlock *MakeFnIfElseBlock(const FnIfBlock *if_block,
                                     const FnElseBlock *else_block);
    FnElseBlock *MakeFnElseBlock(const FnNodeList *block);
    FnNode *MakeIfStmtNode(const ExprNode *value);
    FnNode *MakeElifStmtNode(ExprNode *value);
    FnNode *MakeElseStmtNode();
    FnNode *MakeForInStmtNode(const std::string &var_name,
                              const ExprNode *value);

    SQLNode *MakeCmdNode(node::CmdType cmd_type);
    SQLNode *MakeCmdNode(node::CmdType cmd_type, const std::string &arg);
    SQLNode *MakeCmdNode(node::CmdType cmd_type, const std::string &index_name,
                         const std::string &table_name);
    SQLNode *MakeCreateIndexNode(const std::string &index_name,
                                 const std::string &table_name,
                                 ColumnIndexNode *index);
    // Make NodeList
    SQLNode *MakeExplainNode(const QueryNode *query,
                             node::ExplainType explain_type);
    SQLNodeList *MakeNodeList(SQLNode *node_ptr);
    SQLNodeList *MakeNodeList();

    ExprListNode *MakeExprList(ExprNode *node_ptr);
    ExprListNode *MakeExprList();

    DatasetNode *MakeDataset(const std::string &table);
    MapNode *MakeMapNode(const NodePointVector &nodes);
    node::FnForInBlock *MakeForInBlock(FnForInNode *for_in_node,
                                       FnNodeList *block);

    PlanNode *MakeSelectPlanNode(PlanNode *node);

    PlanNode *MakeGroupPlanNode(PlanNode *node, const ExprListNode *by_list);

    PlanNode *MakeProjectPlanNode(
        PlanNode *node, const std::string &table,
        const PlanNodeList &project_list,
        const std::vector<std::pair<uint32_t, uint32_t>> &pos_mapping);

    PlanNode *MakeLimitPlanNode(PlanNode *node, int limit_cnt);

    CreatePlanNode *MakeCreateTablePlanNode(std::string table_name,
                                            const NodePointVector &column_list);

    CmdPlanNode *MakeCmdPlanNode(const CmdNode *node);

    InsertPlanNode *MakeInsertPlanNode(const InsertStmt *node);

    FuncDefPlanNode *MakeFuncPlanNode(const FnNodeFnDef *node);

    PlanNode *MakeRenamePlanNode(PlanNode *node, const std::string alias_name);

    PlanNode *MakeSortPlanNode(PlanNode *node, const OrderByNode *order_list);

    PlanNode *MakeUnionPlanNode(PlanNode *left, PlanNode *right,
                                const bool is_all);

    PlanNode *MakeDistinctPlanNode(PlanNode *node);

    vm::PhysicalOpNode *RegisterNode(vm::PhysicalOpNode *node_ptr) {
        physical_plan_node_list_.push_back(node_ptr);
        return node_ptr;
    }

    node::ExprNode *MakeEqualCondition(const std::string &db1,
                                       const std::string &table1,
                                       const std::string &db2,
                                       const std::string &table2,
                                       const node::ExprListNode *expr_list);

    node::ExprNode *MakeAndExpr(ExprListNode *expr_list);
    node ::ExprListNode *BuildExprListFromSchemaSource(
        const vm::ColumnSourceList column_sources,
        const vm::SchemaSourceList &schema_souces);

 private:
    ProjectNode *MakeProjectNode(const int32_t pos, const std::string &name,
                                 const bool is_aggregation,
                                 node::ExprNode *expression,
                                 node::FrameNode *frame);

    SQLNode *RegisterNode(SQLNode *node_ptr) {
        parser_node_list_.push_back(node_ptr);
        return node_ptr;
    }

    ExprNode *RegisterNode(ExprNode *node_ptr) {
        parser_node_list_.push_back(node_ptr);
        return node_ptr;
    }

    FnNode *RegisterNode(FnNode *node_ptr) {
        parser_node_list_.push_back(node_ptr);
        return node_ptr;
    }
    PlanNode *RegisterNode(PlanNode *node_ptr) {
        plan_node_list_.push_back(node_ptr);
        return node_ptr;
    }

    SQLNodeList *RegisterNode(SQLNodeList *node_ptr) {
        sql_node_list_list_.push_back(node_ptr);
        return node_ptr;
    }

    std::list<SQLNode *> parser_node_list_;
    std::list<SQLNodeList *> sql_node_list_list_;
    std::list<node::PlanNode *> plan_node_list_;
    std::list<node::BatchPlanNode *> batch_plan_node_list_;
    std::list<vm::PhysicalOpNode *> physical_plan_node_list_;
};

}  // namespace node
}  // namespace fesql
#endif  // SRC_NODE_NODE_MANAGER_H_
