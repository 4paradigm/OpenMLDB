/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * memory_manager.h
 *      负责FeSQL的基础元件（SQLNode, PlanNode)的创建和销毁
 *      SQL的语法解析树、查询计划里面维护的只是这些节点的指针或者引用
 * Author: chenjing
 * Date: 2019/10/28
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_NODE_NODE_MANAGER_H_
#define SRC_NODE_NODE_MANAGER_H_

#include <ctype.h>
#include <list>
#include <string>
#include "node/plan_node.h"
#include "node/sql_node.h"

namespace fesql {
namespace node {
class NodeManager {
 public:
    NodeManager() {}

    ~NodeManager() {
        for (auto sql_node_ite = parser_node_list_.begin();
             sql_node_ite != parser_node_list_.end(); ++sql_node_ite) {
            delete (*sql_node_ite);
            sql_node_ite = parser_node_list_.erase(sql_node_ite);
        }

        for (auto plan_node_ite = plan_node_list_.begin();
             plan_node_ite != plan_node_list_.end(); ++plan_node_ite) {
            delete (*plan_node_ite);
            plan_node_ite = plan_node_list_.erase(plan_node_ite);
        }

        for (auto sql_node_list_iter = sql_node_list_list_.begin();
             sql_node_list_iter != sql_node_list_list_.end();
             ++sql_node_list_iter) {
            delete (*sql_node_list_iter);
            sql_node_list_iter = sql_node_list_list_.erase(sql_node_list_iter);
        }
    }

    int GetParserNodeListSize() { return parser_node_list_.size(); }

    int GetPlanNodeListSize() { return plan_node_list_.size(); }

    // Make xxxPlanNode
    PlanNode *MakePlanNode(const PlanType &type);
    PlanNode *MakeLeafPlanNode(const PlanType &type);
    PlanNode *MakeUnaryPlanNode(const PlanType &type);
    PlanNode *MakeBinaryPlanNode(const PlanType &type);
    PlanNode *MakeMultiPlanNode(const PlanType &type);
    PlanNode *MakeMergeNode(int column_size);
    WindowPlanNode *MakeWindowPlanNode(int w_id);
    ProjectListPlanNode *MakeProjectListPlanNode(const std::string &table,
                                                 WindowPlanNode *w);
    WindowPlanNode *MakeWindowPlanNode(int64_t start, int64_t end,
                                       bool is_range_between);
    ScanPlanNode *MakeSeqScanPlanNode(const std::string &table);
    ScanPlanNode *MakeIndexScanPlanNode(const std::string &table);
    ProjectPlanNode *MakeProjectPlanNode(node::SQLNode *expression,
                                         const std::string &name,
                                         const std::string &table,
                                         const std::string &w);
    // Make SQLxxx Node
    SQLNode *MakeSQLNode(const SQLNodeType &type);
    SQLNode *MakeSelectStmtNode(SQLNodeList *select_list_ptr_,
                                SQLNodeList *tableref_list_ptr,
                                SQLNodeList *window_clause_ptr,
                                SQLNode *limit_clause_ptr);
    SQLNode *MakeTableNode(const std::string &name, const std::string &alias);
    ExprNode *MakeFuncNode(const std::string &name, SQLNodeList *args,
                           SQLNode *over);
    SQLNode *MakeWindowDefNode(const std::string &name);
    SQLNode *MakeWindowDefNode(ExprListNode *partitions, ExprListNode *orders,
                               SQLNode *frame);
    SQLNode *MakeOrderByNode(SQLNode *node_ptr);
    SQLNode *MakeFrameNode(SQLNode *start, SQLNode *end);
    SQLNode *MakeFrameBound(SQLNodeType bound_type);
    SQLNode *MakeFrameBound(SQLNodeType bound_type, ExprNode *offset);
    SQLNode *MakeRangeFrameNode(SQLNode *node_ptr);
    SQLNode *MakeRowsFrameNode(SQLNode *node_ptr);
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
    SQLNode *MakeIndexVersionNode(const std::string &version);
    SQLNode *MakeIndexVersionNode(const std::string &version, int count);

    SQLNode *MakeResTargetNode(ExprNode *node_ptr, const std::string &name);

    TypeNode *MakeTypeNode(fesql::type::Type base);
    TypeNode *MakeTypeNode(fesql::type::Type base, fesql::type::Type v1);
    TypeNode *MakeTypeNode(fesql::type::Type base, fesql::type::Type v1,
                           fesql::type::Type v2);
    ExprNode *MakeColumnRefNode(const std::string &column_name,
                                const std::string &relation_name);
    ExprNode *MakeBinaryExprNode(ExprNode *left, ExprNode *right,
                                 FnOperator op);
    ExprNode *MakeUnaryExprNode(ExprNode *left, FnOperator op);
    ExprNode *MakeFnIdNode(const std::string &name);
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

    FnNode *MakeFnNode(const SQLNodeType &type);
    FnNodeList *MakeFnListNode();
    FnNode *MakeFnDefNode(const FnNode *header, const FnNodeList *block);
    FnNode *MakeFnHeaderNode(const std::string &name, FnNodeList *plist,
                             const TypeNode *return_type);

    FnNode *MakeFnParaNode(const std::string &name, const TypeNode *para_type);
    FnNode *MakeAssignNode(const std::string &name, ExprNode *expression);
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
    // Make NodeList
    SQLNodeList *MakeNodeList(SQLNode *node_ptr);
    SQLNodeList *MakeNodeList();

    ExprListNode *MakeExprList(ExprNode *node_ptr);
    ExprListNode *MakeExprList();

    node::FnForInBlock *MakeForInBlock(FnForInNode *for_in_node,
                                       FnNodeList *block);

 private:
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
};

}  // namespace node
}  // namespace fesql
#endif  // SRC_NODE_NODE_MANAGER_H_
