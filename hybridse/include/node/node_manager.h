/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// memory_manager.h
//     负责HybridSe的基础元件（SQLNode, PlanNode)的创建和销毁
//     SQL的语法解析树、查询计划里面维护的只是这些节点的指针或者引用

#ifndef INCLUDE_NODE_NODE_MANAGER_H_
#define INCLUDE_NODE_NODE_MANAGER_H_

#include <ctype.h>
#include <list>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_object.h"
#include "node/batch_plan_node.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "node/type_node.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace node {

class NodeManager {
 public:
    NodeManager();

    ~NodeManager();

    int GetNodeListSize() {
        int node_size = node_list_.size();
        DLOG(INFO) << "GetNodeListSize: " << node_size;
        return node_size;
    }

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
        bool is_distinct, SqlNodeList *select_list_ptr,
        SqlNodeList *tableref_list_ptr, ExprNode *where_expr,
        ExprListNode *group_expr_list, ExprNode *having_expr,
        ExprNode *order_expr_list, SqlNodeList *window_list_ptr,
        SqlNode *limit_ptr);
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
    CastExprNode *MakeCastNode(const node::DataType cast_type, ExprNode *expr);
    WhenExprNode *MakeWhenNode(ExprNode *when_expr, ExprNode *then_expr);
    ExprNode *MakeSimpleCaseWhenNode(ExprNode *case_expr,
                                     ExprListNode *when_list_expr,
                                     ExprNode *else_expr);
    ExprNode *MakeSearchedCaseWhenNode(ExprListNode *when_list_expr,
                                       ExprNode *else_expr);
    CallExprNode *MakeFuncNode(const std::string &name, ExprListNode *args,
                               const SqlNode *over);
    CallExprNode *MakeFuncNode(FnDefNode *fn, ExprListNode *args,
                               const SqlNode *over);
    CallExprNode *MakeFuncNode(const std::string &name,
                               const std::vector<ExprNode *> &args,
                               const SqlNode *over);
    CallExprNode *MakeFuncNode(FnDefNode *fn,
                               const std::vector<ExprNode *> &args,
                               const SqlNode *over);

    QueryExpr *MakeQueryExprNode(const QueryNode *query);
    SqlNode *MakeWindowDefNode(const std::string &name);
    SqlNode *MakeWindowDefNode(ExprListNode *partitions, ExprNode *orders,
                               SqlNode *frame);
    SqlNode *MakeWindowDefNode(ExprListNode *partitions, ExprNode *orders,
                               SqlNode *frame, bool opt_open_interval_window);
    SqlNode *MakeWindowDefNode(SqlNodeList *union_tables,
                               ExprListNode *partitions, ExprNode *orders,
                               SqlNode *frame, bool opt_open_interval_window,
                               bool instance_not_in_window);
    WindowDefNode *MergeWindow(const WindowDefNode *w1,
                               const WindowDefNode *w2);
    OrderExpression* MakeOrderExpression(const ExprNode* expr, const bool is_asc);
    OrderByNode *MakeOrderByNode(const ExprListNode *order_expressions);
    SqlNode *MakeFrameExtent(SqlNode *start, SqlNode *end);
    SqlNode *MakeFrameBound(BoundType bound_type);
    SqlNode *MakeFrameBound(BoundType bound_type, ExprNode *offset);
    SqlNode *MakeFrameBound(BoundType bound_type, int64_t offset);
    SqlNode *MakeFrameNode(FrameType frame_type, SqlNode *node_ptr,
                           ExprNode *frame_size);
    SqlNode *MakeFrameNode(FrameType frame_type, SqlNode *node_ptr);
    SqlNode *MakeFrameNode(FrameType frame_type, SqlNode *node_ptr,
                           int64_t maxsize);
    SqlNode *MakeFrameNode(FrameType frame_type, FrameExtent *frame_range,
                           FrameExtent *frame_rows, int64_t maxsize);
    FrameNode *MergeFrameNode(const FrameNode *frame1, const FrameNode *frame2);
    SqlNode *MakeLimitNode(int count);

    SqlNode *MakeInsertTableNode(const std::string &table_name,
                                 const ExprListNode *column_names,
                                 const ExprListNode *values);
    SqlNode *MakeCreateTableNode(bool op_if_not_exist,
                                 const std::string &db_name,
                                 const std::string &table_name,
                                 SqlNodeList *column_desc_list,
                                 SqlNodeList *partition_meta_list);
    SqlNode *MakeColumnDescNode(const std::string &column_name,
                                const DataType data_type, bool op_not_null);
    SqlNode *MakeColumnIndexNode(SqlNodeList *keys, SqlNode *ts, SqlNode *ttl,
                                 SqlNode *version);
    SqlNode *MakeColumnIndexNode(SqlNodeList *index_item_list);
    SqlNode *MakeIndexKeyNode(const std::string &key);
    SqlNode *MakeIndexKeyNode(const std::vector<std::string> &keys);
    SqlNode *MakeIndexTsNode(const std::string &ts);
    SqlNode *MakeIndexTTLNode(ExprListNode *ttl_expr);
    SqlNode *MakeIndexTTLTypeNode(const std::string &ttl_type);
    SqlNode *MakeIndexVersionNode(const std::string &version);
    SqlNode *MakeIndexVersionNode(const std::string &version, int count);

    SqlNode *MakeResTargetNode(ExprNode *node_ptr, const std::string &name);

    TypeNode *MakeTypeNode(hybridse::node::DataType base);
    TypeNode *MakeTypeNode(hybridse::node::DataType base,
                           const hybridse::node::TypeNode *v1);
    TypeNode *MakeTypeNode(hybridse::node::DataType base,
                           hybridse::node::DataType v1);
    TypeNode *MakeTypeNode(hybridse::node::DataType base,
                           hybridse::node::DataType v1,
                           hybridse::node::DataType v2);
    OpaqueTypeNode *MakeOpaqueType(size_t bytes);
    RowTypeNode *MakeRowType(const std::vector<const vm::Schema *> &schema);
    RowTypeNode *MakeRowType(const vm::SchemasContext *schemas_ctx);

    ColumnRefNode *MakeColumnRefNode(const std::string &column_name,
                                     const std::string &relation_name,
                                     const std::string &db_name);
    ColumnRefNode *MakeColumnRefNode(const std::string &column_name,
                                     const std::string &relation_name);
    ColumnIdNode *MakeColumnIdNode(size_t column_id);
    GetFieldExpr *MakeGetFieldExpr(ExprNode *input,
                                   const std::string &column_name,
                                   size_t column_id);
    GetFieldExpr *MakeGetFieldExpr(ExprNode *input, size_t column_id);

    CondExpr *MakeCondExpr(ExprNode *condition, ExprNode *left,
                           ExprNode *right);

    BetweenExpr *MakeBetweenExpr(ExprNode *expr, ExprNode *left,
                                 ExprNode *right, const bool is_not_between);
    BinaryExpr *MakeBinaryExprNode(ExprNode *left, ExprNode *right,
                                   FnOperator op);
    UnaryExpr *MakeUnaryExprNode(ExprNode *left, FnOperator op);
    ExprIdNode *MakeExprIdNode(const std::string &name);
    ExprIdNode *MakeUnresolvedExprId(const std::string &name);

    // Make Fn Node
    ConstNode *MakeConstNode(bool value);
    ConstNode *MakeConstNode(int16_t value);
    ConstNode *MakeConstNode(int value);
    ConstNode *MakeConstNode(int value, TTLType ttl_type);
    ConstNode *MakeConstNode(int64_t value, DataType unit);
    ConstNode *MakeConstNode(int64_t value);
    ConstNode *MakeConstNode(int64_t value, TTLType ttl_type);
    ConstNode *MakeConstNode(float value);
    ConstNode *MakeConstNode(double value);
    ConstNode *MakeConstNode(const std::string &value);
    ConstNode *MakeConstNode(const char *value);
    ConstNode *MakeConstNode();
    ConstNode *MakeConstNode(DataType type);
    ParameterExpr *MakeParameterExpr(int position);

    AllNode *MakeAllNode(const std::string &relation_name);
    AllNode *MakeAllNode(const std::string &relation_name,
                         const std::string &db_name);

    FnNode *MakeFnNode(const SqlNodeType &type);
    FnNodeList *MakeFnListNode();
    FnNodeList *MakeFnListNode(FnNode* fn_node);
    FnNode *MakeFnDefNode(const FnNode *header, FnNodeList *block);
    FnNode *MakeFnHeaderNode(const std::string &name, FnNodeList *plist,
                             const TypeNode *return_type);

    FnParaNode *MakeFnParaNode(const std::string &name,
                               const TypeNode *para_type);
    FnNode *MakeAssignNode(const std::string &name, ExprNode *expression);
    FnNode *MakeAssignNode(const std::string &name, ExprNode *expression,
                           const FnOperator op);
    FnNode *MakeReturnStmtNode(ExprNode *value);
    FnIfBlock *MakeFnIfBlock(FnIfNode *if_node, FnNodeList *block);
    FnElifBlock *MakeFnElifBlock(FnElifNode *elif_node, FnNodeList *block);
    FnIfElseBlock *MakeFnIfElseBlock(FnIfBlock *if_block,
                                     const std::vector<FnNode *>& elif_blocks,
                                     FnElseBlock *else_block);
    FnElseBlock *MakeFnElseBlock(FnNodeList *block);
    FnNode *MakeIfStmtNode(ExprNode *value);
    FnNode *MakeElifStmtNode(ExprNode *value);
    FnNode *MakeElseStmtNode();
    FnNode *MakeForInStmtNode(const std::string &var_name, ExprNode *value);

    SqlNode *MakeCmdNode(node::CmdType cmd_type);
    SqlNode *MakeCmdNode(node::CmdType cmd_type, const std::string &arg);
    SqlNode *MakeCmdNode(node::CmdType cmd_type, const std::string &index_name,
                         const std::string &table_name);
    SqlNode *MakeCreateIndexNode(const std::string &index_name,
                                 const std::string &table_name,
                                 ColumnIndexNode *index);

    // Make NodeList
    SqlNode *MakeExplainNode(const QueryNode *query,
                             node::ExplainType explain_type);
    SqlNodeList *MakeNodeList(SqlNode *node_ptr);
    SqlNodeList *MakeNodeList();

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

    CreatePlanNode *MakeCreateTablePlanNode(
        const std::string &table_name, int replica_num, int partition_num,
        const NodePointVector &column_list,
        const NodePointVector &partition_meta_list);

    CreateProcedurePlanNode *MakeCreateProcedurePlanNode(
        const std::string &sp_name, const NodePointVector &input_parameter_list,
        const PlanNodeList &inner_plan_node_list);

    CreateIndexPlanNode* MakeCreateCreateIndexPlanNode(const CreateIndexNode* node);
    SqlNode *MakeCreateProcedureNode(const std::string &sp_name,
                                              SqlNodeList *input_parameter_list,
                                              SqlNodeList *inner_node_list);

    CmdPlanNode *MakeCmdPlanNode(const CmdNode *node);

    InsertPlanNode *MakeInsertPlanNode(const InsertStmt *node);
    ExplainPlanNode *MakeExplainPlanNode(const ExplainNode *node);
    FuncDefPlanNode *MakeFuncPlanNode(FnNodeFnDef *node);

    PlanNode *MakeRenamePlanNode(PlanNode *node, const std::string alias_name);

    PlanNode *MakeSortPlanNode(PlanNode *node, const OrderByNode *order_list);

    PlanNode *MakeUnionPlanNode(PlanNode *left, PlanNode *right,
                                const bool is_all);

    PlanNode *MakeDistinctPlanNode(PlanNode *node);

    node::ExprNode *MakeAndExpr(ExprListNode *expr_list);

    node::FrameNode *MergeFrameNodeWithCurrentHistoryFrame(FrameNode *frame1);

    ExternalFnDefNode *MakeExternalFnDefNode(
        const std::string &function_name, void *function_ptr,
        const node::TypeNode *ret_type, bool ret_nullable,
        const std::vector<const node::TypeNode *> &arg_types,
        const std::vector<int> &arg_nullable, int variadic_pos,
        bool return_by_arg);

    ExternalFnDefNode *MakeUnresolvedFnDefNode(
        const std::string &function_name);

    UdfDefNode *MakeUdfDefNode(FnNodeFnDef *def);

    UdfByCodeGenDefNode *MakeUdfByCodeGenDefNode(
        const std::string &name,
        const std::vector<const node::TypeNode *> &arg_types,
        const std::vector<int> &arg_nullable, const node::TypeNode *ret_type,
        bool ret_nullable);

    UdafDefNode *MakeUdafDefNode(const std::string &name,
                                 const std::vector<const TypeNode *> &arg_types,
                                 ExprNode *init, FnDefNode *update_func,
                                 FnDefNode *merge_func, FnDefNode *output_func);
    LambdaNode *MakeLambdaNode(const std::vector<ExprIdNode *> &args,
                               ExprNode *body);

    SqlNode *MakePartitionMetaNode(RoleType role_type,
                                   const std::string &endpoint);

    SqlNode *MakeReplicaNumNode(int num);

    SqlNode *MakePartitionNumNode(int num);

    SqlNode *MakeDistributionsNode(SqlNodeList *distribution_list);

    SqlNode *MakeCreateProcedureNode(const std::string &sp_name,
                                     SqlNodeList *input_parameter_list,
                                     SqlNode *inner_node);

    SqlNode *MakeInputParameterNode(bool is_constant,
                                    const std::string &column_name,
                                    DataType data_type);

    template <typename T>
    T *RegisterNode(T *node_ptr) {
        node_list_.push_back(node_ptr);
        SetNodeUniqueId(node_ptr);
        return node_ptr;
    }

 private:
    ProjectNode *MakeProjectNode(const int32_t pos, const std::string &name,
                                 const bool is_aggregation,
                                 node::ExprNode *expression,
                                 node::FrameNode *frame);

    void SetNodeUniqueId(ExprNode *node);
    void SetNodeUniqueId(TypeNode *node);
    void SetNodeUniqueId(PlanNode *node);
    void SetNodeUniqueId(vm::PhysicalOpNode *node);

    template <typename T>
    void SetNodeUniqueId(T *node) {
        node->SetNodeId(other_node_idx_counter_++);
    }

    std::list<base::FeBaseObject *> node_list_;

    // unique id counter for various types of node
    size_t expr_idx_counter_ = 1;
    size_t type_idx_counter_ = 1;
    size_t plan_idx_counter_ = 1;
    size_t physical_plan_idx_counter_ = 1;
    size_t other_node_idx_counter_ = 1;
    size_t exprid_idx_counter_ = 0;
};

}  // namespace node
}  // namespace hybridse
#endif  // INCLUDE_NODE_NODE_MANAGER_H_
