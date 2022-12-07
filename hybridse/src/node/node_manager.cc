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

#include "node/node_manager.h"

#include <string>
#include <utility>
#include <vector>

namespace hybridse {
namespace node {

NodeManager::NodeManager() {}

NodeManager::~NodeManager() {
    for (auto node : node_list_) {
        delete node;
    }
}

QueryNode *NodeManager::MakeSelectQueryNode(bool is_distinct, SqlNodeList *select_list_ptr,
                                            SqlNodeList *tableref_list_ptr, ExprNode *where_expr,
                                            ExprListNode *group_expr_list, ExprNode *having_expr,
                                            ExprNode *order_expr_list, SqlNodeList *window_list_ptr,
                                            SqlNode *limit_ptr) {
    SelectQueryNode *node_ptr =
        new SelectQueryNode(is_distinct, select_list_ptr, tableref_list_ptr, where_expr, group_expr_list, having_expr,
                            dynamic_cast<OrderByNode *>(order_expr_list), window_list_ptr, limit_ptr);
    RegisterNode(node_ptr);
    return node_ptr;
}

QueryNode *NodeManager::MakeUnionQueryNode(QueryNode *left, QueryNode *right, bool is_all) {
    UnionQueryNode *node_ptr = new UnionQueryNode(left, right, is_all);
    RegisterNode(node_ptr);
    return node_ptr;
}
TableRefNode *NodeManager::MakeTableNode(const std::string &name, const std::string &alias) {
    return MakeTableNode("", name, alias);
}
TableRefNode *NodeManager::MakeTableNode(const std::string& db, const std::string &name, const std::string &alias) {
    TableRefNode *node_ptr = new TableNode(db, name, alias);
    RegisterNode(node_ptr);
    return node_ptr;
}

TableRefNode *NodeManager::MakeJoinNode(const TableRefNode *left, const TableRefNode *right, const JoinType type,
                                        const ExprNode *condition, const std::string alias) {
    TableRefNode *node_ptr = new JoinNode(left, right, type, nullptr, condition, alias);
    RegisterNode(node_ptr);
    return node_ptr;
}

TableRefNode *NodeManager::MakeLastJoinNode(const TableRefNode *left, const TableRefNode *right, const ExprNode *orders,
                                            const ExprNode *condition, const std::string alias) {
    if (nullptr != orders && node::kExprOrder != orders->GetExprType()) {
        LOG(WARNING) << "fail to create last join node with invalid order type " + NameOfSqlNodeType(orders->GetType());
        return nullptr;
    }
    TableRefNode *node_ptr =
        new JoinNode(left, right, node::kJoinTypeLast, dynamic_cast<const OrderByNode *>(orders), condition, alias);
    RegisterNode(node_ptr);
    return node_ptr;
}

TableRefNode *NodeManager::MakeQueryRefNode(const QueryNode *sub_query, const std::string &alias) {
    TableRefNode *node_ptr = new QueryRefNode(sub_query, alias);
    RegisterNode(node_ptr);
    return node_ptr;
}
SqlNode *NodeManager::MakeResTargetNode(ExprNode *node, const std::string &name) {
    ResTarget *node_ptr = new ResTarget(name, node);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeLimitNode(int count) {
    LimitNode *node_ptr = new LimitNode(count);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeWindowDefNode(ExprListNode *partitions, ExprNode *orders, SqlNode *frame) {
    return MakeWindowDefNode(nullptr, partitions, orders, frame, false, false, false);
}
SqlNode *NodeManager::MakeWindowDefNode(ExprListNode *partitions, ExprNode *orders, SqlNode *frame,
                                        bool exclude_current_time) {
    return MakeWindowDefNode(nullptr, partitions, orders, frame, exclude_current_time, false, false);
}
SqlNode *NodeManager::MakeWindowDefNode(SqlNodeList *union_tables, ExprListNode *partitions, ExprNode *orders,
                                        SqlNode *frame, bool exclude_current_time, bool exclude_current_row,
                                        bool instance_not_in_window) {
    WindowDefNode *node_ptr = new WindowDefNode();
    if (nullptr != orders) {
        if (node::kExprOrder != orders->GetExprType()) {
            LOG(WARNING) << "fail to create window node with invalid order type " +
                                NameOfSqlNodeType(orders->GetType());
            delete node_ptr;
            return nullptr;
        }
        node_ptr->SetOrders(dynamic_cast<OrderByNode *>(orders));
    }
    node_ptr->set_exclude_current_time(exclude_current_time);
    node_ptr->set_exclude_current_row(exclude_current_row);
    node_ptr->set_instance_not_in_window(instance_not_in_window);
    node_ptr->set_union_tables(union_tables);
    node_ptr->SetPartitions(partitions);
    node_ptr->SetFrame(dynamic_cast<FrameNode *>(frame));
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeWindowDefNode(const std::string &name) {
    WindowDefNode *node_ptr = new WindowDefNode();
    node_ptr->SetName(name);
    return RegisterNode(node_ptr);
}

WindowDefNode *NodeManager::MergeWindow(const WindowDefNode *w1, const WindowDefNode *w2) {
    if (nullptr == w1 || nullptr == w2) {
        LOG(WARNING) << "Fail to Merge Window: input windows are null";
        return nullptr;
    }
    return dynamic_cast<WindowDefNode *>(MakeWindowDefNode(
        w1->union_tables(), w1->GetPartitions(), w1->GetOrders(), MergeFrameNode(w1->GetFrame(), w2->GetFrame()),
        w1->exclude_current_time(), w1->exclude_current_row(), w1->instance_not_in_window()));
}
FrameNode *NodeManager::MergeFrameNodeWithCurrentHistoryFrame(FrameNode *frame1) {
    if (nullptr == frame1) {
        return nullptr;
    }

    switch (frame1->frame_type()) {
        case kFrameRows: {
            return MergeFrameNode(
                frame1,
                dynamic_cast<FrameNode *>(MakeFrameNode(
                    kFrameRows, nullptr,
                    dynamic_cast<FrameExtent *>(MakeFrameExtent(MakeFrameBound(kCurrent), MakeFrameBound(kCurrent))),
                    0)));
        }
        default: {
            return frame1;
        }
    }
    return nullptr;
}
FrameNode *NodeManager::MergeFrameNode(const FrameNode *frame1, const FrameNode *frame2) {
    if (nullptr == frame1 || nullptr == frame2) {
        LOG(WARNING) << "Fail to Merge Frame: input frames are null";
        return nullptr;
    }
    FrameType frame_type =
        frame1->frame_type() == frame2->frame_type() ? frame1->frame_type() : kFrameRowsMergeRowsRange;
    FrameExtent *frame_range = nullptr;
    if (nullptr == frame1->frame_range()) {
        frame_range = frame2->frame_range();
    } else if (nullptr == frame2->frame_range()) {
        frame_range = frame1->frame_range();
    } else {
        FrameBound *start1 = frame1->frame_range()->start();
        FrameBound *start2 = frame2->frame_range()->start();

        FrameBound *end1 = frame1->frame_range()->end();
        FrameBound *end2 = frame2->frame_range()->end();

        int start_compared = FrameBound::Compare(start1, start2);
        int end_compared = FrameBound::Compare(end1, end2);
        FrameBound *start = start_compared < 1 ? start1 : start2;
        FrameBound *end = end_compared >= 1 ? end1 : end2;
        frame_range = dynamic_cast<FrameExtent *>(MakeFrameExtent(start, end));
    }

    FrameExtent *frame_rows = nullptr;
    if (nullptr == frame1->frame_rows()) {
        frame_rows = frame2->frame_rows();
    } else if (nullptr == frame2->frame_rows()) {
        frame_rows = frame1->frame_rows();
    } else {
        FrameBound *start1 = frame1->frame_rows()->start();
        FrameBound *start2 = frame2->frame_rows()->start();
        int start_compared = FrameBound::Compare(start1, start2);
        FrameBound *start = start_compared < 1 ? start1 : start2;

        FrameBound *end1 = frame1->frame_rows()->end();
        FrameBound *end2 = frame2->frame_rows()->end();
        int end_compared = FrameBound::Compare(end1, end2);
        FrameBound *end = end_compared >= 1 ? end1 : end2;
        frame_rows = dynamic_cast<FrameExtent *>(MakeFrameExtent(start, end));
    }
    int64_t maxsize = frame1->frame_maxsize() == 0 ? frame2->frame_maxsize() : frame1->frame_maxsize();

    return dynamic_cast<FrameNode *>(MakeFrameNode(frame_type, frame_range, frame_rows, maxsize));
}
SqlNode *NodeManager::MakeFrameBound(BoundType bound_type) {
    FrameBound *node_ptr = new FrameBound(bound_type);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeFrameBound(BoundType bound_type, ExprNode *expr) {
    if (kExprPrimary != expr->expr_type_) {
        LOG(WARNING) << "cannot create window frame, only support "
                        "const number and const time offset of frame";
        return nullptr;
    }
    ConstNode *primary = dynamic_cast<ConstNode *>(expr);
    int64_t offset;
    switch (primary->GetDataType()) {
        case node::DataType::kInt16:
        case node::DataType::kInt32:
        case node::DataType::kInt64: {
            offset = primary->GetAsInt64();
            FrameBound *node_ptr = new FrameBound(bound_type, offset, false);
            return RegisterNode(node_ptr);
        }
        case node::DataType::kDay:
        case node::DataType::kHour:
        case node::DataType::kMinute:
        case node::DataType::kSecond: {
            offset = (primary->GetMillis());
            FrameBound *node_ptr = new FrameBound(bound_type, offset, true);
            return RegisterNode(node_ptr);
        } break;
        default: {
            LOG(WARNING) << "cannot create window frame, only support "
                            "integer and time offset of frame";
            return nullptr;
        }
    }
}
SqlNode *NodeManager::MakeFrameBound(BoundType bound_type, int64_t offset) {
    FrameBound *node_ptr = new FrameBound(bound_type, offset, false);
    return RegisterNode(node_ptr);
}
FrameExtent *NodeManager::MakeFrameExtent(SqlNode *start, SqlNode *end) {
    FrameExtent *node_ptr = new FrameExtent(dynamic_cast<FrameBound *>(start), dynamic_cast<FrameBound *>(end));
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeFrameNode(FrameType frame_type, SqlNode *frame_extent) {
    int64_t max_size = 0;
    return MakeFrameNode(frame_type, frame_extent, max_size);
}
SqlNode *NodeManager::MakeFrameNode(FrameType frame_type, SqlNode *frame_extent, ExprNode *frame_size) {
    if (nullptr != frame_extent && node::kFrameExtent != frame_extent->type_) {
        LOG(WARNING) << "Fail Make Frame Node: 2nd arg isn't frame extent";
        return nullptr;
    }

    if (nullptr != frame_size && node::kExprPrimary != frame_size->expr_type_) {
        LOG(WARNING) << "Fail Make Frame Node: 3nd arg isn't const expression";
        return nullptr;
    }

    int64_t max_size = 0;
    if (nullptr != frame_size) {
        max_size = dynamic_cast<ConstNode *>(frame_size)->GetAsInt64();
        if (max_size <= 0) {
            LOG(WARNING) << "Invalid Frame MaxSize: MAXSIZE <= 0";
            return nullptr;
        }
    }

    return MakeFrameNode(frame_type, frame_extent, max_size);
}

SqlNode *NodeManager::MakeFrameNode(FrameType frame_type, SqlNode *frame_extent, int64_t maxsize) {
    if (nullptr != frame_extent && node::kFrameExtent != frame_extent->type_) {
        LOG(WARNING) << "Fail Make Frame Node: 2nd arg isn't frame extent";
        return nullptr;
    }

    switch (frame_type) {
        case kFrameRows: {
            FrameNode *node_ptr =
                new FrameNode(frame_type, nullptr, dynamic_cast<FrameExtent *>(frame_extent), maxsize);
            return RegisterNode(node_ptr);
        }
        case kFrameRange:
        case kFrameRowsRange:
        case kFrameRowsMergeRowsRange: {
            FrameNode *node_ptr =
                new FrameNode(frame_type, dynamic_cast<FrameExtent *>(frame_extent), nullptr, maxsize);
            return RegisterNode(node_ptr);
        }
    }
    return nullptr;
}

SqlNode *NodeManager::MakeFrameNode(FrameType frame_type, FrameExtent *frame_range, FrameExtent *frame_rows,
                                    int64_t maxsize) {
    FrameNode *node_ptr = new FrameNode(frame_type, frame_range, frame_rows, maxsize);
    return RegisterNode(node_ptr);
}
OrderExpression *NodeManager::MakeOrderExpression(const ExprNode *expr, const bool is_asc) {
    OrderExpression *node_ptr = new OrderExpression(expr, is_asc);
    return RegisterNode(node_ptr);
}
OrderByNode *NodeManager::MakeOrderByNode(const ExprListNode *order_expressions) {
    OrderByNode *node_ptr = new OrderByNode(order_expressions);
    return RegisterNode(node_ptr);
}

ColumnRefNode *NodeManager::MakeColumnRefNode(const std::string &column_name, const std::string &relation_name,
                                              const std::string &db_name) {
    ColumnRefNode *node_ptr = new ColumnRefNode(column_name, relation_name, db_name);

    return RegisterNode(node_ptr);
}

ColumnIdNode *NodeManager::MakeColumnIdNode(size_t column_id) { return RegisterNode(new ColumnIdNode(column_id)); }

GetFieldExpr *NodeManager::MakeGetFieldExpr(ExprNode *input, const std::string &column_name, size_t column_id) {
    return RegisterNode(new GetFieldExpr(input, column_name, column_id));
}
GetFieldExpr *NodeManager::MakeGetFieldExpr(ExprNode *input, size_t idx) {
    return RegisterNode(new GetFieldExpr(input, std::to_string(idx), idx));
}

ColumnRefNode *NodeManager::MakeColumnRefNode(const std::string &column_name, const std::string &relation_name) {
    return MakeColumnRefNode(column_name, relation_name, "");
}
CastExprNode *NodeManager::MakeCastNode(const node::DataType cast_type, ExprNode *expr) {
    CastExprNode *node_ptr = new CastExprNode(cast_type, expr);
    return RegisterNode(node_ptr);
}
WhenExprNode *NodeManager::MakeWhenNode(ExprNode *when_expr, ExprNode *then_expr) {
    WhenExprNode *node_ptr = new WhenExprNode(when_expr, then_expr);
    return RegisterNode(node_ptr);
}
ExprNode *NodeManager::MakeSimpleCaseWhenNode(ExprNode *case_expr, ExprListNode *when_list_expr, ExprNode *else_expr) {
    if (nullptr == when_list_expr || when_list_expr->GetChildNum() == 0) {
        return nullptr;
    }
    ExprListNode *bool_when_expr = MakeExprList();
    for (auto expr : when_list_expr->children_) {
        if (nullptr == expr || kExprWhen != expr->expr_type_) {
            return nullptr;
        }
        auto when_expr = dynamic_cast<WhenExprNode *>(expr);

        bool_when_expr->AddChild(
            MakeWhenNode(MakeBinaryExprNode(case_expr, when_expr->when_expr(), kFnOpEq), when_expr->then_expr()));
    }
    return MakeSearchedCaseWhenNode(bool_when_expr, else_expr);
}
ExprNode *NodeManager::MakeSearchedCaseWhenNode(ExprListNode *when_list_expr, ExprNode *else_expr) {
    if (nullptr == else_expr) {
        else_expr = MakeConstNode();
    }
    CaseWhenExprNode *node_ptr = new CaseWhenExprNode(when_list_expr, else_expr);
    return RegisterNode(node_ptr);
}

CallExprNode *NodeManager::MakeFuncNode(const std::string &name, const std::vector<ExprNode *> &args,
                                        const SqlNode *over) {
    ExprListNode args_node;
    for (auto child : args) {
        args_node.AddChild(child);
    }
    FnDefNode *def_node = dynamic_cast<FnDefNode *>(MakeUnresolvedFnDefNode(name));
    CallExprNode *node_ptr = new CallExprNode(def_node, &args_node, dynamic_cast<const WindowDefNode *>(over));
    return RegisterNode(node_ptr);
}

CallExprNode *NodeManager::MakeFuncNode(const std::string &name, ExprListNode *list_ptr, const SqlNode *over) {
    FnDefNode *def_node = dynamic_cast<FnDefNode *>(MakeUnresolvedFnDefNode(name));
    CallExprNode *node_ptr = new CallExprNode(def_node, list_ptr, dynamic_cast<const WindowDefNode *>(over));
    return RegisterNode(node_ptr);
}

CallExprNode *NodeManager::MakeFuncNode(FnDefNode *fn, ExprListNode *list_ptr, const SqlNode *over) {
    CallExprNode *node_ptr = new CallExprNode(fn, list_ptr, dynamic_cast<const WindowDefNode *>(over));
    return RegisterNode(node_ptr);
}

CallExprNode *NodeManager::MakeFuncNode(FnDefNode *fn, const std::vector<ExprNode *> &args, const SqlNode *over) {
    ExprListNode args_node;
    for (auto child : args) {
        args_node.AddChild(child);
    }
    CallExprNode *node_ptr = new CallExprNode(fn, &args_node, dynamic_cast<const WindowDefNode *>(over));
    return RegisterNode(node_ptr);
}

ConstNode *NodeManager::MakeConstNode(bool value) { return RegisterNode(new ConstNode(value)); }
ConstNode *NodeManager::MakeConstNode(int16_t value) { return RegisterNode(new ConstNode(value)); }
ConstNode *NodeManager::MakeConstNode(int value) { return RegisterNode(new ConstNode(value)); }

ConstNode *NodeManager::MakeConstNode(int value, TTLType ttl_type) {
    return RegisterNode(new ConstNode(value, ttl_type));
}

ConstNode *NodeManager::MakeConstNode(int64_t value) { return RegisterNode(new ConstNode(value)); }

ConstNode *NodeManager::MakeConstNode(int64_t value, TTLType ttl_type) {
    return RegisterNode(new ConstNode(value, ttl_type));
}

ConstNode *NodeManager::MakeConstNode(int64_t value, DataType time_type) {
    return RegisterNode(new ConstNode(value, time_type));
}

ConstNode *NodeManager::MakeConstNode(float value) { return RegisterNode(new ConstNode(value)); }

ConstNode *NodeManager::MakeConstNode(double value) { return RegisterNode(new ConstNode(value)); }

ConstNode *NodeManager::MakeConstNode(const char *value) { return RegisterNode(new ConstNode(value)); }
ConstNode *NodeManager::MakeConstNode(const std::string &value) { return RegisterNode(new ConstNode(value)); }
ConstNode *NodeManager::MakeConstNode() { return RegisterNode(new ConstNode()); }

ConstNode *NodeManager::MakeConstNode(DataType type) { return RegisterNode(new ConstNode(type)); }
ParameterExpr *NodeManager::MakeParameterExpr(int position) {
    ParameterExpr *node_ptr = new ParameterExpr(position);
    return RegisterNode(node_ptr);
}
ExprIdNode *NodeManager::MakeExprIdNode(const std::string &name) {
    return RegisterNode(new ::hybridse::node::ExprIdNode(name, exprid_idx_counter_++));
}
ExprIdNode *NodeManager::MakeUnresolvedExprId(const std::string &name) {
    return RegisterNode(new ::hybridse::node::ExprIdNode(name, -1));
}

BinaryExpr *NodeManager::MakeBinaryExprNode(ExprNode *left, ExprNode *right, FnOperator op) {
    ::hybridse::node::BinaryExpr *bexpr = new ::hybridse::node::BinaryExpr(op);
    bexpr->AddChild(left);
    bexpr->AddChild(right);
    return RegisterNode(bexpr);
}

UnaryExpr *NodeManager::MakeUnaryExprNode(ExprNode *left, FnOperator op) {
    ::hybridse::node::UnaryExpr *uexpr = new ::hybridse::node::UnaryExpr(op);
    uexpr->AddChild(left);
    return RegisterNode(uexpr);
}

SqlNode *NodeManager::MakeCreateTableNode(bool op_if_not_exist, const std::string &db_name,
                                          const std::string &table_name, SqlNodeList *column_desc_list,
                                          SqlNodeList *table_option_list) {
    CreateStmt *node_ptr = new CreateStmt(db_name, table_name, op_if_not_exist);
    FillSqlNodeList2NodeVector(column_desc_list, *(node_ptr->MutableColumnDefList()));
    FillSqlNodeList2NodeVector(table_option_list, *(node_ptr->MutableTableOptionList()));
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeColumnIndexNode(SqlNodeList *index_item_list) {
    ColumnIndexNode *index_ptr = new ColumnIndexNode();
    if (nullptr != index_item_list && 0 != index_item_list->GetSize()) {
        for (auto node_ptr : index_item_list->GetList()) {
            switch (node_ptr->GetType()) {
                case kIndexKey:
                    index_ptr->SetKey(dynamic_cast<IndexKeyNode *>(node_ptr)->GetKey());
                    break;
                case kIndexTs:
                    index_ptr->SetTs(dynamic_cast<IndexTsNode *>(node_ptr)->GetColumnName());
                    break;
                case kIndexVersion:
                    index_ptr->SetVersion(dynamic_cast<IndexVersionNode *>(node_ptr)->GetColumnName());

                    index_ptr->SetVersionCount(dynamic_cast<IndexVersionNode *>(node_ptr)->GetCount());
                    break;
                case kIndexTTL: {
                    IndexTTLNode *ttl_node = dynamic_cast<IndexTTLNode *>(node_ptr);
                    index_ptr->SetTTL(ttl_node->GetTTLExpr());
                    break;
                }
                case kIndexTTLType: {
                    IndexTTLTypeNode *ttl_type_node = dynamic_cast<IndexTTLTypeNode *>(node_ptr);
                    index_ptr->set_ttl_type(ttl_type_node->ttl_type());
                    break;
                }
                default: {
                    LOG(WARNING) << "can not handle type " << NameOfSqlNodeType(node_ptr->GetType())
                                 << " for column index";
                }
            }
        }
    }
    return RegisterNode(index_ptr);
}
SqlNode *NodeManager::MakeColumnIndexNode(SqlNodeList *keys, SqlNode *ts, SqlNode *ttl, SqlNode *version) {
    SqlNode *node_ptr = new SqlNode(kColumnIndex, 0, 0);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeColumnDescNode(const std::string &column_name, const DataType data_type, bool op_not_null,
                                         ExprNode *default_value) {
    SqlNode *node_ptr = new ColumnDefNode(column_name, data_type, op_not_null, default_value);
    return RegisterNode(node_ptr);
}

SqlNodeList *NodeManager::MakeNodeList() {
    SqlNodeList *new_list_ptr = new SqlNodeList();
    RegisterNode(new_list_ptr);
    return new_list_ptr;
}

SqlNodeList *NodeManager::MakeNodeList(SqlNode *node) {
    SqlNodeList *new_list_ptr = new SqlNodeList();
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

ArrayExpr *NodeManager::MakeArrayExpr() {
    ArrayExpr *expr = new ArrayExpr();
    return RegisterNode(expr);
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

PlanNode *NodeManager::MakeTablePlanNode(const std::string& db, const std::string &table_name) {
    PlanNode *node_ptr = new TablePlanNode(db, table_name);
    return RegisterNode(node_ptr);
}

PlanNode *NodeManager::MakeRenamePlanNode(PlanNode *node, std::string alias_name) {
    PlanNode *node_ptr = new RenamePlanNode(node, alias_name);
    return RegisterNode(node_ptr);
}

FilterPlanNode *NodeManager::MakeFilterPlanNode(PlanNode *node, const ExprNode *condition) {
    node::FilterPlanNode *node_ptr = new FilterPlanNode(node, condition);
    RegisterNode(node_ptr);
    return node_ptr;
}

WindowPlanNode *NodeManager::MakeWindowPlanNode(int w_id) {
    WindowPlanNode *node_ptr = new WindowPlanNode(w_id);
    RegisterNode(node_ptr);
    return node_ptr;
}

ProjectListNode *NodeManager::MakeProjectListPlanNode(const WindowPlanNode *w_ptr, const bool need_agg) {
    ProjectListNode *node_ptr = new ProjectListNode(w_ptr, need_agg);
    RegisterNode(node_ptr);
    return node_ptr;
}

FnNode *NodeManager::MakeFnHeaderNode(const std::string &name, FnNodeList *plist, const TypeNode *return_type) {
    ::hybridse::node::FnNodeFnHeander *fn_header = new FnNodeFnHeander(name, plist, return_type);
    return RegisterNode(fn_header);
}

FnNode *NodeManager::MakeFnDefNode(const FnNode *header, FnNodeList *block) {
    ::hybridse::node::FnNodeFnDef *fn_def = new FnNodeFnDef(dynamic_cast<const FnNodeFnHeander *>(header), block);
    return RegisterNode(fn_def);
}
FnNode *NodeManager::MakeAssignNode(const std::string &name, ExprNode *expression) {
    auto var = MakeExprIdNode(name);
    ::hybridse::node::FnAssignNode *fn_assign = new hybridse::node::FnAssignNode(var, expression);
    return RegisterNode(fn_assign);
}

FnNode *NodeManager::MakeAssignNode(const std::string &name, ExprNode *expression, const FnOperator op) {
    auto lhs_var = MakeExprIdNode(name);
    auto rhs_var = MakeUnresolvedExprId(name);
    ::hybridse::node::FnAssignNode *fn_assign =
        new hybridse::node::FnAssignNode(lhs_var, MakeBinaryExprNode(rhs_var, expression, op));
    return RegisterNode(fn_assign);
}
FnNode *NodeManager::MakeReturnStmtNode(ExprNode *value) {
    FnNode *fn_node = new FnReturnStmt(value);
    return RegisterNode(fn_node);
}

FnNode *NodeManager::MakeIfStmtNode(ExprNode *value) {
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
FnNode *NodeManager::MakeFnNode(const SqlNodeType &type) { return RegisterNode(new FnNode(type)); }

FnNodeList *NodeManager::MakeFnListNode() {
    FnNodeList *fn_list = new FnNodeList();
    RegisterNode(fn_list);
    return fn_list;
}
FnNodeList *NodeManager::MakeFnListNode(node::FnNode *fn_node) {
    FnNodeList *fn_list = new FnNodeList();
    fn_list->AddChild(fn_node);
    RegisterNode(fn_list);
    return fn_list;
}

FnIfBlock *NodeManager::MakeFnIfBlock(FnIfNode *if_node, FnNodeList *block) {
    ::hybridse::node::FnIfBlock *if_block = new ::hybridse::node::FnIfBlock(if_node, block);
    RegisterNode(if_block);
    return if_block;
}

FnElifBlock *NodeManager::MakeFnElifBlock(FnElifNode *elif_node, FnNodeList *block) {
    ::hybridse::node::FnElifBlock *elif_block = new ::hybridse::node::FnElifBlock(elif_node, block);
    RegisterNode(elif_block);
    return elif_block;
}
FnIfElseBlock *NodeManager::MakeFnIfElseBlock(FnIfBlock *if_block, const std::vector<FnNode *> &elif_blocks,
                                              FnElseBlock *else_block) {
    ::hybridse::node::FnIfElseBlock *if_else_block =
        new ::hybridse::node::FnIfElseBlock(if_block, elif_blocks, else_block);
    RegisterNode(if_else_block);
    return if_else_block;
}
FnElseBlock *NodeManager::MakeFnElseBlock(FnNodeList *block) {
    ::hybridse::node::FnElseBlock *else_block = new ::hybridse::node::FnElseBlock(block);
    RegisterNode(else_block);
    return else_block;
}

FnParaNode *NodeManager::MakeFnParaNode(const std::string &name, const TypeNode *para_type) {
    auto expr_id = MakeExprIdNode(name);
    expr_id->SetOutputType(para_type);
    ::hybridse::node::FnParaNode *para_node = new ::hybridse::node::FnParaNode(expr_id);
    return RegisterNode(para_node);
}
SqlNode *NodeManager::MakeIndexKeyNode(const std::string &key) {
    SqlNode *node_ptr = new IndexKeyNode(key);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeIndexKeyNode(const std::vector<std::string> &keys) {
    SqlNode *node_ptr = new IndexKeyNode(keys);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeIndexTsNode(const std::string &ts) {
    SqlNode *node_ptr = new IndexTsNode(ts);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeIndexTTLNode(ExprListNode *ttl_expr) {
    SqlNode *node_ptr = new IndexTTLNode(ttl_expr);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeIndexTTLTypeNode(const std::string &ttl_type) {
    SqlNode *node_ptr = new IndexTTLTypeNode(ttl_type);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeIndexVersionNode(const std::string &version) {
    SqlNode *node_ptr = new IndexVersionNode(version);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeIndexVersionNode(const std::string &version, int count) {
    SqlNode *node_ptr = new IndexVersionNode(version, count);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeCmdNode(node::CmdType cmd_type) {
    SqlNode *node_ptr = new CmdNode(cmd_type);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeCmdNode(node::CmdType cmd_type, const std::string &arg) {
    CmdNode *node_ptr = new CmdNode(cmd_type);
    node_ptr->AddArg(arg);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeCmdNode(node::CmdType cmd_type, const std::vector<std::string> &args) {
    CmdNode *node_ptr = new CmdNode(cmd_type);
    for (auto const & arg : args) {
        node_ptr->AddArg(arg);
    }
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeCmdNode(node::CmdType cmd_type, const std::string &arg1,
                                  const std::string &arg2) {
    CmdNode *node_ptr = new CmdNode(cmd_type);
    node_ptr->AddArg(arg1);
    node_ptr->AddArg(arg2);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeCreateIndexNode(const std::string &index_name,
                                          const std::string &db_name,
                                          const std::string &table_name,
                                          ColumnIndexNode *index) {
    CreateIndexNode *node_ptr = new CreateIndexNode(index_name, db_name, table_name, index);
    return RegisterNode(node_ptr);
}

DeployNode *NodeManager::MakeDeployStmt(const std::string &name, const SqlNode *stmt, const std::string &stmt_str,
                                        const std::shared_ptr<OptionsMap> options, bool if_not_exist) {
    DeployNode *node = new DeployNode(name, stmt, stmt_str, std::move(options), if_not_exist);
    return RegisterNode(node);
}

DeployPlanNode *NodeManager::MakeDeployPlanNode(const std::string &name, const SqlNode *stmt,
                                                const std::string &stmt_str, const std::shared_ptr<OptionsMap> options,
                                                bool if_not_exist) {
    DeployPlanNode *node = new DeployPlanNode(name, stmt, stmt_str, std::move(options), if_not_exist);
    return RegisterNode(node);
}
DeleteNode* NodeManager::MakeDeleteNode(DeleteTarget target, std::string_view job_id,
        const std::string& db_name, const std::string& table, node::ExprNode* where_expr) {
    auto node = new DeleteNode(target, std::string(job_id.data(), job_id.size()), db_name, table, where_expr);
    return RegisterNode(node);
}
DeletePlanNode* NodeManager::MakeDeletePlanNode(const DeleteNode* n) {
    auto node = new DeletePlanNode(n->GetTarget(), n->GetJobId(),
            n->GetDbName(), n->GetTableName(), n->GetCondition());
    return RegisterNode(node);
}
LoadDataNode *NodeManager::MakeLoadDataNode(const std::string &file_name, const std::string &db,
                                            const std::string &table,
                                            const std::shared_ptr<OptionsMap> options,
                                            const std::shared_ptr<OptionsMap> config_option) {
    LoadDataNode *node = new LoadDataNode(file_name, db, table, std::move(options), std::move(config_option));
    return RegisterNode(node);
}
LoadDataPlanNode *NodeManager::MakeLoadDataPlanNode(const std::string &file_name, const std::string &db,
                                                    const std::string &table, const std::shared_ptr<OptionsMap> options,
                                                    const std::shared_ptr<OptionsMap> config_option) {
    LoadDataPlanNode *node = new LoadDataPlanNode(file_name, db, table, options, config_option);
    return RegisterNode(node);
}

CreateFunctionPlanNode *NodeManager::MakeCreateFunctionPlanNode(const std::string &function_name,
                                                               const TypeNode* return_type,
                                                               const NodePointVector& args_type,
                                                               bool is_aggregate,
                                                               std::shared_ptr<OptionsMap> options) {
    auto node = new CreateFunctionPlanNode(function_name, return_type, args_type, is_aggregate, options);
    return RegisterNode(node);
}

SelectIntoNode *NodeManager::MakeSelectIntoNode(const QueryNode *query, const std::string &query_str,
                                                const std::string &out_file, const std::shared_ptr<OptionsMap> options,
                                                const std::shared_ptr<OptionsMap> config_option) {
    SelectIntoNode* node = new SelectIntoNode(query, query_str, out_file, std::move(options), std::move(config_option));
    return RegisterNode(node);
}

SelectIntoPlanNode *NodeManager::MakeSelectIntoPlanNode(PlanNode *query, const std::string &query_str,
                                                        const std::string &out_file,
                                                        const std::shared_ptr<OptionsMap> options,
                                                        const std::shared_ptr<OptionsMap> config_option) {
    SelectIntoPlanNode* node = new SelectIntoPlanNode(query, query_str, out_file, options, config_option);
    return RegisterNode(node);
}

SetNode* NodeManager::MakeSetNode(const node::VariableScope scope, const std::string &key, const
                                                      ConstNode *value) {
    SetNode* node = new SetNode(scope, key, value);
    return RegisterNode(node);
}
SetPlanNode* NodeManager::MakeSetPlanNode(const SetNode *set_node) {
    SetPlanNode* node = new SetPlanNode(set_node->Scope(), set_node->Key(), set_node->Value());
    return RegisterNode(node);
}

AllNode *NodeManager::MakeAllNode(const std::string &relation_name) { return MakeAllNode(relation_name, ""); }

AllNode *NodeManager::MakeAllNode(const std::string &relation_name, const std::string &db_name) {
    return RegisterNode(new AllNode(relation_name, db_name));
}

SqlNode *NodeManager::MakeInsertTableNode(const std::string &db_name, const std::string &table_name,
                                          const ExprListNode *columns_expr, const ExprListNode *values) {
    if (nullptr == columns_expr) {
        InsertStmt *node_ptr = new InsertStmt(db_name, table_name, values->children_);
        return RegisterNode(node_ptr);
    } else {
        std::vector<std::string> column_names;
        for (auto expr : columns_expr->children_) {
            switch (expr->GetExprType()) {
                case kExprColumnRef: {
                    ColumnRefNode *column_ref = dynamic_cast<ColumnRefNode *>(expr);
                    column_names.push_back(column_ref->GetColumnName());
                    break;
                }
                default: {
                    LOG(WARNING) << "Can't not handle insert column name with type"
                                 << ExprTypeName(expr->GetExprType());
                }
            }
        }
        InsertStmt *node_ptr = new InsertStmt(db_name, table_name, column_names, values->children_);
        return RegisterNode(node_ptr);
    }
}

DatasetNode *NodeManager::MakeDataset(const std::string &table) { return RegisterNode(new DatasetNode(table)); }

MapNode *NodeManager::MakeMapNode(const NodePointVector &nodes) { return RegisterNode(new MapNode(nodes)); }

TypeNode *NodeManager::MakeTypeNode(hybridse::node::DataType base) {
    TypeNode *node_ptr = new TypeNode(base);
    RegisterNode(node_ptr);
    return node_ptr;
}
TypeNode *NodeManager::MakeTypeNode(hybridse::node::DataType base, const hybridse::node::TypeNode *v1) {
    TypeNode *node_ptr = new TypeNode(base, v1);
    RegisterNode(node_ptr);
    return node_ptr;
}
TypeNode *NodeManager::MakeTypeNode(hybridse::node::DataType base, hybridse::node::DataType v1) {
    TypeNode *node_ptr = new TypeNode(base, MakeTypeNode(v1));
    RegisterNode(node_ptr);
    return node_ptr;
}
TypeNode *NodeManager::MakeTypeNode(hybridse::node::DataType base, hybridse::node::DataType v1,
                                    hybridse::node::DataType v2) {
    TypeNode *node_ptr = new TypeNode(base, MakeTypeNode(v1), MakeTypeNode(v2));
    RegisterNode(node_ptr);
    return node_ptr;
}
FixedArrayType *NodeManager::MakeArrayType(const TypeNode *ele_ty, uint64_t sz) {
    return RegisterNode(new FixedArrayType(ele_ty, sz));
}

OpaqueTypeNode *NodeManager::MakeOpaqueType(size_t bytes) { return RegisterNode(new OpaqueTypeNode(bytes)); }
RowTypeNode *NodeManager::MakeRowType(const std::vector<const codec::Schema *> &schema_source) {
    return RegisterNode(new RowTypeNode(schema_source));
}
RowTypeNode *NodeManager::MakeRowType(const vm::SchemasContext *schemas_ctx) {
    return RegisterNode(new RowTypeNode(schemas_ctx));
}

FnNode *NodeManager::MakeForInStmtNode(const std::string &var_name, ExprNode *expression) {
    auto var = MakeExprIdNode(var_name);
    FnForInNode *node_ptr = new FnForInNode(var, expression);
    return RegisterNode(node_ptr);
}

FnForInBlock *NodeManager::MakeForInBlock(FnForInNode *for_in_node, FnNodeList *block) {
    FnForInBlock *node_ptr = new FnForInBlock(for_in_node, block);
    RegisterNode(node_ptr);
    return node_ptr;
}
PlanNode *NodeManager::MakeJoinNode(PlanNode *left, PlanNode *right, JoinType join_type, const OrderByNode *order_by,
                                    const ExprNode *condition) {
    node::JoinPlanNode *node_ptr = new JoinPlanNode(left, right, join_type, order_by, condition);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeGroupPlanNode(PlanNode *node, const ExprListNode *by_list) {
    node::GroupPlanNode *node_ptr = new GroupPlanNode(node, by_list);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeLimitPlanNode(PlanNode *node, int limit_cnt) {
    node::LimitPlanNode *node_ptr = new LimitPlanNode(node, limit_cnt);
    return RegisterNode(node_ptr);
}
ProjectNode *NodeManager::MakeProjectNode(const int32_t pos, const std::string &name, const bool is_aggregation,
                                          node::ExprNode *expression, node::FrameNode *frame) {
    node::ProjectNode *node_ptr = new ProjectNode(pos, name, is_aggregation, expression, frame);
    RegisterNode(node_ptr);
    return node_ptr;
}
CreatePlanNode *NodeManager::MakeCreateTablePlanNode(const std::string &db_name, const std::string &table_name,
                                                     const NodePointVector &column_list,
                                                     const NodePointVector &table_option_list,
                                                     const bool if_not_exist) {
    node::CreatePlanNode *node_ptr =
        new CreatePlanNode(db_name, table_name, column_list, if_not_exist, table_option_list);
    RegisterNode(node_ptr);
    return node_ptr;
}

CreateProcedurePlanNode *NodeManager::MakeCreateProcedurePlanNode(const std::string &sp_name,
                                                                  const NodePointVector &input_parameter_list,
                                                                  const PlanNodeList &inner_plan_node_list) {
    node::CreateProcedurePlanNode *node_ptr =
        new CreateProcedurePlanNode(sp_name, input_parameter_list, inner_plan_node_list);
    RegisterNode(node_ptr);
    return node_ptr;
}

CmdPlanNode *NodeManager::MakeCmdPlanNode(const CmdNode *node) {
    node::CmdPlanNode *node_ptr = new CmdPlanNode(node->GetCmdType(), node->GetArgs());
    node_ptr->SetIfNotExists(node->IsIfNotExists());
    node_ptr->SetIfExists(node->IsIfExists());
    RegisterNode(node_ptr);
    return node_ptr;
}
InsertPlanNode *NodeManager::MakeInsertPlanNode(const InsertStmt *node) {
    node::InsertPlanNode *node_ptr = new InsertPlanNode(node);
    RegisterNode(node_ptr);
    return node_ptr;
}
ExplainPlanNode *NodeManager::MakeExplainPlanNode(const ExplainNode *node) {
    node::ExplainPlanNode *node_ptr = new ExplainPlanNode(node);
    RegisterNode(node_ptr);
    return node_ptr;
}
FuncDefPlanNode *NodeManager::MakeFuncPlanNode(FnNodeFnDef *node) {
    node::FuncDefPlanNode *node_ptr = new FuncDefPlanNode(node);
    RegisterNode(node_ptr);
    return node_ptr;
}
CreateIndexPlanNode *NodeManager::MakeCreateCreateIndexPlanNode(const CreateIndexNode *node) {
    node::CreateIndexPlanNode *node_ptr = new CreateIndexPlanNode(node);
    RegisterNode(node_ptr);
    return node_ptr;
}
QueryExpr *NodeManager::MakeQueryExprNode(const QueryNode *query) { return RegisterNode(new QueryExpr(query)); }
PlanNode *NodeManager::MakeSortPlanNode(PlanNode *node, const OrderByNode *order_list) {
    node::SortPlanNode *node_ptr = new SortPlanNode(node, order_list);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeUnionPlanNode(PlanNode *left, PlanNode *right, const bool is_all) {
    node::UnionPlanNode *node_ptr = new UnionPlanNode(left, right, is_all);
    return RegisterNode(node_ptr);
}
PlanNode *NodeManager::MakeDistinctPlanNode(PlanNode *node) {
    node::DistinctPlanNode *node_ptr = new DistinctPlanNode(node);
    return RegisterNode(node_ptr);
}
SqlNode *NodeManager::MakeExplainNode(const QueryNode *query, ExplainType explain_type) {
    node::ExplainNode *node_ptr = new ExplainNode(query, explain_type);
    return RegisterNode(node_ptr);
}
ProjectNode *NodeManager::MakeAggProjectNode(const int32_t pos, const std::string &name, node::ExprNode *expression,
                                             node::FrameNode *frame) {
    return MakeProjectNode(pos, name, true, expression, frame);
}
ProjectNode *NodeManager::MakeRowProjectNode(const int32_t pos, const std::string &name, node::ExprNode *expression) {
    return MakeProjectNode(pos, name, false, expression, nullptr);
}

BetweenExpr *NodeManager::MakeBetweenExpr(ExprNode *expr, ExprNode *left, ExprNode *right, const bool is_not) {
    BetweenExpr *node = new BetweenExpr(expr, left, right);
    node->set_is_not_between(is_not);
    return RegisterNode(node);
}
InExpr *NodeManager::MakeInExpr(ExprNode* lhs, ExprNode* in_list, bool is_not) {
    InExpr* in_expr = new InExpr(lhs, in_list, is_not);
    return RegisterNode(in_expr);
}
EscapedExpr *NodeManager::MakeEscapeExpr(ExprNode* pattern, ExprNode* escape) {
    EscapedExpr* escape_expr = new EscapedExpr(pattern, escape);
    return RegisterNode(escape_expr);
}
ExprNode *NodeManager::MakeAndExpr(ExprListNode *expr_list) {
    if (node::ExprListNullOrEmpty(expr_list)) {
        return nullptr;
    }
    ExprNode *left_node = expr_list->children_[0];
    if (1 == expr_list->children_.size()) {
        return left_node;
    }
    for (size_t i = 1; i < expr_list->children_.size(); i++) {
        left_node = MakeBinaryExprNode(left_node, expr_list->children_[i], node::kFnOpAnd);
    }
    return left_node;
}

ExternalFnDefNode *NodeManager::MakeExternalFnDefNode(const std::string &function_name, void *function_ptr,
                                                      const node::TypeNode *ret_type, bool ret_nullable,
                                                      const std::vector<const node::TypeNode *> &arg_types,
                                                      const std::vector<int> &arg_nullable, int variadic_pos,
                                                      bool return_by_arg) {
    return RegisterNode(new node::ExternalFnDefNode(function_name, function_ptr, ret_type, ret_nullable, arg_types,
                                                    arg_nullable, variadic_pos, return_by_arg));
}

DynamicUdfFnDefNode *NodeManager::MakeDynamicUdfFnDefNode(const std::string &function_name, void *function_ptr,
                                                      const node::TypeNode *ret_type, bool ret_nullable,
                                                      const std::vector<const node::TypeNode *> &arg_types,
                                                      const std::vector<int> &arg_nullable, bool return_by_arg,
                                                      ExternalFnDefNode *init_node) {
    return RegisterNode(new node::DynamicUdfFnDefNode(function_name, function_ptr, ret_type, ret_nullable, arg_types,
                                                    arg_nullable, return_by_arg, init_node));
}

node::ExternalFnDefNode *NodeManager::MakeUnresolvedFnDefNode(const std::string &function_name) {
    return RegisterNode(new node::ExternalFnDefNode(function_name, nullptr, nullptr, true, {}, {}, -1, false));
}

node::UdfDefNode *NodeManager::MakeUdfDefNode(FnNodeFnDef *def) { return RegisterNode(new node::UdfDefNode(def)); }

node::UdfByCodeGenDefNode *NodeManager::MakeUdfByCodeGenDefNode(const std::string &name,
                                                                const std::vector<const node::TypeNode *> &arg_types,
                                                                const std::vector<int> &arg_nullable,
                                                                const node::TypeNode *ret_type, bool ret_nullable) {
    return RegisterNode(new node::UdfByCodeGenDefNode(name, arg_types, arg_nullable, ret_type, ret_nullable));
}

node::UdafDefNode *NodeManager::MakeUdafDefNode(const std::string &name, const std::vector<const TypeNode *> &arg_types,
                                                ExprNode *init, FnDefNode *update_func, FnDefNode *merge_func,
                                                FnDefNode *output_func) {
    return RegisterNode(new node::UdafDefNode(name, arg_types, init, update_func, merge_func, output_func));
}

LambdaNode *NodeManager::MakeLambdaNode(const std::vector<ExprIdNode *> &args, ExprNode *body) {
    return RegisterNode(new node::LambdaNode(args, body));
}

CondExpr *NodeManager::MakeCondExpr(ExprNode *condition, ExprNode *left, ExprNode *right) {
    return RegisterNode(new CondExpr(condition, left, right));
}

SqlNode *NodeManager::MakePartitionMetaNode(RoleType role_type, const std::string &endpoint) {
    SqlNode *node_ptr = new PartitionMetaNode(endpoint, role_type);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeReplicaNumNode(int num) {
    SqlNode *node_ptr = new ReplicaNumNode(num);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeStorageModeNode(StorageMode storage_mode) {
    SqlNode *node_ptr = new StorageModeNode(storage_mode);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakePartitionNumNode(int num) {
    SqlNode *node_ptr = new PartitionNumNode(num);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeDistributionsNode(const NodePointVector& distribution_list) {
    DistributionsNode *index_ptr = new DistributionsNode(distribution_list);
    return RegisterNode(index_ptr);
}

SqlNode *NodeManager::MakeCreateProcedureNode(const std::string &sp_name, SqlNodeList *input_parameter_list,
                                              SqlNode *inner_node) {
    CreateSpStmt *node_ptr = new CreateSpStmt(sp_name);
    FillSqlNodeList2NodeVector(input_parameter_list, node_ptr->GetInputParameterList());
    std::vector<SqlNode *> &list = node_ptr->GetInnerNodeList();
    list.push_back(inner_node);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeCreateFunctionNode(const std::string function_name, DataType return_type,
        const std::vector<DataType>& args_type, bool is_aggregate, std::shared_ptr<OptionsMap> options) {
    auto return_type_node = MakeTypeNode(return_type);
    NodePointVector type_node_vec;
    for (const auto type : args_type) {
        type_node_vec.push_back(MakeTypeNode(type));
    }
    auto node_ptr = new CreateFunctionNode(function_name, return_type_node, type_node_vec, is_aggregate, options);
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeCreateProcedureNode(const std::string &sp_name, SqlNodeList *input_parameter_list,
                                              SqlNodeList *inner_node_list) {
    CreateSpStmt *node_ptr = new CreateSpStmt(sp_name);
    FillSqlNodeList2NodeVector(input_parameter_list, node_ptr->GetInputParameterList());
    FillSqlNodeList2NodeVector(inner_node_list, node_ptr->GetInnerNodeList());
    return RegisterNode(node_ptr);
}

SqlNode *NodeManager::MakeInputParameterNode(bool is_constant, const std::string &column_name, DataType data_type) {
    SqlNode *node_ptr = new InputParameterNode(column_name, data_type, is_constant);
    return RegisterNode(node_ptr);
}

void NodeManager::SetNodeUniqueId(ExprNode *node) { node->SetNodeId(expr_idx_counter_++); }
void NodeManager::SetNodeUniqueId(TypeNode *node) { node->SetNodeId(type_idx_counter_++); }
void NodeManager::SetNodeUniqueId(PlanNode *node) { node->SetNodeId(plan_idx_counter_++); }
void NodeManager::SetNodeUniqueId(vm::PhysicalOpNode *node) { node->SetNodeId(physical_plan_idx_counter_++); }

}  // namespace node
}  // namespace hybridse
