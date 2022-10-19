/*
 * Copyright 2021 4paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "passes/physical/long_window_optimized.h"

#include <string>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "vm/engine.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

static const absl::flat_hash_set<absl::string_view> WHERE_FUNS = {
    "count_where", "sum_where", "avg_where", "min_where", "max_where",
};

LongWindowOptimized::LongWindowOptimized(PhysicalPlanContext* plan_ctx) : TransformUpPysicalPass(plan_ctx) {
    std::vector<std::string> windows;
    const auto* options = plan_ctx_->GetOptions();
    if (!options) {
        LOG(ERROR) << "plan_ctx option is empty";
        return;
    }

    boost::split(windows, options->at(vm::LONG_WINDOWS), boost::is_any_of(","));
    for (auto& w : windows) {
        std::vector<std::string> window_info;
        boost::split(window_info, w, boost::is_any_of(":"));
        boost::trim(window_info[0]);
        long_windows_.insert(window_info[0]);
    }
}

bool LongWindowOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    if (vm::kPhysicalOpProject != in->GetOpType()) {
        return false;
    }

    auto project_op = dynamic_cast<vm::PhysicalProjectNode*>(in);
    if (project_op->project_type_ != vm::kAggregation) {
        return false;
    }

    auto project_aggr_op = dynamic_cast<vm::PhysicalAggregationNode*>(project_op);
    // TODO(zhanghao): we only support transform PhysicalAggregationNode with one and only one window aggregation op
    // we may remove this constraint in a later optimization
    if (!VerifySingleAggregation(project_op)) {
        LOG(WARNING) << "we only support transform PhysicalAggregationNode with one and only one window aggregation op";
        return false;
    }

    // this case shouldn't happen as we add the LongWindowOptimized pass only when `long_windows` option exists
    if (long_windows_.empty()) {
        LOG(ERROR) << "Long Windows is empty";
        return false;
    }

    const auto& projects = project_aggr_op->project();
    for (size_t i = 0; i < projects.size(); i++) {
        const auto* expr = projects.GetExpr(i);
        if (expr->GetExprType() == node::kExprCall) {
            const auto* call_expr = dynamic_cast<const node::CallExprNode*>(expr);
            const auto* window = call_expr->GetOver();
            if (window == nullptr) continue;

            // skip ANONYMOUS_WINDOW
            if (!window->GetName().empty()) {
                if (long_windows_.count(window->GetName())) {
                    return OptimizeWithPreAggr(project_aggr_op, i, output);
                }
            }
        }
    }

    return true;
}

bool LongWindowOptimized::OptimizeWithPreAggr(vm::PhysicalAggregationNode* in, int idx, PhysicalOpNode** output) {
    *output = in;

    if (in->producers()[0]->GetOpType() != vm::kPhysicalOpRequestUnion) {
        return false;
    }
    auto req_union_op = dynamic_cast<vm::PhysicalRequestUnionNode*>(in->producers()[0]);
    if (!req_union_op->window_unions_.Empty()) {
        LOG(WARNING) << "Not support optimization of RequestUnionOp with window unions";
        return false;
    }
    const auto& projects = in->project();
    auto orig_data_provider = dynamic_cast<vm::PhysicalDataProviderNode*>(req_union_op->GetProducer(1));
    auto aggr_op = dynamic_cast<const node::CallExprNode*>(projects.GetExpr(idx));
    auto window = aggr_op->GetOver();

    auto s = CheckCallExpr(aggr_op);
    if (!s.ok()) {
        LOG(ERROR) << s.status();
        return false;
    }

    const std::string& db_name = orig_data_provider->GetDb();
    const std::string& table_name = orig_data_provider->GetName();
    std::string func_name = aggr_op->GetFnDef()->GetName();
    std::string aggr_col = ConcatExprList({aggr_op->children_.front()});
    std::string filter_col = std::string(s->filter_col_name);
    std::string partition_col;
    if (window->GetPartitions()) {
        partition_col = ConcatExprList(window->GetPartitions()->children_);
    } else {
        partition_col = ConcatExprList(req_union_op->window().partition().keys()->children_);
    }

    std::string order_col;
    if (window->GetOrders()) {
        order_col = ConcatExprList(window->GetOrders()->children_);
    } else {
        auto orders = req_union_op->window().sort().orders()->order_expressions();
        for (size_t i = 0; i < orders->GetChildNum(); i++) {
            auto order = dynamic_cast<node::OrderExpression*>(orders->GetChild(i));
            if (order == nullptr || order->expr() == nullptr) {
                LOG(ERROR) << "OrderBy col is empty";
                return false;
            }
            auto col_ref = dynamic_cast<const node::ColumnRefNode*>(order->expr());
            if (!col_ref) {
                LOG(ERROR) << "OrderBy Col is not ColumnRefNode";
                return false;
            }
            if (order_col.empty()) {
                order_col = col_ref->GetColumnName();
            } else {
                order_col = absl::StrCat(order_col, ",", col_ref->GetColumnName());
            }
        }
    }

    auto table_infos =
        catalog_->GetAggrTables(db_name, table_name, func_name, aggr_col, partition_col, order_col, filter_col);
    if (table_infos.empty()) {
        LOG(WARNING) << absl::StrCat("No Pre-aggregation tables exists for ", db_name, ".", table_name, ": ", func_name,
                                     "(", aggr_col, ")", " partition by ", partition_col, " order by ", order_col);
        return false;
    }

    // TODO(zhanghao): optimize the selection of the best pre-aggregation tables
    auto table = catalog_->GetTable(table_infos[0].aggr_db, table_infos[0].aggr_table);
    if (!table) {
        LOG(ERROR) << "Fail to get table handler for pre-aggregation table " << table_infos[0].aggr_db << "."
                   << table_infos[0].aggr_table;
        return false;
    }

    vm::PhysicalTableProviderNode* aggr = nullptr;
    auto status = plan_ctx_->CreateOp<vm::PhysicalTableProviderNode>(&aggr, table);
    if (!status.isOK()) {
        LOG(ERROR) << "Fail to create PhysicalTableProviderNode for pre-aggregation table " << table_infos[0].aggr_db
                   << "." << table_infos[0].aggr_table << ": " << status;
        return false;
    }

    if (table->GetIndex().size() != 1) {
        LOG(ERROR) << "PreAggregation table index size != 1";
        return false;
    }
    auto index = table->GetIndex().cbegin()->second;
    auto nm = plan_ctx_->node_manager();

    auto request = req_union_op->GetProducer(0);
    auto raw = req_union_op->GetProducer(1);

    // generate an aggregation window for the aggr table
    auto req_window = req_union_op->window();
    auto partitions = nm->MakeExprList();
    for (size_t i = 0; i < index.keys.size(); i++) {
        auto col_ref = nm->MakeColumnRefNode(index.keys[i].name, table->GetName(), table->GetDatabase());
        partitions->AddChild(col_ref);
    }
    vm::RequestWindowOp aggr_window(partitions);

    auto order_col_ref =
        nm->MakeColumnRefNode((*table->GetSchema())[index.ts_pos].name(), table->GetName(), table->GetDatabase());
    auto order_expr = nm->MakeOrderExpression(order_col_ref, true);
    auto orders = nm->MakeExprList();
    orders->AddChild(order_expr);

    auto partition_by = nm->MakeExprList();
    for (size_t i = 0; i < index.keys.size(); i++) {
        auto col_ref = nm->MakeColumnRefNode((*table->GetSchema())[index.keys[i].idx].name(), table->GetName(),
                                             table->GetDatabase());
        partition_by->AddChild(col_ref);
    }

    aggr_window.sort_.orders_ = nm->MakeOrderByNode(orders);
    aggr_window.name_ = req_window.name();
    aggr_window.range_ = req_window.range_;
    aggr_window.range_.range_key_ = order_col_ref;
    aggr_window.partition_.keys_ = partition_by;

    vm::PhysicalRequestAggUnionNode* request_aggr_union = nullptr;
    status = plan_ctx_->CreateOp<vm::PhysicalRequestAggUnionNode>(
        &request_aggr_union, request, raw, aggr, req_union_op->window(), aggr_window,
        req_union_op->instance_not_in_window(), req_union_op->exclude_current_time(),
        req_union_op->output_request_row(), aggr_op);
    if (req_union_op->exclude_current_row_) {
        request_aggr_union->set_out_request_row(false);
    }
    if (!status.isOK()) {
        LOG(ERROR) << "Fail to create PhysicalRequestAggUnionNode: " << status;
        return false;
    }

    vm::PhysicalReduceAggregationNode* reduce_aggr = nullptr;
    auto condition = in->having_condition_.condition();
    if (condition) {
        condition = condition->DeepCopy(plan_ctx_->node_manager());
    }

    status = plan_ctx_->CreateOp<vm::PhysicalReduceAggregationNode>(&reduce_aggr, request_aggr_union, in->project(),
                                                                    condition, in);

    auto ctx = reduce_aggr->schemas_ctx();
    if (ctx->GetSchemaSourceSize() != 1 || ctx->GetSchema(0)->size() != 1) {
        LOG(ERROR) << "PhysicalReduceAggregationNode schema is unexpected";
        return false;
    }
    request_aggr_union->UpdateParentSchema(ctx);

    if (!status.isOK()) {
        LOG(ERROR) << "Fail to create PhysicalReduceAggregationNode: " << status;
        return false;
    }
    DLOG(INFO) << "[LongWindowOptimized] Before transform sql:\n" << (*output)->GetTreeString();
    *output = reduce_aggr;
    DLOG(INFO) << "[LongWindowOptimized] After transform sql:\n" << (*output)->GetTreeString();
    return true;
}

bool LongWindowOptimized::VerifySingleAggregation(vm::PhysicalProjectNode* op) { return op->project().size() == 1; }

std::string LongWindowOptimized::ConcatExprList(std::vector<node::ExprNode*> exprs, const std::string& delimiter) {
    std::string str = "";
    for (const auto expr : exprs) {
        std::string expr_val;
        if (expr->GetExprType() == node::kExprAll) {
            expr_val = expr->GetExprString();
        } else if (expr->GetExprType() == node::kExprColumnRef) {
            expr_val = dynamic_cast<node::ColumnRefNode*>(expr)->GetColumnName();
        } else {
            LOG(ERROR) << "non support expr type in ConcatExprList";
            return "";
        }
        if (str.empty()) {
            str = absl::StrCat(str, expr_val);
        } else {
            str = absl::StrCat(str, delimiter, expr_val);
        }
    }
    return str;
}


// type check of count_where condition node
// left -> column ref
// right -> constant
absl::StatusOr<absl::string_view> CheckCountWhereCond(const node::ExprNode* lhs, const node::ExprNode* rhs) {
    if (lhs->GetExprType() != node::ExprType::kExprColumnRef) {
        return absl::UnimplementedError(absl::StrCat("expect left as column reference but get ", lhs->GetExprString()));
    }
    if (rhs->GetExprType() != node::ExprType::kExprPrimary) {
        return absl::UnimplementedError(absl::StrCat("expect right as constant but get ", rhs->GetExprString()));
    }

    return dynamic_cast<const node::ColumnRefNode*>(lhs)->GetColumnName();
}

// left  -> * or column name
// right -> BinaryExpr of
//       lhs column name and rhs constant, or versa
//       op -> (eq, ne, gt, lt, ge, le)
absl::StatusOr<absl::string_view> CheckCountWhereArgs(const node::ExprNode* right) {
    if (right->GetExprType() != node::ExprType::kExprBinary) {
        return absl::UnimplementedError(absl::StrCat("[Long Window] ExprType ",
                                                     node::ExprTypeName(right->GetExprType()),
                                                     " not implemented as count_where condition"));
    }
    auto* bin_expr = dynamic_cast<const node::BinaryExpr*>(right);
    if (bin_expr == nullptr) {
        return absl::UnknownError("[Long Window] right can't cast to binary expr");
    }

    auto s1 = CheckCountWhereCond(right->GetChild(0), right->GetChild(1));
    auto s2 = CheckCountWhereCond(right->GetChild(1), right->GetChild(0));
    if (!s1.ok() && !s2.ok()) {
        return absl::UnimplementedError(
            absl::StrCat("[Long Window] cond as ", right->GetExprString(), " not support: ", s1.status().message()));
    }

    switch (bin_expr->GetOp()) {
        case node::FnOperator::kFnOpLe:
        case node::FnOperator::kFnOpLt:
        case node::FnOperator::kFnOpGt:
        case node::FnOperator::kFnOpGe:
        case node::FnOperator::kFnOpNeq:
        case node::FnOperator::kFnOpEq:
            break;
        default:
            return absl::UnimplementedError(
                absl::StrCat("[Long Window] filter cond operator ", node::ExprOpTypeName(bin_expr->GetOp())));
    }

    if (s1.ok()) {
        return s1.value();
    }
    return s2.value();
}

// Supported:
// - count(col) or count(*)
// - sum(col)
// - min(col)
// - max(col)
// - avg(col)
// - count_where(col, simple_expr)
// - count_where(*, simple_expr)
//
// simple_expr can be
// - BinaryExpr
//   - operand nodes of the expr can only be column ref and const node
//   - with operator:
//     - eq
//     - neq
//     - lt
//     - gt
//     - le
//     - ge
absl::StatusOr<LongWindowOptimized::AggInfo> LongWindowOptimized::CheckCallExpr(const node::CallExprNode* call) {
    if (call->GetChildNum() != 1 && call->GetChildNum() != 2) {
        return absl::UnimplementedError(
            absl::StrCat("expect call function with argument number 1 or 2, but got ", call->GetExprString()));
    }

    // count/sum/min/max/avg
    auto expr_type = call->GetChild(0)->GetExprType();

    absl::string_view key_col;
    absl::string_view filter_col;
    if (expr_type == node::kExprColumnRef) {
        auto* col_ref = dynamic_cast<const node::ColumnRefNode*>(call->GetChild(0));
        key_col = col_ref->GetColumnName();
    } else if (expr_type == node::kExprAll) {
        key_col = call->GetChild(0)->GetExprString();
    } else {
        return absl::UnimplementedError(
            absl::StrCat("[Long Window] first arg to op is not column or * :", call->GetExprString()));
    }

    if (call->GetChildNum() == 2) {
        if (absl::c_none_of(WHERE_FUNS, [&call](absl::string_view e) { return call->GetFnDef()->GetName() == e; })) {
            return absl::UnimplementedError(absl::StrCat(call->GetFnDef()->GetName(), " not implemented"));
        }

        // count_where
        auto s = CheckCountWhereArgs(call->GetChild(1));
        if (!s.ok()) {
            return s.status();
        }
        filter_col = s.value();
    }

    return AggInfo{key_col, filter_col};
}

}  // namespace passes
}  // namespace hybridse
