/**
 * Copyright (c) 2024 4Paradigm
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

#include "rewriter/ast_rewriter.h"

#include <memory>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "plan/plan_api.h"
#include "zetasql/parser/parse_tree_manual.h"
#include "zetasql/parser/parser.h"
#include "zetasql/parser/unparser.h"

namespace hybridse {
namespace rewriter {

// unparser that make some rewrites so outputed SQL is
// compatible with ANSI SQL as much as can
class LastJoinRewriteUnparser : public zetasql::parser::Unparser {
 public:
    explicit LastJoinRewriteUnparser(std::string* unparsed) : zetasql::parser::Unparser(unparsed) {}
    ~LastJoinRewriteUnparser() override {}
    LastJoinRewriteUnparser(const LastJoinRewriteUnparser&) = delete;
    LastJoinRewriteUnparser& operator=(const LastJoinRewriteUnparser&) = delete;

    void visitASTSelect(const zetasql::ASTSelect* node, void* data) override {
        while (true) {
            absl::string_view filter_col;

            // 1. filter condition is 'col = 1'
            if (node->where_clause() != nullptr &&
                node->where_clause()->expression()->node_kind() == zetasql::AST_BINARY_EXPRESSION) {
                auto expr = node->where_clause()->expression()->GetAsOrNull<zetasql::ASTBinaryExpression>();
                if (expr && expr->op() == zetasql::ASTBinaryExpression::Op::EQ && !expr->is_not()) {
                    {
                        auto lval = expr->lhs()->GetAsOrNull<zetasql::ASTIntLiteral>();
                        auto rval = expr->rhs()->GetAsOrNull<zetasql::ASTPathExpression>();
                        if (lval && rval && lval->image() == "1") {
                            // TODO(someone):
                            // 1. consider lval->iamge() as '1L'
                            // 2. consider rval as <tb_name>.<row_id>
                            filter_col = rval->last_name()->GetAsStringView();
                        }
                    }
                    if (filter_col.empty()) {
                        auto lval = expr->rhs()->GetAsOrNull<zetasql::ASTIntLiteral>();
                        auto rval = expr->lhs()->GetAsOrNull<zetasql::ASTPathExpression>();
                        if (lval && rval && lval->image() == "1") {
                            // TODO(someone):
                            // 1. consider lval->iamge() as '1L'
                            // 2. consider rval as <tb_name>.<row_id>
                            filter_col = rval->last_name()->GetAsStringView();
                        }
                    }
                }
            }

            // 2. FROM a subquery: SELECT ... t1 LEFT JOIN t2 WINDOW
            const zetasql::ASTPathExpression* join_lhs_key = nullptr;
            const zetasql::ASTPathExpression* join_rhs_key = nullptr;
            if (node->from_clause() == nullptr) {
                break;
            }
            auto sub = node->from_clause()->table_expression()->GetAsOrNull<zetasql::ASTTableSubquery>();
            if (!sub) {
                break;
            }
            auto subquery = sub->subquery();
            if (subquery->with_clause() != nullptr || subquery->order_by() != nullptr ||
                subquery->limit_offset() != nullptr) {
                break;
            }

            auto inner_select = subquery->query_expr()->GetAsOrNull<zetasql::ASTSelect>();
            if (!inner_select) {
                break;
            }
            // select have window
            if (inner_select->window_clause() == nullptr || inner_select->from_clause() == nullptr) {
                break;
            }

            // 3. CHECK FROM CLAUSE: must 't1 LEFT JOIN t2 on t1.key = t2.key'
            if (!inner_select->from_clause()) {
                break;
            }
            auto join = inner_select->from_clause()->table_expression()->GetAsOrNull<zetasql::ASTJoin>();
            if (join == nullptr || join->join_type() != zetasql::ASTJoin::LEFT || join->on_clause() == nullptr) {
                break;
            }
            auto on_expr = join->on_clause()->expression()->GetAsOrNull<zetasql::ASTBinaryExpression>();
            if (on_expr == nullptr || on_expr->is_not() || on_expr->op() != zetasql::ASTBinaryExpression::EQ) {
                break;
            }

            // still might null
            join_lhs_key = on_expr->lhs()->GetAsOrNull<zetasql::ASTPathExpression>();
            join_rhs_key = on_expr->rhs()->GetAsOrNull<zetasql::ASTPathExpression>();
            if (join_lhs_key == nullptr || join_rhs_key == nullptr) {
                break;
            }

            // 3. CHECK row_id is row_number() over w FROM select_list
            bool found = false;
            absl::string_view window_name;
            for (auto col : inner_select->select_list()->columns()) {
                if (col->alias() && col->alias()->GetAsStringView() == filter_col) {
                    auto agg_func = col->expression()->GetAsOrNull<zetasql::ASTAnalyticFunctionCall>();
                    if (!agg_func || !agg_func->function()) {
                        break;
                    }

                    auto w = agg_func->window_spec();
                    if (!w || w->base_window_name() == nullptr) {
                        break;
                    }
                    window_name = w->base_window_name()->GetAsStringView();

                    auto ph = agg_func->function()->function();
                    if (ph->num_names() == 1 &&
                        absl::AsciiStrToLower(ph->first_name()->GetAsStringView()) == "row_number") {
                        opt_out_row_number_col_ = col;
                        found = true;
                        break;
                    }
                }
            }
            if (!found || window_name.empty()) {
                break;
            }

            // 4. CHECK WINDOW CLAUSE
            {
                if (inner_select->window_clause()->windows().size() != 1) {
                    // targeting single window only
                    break;
                }
                auto win = inner_select->window_clause()->windows().front();
                if (win->name()->GetAsStringView() != window_name) {
                    break;
                }
                auto spec = win->window_spec();
                if (spec->window_frame() != nullptr || spec->partition_by() == nullptr || spec->order_by() == nullptr) {
                    // TODO(someone): allow unbounded window frame
                    break;
                }

                // PARTITION BY contains join_lhs_key
                // ORDER BY is join_rhs_key
                bool partition_meet = false;
                for (auto expr : spec->partition_by()->partitioning_expressions()) {
                    auto e = expr->GetAsOrNull<zetasql::ASTPathExpression>();
                    if (e) {
                        if (e->last_name()->GetAsStringView() == join_lhs_key->last_name()->GetAsStringView()) {
                            partition_meet = true;
                        }
                    }
                }

                if (!partition_meet) {
                    break;
                }

                if (spec->order_by()->ordering_expressions().size() != 1) {
                    break;
                }

                if (spec->order_by()->ordering_expressions().front()->ordering_spec() !=
                    zetasql::ASTOrderingExpression::DESC) {
                    break;
                }

                auto e = spec->order_by()
                             ->ordering_expressions()
                             .front()
                             ->expression()
                             ->GetAsOrNull<zetasql::ASTPathExpression>();
                if (!e) {
                    break;
                }

                // rewrite
                {
                    opt_out_window_ = inner_select->window_clause();
                    opt_out_where_ = node->where_clause();
                    opt_join_ = join;
                    opt_in_last_join_order_by_ = e;
                    absl::Cleanup clean = [&]() {
                        opt_out_window_ = nullptr;
                        opt_out_where_ = nullptr;
                        opt_out_row_number_col_ = nullptr;
                        opt_join_ = nullptr;
                    };

                    // inline zetasql::parser::Unparser::visitASTSelect(node, data);
                    {
                        PrintOpenParenIfNeeded(node);
                        println();
                        print("SELECT");
                        if (node->hint() != nullptr) {
                            node->hint()->Accept(this, data);
                        }
                        if (node->anonymization_options() != nullptr) {
                            print("WITH ANONYMIZATION OPTIONS");
                            node->anonymization_options()->Accept(this, data);
                        }
                        if (node->distinct()) {
                            print("DISTINCT");
                        }

                        // Visit all children except hint() and anonymization_options, which we
                        // processed above.  We can't just use visitASTChildren(node, data) because
                        // we need to insert the DISTINCT modifier after the hint and anonymization
                        // nodes and before everything else.
                        for (int i = 0; i < node->num_children(); ++i) {
                            const zetasql::ASTNode* child = node->child(i);
                            if (child != node->hint() && child != node->anonymization_options()) {
                                child->Accept(this, data);
                            }
                        }

                        println();
                        PrintCloseParenIfNeeded(node);
                    }

                    return;
                }
            }

            break;
        }

        zetasql::parser::Unparser::visitASTSelect(node, data);
    }

    void visitASTJoin(const zetasql::ASTJoin* node, void* data) override {
        if (opt_join_ && opt_join_ == node) {
            node->child(0)->Accept(this, data);

            if (node->join_type() == zetasql::ASTJoin::COMMA) {
                print(",");
            } else {
                println();
                if (node->natural()) {
                    print("NATURAL");
                }
                print("LAST");
                print(node->GetSQLForJoinHint());

                print("JOIN");
            }
            println();

            // This will print hints, the rhs, and the ON or USING clause.
            for (int i = 1; i < node->num_children(); i++) {
                node->child(i)->Accept(this, data);
                if (opt_in_last_join_order_by_ && node->child(i)->IsTableExpression()) {
                    print("ORDER BY");
                    opt_in_last_join_order_by_->Accept(this, data);
                }
            }

            return;
        }

        zetasql::parser::Unparser::visitASTJoin(node, data);
    }

    void visitASTSelectList(const zetasql::ASTSelectList* node, void* data) override {
        println();
        {
            for (int i = 0; i < node->num_children(); i++) {
                if (opt_out_row_number_col_ && node->columns(i) == opt_out_row_number_col_) {
                    continue;
                }
                if (i > 0) {
                    println(",");
                }
                node->child(i)->Accept(this, data);
            }
        }
    }

    void visitASTWindowClause(const zetasql::ASTWindowClause* node, void* data) override {
        if (opt_out_window_ && opt_out_window_ == node) {
            return;
        }

        zetasql::parser::Unparser::visitASTWindowClause(node, data);
    }

    void visitASTWhereClause(const zetasql::ASTWhereClause* node, void* data) override {
        if (opt_out_where_ && opt_out_where_ == node) {
            return;
        }
        zetasql::parser::Unparser::visitASTWhereClause(node, data);
    }

 private:
    const zetasql::ASTWindowClause* opt_out_window_ = nullptr;
    const zetasql::ASTWhereClause* opt_out_where_ = nullptr;
    const zetasql::ASTSelectColumn* opt_out_row_number_col_ = nullptr;
    const zetasql::ASTJoin* opt_join_ = nullptr;
    const zetasql::ASTPathExpression* opt_in_last_join_order_by_ = nullptr;
};

// SELECT:
//   WHERE col = 0
//   FROM (subquery):
//     subquery is UNION ALL, or contains left-most query is UNION ALL
//     and UNION ALL is select const ..., 0 as col UNION ALL (select .., 1 as col table)
class RequestQueryRewriteUnparser : public zetasql::parser::Unparser {
 public:
    explicit RequestQueryRewriteUnparser(std::string* unparsed) : zetasql::parser::Unparser(unparsed) {}
    ~RequestQueryRewriteUnparser() override {}
    RequestQueryRewriteUnparser(const RequestQueryRewriteUnparser&) = delete;
    RequestQueryRewriteUnparser& operator=(const RequestQueryRewriteUnparser&) = delete;

    void visitASTSelect(const zetasql::ASTSelect* node, void* data) override {
        while (true) {
            if (outer_most_select_ != nullptr) {
                break;
            }

            outer_most_select_ = node;
            if (node->where_clause() == nullptr) {
                break;
            }
            absl::string_view filter_col;
            const zetasql::ASTExpression* filter_expr;

            // 1. filter condition is 'col = 0'
            if (node->where_clause()->expression()->node_kind() != zetasql::AST_BINARY_EXPRESSION) {
                break;
            }
            auto expr = node->where_clause()->expression()->GetAsOrNull<zetasql::ASTBinaryExpression>();
            if (!expr || expr->op() != zetasql::ASTBinaryExpression::Op::EQ || expr->is_not()) {
                break;
            }
            {
                auto rval = expr->rhs()->GetAsOrNull<zetasql::ASTPathExpression>();
                if (rval) {
                    // TODO(someone):
                    // 2. consider rval as <tb_name>.<row_id>
                    filter_col = rval->last_name()->GetAsStringView();
                    filter_expr = expr->lhs();
                }
            }
            if (filter_col.empty()) {
                auto rval = expr->lhs()->GetAsOrNull<zetasql::ASTPathExpression>();
                if (rval) {
                    // TODO(someone):
                    // 2. consider rval as <tb_name>.<row_id>
                    filter_col = rval->last_name()->GetAsStringView();
                    filter_expr = expr->rhs();
                }
            }
            if (filter_col.empty() || !filter_expr) {
                break;
            }

            if (node->from_clause() == nullptr) {
                break;
            }
            auto sub = node->from_clause()->table_expression()->GetAsOrNull<zetasql::ASTTableSubquery>();
            if (!sub) {
                break;
            }
            auto subquery = sub->subquery();

            findUnionAllForQuery(subquery, filter_col, filter_expr, node->where_clause());

            break;  // fallback normal
        }

        zetasql::parser::Unparser::visitASTSelect(node, data);
    }

    void visitASTSetOperation(const zetasql::ASTSetOperation* node, void* data) override {
        if (node == detected_request_block_) {
            node->inputs().back()->Accept(this, data);
        } else {
            zetasql::parser::Unparser::visitASTSetOperation(node, data);
        }
    }

    void visitASTQueryStatement(const zetasql::ASTQueryStatement* node, void* data) override {
        node->query()->Accept(this, data);
        if (!list_.empty() && !node->config_clause()) {
            constSelectListAsConfigClause(list_, data);
        } else {
            if (node->config_clause() != nullptr) {
                println();
                node->config_clause()->Accept(this, data);
            }
        }
    }

    void visitASTWhereClause(const zetasql::ASTWhereClause* node, void* data) override {
        if (node != filter_clause_) {
            zetasql::parser::Unparser::visitASTWhereClause(node, data);
        }
    }

 private:
    void findUnionAllForQuery(const zetasql::ASTQuery* query, absl::string_view label_name,
                              const zetasql::ASTExpression* filter_expr, const zetasql::ASTWhereClause* filter) {
        if (!query) {
            return;
        }
        auto qe = query->query_expr();
        switch (qe->node_kind()) {
            case zetasql::AST_SET_OPERATION: {
                auto set = qe->GetAsOrNull<zetasql::ASTSetOperation>();
                if (set && set->op_type() == zetasql::ASTSetOperation::UNION && set->distinct() == false &&
                    set->hint() == nullptr && set->inputs().size() == 2) {
                    [[maybe_unused]] bool ret =
                        findUnionAllInput(set->inputs().at(0), set->inputs().at(1), label_name, filter_expr, filter) ||
                        findUnionAllInput(set->inputs().at(0), set->inputs().at(1), label_name, filter_expr, filter);
                    if (ret) {
                        detected_request_block_ = set;
                    }
                }
                break;
            }
            case zetasql::AST_QUERY: {
                findUnionAllForQuery(qe->GetAsOrNull<zetasql::ASTQuery>(), label_name, filter_expr, filter);
                break;
            }
            case zetasql::AST_SELECT: {
                auto select = qe->GetAsOrNull<zetasql::ASTSelect>();
                if (select->from_clause() &&
                    select->from_clause()->table_expression()->node_kind() == zetasql::AST_TABLE_SUBQUERY) {
                    auto sub = select->from_clause()->table_expression()->GetAsOrNull<zetasql::ASTTableSubquery>();
                    if (sub && sub->subquery()) {
                        findUnionAllForQuery(sub->subquery(), label_name, filter_expr, filter);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    void constSelectListAsConfigClause(const std::vector<const zetasql::ASTExpression*>& selects, void* data) {
        print("CONFIG (execute_mode = 'request', values = (");
        for (int i = 0; i < selects.size(); ++i) {
            selects.at(i)->Accept(this, data);
            if (i + 1 < selects.size()) {
                print(",");
            }
        }
        print(") )");
    }

    bool findUnionAllInput(const zetasql::ASTQueryExpression* lhs, const zetasql::ASTQueryExpression* rhs,
                           absl::string_view label_name, const zetasql::ASTExpression* filter_expr,
                           const zetasql::ASTWhereClause* filter) {
        // lhs is select const + label_name of value 0
        auto lselect = lhs->GetAsOrNull<zetasql::ASTSelect>();
        if (!lselect || lselect->num_children() > 1) {
            // only select_list required, otherwise size > 1
            return false;
        }

        bool has_label_col_0 = false;
        const zetasql::ASTExpression* label_expr_0 = nullptr;
        std::vector<const zetasql::ASTExpression*> vec;
        for (auto col : lselect->select_list()->columns()) {
            if (col->alias() && col->alias()->GetAsStringView() == label_name) {
                has_label_col_0 = true;
                label_expr_0 = col->expression();
            } else {
                vec.push_back(col->expression());
            }
        }

        // rhs is simple selects from table + label_name of value 1
        auto rselect = rhs->GetAsOrNull<zetasql::ASTSelect>();
        if (!rselect || rselect->num_children() > 2 || !rselect->from_clause()) {
            // only select_list + from_clause required
            return false;
        }
        if (rselect->from_clause()->table_expression()->node_kind() != zetasql::AST_TABLE_PATH_EXPRESSION) {
            return false;
        }

        bool has_label_col_1 = false;
        const zetasql::ASTExpression* label_expr_1 = nullptr;
        for (auto col : rselect->select_list()->columns()) {
            if (col->alias() && col->alias()->GetAsStringView() == label_name) {
                has_label_col_1 = true;
                label_expr_1 = col->expression();
            }
        }

        LOG(INFO) << "label expr 0: " << label_expr_0->SingleNodeDebugString();
        LOG(INFO) << "label expr 1: " << label_expr_1->SingleNodeDebugString();
        LOG(INFO) << "filter expr: " << filter_expr->SingleNodeDebugString();

        if (has_label_col_0 && has_label_col_1 &&
            label_expr_0->SingleNodeDebugString() != label_expr_1->SingleNodeDebugString() &&
            label_expr_0->SingleNodeDebugString() == filter_expr->SingleNodeDebugString()) {
            list_ = vec;
            filter_clause_ = filter;
            return true;
        }

        return false;
    }

 private:
    const zetasql::ASTSelect* outer_most_select_ = nullptr;
    // detected request query block, set by when visiting outer most query
    const zetasql::ASTSetOperation* detected_request_block_ = nullptr;
    const zetasql::ASTWhereClause* filter_clause_;

    std::vector<const zetasql::ASTExpression*> list_;
};

absl::StatusOr<std::string> Rewrite(absl::string_view query) {
    auto str = std::string(query);
    {
        std::unique_ptr<zetasql::ParserOutput> ast;
        auto s = hybridse::plan::ParseStatement(str, &ast);
        if (!s.ok()) {
            return s;
        }

        if (ast->statement() && ast->statement()->node_kind() == zetasql::AST_QUERY_STATEMENT) {
            std::string unparsed_;
            LastJoinRewriteUnparser unparser(&unparsed_);
            ast->statement()->Accept(&unparser, nullptr);
            unparser.FlushLine();
            str = unparsed_;
        }
    }
    {
        std::unique_ptr<zetasql::ParserOutput> ast;
        auto s = hybridse::plan::ParseStatement(str, &ast);
        if (!s.ok()) {
            return s;
        }

        if (ast->statement() && ast->statement()->node_kind() == zetasql::AST_QUERY_STATEMENT) {
            std::string unparsed_;
            RequestQueryRewriteUnparser unparser(&unparsed_);
            ast->statement()->Accept(&unparser, nullptr);
            unparser.FlushLine();
            str = unparsed_;
        }
    }

    return str;
}

}  // namespace rewriter
}  // namespace hybridse
