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

#include "plan/plan_api.h"
#include "zetasql/parser/parse_tree_manual.h"
#include "zetasql/parser/parser.h"
#include "zetasql/parser/unparser.h"

namespace hybridse {
namespace rewriter {

// unparser that make some rewrites so outputed SQL is
// compatible with ANSI SQL as much as can
class ANSISQLRewriteUnparser : public zetasql::parser::Unparser {
 public:
    explicit ANSISQLRewriteUnparser(std::string* unparsed) : zetasql::parser::Unparser(unparsed) {}
    ~ANSISQLRewriteUnparser() override {}
    ANSISQLRewriteUnparser(const ANSISQLRewriteUnparser&) = delete;
    ANSISQLRewriteUnparser& operator=(const ANSISQLRewriteUnparser&) = delete;

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

            auto select = subquery->query_expr()->GetAsOrNull<zetasql::ASTSelect>();
            // select have window
            if (select->window_clause() == nullptr || select->from_clause() == nullptr) {
                break;
            }

            // 3. CHECK FROM CLAUSE: must 't1 LEFT JOIN t2 on t1.key = t2.key'
            auto join = select->from_clause()->table_expression()->GetAsOrNull<zetasql::ASTJoin>();
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
            for (auto col : select->select_list()->columns()) {
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
                        found = true;
                    }
                }
            }
            if (!found || window_name.empty()) {
                break;
            }

            // 4. CHECK WINDOW CLAUSE
            {
                if (select->window_clause()->windows().size() != 1) {
                    // targeting single window only
                    break;
                }
                auto win = select->window_clause()->windows().front();
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
                        if (child == node->from_clause()) {
                            // this from subquery will simplified to join
                            println();
                            print("FROM");
                            visitASTJoinRewrited(join, e, data);
                        } else if (child != node->hint() && child != node->anonymization_options() &&
                                   child != node->where_clause()) {
                            child->Accept(this, data);
                        }
                    }

                    println();
                    PrintCloseParenIfNeeded(node);
                    return;
                }
            }

            break;
        }

        zetasql::parser::Unparser::visitASTSelect(node, data);
    }

    void visitASTJoinRewrited(const zetasql::ASTJoin* node, const zetasql::ASTPathExpression* order, void* data) {
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
            if (node->child(i) == node->rhs() && order) {
                // optional order by after rhs
                print("ORDER BY");
                order->Accept(this, data);
            }
        }
    }
};

absl::StatusOr<std::string> Rewrite(absl::string_view query) {
    std::unique_ptr<zetasql::ParserOutput> ast;
    auto s = hybridse::plan::ParseStatement(query, &ast);
    if (!s.ok()) {
        return s;
    }

    if (ast->statement() && ast->statement()->node_kind() == zetasql::AST_QUERY_STATEMENT) {
        std::string unparsed_;
        ANSISQLRewriteUnparser unparser(&unparsed_);
        ast->statement()->Accept(&unparser, nullptr);
        unparser.FlushLine();
        return unparsed_;
    }

    return std::string(query);
}

}  // namespace rewriter
}  // namespace hybridse
