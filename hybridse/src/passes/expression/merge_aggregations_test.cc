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

#include "passes/expression/merge_aggregations.h"
#include "passes/expression/expr_pass_test.h"
#include "passes/expression/simplify.h"
#include "passes/resolve_fn_and_attrs.h"
#include "udf/literal_traits.h"

namespace hybridse {
namespace passes {

class MergeAggregationsTest : public ExprPassTestBase {};

TEST_F(MergeAggregationsTest, Test) {
    auto schema = udf::MakeLiteralSchema<int32_t, float, double, int64_t>();
    schemas_ctx_.BuildTrivial({&schema});

    std::vector<std::string> non_merge_cases = {"0",
                                                "col_0",
                                                "col_1 * col_2",
                                                "sum(col_0 + sum(col_1)) over w1",
                                                "lag(col_0, 1) over w1",
                                                "sum(col_0 + lag(col_0, 1)) over w1"};

    std::vector<std::string> merge_cases = {"sum(col_0 + 1) over w1", "sum(col_1 + 1) over w1",
                                            "distinct_count(col_2) over w1",
                                            "topn_frequency(col_3, 3) over w1 "};

    std::string sql = "select \n";
    for (size_t i = 0; i < non_merge_cases.size(); ++i) {
        sql.append(non_merge_cases[i]);
        sql.append(",\n");
    }
    for (size_t i = 0; i < merge_cases.size(); ++i) {
        sql.append(merge_cases[i]);
        if (i < merge_cases.size() - 1) {
            sql.append(",\n");
        }
    }
    sql.append(
        "from t1 window w1 as (partition by col_1 order by col_3 rows between "
        "3 preceding and current row);");

    node::LambdaNode* function_let = nullptr;
    InitFunctionLet(sql, &function_let);
    node::ExprNode* origin = function_let->body()->DeepCopy(node_manager());

    MergeAggregations pass;
    node::ExprNode* output = nullptr;
    Status status = ApplyPass(&pass, function_let, &output);
    ASSERT_TRUE(status.isOK()) << status;

    ResolveFnAndAttrs resolver(&ctx_);
    status = resolver.Apply(&ctx_, output, &output);
    ASSERT_TRUE(status.isOK()) << status;

    ExprSimplifier simplifier;
    status = simplifier.Apply(&ctx_, output, &output);
    ASSERT_TRUE(status.isOK()) << status;

    auto is_opt = [](const node::ExprNode* expr) {
        return expr->GetExprType() == node::kExprGetField &&
               expr->GetChild(0)->GetExprType() == node::kExprCall &&
               dynamic_cast<node::CallExprNode*>(expr->GetChild(0))
                       ->GetFnDef()
                       ->GetName()
                       .rfind("merged_window_agg") == 0;
    };

    ASSERT_EQ(merge_cases.size() + non_merge_cases.size(), output->GetChildNum());

    for (size_t i = 0; i < non_merge_cases.size(); ++i) {
        auto expr = output->GetChild(i);
        ASSERT_TRUE(!is_opt(expr))
            << "Illegal optimized at " << i << ": " << non_merge_cases[i];
    }
    node::ExprNode* merged = nullptr;
    for (size_t i = 0; i < merge_cases.size(); ++i) {
        size_t offset = non_merge_cases.size();
        auto expr = output->GetChild(offset + i);
        ASSERT_TRUE(is_opt(expr))
            << "Not optimized at " << i << ": " << merge_cases[i];
        auto output_index =
            dynamic_cast<node::GetFieldExpr*>(expr)->GetColumnID();
        LOG(INFO) << "Optimize " << merge_cases[i] << " -> [" << output_index
                  << "]"
                  << "\nBefore optimize: \n"
                  << origin->GetChild(offset + i)->GetTreeString();
        if (i == 0) {
            merged = expr->GetChild(0);
        }
    }
    LOG(INFO) << "Merged aggregation:\n" << merged->GetTreeString();
}

}  // namespace passes
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
