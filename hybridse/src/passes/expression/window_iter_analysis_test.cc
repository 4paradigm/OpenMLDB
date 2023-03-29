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

#include "passes/expression/window_iter_analysis.h"
#include <tuple>
#include "passes/expression/expr_pass_test.h"
#include "udf/literal_traits.h"

namespace hybridse {
namespace passes {

class WindowIterAnalysisTest : public ExprPassTestBase {};

TEST_F(WindowIterAnalysisTest, Test) {
    auto schema = udf::MakeLiteralSchema<int32_t, float, double>();
    schemas_ctx_.BuildTrivial({&schema});

    std::vector<std::tuple<std::string, size_t>> cases = {
        {"0", 0},
        {"col_0", 0},
        {"col_1 * col_2", 0},
        {"sum(col_0 + 1) over w1", 1},
        {"sum(col_0 + sum(col_1)) over w1", 2},
        {"sum(col_0 + sum(col_1 + sum(col_2))) over w1", 3},
        {"lag(col_0, 1) over w1", 1},
        {"count(window_split(cast(col_0 as string), \",\")) over w1 ", 1},
    };

    std::string sql = "select \n";
    for (size_t i = 0; i < cases.size(); ++i) {
        sql.append(std::get<0>(cases[i]));
        if (i < cases.size() - 1) {
            sql.append(",\n");
        }
    }
    sql.append(
        "from t1 window w1 as (partition by col_1 order by col_3 rows between "
        "3 preceding and current row);");

    node::LambdaNode* function_let = nullptr;
    InitFunctionLet(sql, &function_let);

    passes::WindowIterAnalysis window_dep_analyzer(&ctx_);

    auto row_arg = function_let->GetArg(0);
    auto window_arg = function_let->GetArg(1);
    Status status = window_dep_analyzer.VisitFunctionLet(row_arg, window_arg,
                                                         function_let->body());
    ASSERT_TRUE(status.isOK()) << status.str();

    auto expr_list = function_let->body();
    for (size_t i = 0; i < expr_list->GetChildNum(); ++i) {
        auto expr = expr_list->GetChild(i);
        WindowIterRank rank;
        window_dep_analyzer.GetRank(expr, &rank);
        LOG(INFO) << std::get<0>(cases[i]) << ": " << rank.rank;
        ASSERT_EQ(std::get<1>(cases[i]), rank.rank);
    }
}

}  // namespace passes
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
