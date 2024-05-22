/**
 * Copyright 2024 OpenMLDB authors
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
#include <string>

#include "gtest/gtest.h"
#include "plan/plan_api.h"
#include "zetasql/parser/parser.h"

namespace hybridse {
namespace rewriter {

class ASTRewriterTest : public ::testing::Test {};

TEST_F(ASTRewriterTest, LastJoin) {
    std::string str = R"(
SELECT id, val, k, ts, idr, valr FROM (
  SELECT t1.*, t2.id as idr, t2.val as valr, row_number() over w as any_id
  FROM t1 LEFT JOIN t2 ON t1.k = t2.k
  WINDOW w as (PARTITION BY t1.id,t1.k order by t2.ts desc)
) t WHERE any_id = 1)";

    auto s = hybridse::rewriter::Rewrite(str);
    ASSERT_TRUE(s.ok()) << s.status();

    ASSERT_EQ(R"(SELECT
  id,
  val,
  k,
  ts,
  idr,
  valr
FROM t1
LAST JOIN
t2 ORDER BY t2.ts
ON t1.k = t2.k
)",
              s.value());

    std::unique_ptr<zetasql::ParserOutput> out;
    auto ss = ::hybridse::plan::ParseStatement(s.value(), &out);
    ASSERT_TRUE(ss.ok()) << ss;
}

}  // namespace rewriter
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
