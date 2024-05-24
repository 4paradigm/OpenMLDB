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
#include <vector>

#include "absl/strings/ascii.h"
#include "gtest/gtest.h"
#include "plan/plan_api.h"
#include "zetasql/parser/parser.h"

namespace hybridse {
namespace rewriter {

struct Case {
    absl::string_view in;
    absl::string_view out;
};

class ASTRewriterTest : public ::testing::TestWithParam<Case> {};

std::vector<Case> strip_cases = {
    {R"s(
  SELECT id, val, k, ts, idr, valr FROM (
    SELECT t1.*, t2.id as idr, t2.val as valr, row_number() over w as any_id
    FROM t1 LEFT JOIN t2 ON t1.k = t2.k
    WINDOW w as (PARTITION BY t1.id,t1.k order by t2.ts desc)
    ) t WHERE any_id = 1)s",
     R"(
SELECT
  id,
  val,
  k,
  ts,
  idr,
  valr
FROM
  t1
  LAST JOIN
  t2
  ORDER BY t2.ts
  ON t1.k = t2.k)"},
    {R"(
SELECT id, k, agg
FROM (
  SELECT id, k, label, count(val) over w as agg
  FROM (
    SELECT  6 as id, "xxx" as val, 10 as k, 9000 as ts, 0 as label
    UNION ALL
    SELECT *, 1 as label FROM t1
  ) t
  WINDOW w as (PARTITION BY k ORDER BY ts rows between unbounded preceding and current row)
) t WHERE label = 0)",
     R"(
SELECT
  id,
  k,
  agg
FROM
  (
    SELECT
      id,
      k,
      label,
      count(val) OVER (w) AS agg
    FROM
      (
        SELECT
          *,
          1 AS label
        FROM
          t1
      ) AS t
    WINDOW w AS (PARTITION BY k
      ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ) AS t
CONFIG (execute_mode = 'request', values = (6, "xxx", 10, 9000) )
)"},
    // simplist request query
    {R"s(
  SELECT id, k
  FROM (
    SELECT  6 as id, "xxx" as val, 10 as k, 9000 as ts, 0 as label
    UNION ALL
    SELECT *, 1 as label FROM t1
  ) t WHERE label = 0)s",
     R"s(SELECT
  id,
  k
FROM
  (
    SELECT
      *,
      1 AS label
    FROM
      t1
  ) AS t
CONFIG (execute_mode = 'request', values = (6, "xxx", 10, 9000) )
)s"},

    // TPC-C case
    {R"(SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT
  FROM (
    SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT, label FROM (
      SELECT 1 AS C_ID, 1 AS C_D_ID, 1 AS C_W_ID, "John" AS C_FIRST, "M" AS C_MIDDLE, "Smith" AS C_LAST, "123 Main St" AS C_STREET_1, "Apt 101" AS C_STREET_2, "Springfield" AS C_CITY, "IL" AS C_STATE, 12345 AS C_ZIP, "555-123-4567" AS C_PHONE, timestamp("2024-01-01 00:00:00") AS C_SINCE, "BC" AS C_CREDIT, 10000.0 AS C_CREDIT_LIM, 0.5 AS C_DISCOUNT, 5000.0 AS C_BALANCE, 0.0 AS C_YTD_PAYMENT, 0 AS C_PAYMENT_CNT, 0 AS C_DELIVERY_CNT, "Additional customer data..." AS C_DATA, 0 as label
      UNION ALL
      SELECT *, 1 as label FROM CUSTOMER
    ) t
  ) t WHERE label = 0)",
     R"s(
SELECT
  C_ID,
  C_CITY,
  C_STATE,
  C_CREDIT,
  C_CREDIT_LIM,
  C_BALANCE,
  C_PAYMENT_CNT,
  C_DELIVERY_CNT
FROM
  (
    SELECT
      C_ID,
      C_CITY,
      C_STATE,
      C_CREDIT,
      C_CREDIT_LIM,
      C_BALANCE,
      C_PAYMENT_CNT,
      C_DELIVERY_CNT,
      label
    FROM
      (
        SELECT
          *,
          1 AS label
        FROM
          CUSTOMER
      ) AS t
  ) AS t
CONFIG (execute_mode = 'request', values = (1, 1, 1, "John", "M", "Smith", "123 Main St", "Apt 101",
"Springfield", "IL", 12345, "555-123-4567", timestamp("2024-01-01 00:00:00"), "BC", 10000.0, 0.5, 5000.0,
0.0, 0, 0, "Additional customer data...") )
  )s"},

    {R"(
SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT
  FROM (
    SELECT C_ID, C_CITY, C_STATE, C_CREDIT, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_DELIVERY_CNT, label FROM (
      SELECT 1 AS C_ID, 1 AS C_D_ID, 1 AS C_W_ID, "John" AS C_FIRST, "M" AS C_MIDDLE, "Smith" AS C_LAST, "123 Main St" AS C_STREET_1, "Apt 101" AS C_STREET_2, "Springfield" AS C_CITY, "IL" AS C_STATE, 12345 AS C_ZIP, "555-123-4567" AS C_PHONE, timestamp("2024-01-01 00:00:00") AS C_SINCE, "BC" AS C_CREDIT, 10000.0 AS C_CREDIT_LIM, 0.5 AS C_DISCOUNT, 9000.0 AS C_BALANCE, 0.0 AS C_YTD_PAYMENT, 0 AS C_PAYMENT_CNT, 0 AS C_DELIVERY_CNT, "Additional customer data..." AS C_DATA, 0 as label
      UNION ALL
      SELECT *, 1 as label FROM CUSTOMER
    ) t
  ) t WHERE label = 0)",
     R"(
SELECT
  C_ID,
  C_CITY,
  C_STATE,
  C_CREDIT,
  C_CREDIT_LIM,
  C_BALANCE,
  C_PAYMENT_CNT,
  C_DELIVERY_CNT
FROM
  (
    SELECT
      C_ID,
      C_CITY,
      C_STATE,
      C_CREDIT,
      C_CREDIT_LIM,
      C_BALANCE,
      C_PAYMENT_CNT,
      C_DELIVERY_CNT,
      label
    FROM
      (
        SELECT
          *,
          1 AS label
        FROM
          CUSTOMER
      ) AS t
  ) AS t
CONFIG (execute_mode = 'request', values = (1, 1, 1, "John", "M", "Smith", "123 Main St", "Apt 101",
"Springfield", "IL", 12345, "555-123-4567", timestamp("2024-01-01 00:00:00"), "BC", 10000.0, 0.5, 9000.0,
0.0, 0, 0, "Additional customer data...") )
)"},
};

INSTANTIATE_TEST_SUITE_P(Rules, ASTRewriterTest, ::testing::ValuesIn(strip_cases));

TEST_P(ASTRewriterTest, Correctness) {
    auto& c = GetParam();

    auto s = hybridse::rewriter::Rewrite(c.in);
    ASSERT_TRUE(s.ok()) << s.status();

    ASSERT_EQ(absl::StripAsciiWhitespace(c.out), absl::StripAsciiWhitespace(s.value()));

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
