package com._4paradigm.fesql.spark.sql

class QuerySuites extends SQLBaseSuite {

  testCases("cases/query/fz_sql.yaml")
  testCases("cases/query/group_query.yaml")
  testCases("cases/query/last_join_query.yaml")
  testCases("cases/query/last_join_window_query.yaml")
  testCases("cases/query/operator_query.yaml")
  testCases("cases/query/simple_query.yaml")
  testCases("cases/query/udaf_query.yaml")
  testCases("cases/query/udf_query.yaml")
  testCases("cases/query/window_query.yaml")
  testCases("cases/query/window_with_union_query.yaml")

  testCases("cases/integration/v1/expression/test_arithmetic.yaml")
  testCases("cases/integration/v1/expression/test_compare.yaml")
  testCases("cases/integration/v1/expression/test_condition.yaml")
  testCases("cases/integration/v1/expression/test_logic.yaml")
  testCases("cases/integration/v1/expression/test_type.yaml")

  testCases("cases/integration/v1/test_feature_zero_function.yaml")
  testCases("cases/integration/v1/test_fz_sql.yaml")
  testCases("cases/integration/v1/test_index_optimized.yaml")
  testCases("cases/integration/v1/test_last_join.yaml")

  testCases("cases/integration/v1/select/test_select_sample.yaml")
  testCases("cases/integration/v1/select/test_sub_select.yaml")
  // testCases("cases/integration/v1/select/test_where.yaml")

  testCases("cases/integration/v1/function/test_udaf_function.yaml")
  testCases("cases/integration/v1/function/test_udf_function.yaml")
  testCases("cases/integration/v1/function/test_calculate.yaml")
  testCases("cases/integration/v1/function/test_date.yaml")
  testCases("cases/integration/v1/function/test_string.yaml")

  testCases("cases/integration/v1/test_window_exclude_current_time.yaml")
  testCases("cases/integration/v1/window/test_window_row.yaml")
  testCases("cases/integration/v1/window/test_window_row_range.yaml")
  testCases("cases/integration/v1/window/test_window_union.yaml")

  testCases("cases/integration/cluster/test_window_row.yaml")
  testCases("cases/integration/cluster/test_window_row_range.yaml")
  testCases("cases/integration/cluster/window_and_lastjoin.yaml")

  testCases("cases/integration/error/error_window.yaml")

  testCases("cases/integration/spark/test_fqz_studio.yaml")
  testCases("cases/integration/spark/test_ads.yaml")
  testCases("cases/integration/spark/test_news.yaml")
  testCases("cases/integration/spark/test_jd.yaml")
  testCases("cases/integration/spark/test_credit.yaml")

  // TODO: fix if java cases support not inputs
  // testCases("cases/query/const_query.yaml")

  // TODO: fix at(0)
  // testCases("cases/query/udaf_query.yaml")

}
