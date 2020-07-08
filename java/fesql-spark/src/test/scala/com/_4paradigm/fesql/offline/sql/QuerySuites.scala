package com._4paradigm.fesql.offline.sql

class QuerySuites extends SQLBaseSuite {

  testCases("cases/query/simple_query.yaml")
  testCases("cases/query/window_query.yaml")
  testCases("cases/query/last_join_query.yaml")
  testCases("cases/query/last_join_window_query.yaml")
  testCases("cases/query/window_with_union_query.yaml")
  testCases("cases/integration/v1/test_window_row.yaml")

  // TODO: fix at(0)
  // testCases("cases/query/udaf_query.yaml")

}
