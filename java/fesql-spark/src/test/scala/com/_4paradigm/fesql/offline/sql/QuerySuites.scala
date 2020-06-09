package com._4paradigm.fesql.offline.sql

class QuerySuites extends SQLBaseSuite {

  testCases("cases/query/simple_query.yaml")
  testCases("cases/query/window_query.yaml")
  testCases("cases/query/last_join_query.yaml")
  testCases("cases/query/last_join_window_query.yaml")

  // TODO: fix simple project with constant value
  //testCases("cases/query/window_with_union_query.yaml")

  // TODO: fix at(0)
  // testCases("cases/query/udaf_query.yaml")

}
