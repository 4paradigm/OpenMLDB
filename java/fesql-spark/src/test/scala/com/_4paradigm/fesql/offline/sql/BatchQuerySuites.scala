package com._4paradigm.fesql.offline.sql

class BatchQuerySuites extends SQLBaseSuite {

  // TODO: group
  // testCases("cases/query/group_query.yaml")

  testCases("cases/query/simple_query.yaml")
  testCases("cases/query/window_query.yaml")
  testCases("cases/query/last_join_query.yaml")
  testCases("cases/query/last_join_window_query.yaml")
}
