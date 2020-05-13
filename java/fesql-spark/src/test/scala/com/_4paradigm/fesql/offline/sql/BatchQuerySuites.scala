package com._4paradigm.fesql.offline.sql

class BatchQuerySuites extends SQLBaseSuite {

  // TODO: group
  // testCases("cases/batch_query/group_query.yaml")

  testCases("cases/batch_query/simple_query.yaml")
  testCases("cases/batch_query/window_query.yaml")
  testCases("cases/batch_query/last_join_query.yaml")
  testCases("cases/batch_query/last_join_window_query.yaml")
  testCases("cases/batch_query/window_with_union_query.yaml")
}
