package com._4paradigm.fesql.offline


class TestSparkPlanner extends SparkTestSuite {

  test("Spark planner smoke test") {
    val sess = getSparkSession

    val table = sess.createDataFrame(Seq(
      (0.toShort, 0, 0L, 0.0f, 0.0, "0"),
      (1.toShort, 1, 1L, 1.0f, 1.0, "0")
    ))

    val planner = new SparkPlanner(sess)
    val res = planner.plan("select *, _1 + 1 from t;", Map("t" -> table))

    val output = res.getDf(sess)
    output.show()
  }
}
