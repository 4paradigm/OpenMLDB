package com._4paradigm.fesql.offline

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


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

  test("Window plan smoke test") {
    val sess = getSparkSession

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", LongType),
      StructField("amt", DoubleType)
    ))

    val data = Seq(
      (0, 1L, 1.0),
      (0, 2L, 2.0),
      (0, 3L, 3.0),
      (0, 4L, 4.0),
      (0, 5L, 5.0),
      (0, 6L, 6.0),
      (1, 4L, 1.0),
      (1, 3L, 2.0),
      (1, 2L, 3.0),
      (1, 1L, 4.0),
      (2, 10L, 1.0),
      (2, 14L, 2.0),
      (2, 11L, 3.0)
    )

    val table = sess.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)

    val sql ="""
       | SELECT id, `time`, amt, sum(amt) OVER w AS w_amt_sum FROM t
       | WINDOW w AS (
       |    PARTITION BY id
       |    ORDER BY `time`
       |    ROWS BETWEEN 3 PRECEDING AND 0 FOLLOWING);"
     """.stripMargin

    val config =  Map(
      "fesql.group.partitions" -> 1
    )

    val planner = new SparkPlanner(sess, config)
    val res = planner.plan(sql, Map("t" -> table))
    val output = res.getDf(sess)
    output.show()
  }
}
