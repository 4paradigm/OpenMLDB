package com._4paradigm.fesql.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._


class TestGroupByPlan extends SparkTestSuite {

  test("GroupBy test") {
    val sess = getSparkSession

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time2", LongType),
      StructField("amt", DoubleType)
    ))

    val t1 = sess.createDataFrame(Seq(
      (0, 1L, 1.0),
      (3, 3L, 3.0),
      (2, 10L, 4.0),
      (0, 2L, 2.0),
      (2, 13L, 3.0),
    ).map(Row.fromTuple(_)).asJava, schema)

    val planner = new SparkPlanner(sess)
    val res = planner.plan("select id, sum(id), sum(time2), min(amt) from t1 group by id;", Map("t1" -> t1))

    val output = res.getDf()
    output.show()
  }

}
