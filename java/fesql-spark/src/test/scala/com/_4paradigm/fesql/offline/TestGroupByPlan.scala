package com._4paradigm.fesql.offline

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestGroupByPlan extends SparkTestSuite {


  test("GroupBy test") {
    val sess = getSparkSession

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", LongType),
      StructField("amt", DoubleType)
    ))

    val t1 = sess.createDataFrame(Seq(
      (0, 1L, 1.0),
      (0, 2L, 2.0),
      (1, 3L, 3.0),
      (2, 10L, 4.0)
    ).map(Row.fromTuple(_)).asJava, schema)

    val planner = new SparkPlanner(sess)
    val res = planner.plan("select id, max(amt) from t1 group by id;", Map("t1" -> t1))
    //val res = planner.plan("select id from t1;", Map("t1" -> t1))

    val output = res.getDf(sess)
    output.show()
  }

}
