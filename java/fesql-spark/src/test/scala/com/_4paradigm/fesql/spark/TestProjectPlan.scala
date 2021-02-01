package com._4paradigm.fesql.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestProjectPlan extends SparkTestSuite {

  test("GroupBy and limit test") {
    val sess = getSparkSession

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time2", IntegerType)
    ))

    val t1 = sess.createDataFrame(Seq(
      (100, 1),
      (3, 3),
      (2, 10),
      (0, 2),
      (2, 13),
    ).map(Row.fromTuple(_)).asJava, schema)

    val planner = new SparkPlanner(sess)

    val res = planner.plan("select id + 1, id + 2 from t1;", Map("t1" -> t1))
    val output = res.getDf()
    output.show()

  }


}
