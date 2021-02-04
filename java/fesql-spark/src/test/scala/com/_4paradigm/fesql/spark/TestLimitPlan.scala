package com._4paradigm.fesql.spark

import com._4paradigm.fesql.spark.api.FesqlSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestLimitPlan extends SparkTestSuite {

  test("GroupBy and limit test") {
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

    val res = planner.plan("select id, max(time2), min(amt) from t1 group by id limit 2;", Map("t1" -> t1))
    val output = res.getDf()
    assert(output.count()==2);

    val res2 = planner.plan("select id, max(time2), min(amt) from t1 group by id limit 1;", Map("t1" -> t1))
    val output2 = res2.getDf()
    assert(output2.count()==1);
  }

  test("Project and limit test") {
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

    val res = planner.plan("select id, time2 + 10 from t1 limit 2;", Map("t1" -> t1))
    val output = res.getDf()
    assert(output.count()==2);

    val res2 = planner.plan("select id, time2 + 10 from t1 limit 1;", Map("t1" -> t1))
    val output2 = res2.getDf()
    assert(output2.count()==1);
  }

  test("Simple project and limit test") {
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

    val res = planner.plan("select id, time2 from t1 limit 2;", Map("t1" -> t1))
    val output = res.getDf()
    assert(output.count()==2);

    val res2 = planner.plan("select id, time2 from t1 limit 1;", Map("t1" -> t1))
    val output2 = res2.getDf()
    assert(output2.count()==1);
  }

}
