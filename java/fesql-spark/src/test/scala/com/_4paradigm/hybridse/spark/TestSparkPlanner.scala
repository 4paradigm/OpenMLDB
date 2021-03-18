/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.hybridse.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestSparkPlanner extends SparkTestSuite {

  test("Project plan smoke test") {
    val sess = getSparkSession

    val table = sess.createDataFrame(Seq(
      (0.toShort, 0, 0L, 0.0f, 0.0, "0"),
      (1.toShort, 1, 1L, 1.0f, 1.0, "0")
    ))

    val planner = new SparkPlanner(sess)
    val res = planner.plan("select *, _1 + 1, inc(_1) from t;", Map("t" -> table))

    val output = res.getDf()
    output.show()
  }

  test("Project plan with simple project test") {
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
    val res = planner.plan("select id as new_id, 0.0 as col2 from t1;", Map("t1" -> t1))

    val output = res.getDf()
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

    val config = new FeSQLConfig
    config.groupPartitions = 1

    val planner = new SparkPlanner(sess, config)
    val res = planner.plan(sql, Map("t" -> table))
    val output = res.getDf()
    output.show()
  }


  test("Join plan smoke test") {
    val sess = getSparkSession

    val schemaLeft = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", LongType),
      StructField("amt", DoubleType)
    ))
    val schemaRight = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", LongType),
      StructField("str", StringType)
    ))

    val left = sess.createDataFrame(Seq(
      (0, 1L, 1.0),
      (0, 2L, 2.0),
      (1, 3L, 3.0),
      (2, 10L, 4.0)
    ).map(Row.fromTuple(_)).asJava, schemaLeft)

    val right = sess.createDataFrame(Seq(
      (0, 1L, "x"),
      (0, 2L, "y"),
      (1, 2L, "z")
    ).map(Row.fromTuple(_)).asJava, schemaRight)

    val sql = "SELECT * FROM t1 left join t2 on t1.id = t2.id and t1.`time` <= t2.`time`;"

    val planner = new SparkPlanner(sess)
    val res = planner.plan(sql, Map("t1" -> left, "t2" -> right))
    val output = res.getDf()
    output.show()
  }
}
