/*
 * java/fesql-spark/src/test/scala/com/_4paradigm/fesql/spark/TestDate.scala
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

package com._4paradigm.fesql.spark

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestDate extends SparkTestSuite {

  test("Test date before GMT 0") {
    val sess = getSparkSession

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", TimestampType),
      StructField("amt", DoubleType)
    ))

    val data = Seq(
      (0, Timestamp.valueOf("0001-01-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("1899-04-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("1900-01-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("1969-01-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("2000-08-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("2019-09-11 0:0:0"), 1.0)
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
    assert(output.count == 2)
  }
}
