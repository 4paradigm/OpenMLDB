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

package com._4paradigm.openmldb.batch.end2end

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

class TestWindowSkewOptAndParallelization extends SparkTestSuite {

  test("Test end2end window skew optimization and window parallelization with union") {

    getSparkSession.conf.set("spark.openmldb.window.parallelization", true)
    getSparkSession.conf.set("spark.openmldb.window.skew.opt", true)
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val sqlText =
      """
        | SELECT col1, col2, sum(col3) OVER w1 as w1_col3_sum
        | FROM t1
        | WINDOW w1 AS (
        |     UNION (select c1 as col1, c2 as col2, c3 as col3, "NA" as col4 from tb)
        |     PARTITION BY t1.col1
        |     ORDER BY t1.col2
        |     ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW
        |     )
        | limit 10;
        |""".stripMargin

    val data1 = Seq(
      Row(5, 2, 2, "a"),
      Row(5, 4, 4, "b"),
      Row(5, 6, 6, "c"))
    val schema1 = StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", IntegerType),
      StructField("col3", IntegerType),
      StructField("col4", StringType)))

    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema1)

    sess.registerTable("t1", df1)

    val data2 = Seq(
      Row(5, 1, 1, "e"),
      Row(5, 3, 3, "f"),
      Row(5, 5, 5, "g"))
    val schema2 = StructType(List(
      StructField("c1", IntegerType),
      StructField("c2", IntegerType),
      StructField("c3", IntegerType),
      StructField("c4", StringType)))

    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)

    sess.registerTable("tb", df2)

    val outputDf = sess.sql(sqlText)

    val compareData = Seq(
      Row(5, 2, 3),
      Row(5, 4, 10),
      Row(5, 6, 18))

    val compareSchema = StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", IntegerType),
      StructField("w1_col3_sum", IntegerType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), compareDf, false))
  }

  test("Test end2end window skew optimization") {

    getSparkSession.conf.set("spark.openmldb.window.parallelization", true)
    getSparkSession.conf.set("spark.openmldb.window.skew.opt", true)
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, "tom", 100, 1),
      Row(2, "amy", 200, 2),
      Row(3, "tom", 300, 3),
      Row(4, "amy", 400, 4),
      Row(5, "tom", 500, 5))
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", StringType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText ="""
                   | SELECT sum(trans_amount) OVER w AS w_sum_amount FROM t1
                   | WINDOW w AS (
                   |    PARTITION BY user
                   |    ORDER BY trans_time
                   |    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW);
     """.stripMargin

    val outputDf = sess.sql(sqlText)

    val sparksqlOutputDf = sess.sparksql(sqlText)
    // Notice that the sum column type is different for SparkSQL and OpenMLDB batch
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }
}
