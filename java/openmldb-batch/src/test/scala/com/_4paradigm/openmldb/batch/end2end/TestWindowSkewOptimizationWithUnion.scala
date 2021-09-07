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

class TestWindowSkewOptimizationWithUnion extends SparkTestSuite {

  test("Test window union in INSTANCE_NOT_IN_WINDOW") {

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
        |     INSTANCE_NOT_IN_WINDOW)
        | limit 10;
        |""".stripMargin

    val data1 = Seq(
      Row(5, 2, 1.1, "a"),
      Row(5, 4, 2.2, "b"),
      Row(5, 6, 3.3, "c"))
    val schema1 = StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", IntegerType),
      StructField("col3", DoubleType),
      StructField("col4", StringType)))

    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema1)

    sess.registerTable("t1", df1)

    val data2 = Seq(
      Row(5, 1, 1.0, "e"),
      Row(5, 3, 2.0, "f"),
      Row(5, 5, 3.0, "g"))
    val schema2 = StructType(List(
      StructField("c1", IntegerType),
      StructField("c2", IntegerType),
      StructField("c3", DoubleType),
      StructField("c4", StringType)))

    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)

    sess.registerTable("tb", df2)

    val outputDf = sess.sql(sqlText)

    val compareData = Seq(
      Row(5, 2, 2.1),
      Row(5, 4, 5.2),
      Row(5, 6, 8.3))

    val compareSchema = StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", IntegerType),
      StructField("w1_col3_sum", DoubleType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), compareDf, false))
  }
}
