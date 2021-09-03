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
import com._4paradigm.openmldb.batch.utils.SparkUtil.approximateDfEqual
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

class TestWindowSkewOptimizationInLastJoin extends SparkTestSuite{

  test("Test windowskew optimization in last join") {

    getSparkSession.conf.set("spark.openmldb.window.skew.opt", true)
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val sqlText ="""
                   | SELECT t1.col1 as id, t1.col2 as t1_col2, t1.col5 as t1_col5,
                   | sum(t1.col1) OVER w1 as w1_col1_sum, sum(t2.col2) OVER w1 as w1_t2_col2_sum,
                   | sum(t1.col5) OVER w1 as w1_col5_sum,
                   | str1 as t2_str1 FROM t1
                   | last join t2 order by t2.col5 on t1.col1=t2.col1 and t1.col5 = t2.col5
                   | WINDOW w1 AS (
                   | PARTITION BY t1.col2
                   | ORDER BY t1.col5
                   | ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) limit 10;
                   |""".stripMargin

    val data1 = Seq(
      Row("0", 1, 5, 1.1, 11.1, 1, "1"),
      Row("0", 2, 5, 2.2, 22.2, 2, "22"),
      Row("1", 3, 55, 3.3, 33.3, 1, "333"),
      Row("1", 4, 55, 4.4, 44.4, 2, "4444"),
      Row("2", 5, 55, 5.5, 55.5, 3, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    )

    val schema1 = StructType(List(
      StructField("col0", StringType),
      StructField("col1", IntegerType),
      StructField("col2", IntegerType),
      StructField("col3", DoubleType),
      StructField("col4", DoubleType),
      StructField("col5", IntegerType),
      StructField("col6", StringType)))

    val data2 = Seq(
      Row("2", "EEEEE", 5.5, 550.5, 550, 5, 3),
      Row("1", "DDDD", 4.4, 440.4, 550, 4, 2),
      Row("1", "CCC", 3.3, 330.3, 550, 3, 1),
      Row("0", "BB", 2.2, 220.2, 50, 2, 2),
      Row("0", "A", 1.1, 110.1, 50, 1, 1)
    )

    val schema2 = StructType(List(
      StructField("str0", StringType),
      StructField("str1", StringType),
      StructField("col3", DoubleType),
      StructField("col4", DoubleType),
      StructField("col2", IntegerType),
      StructField("col1", IntegerType),
      StructField("col5", IntegerType)))

    val data3 = Seq(
      Row(1, 5, 1, 1, 50, 1, "A"),
      Row(2, 5, 2, 3, 100, 3, "BB"),
      Row(3, 55, 1, 3, 550, 1, "CCC"),
      Row(4, 55, 2, 7, 1100, 3, "DDDD"),
      Row(5, 55, 3, 12, 1650, 6, "EEEEE")
    )

    val schema3 = StructType(List(
      StructField("id", IntegerType),
      StructField("t1_col2", IntegerType),
      StructField("t1_col5", IntegerType),
      StructField("w1_col1_sum", IntegerType),
      StructField("w1_t2_col2_sum", IntegerType),
      StructField("w1_col5_sum", IntegerType),
      StructField("t2_str1", StringType)))


    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema1)
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)
    val df3 = spark.createDataFrame(spark.sparkContext.makeRDD(data3), schema3)

    sess.registerTable("t1", df1)
    sess.registerTable("t2", df2)

    val outputDf = sess.sql(sqlText)

    assert(approximateDfEqual(outputDf.getSparkDf(), df3, false))
  }
}
