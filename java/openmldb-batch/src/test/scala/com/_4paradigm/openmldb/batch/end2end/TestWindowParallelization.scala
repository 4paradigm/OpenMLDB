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
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class TestWindowParallelization extends SparkTestSuite {

  test("Test end2end window paralleization in last join") {

    getSparkSession.conf.set("openmldb.window.parallelization",true)
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val sqlText ="""
                   | SELECT sum(t1.col1) over w1 as sum_t1_col1, str1 as t2_str1
                   | FROM t1
                   | last join t2 order by t2.col1
                   | on t1.col1 = t2.col1 and t1.col2= t2.col0
                   | WINDOW w1 AS (
                   |  PARTITION BY t1.col2 ORDER BY t1.col1
                   |  ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW
                   | ) limit 10;
                   |""".stripMargin

    val data1 = Seq(
      Row("0", 1, 5),
      Row("0", 2, 5),
      Row("1", 3, 55),
      Row("1", 4, 55),
      Row("2", 5, 55)
    )

    val schema1 = StructType(List(
      StructField("col0", StringType),
      StructField("col1", IntegerType),
      StructField("col2", IntegerType)))

    val data2 = Seq(
      Row("2", "EEEEE", 55, 5),
      Row("1", "DDDD", 55, 4),
      Row("1", "CCC", 55, 3),
      Row("0", "BB", 5, 2),
      Row("0", "A", 5, 1)
    )

    val schema2 = StructType(List(
      StructField("str0", StringType),
      StructField("str1", StringType),
      StructField("col0", IntegerType),
      StructField("col1", IntegerType)))

    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema1)
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)
    sess.registerTable("t1", df1)
    sess.registerTable("t2", df2)
    val outputDf = sess.sql(sqlText)

    val compareData = Seq(
      Row(12, "EEEEE"),
      Row(3, "CCC"),
      Row(1, "A"),
      Row(3, "BB"),
      Row(7, "DDDD"))

    val compareSchema = StructType(List(
      StructField("sum_t1_col1", IntegerType),
      StructField("t2_str1", StringType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), compareDf, false))
  }

  test("Test end2end window paralleization in a complex case") {

    getSparkSession.conf.set("spark.openmldb.window.parallelization", true)
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val sqlText =
      """
        |select
        |  id, t2.c1, t2.c2, t1.c3,
        |  sum(t1.c3) OVER w1 as w1_c3_sum, sum(t2.w2_c2_sum) OVER w1 as w2_c2_sum,
        |  sum(t1.w3_c3_sum) OVER w1 as w3_c3_sum
        |  from (
        |    select id, c1, c2, c3, c4, sum(d1.c2) OVER w2 as w2_c2_sum from d1
        |      WINDOW w2 AS (PARTITION BY d1.c1 ORDER BY d1.c4 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) as t2
        |  last join (
        |    select c1, c3, c4, sum(d2.c3) OVER w3 as w3_c3_sum from d2
        |      WINDOW w3 AS (PARTITION BY d2.c1 ORDER BY d2.c4 ROWS_RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)) as t1
        |  ORDER BY t1.c4 on t2.c1=t1.c1
        |  WINDOW w1 AS (PARTITION BY t2.c1 ORDER BY t2.c4 ROWS_RANGE BETWEEN 1 PRECEDING AND CURRENT ROW);
        """.stripMargin

    val data1 = Seq(
      Row(1,"aa",20,30, 1590738990000L),
      Row(2,"aa",21,31, 1590738990001L),
      Row(3,"aa",22,32, 1590738990002L),
      Row(4,"bb",23,33, 1590738990003L),
      Row(5,"bb",24,34, 1590738990004L))

    val data2 = Seq(
      Row(1,"aa",20,30, 1590738990000L),
      Row(2,"aa",21,31, 1590738990001L),
      Row(3,"aa",22,32, 1590738990002L),
      Row(4,"bb",23,33, 1590738990003L),
      Row(5,"bb",24,34, 1590738990004L))

    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("c1", StringType),
      StructField("c2", IntegerType),
      StructField("c3", IntegerType),
      StructField("c4", LongType)))


    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema)
    sess.registerTable("d1", df1)
    sess.registerTable("d2", df2)

    val outputDf = sess.sql(sqlText)

    val compareData = Seq(
      Row(4, "bb", 23, 34, 34, 23, 67),
      Row(5, "bb", 24, 34 ,68, 70, 134),
      Row(1, "aa", 20, 32, 32, 20, 63),
      Row(2, "aa", 21, 32, 64, 61, 126),
      Row(3, "aa", 22, 32, 64, 84, 126))

    val compareSchema = StructType(List(
      StructField("id", IntegerType),
      StructField("c1", StringType),
      StructField("c2", IntegerType),
      StructField("c3", IntegerType),
      StructField("w1_c3_sum", IntegerType),
      StructField("w2_c2_sum", IntegerType),
      StructField("w3_c4_sum", IntegerType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), compareDf, false))
  }
}
