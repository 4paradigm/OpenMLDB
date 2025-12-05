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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class TestWindow extends SparkTestSuite {

  test("Test end2end window aggregation") {

    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, "tom", 100, 1),
      Row(2, "amy", 200, 2),
      Row(3, "tom", 300, 3),
      Row(4, "amy", 400, 4),
      Row(5, "tom", 500, 5),
      Row(6, "amy", 600, 6),
      Row(7, "tom", 700, 7),
      Row(8, "amy", 800, 8),
      Row(9, "tom", 900, 9),
      Row(10, "amy", 1000, 10))
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
    // Notice that the sum column type is different for SparkSQL and SparkFE
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test window aggregation with extra window attributes") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
          Row(1,  99000, 111, 21),
          Row(2, 100000, 111, 22),
          Row(3, 101000, 111, 23),
          Row(4, 102000, 111, 44),
          Row(5, 100000, 114, 56),
          Row(6, 102000, 114, 52))
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("ts", IntegerType),
      StructField("g", IntegerType),
      StructField("val", IntegerType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText ="""
       | select
       |   id,
       |   count(val) over w as cnt,
       |   max(val) over w as mv,
       |   min(val) over w as mi,
       |   lag(val, 1) over w as l1
       | FROM t1 WINDOW w as(
       |   PARTITION by `g` ORDER by `ts`
       |   ROWS_RANGE BETWEEN 3s PRECEDING AND CURRENT ROW MAXSIZE 2 EXCLUDE CURRENT_ROW);
     """.stripMargin

    val outputDf = sess.sql(sqlText)
    outputDf.show()

    val expect = Seq(
         Row(1, 0, null, null, null),
         Row(2, 1, 21, 21, 21),
         Row(3, 2, 22, 21, 22),
         Row(4, 2, 23, 22, 23),
         Row(5, 0, null, null, null),
         Row(6, 1, 56, 56, 56))

    val compareSchema = StructType(List(
      StructField("id", IntegerType),
      StructField("cnt", IntegerType),
      StructField("mv", IntegerType),
      StructField("mi", IntegerType),
      StructField("l1", IntegerType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(expect), compareSchema)

    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), compareDf, false))
  }

  test("Test end2end WINDOW without ORDER BY") {

    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    // test with small set, ordering is undetermined for WINDOW without ORDER BY
    val data = Seq(
      Row(1, "tom", 100, 1),
      Row(2, "amy", 200, 2),
      Row(3, "tom", 300, 3),
      Row(4, "tom", 400, 4))

    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", StringType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText =
      """
        | SELECT id,sum(trans_amount) OVER w AS w_sum_amount FROM t1
        | WINDOW w AS (
        |    PARTITION BY user
        |    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
   """.stripMargin

    val outputDf = sess.sql(sqlText)

    val sparksqlOutputDf = sess.sparksql(sqlText)
    outputDf.show()
    sparksqlOutputDf.show()
    // Notice that the sum column type is different for SparkSQL and SparkFE
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }
}
