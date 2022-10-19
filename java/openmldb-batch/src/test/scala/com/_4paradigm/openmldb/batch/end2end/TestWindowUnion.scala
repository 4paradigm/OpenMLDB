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


class TestWindowUnion extends SparkTestSuite {

  test("Test end2end window union") {

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

    val sqlText ="""
                   | SELECT sum(trans_amount) OVER w AS w_sum_amount FROM t1
                   | WINDOW w AS (
                   |    UNION t1
                   |    PARTITION BY user
                   |    ORDER BY trans_time
                   |    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW);
     """.stripMargin

    val outputDf = sess.sql(sqlText)
    val count = outputDf.count()
    val expectedCount = data.size
    assert(count == expectedCount)
  }

  test("Test window union with extra window attributes") {

    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val t1 = Seq(
          Row(1, 100, 111, 21),
          Row(2, 100, 111, 5),
          Row(3, 101, 111, 0),
          Row(4, 102, 111, 0))
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("ts", IntegerType),
      StructField("g", IntegerType),
      StructField("val", IntegerType)))

    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(t1), schema)

    sess.registerTable("t1", df1)
    df1.createOrReplaceTempView("t1")

    val t2 = Seq(
          Row(1,  99, 111, 233),
          Row(1, 100, 111, 200),
          Row(1, 101, 111, 17))
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(t2), schema)
    sess.registerTable("t2", df2)
    df1.createOrReplaceTempView("t2")

    val sqlText ="""
       | select
       |    id, count(val) over w as cnt,
       |    max(val) over w as mv,
       |    min(val) over w as mi,
       | from t1 window w as(
       |    union t2
       |    partition by `g` order by `ts`
       |    rows_range between 3s preceding AND CURRENT ROW
       |    MAXSIZE 2
       |    EXCLUDE CURRENT_ROW EXCLUDE CURRENT_TIME);
     """.stripMargin

    val outputDf = sess.sql(sqlText)
    outputDf.show()

    val expect = Seq(
        Row(1, 1, 233, 233),
        Row(2, 1, 233, 233),
        Row(3, 2, 21,  5),
        Row(4, 2, 17,  0))

    val compareSchema = StructType(List(
      StructField("id", IntegerType),
      StructField("cnt", IntegerType),
      StructField("mv", IntegerType),
      StructField("mi", IntegerType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(expect), compareSchema)

    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), compareDf, false))
  }


  ignore("Test window union after window union") {

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

    val data2 = Seq(
      Row(1, "tom", 100, 1),
      Row(10, "amy", 1000, 10))
    val schema2 = StructType(List(
      StructField("id", IntegerType),
      StructField("user", StringType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    sess.registerTable("t2", df2)

    val sqlText ="""
                   | SELECT
                   |  sum(trans_amount) OVER w1 AS w1_sum_amount,
                   |  sum(trans_amount) OVER w2 AS w2_sum_amount,
                   | FROM t1
                   | WINDOW
                   | w1 AS (UNION t2 PARTITION BY user ORDER BY trans_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW),
                   | w2 AS (UNION t2 PARTITION BY user ORDER BY trans_amount ROWS BETWEEN 10 PRECEDING AND CURRENT ROW);
     """.stripMargin

    val outputDf = sess.sql(sqlText)
    val count = outputDf.count()
    val expectedCount = data.size
    assert(count == expectedCount)
  }

}
