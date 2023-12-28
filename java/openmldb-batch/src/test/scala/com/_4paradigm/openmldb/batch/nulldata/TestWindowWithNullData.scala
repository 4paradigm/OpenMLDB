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

package com._4paradigm.openmldb.batch.nulldata

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class TestWindowWithNullData extends SparkTestSuite {

  test("Test window with null data") {

    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, "tom", 100, 1),
      Row(2, "amy", 200, 2),
      Row(3, "tom", 300, 3),
      Row(4, "amy", 400, 4),
      Row(5, "tom", 500, 5),
      Row(6, "amy", 600, null),
      Row(7, "tom", 700, null),
      Row(8, "amy", 800, null),
      Row(9, "tom", 900, null),
      Row(10, "amy", 1000, null))
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

  test("Test last join to window") {

    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data1 = Seq(Row(1, 1), Row(2, 2))
    val schema1 = StructType(List(
      StructField("id", IntegerType),
      StructField("val", IntegerType)))
    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema1)

    val data2 = Seq(Row(1, 3, 1), Row(1, 3, 2), Row(2, 4, 3))
    val schema2 = StructType(List(
      StructField("id", IntegerType),
      StructField("key", IntegerType),
      StructField("val", IntegerType)))
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)

    sess.registerTable("t1", df1)
    sess.registerTable("t2", df2)

    val sqlText =
      """select t1.id, tx.id as id2, tx.agg from t1 last join
         | (
         | select id, val, sum(val) over w as agg from t2
         | window w as (partition by key order by val rows between 3 preceding and current row)
         | ) tx order by tx.val
         | on t1.id = tx.id
      """.stripMargin

    val outputDf = sess.sql(sqlText)
    outputDf.show()

    val output = Seq(Row(1, 1, 3), Row(2, 2, 3))
    val output_sc = StructType(List(
      StructField("id", IntegerType),
      StructField("id2", IntegerType),
      StructField("agg", IntegerType)))
    val expect_df = spark.createDataFrame(spark.sparkContext.makeRDD(output), output_sc)
    expect_df.show()

    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), expect_df, true))
  }

}
