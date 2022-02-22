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


class TestJoinWithNullData extends SparkTestSuite {

  test("Test left join with null data") {

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
    sess.registerTable("t2", df)
    df.createOrReplaceTempView("t1")
    df.createOrReplaceTempView("t2")

    val sqlText = "SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 on t1.trans_time = t2.trans_time"

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    // Notice that the sum column type is different for SparkSQL and SparkFE
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test last join with null data") {

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
    sess.registerTable("t2", df)

    val sqlText = "SELECT t1.id, t2.id FROM t1 LAST JOIN t2 on t1.trans_time = t2.trans_time"

    val outputDf = sess.sql(sqlText)
    assert(outputDf.collect().size == data.size)
  }

}
