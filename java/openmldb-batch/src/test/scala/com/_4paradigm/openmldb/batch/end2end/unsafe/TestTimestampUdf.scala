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

package com._4paradigm.openmldb.batch.end2end.unsafe

import com._4paradigm.openmldb.batch.UnsaferowoptSparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

import java.sql.Timestamp

class TestTimestampUdf extends UnsaferowoptSparkTestSuite {

  test("Test udf of timestamp for project") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(Timestamp.valueOf("2015-05-08 08:10:25"))
    )

    val schema = StructType(List(
      StructField("col1", TimestampType)))

    val t1 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    t1.createOrReplaceTempView("t1")
    sess.registerTable("t1", t1)

    val sqlText = "SELECT col1, year(col1), month(col1), day(col1) FROM t1"

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test udf of timestamp for window") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(Timestamp.valueOf("2015-05-08 08:10:25"), 1)
    )

    val schema = StructType(List(
      StructField("col1", TimestampType),
      StructField("col2", IntegerType)
    ))

    val t1 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    t1.createOrReplaceTempView("t1")
    sess.registerTable("t1", t1)

    val sqlText ="""
                   | SELECT
                   |   col1,
                   |   year(col1),
                   |   month(col1),
                   |   day(col1),
                   |   sum(col2) OVER w AS w_sum_col1
                   | FROM t1
                   | WINDOW w AS (
                   |    PARTITION BY col2
                   |    ORDER BY col1
                   |    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW);
     """.stripMargin

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

}
