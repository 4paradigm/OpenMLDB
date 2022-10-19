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
import com._4paradigm.openmldb.batch.end2end.DataUtil
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType, TimestampType}

import java.sql.Date

class TestDateUdf extends UnsaferowoptSparkTestSuite {

  test("Test simple project with date columns") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val df = DataUtil.getAllTypesDfWithNull(spark)
    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText = "SELECT date_col FROM t1"

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test project with date columns") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val df = DataUtil.getAllTypesDfWithNull(spark)
    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText = "SELECT int_col + 1 as int_add_one, date_col FROM t1"

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test udf of date for project") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(Date.valueOf("1970-01-02")),
      Row(Date.valueOf("1999-02-21")),
      Row(Date.valueOf("2999-12-31"))
    )

    val schema = StructType(List(
      StructField("col1", DateType)
    ))

    val t1 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    t1.createOrReplaceTempView("t1")
    sess.registerTable("t1", t1)

    val sqlText = "SELECT col1, day(col1), dayofmonth(col1), dayofweek(col1) FROM t1"

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test udf of date for window") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(Date.valueOf("1970-01-02"), 1)
    )

    val schema = StructType(List(
      StructField("col1", DateType),
      StructField("col2", IntegerType)
    ))

    val t1 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    t1.createOrReplaceTempView("t1")
    sess.registerTable("t1", t1)

    val sqlText ="""
                   | SELECT
                   |   col1,
                   |   day(col1),
                   |   dayofmonth(col1),
                   |   dayofweek(col1),
                   |   sum(col2) OVER w AS w_sum_col1
                   | FROM t1
                   | WINDOW w AS (
                   |    PARTITION BY col2
                   |    ORDER BY col2
                   |    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW);
     """.stripMargin

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

}
