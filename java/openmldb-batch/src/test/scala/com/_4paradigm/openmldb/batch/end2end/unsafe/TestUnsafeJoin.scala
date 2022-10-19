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
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField,
  StructType}

class TestUnsafeJoin extends UnsaferowoptSparkTestSuite {

  def testSql(sqlText: String) {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val t1 = DataUtil.getTestDf(spark)
    val t2 = DataUtil.getTestDf(spark)
    sess.registerTable("t1", t1)
    sess.registerTable("t2", t2)
    t1.createOrReplaceTempView("t1")
    t2.createOrReplaceTempView("t2")

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test unsafe left join") {
    // Test different join condictions
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id = t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id != t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id > t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id >= t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id < t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id <= t2.id")

    // Test join with constant values
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id = 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id != 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id > 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id >= 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON 1 > t1.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id <= 1.0")

    // Test with multiple conditions
    testSql("SELECT t1.id as t1_id, t2.id as t2_id FROM t1 LEFT JOIN t2 ON t1.id = t2.id and 100 > t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2" +
      " ON t1.id >= 1 and t2.id > 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2" +
      " ON t1.id >= 1 or t2.id > 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2" +
      " ON t1.id >= 1 or t2.id > 1 and t1.id = t2.id or t1.id < 10 and t2.id > 0.1 or t2.id = 2")
  }

  test("Test unsafe last join") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row("0", 1, 5.toShort, 1.1.toFloat, 11.1, 1L, "1"),
      Row("0", 2, 5.toShort, 2.2.toFloat, 22.2, 2L, "22"),
      Row("1", 3, 55.toShort, 3.3.toFloat, 33.3, 1L, "333"),
      Row("1", 4, 55.toShort, 4.4.toFloat, 44.4, 2L, "4444"),
      Row("2", 5, 55.toShort, 5.5.toFloat, 55.5, 3L, "aaaaa"))
    val schema = StructType(List(
      StructField("col0", StringType),
      StructField("col1", IntegerType),
      StructField("col2", ShortType),
      StructField("col3", FloatType),
      StructField("col4", DoubleType),
      StructField("col5", LongType),
      StructField("col6", StringType)))
    val t1 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    val data2 = Seq(
      Row("2", "EEEEE", 5.5.toFloat, 55.5, 550.toShort, 5, 3L),
      Row("1", "DDDD", 4.4.toFloat, 44.4, 550.toShort, 4, 2L),
      Row("1", "CCC", 3.3.toFloat, 33.3, 550.toShort, 3, 1L),
      Row("0", "BB", 2.2.toFloat, 22.2, 50.toShort, 2, 2L),
      Row("0", "A", 1.1.toFloat, 11.1, 50.toShort, 1, 1L))
    val schema2 = StructType(List(
      StructField("str0", StringType),
      StructField("str1", StringType),
      StructField("col3", FloatType),
      StructField("col4", DoubleType),
      StructField("col2", ShortType),
      StructField("col1", IntegerType),
      StructField("col5", LongType)))
    val t2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)

    sess.registerTable("t1", t1)
    sess.registerTable("t2", t2)

    val sqlText = "SELECT t1.col1 as id, t1.col0 as t1_col0, t1.col1 + t2.col1 + 1 as test_col1, t1.col2 as t1_col2," +
      " str1 FROM t1 last join t2 order by t2.col5 on t1.col1=t2.col1 and t1.col5 >= t2.col5"

    val outputDf = sess.sql(sqlText)
    assert(outputDf.count() == data.size)
  }

  test("Test unsafe last join with arithmetic expression") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val t1 = DataUtil.getTestDf(spark)
    val t2 = DataUtil.getTestDf(spark)
    sess.registerTable("t1", t1)
    sess.registerTable("t2", t2)
    t1.createOrReplaceTempView("t1")
    t2.createOrReplaceTempView("t2")

    val sqlText = "SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LAST JOIN t2 ON t1.id + 1 <= t2.id"
    val outputDf = sess.sql(sqlText)
    assert(outputDf.count() == t1.count())

    val sqlText2 = "SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LAST JOIN t2 ON t1.id > t2.id * 100"
    val outputDf2 = sess.sql(sqlText2)
    assert(outputDf2.count() == t1.count())

    val sqlText3 = "SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LAST JOIN t2" +
      " ON t1.id * 100 + 100 >= t2.id * 100 + 100"
    val outputDf3 = sess.sql(sqlText3)
    assert(outputDf3.count() == t1.count())

    val sqlText4 = "SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LAST JOIN t2" +
      " ON CAST(t1.id AS INT64) + 100 >= t2.id * 100 + 100"
    val outputDf4 = sess.sql(sqlText4)
    assert(outputDf4.count() == t1.count())
  }

}
