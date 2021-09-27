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

package com._4paradigm.openmldb.batch

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite


class TestString extends SparkTestSuite{

  test("Test windowskew") {

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
    outputDf.show()
    assert(outputDf.count == 5)
  }
}
