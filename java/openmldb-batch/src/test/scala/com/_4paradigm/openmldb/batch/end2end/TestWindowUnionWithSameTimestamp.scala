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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

class TestWindowUnionWithSameTimestamp extends SparkTestSuite {

  test("Test window union with same timestamp") {

    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq[Row](
      Row(1, 1L)
    )
    val schema = StructType(List(
      StructField("int_col", IntegerType),
      StructField("long_col", LongType)
    ))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    sess.registerTable("t1", df)

    val data2 = Seq[Row](
      Row(1, 1L),
      Row(1, 1L)
    )
    val schema2 = StructType(List(
      StructField("int_col", IntegerType),
      StructField("long_col", LongType)
    ))
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)
    sess.registerTable("t2", df2)

    val sqlText =
      """
        | SELECT count(int_col) OVER w
        | FROM t1
        | WINDOW w AS (UNION t2 PARTITION BY int_col ORDER BY long_col ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
        |""".stripMargin

    val outputDf = sess.sql(sqlText)
    val outputRow = outputDf.collect()(0)
    // The output of count(int_col) should contain current row from primary table and other rows from union tables
    assert(outputRow.getLong(0) == 3)
  }

}
