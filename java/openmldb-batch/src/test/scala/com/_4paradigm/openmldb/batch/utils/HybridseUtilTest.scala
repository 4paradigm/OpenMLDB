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

package com._4paradigm.openmldb.batch.utils

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.utils.HybridseUtil.autoLoad
import com._4paradigm.openmldb.proto.{Common, Type}
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers.{convertToAnyShouldWrapper, equal}

class HybridseUtilTest extends SparkTestSuite {

  def checkSqlTimestampResult(df: DataFrame): Unit = {
    df.show()
    val l = df.select("ts").rdd.map(r => r(0)).collect.toList
    l.toString() should equal("List(null, 1970-01-01 00:00:00.0, null, null, 2022-02-01 09:00:00.0)")
  }

  test("Test AutoLoad") {
    val col = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kTimestamp).build()
    val cols = new java.util.ArrayList[Common.ColumnDesc]
    cols.add(col)
    val testFile = "file://" + getClass.getResource("/load_data_test_src/sql_timestamp.csv").getPath

    // test format with upper case
    val df = autoLoad(getSparkSession, testFile, "Csv", Map(("header", "true"), ("nullValue", "null")), cols)
    checkSqlTimestampResult(df)
  }

  test("Test AutoLoad Type Timestamp") {
    val col = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kTimestamp).build()
    val cols = new java.util.ArrayList[Common.ColumnDesc]
    cols.add(col)

    val testFile = "file://" + getClass.getResource("/load_data_test_src/sql_timestamp.csv").getPath
    val df = autoLoad(getSparkSession, testFile, "csv", Map(("header", "true"), ("nullValue", "null")), cols)
    checkSqlTimestampResult(df)

    val testFile2 = "file://" + getClass.getResource("/load_data_test_src/long_timestamp.csv").getPath
    val df2 = autoLoad(getSparkSession, testFile2, "csv", Map(("header", "true"), ("nullValue", "null")), cols)
    df2.show()
    val l2 = df2.select("ts").rdd.map(r => r(0)).collect.toList
    l2.toString() should equal("List(null, null, 2022-02-01 09:00:00.0, null)")

    // won't try to parse timestamp format when loading parquet
    val testFile3 = "file://" + getClass.getResource("/load_data_test_src/timestamp.parquet").getPath
    // the format setting in options won't work, autoLoad will use arg2 `format` to load file
    val df3 = autoLoad(getSparkSession, testFile3, "parquet", Map(("header", "true"), ("nullValue", "null"),
      ("format", "csv")), cols)
    df3.show()
    val l3 = df3.select("ts").rdd.map(r => r(0)).collect.toList
    l3.toString() should equal("List(null, 1970-01-01 08:00:00.0, 2022-02-01 17:00:00.0)")
  }
}
