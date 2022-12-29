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

import com._4paradigm.openmldb.batch.PlanContext
import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.utils.HybridseUtil.autoLoad
import com._4paradigm.openmldb.batch.utils.HybridseUtil.hiveLoad
import com._4paradigm.openmldb.proto.{Common, Type}
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.AnalysisException
import org.scalatest.Matchers

class HybridseUtilTest extends SparkTestSuite with Matchers {

  def checkTsColResult(df: DataFrame, expected: String): Unit = {
    df.show()
    val l = df.select("ts").rdd.map(r => r(0)).collect.toList
    l.toString() should equal(expected)
  }

  test("Test AutoLoad Csv") {
    val col = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kTimestamp).build()
    val cols = new java.util.ArrayList[Common.ColumnDesc]
    cols.add(col)
    val testFile = "file://" + getClass.getResource("/load_data_test_src/sql_timestamp.csv").getPath

    // test format with upper case
    val df = autoLoad(getSparkSession, testFile, "Csv", Map(("header", "true"), ("nullValue", "null")), cols)
    checkTsColResult(df, "List(null, 1970-01-01 00:00:00.0, null, null, 2022-02-01 09:00:00.0)")
  }

  test("Test AutoLoad Parquet") {
    // expect ts string
    val col = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kVarchar).build()
    val cols = new java.util.ArrayList[Common.ColumnDesc]
    cols.add(col)
    // but the source col 'ts' is timestamp
    val testFile = "file://" + getClass.getResource("/load_data_test_src/timestamp.parquet").getPath
    val thrown = the[IllegalArgumentException] thrownBy {
      val df = autoLoad(getSparkSession, testFile, "parquet", Map(("header", "true"), ("nullValue", "null")), cols)
      fail("unreachable")
    }
    thrown.getMessage should startWith ("requirement failed: schema mismatch")
  }

  test("Test AutoLoad Type Timestamp") {
    val col = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kTimestamp).build()
    val cols = new java.util.ArrayList[Common.ColumnDesc]
    cols.add(col)

    val testFile = "file://" + getClass.getResource("/load_data_test_src/sql_timestamp.csv").getPath
    val df = autoLoad(getSparkSession, testFile, "csv", Map(("header", "true"), ("nullValue", "null")), cols)
    checkTsColResult(df, "List(null, 1970-01-01 00:00:00.0, null, null, 2022-02-01 09:00:00.0)")

    val testFile2 = "file://" + getClass.getResource("/load_data_test_src/long_timestamp.csv").getPath
    val df2 = autoLoad(getSparkSession, testFile2, "csv", Map(("header", "true"), ("nullValue", "null")), cols)
    checkTsColResult(df2, "List(null, null, 2022-02-01 09:00:00.0, null)")

    // won't try to parse timestamp format when loading parquet
    val testFile3 = "file://" + getClass.getResource("/load_data_test_src/timestamp.parquet").getPath
    // the format setting in options won't work, autoLoad will use arg2 `format` to load file
    val df3 = autoLoad(getSparkSession, testFile3, "parquet", Map(("header", "true"), ("nullValue", "null"),
      ("format", "csv")), cols)
    checkTsColResult(df3, "List(null, 1970-01-01 08:00:00.0, 2022-02-01 17:00:00.0)")
  }

  // TODO(hw): hive test needs metastore service.
  ignore("Test read from hive") {
    // must have a new spark context
    getSparkSession.stop()
    val hiveSession = SparkSession.builder().master("local[4]").appName("hive-support")
      .enableHiveSupport() // static conf
      .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
      .getOrCreate()
    println(hiveSession.conf.getAll)
    hiveSession.sql("CREATE TABLE IF NOT EXISTS src1 (key INT, value STRING NOT NULL) USING hive")
    hiveSession.sql("show tables").show()
    hiveSession.sql("LOAD DATA LOCAL INPATH '" + getClass.getResource("/load_data_test_src/src.csv")
      + "' OVERWRITE INTO TABLE src1")

    // Queries are expressed in HiveQL
    val df = hiveSession.sql("SELECT * FROM src1")
    df.show()

    val cols = new java.util.ArrayList[Common.ColumnDesc]
    val col = Common.ColumnDesc.newBuilder().setName("key").setDataType(Type.DataType.kInt).build()
    val col1 = Common.ColumnDesc.newBuilder().setName("value").setDataType(Type.DataType.kString)
      .setNotNull(true).build()
    cols.add(col)
    cols.add(col1)

    // hive format
    // load data infile 'hive://<hive-table-pattern>' into table ...
    // <hive-table-pattern>: db.table or table
    val testFile = "hive://src1"
    val ctx = new PlanContext("test-hive-tag", hiveSession, null, null)
    val df1 = hiveLoad(ctx, testFile, cols)
    df1.show()
    assert(SparkUtil.approximateDfEqual(df, df1))

    // write to hive
    df1.write.mode("overwrite").saveAsTable("dst")
    val df2 = hiveSession.sql("SELECT * FROM dst")
    df2.show()
    assert(SparkUtil.approximateDfEqual(df1, df2))

    // create database won't store in hive, local warehouse
    df1.write.mode("overwrite").saveAsTable("db1.dst1") // db1 is created by hive cli
    hiveSession.sql("describe extended db1.dst1").show(false)
    hiveSession.sql("drop database if exists db2 cascade;")
    hiveSession.sql("create database db2") // will store in local spark-warehouse
    df1.write.mode("append").saveAsTable("db2.dst2")
    hiveSession.sql("describe extended db2.dst2").show(false)
  }
}
