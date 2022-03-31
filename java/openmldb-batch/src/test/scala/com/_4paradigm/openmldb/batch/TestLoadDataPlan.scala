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

import java.util.Properties

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.nodes.LoadDataPlan.autoLoad
import com._4paradigm.openmldb.proto.{Common, NS, Type}
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.SparkException
import org.scalatest.Matchers

import scala.language.postfixOps

class TestLoadDataPlan extends SparkTestSuite with Matchers {
  var openmldbSession: OpenmldbSession = _
  var openmldbConnector: SqlClusterExecutor = _
  val db = "batch_test"
  val table = "load_data_test"

  override def customizedBefore(): Unit = {
    // load data needs openmldb cluster
    val prop = new Properties
    prop.load(getClass.getResourceAsStream("/test.properties"))
    val cluster = prop.getProperty("openmldb.zk.cluster", "127.0.0.1:6181")
    val path = prop.getProperty("openmldb.zk.root.path", "/onebox")
    getSparkSession.conf.set("openmldb.zk.cluster", cluster)
    getSparkSession.conf.set("openmldb.zk.root.path", path)
    //      set("openmldb.loaddata.mode", "offline") // default is offline

    openmldbSession = new OpenmldbSession(getSparkSession)
    openmldbConnector = openmldbSession.openmldbCatalogService.sqlExecutor

    openmldbConnector.createDB(db)
    // to ensure the offline info is unset
    // TODO(hw): test openmldb cluster doesn't have task manager now, so drop table will fail
    //  DO NOT run offline test until task manager works in test env.
    openmldbConnector.executeDDL(db, s"drop table $table;")
    openmldbConnector.executeDDL(db, s"create table $table(c1 int, c2 int64, " +
      s"c3 double not null);")
    assert(tableExists(db, table))
  }

  def tableExists(db: String, table: String): Boolean = {
    val info = getLatestTableInfo(db, table)
    info.getName.nonEmpty
  }

  def getLatestTableInfo(db: String, table: String): NS.TableInfo = {
    // cache which in openmldb client doesn't refresh quickly, so we enforce refresh it.
    openmldbConnector.refreshCatalog()
    openmldbConnector.getTableInfo(db, table)
  }

  test("Test Only Use SparkSession") {
    val sess = getSparkSession
    val planner = new SparkPlanner(sess)
    val t1 = sess.emptyDataFrame
    try {
      planner.plan("load data infile 'foo.txt' into table t1;", Map("t1" -> t1))
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }
  }

  test("Test Load Type Timestamp") {
    val col = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kTimestamp).build()
    val cols = new java.util.ArrayList[Common.ColumnDesc]
    cols.add(col)

    val testFile = "file://" + getClass.getResource("/load_data_test_src/sql_timestamp.csv").getPath
    val df = autoLoad(getSparkSession.read.option("header", "true").option("nullValue", "null"),
      testFile, "csv", cols)
    df.show()
    val l = df.select("ts").rdd.map(r => r(0)).collect.toList
    l.toString() should equal("List(null, 1970-01-01 00:00:00.0, null, null, 2022-02-01 09:00:00.0)")

    val testFile2 = "file://" + getClass.getResource("/load_data_test_src/long_timestamp.csv").getPath
    val df2 = autoLoad(getSparkSession.read.option("header", "true").option("nullValue", "null"),
      testFile2, "csv", cols)
    df2.show()
    val l2 = df2.select("ts").rdd.map(r => r(0)).collect.toList
    l2.toString() should equal("List(null, null, 2022-02-01 09:00:00.0, null)")

    // won't try to parse timestamp format when loading parquet
    val testFile3 = "file://" + getClass.getResource("/load_data_test_src/timestamp.parquet").getPath
    // the format setting in arg1 won't work, autoLoad will use arg2 format to load file
    val df3 = autoLoad(getSparkSession.read.option("header", "true").option("nullValue", "null").format("csv"),
      testFile3, "parquet", cols)
    df3.show()
    val l3 = df3.select("ts").rdd.map(r => r(0)).collect.toList
    l3.toString() should equal("List(null, 1970-01-01 08:00:00.0, 2022-02-01 17:00:00.0)")
  }

  ignore("Test Load to Openmldb Offline Storage") {
    val originInfo = getLatestTableInfo(db, table)
    assert(!originInfo.hasOfflineTableInfo, s"shouldn't have offline info(maybe recreate table failed), $originInfo")
    // P.S. src csv files have header, and the col names are different with table schema, and no s
    // If soft-copy, we don't read data, can't do schema check. So we don't do restrict schema check now.
    // TODO(hw): do restrict schema check even when soft-copy?
    val testFileWithHeader = "file://" + getClass.getResource("/load_data_test_src/test_with_any_header.csv").getPath

    println("no offline table info now, soft load data with any mode")
    openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
      "options(format='csv', deep_copy=false, null_value='123');")
    // getLatestTableInfo will refresh the table info cache
    val softInfo = getLatestTableInfo(db, table)
    assert(softInfo.hasOfflineTableInfo, s"no offline info $softInfo")
    // the offline info will be the same with load data options
    assert(softInfo.getOfflineTableInfo.getPath == testFileWithHeader)
    assert(softInfo.getOfflineTableInfo.getFormat == "csv")
    assert(softInfo.getOfflineTableInfo.getOptionsMap.get("nullValue") == "123")
    assert(!softInfo.getOfflineTableInfo.getDeepCopy)

    println("soft offline table now, simple deep load data with append mode")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(foo='bar', mode='append');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

    println("soft offline table now, simple deep load data with overwrite mode")
    var res = openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
      "options(format='csv', foo='bar', mode='overwrite');")
    res.show()
    val info = getLatestTableInfo(db, table)
    assert(info.hasOfflineTableInfo, s"no offline info $info")
    assert(info.getOfflineTableInfo.getFormat == "parquet")
    assert(info.getOfflineTableInfo.getOptionsMap.isEmpty)
    assert(info.getOfflineTableInfo.getDeepCopy)

    println("deep load data again with strange delimiter, null at non-null column c3, fail")
    try {
      res = openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(format='csv', delimiter='++', mode='overwrite');")
      fail("unreachable")
    } catch {
      case e: SparkException => println("It should catch this: " + e.toString)
      // should equal("The 2th field 'c3' of input row cannot be null.")
    }

    println("deep load data when offline info is exist, and with 'errorifexists' mode")
    try {
      res = openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(foo='bar', mode='error_if_exists');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

    println("deep load data with invalid format option")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(format='txt', mode='overwrite');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

    println("deep offline table now, soft load data with any mode")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(deep_copy=false, mode='append');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }
  }

  test("Test LoadData to Openmldb Online Storage") {
    openmldbSession.getOpenmldbBatchConfig.loadDataMode = "online"
    val testFile = "file://" + getClass.getResource("/load_data_test_src/test_with_any_header.csv").getPath
    println("simple load to online storage")
    openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(mode='append');")

    println("online storage doesn't support soft copy")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(mode='append', deep_copy=false);")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }
    println("online storage doesn't support overwrite mode")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(mode='overwrite', header=false);")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }
    try {
      val testNonNull = "file://" + getClass.getResource("/load_data_test_src/test_non_null.csv").getPath
      openmldbSession.openmldbSql(s"load data infile '$testNonNull' into table $db.$table options(mode='append');")

      fail("unreachable")
    } catch {
      case e: SparkException => println("It should catch this: " + e.toString)
      // should equal("The 2th field 'c3' of input row cannot be null.")
    }
  }
}
