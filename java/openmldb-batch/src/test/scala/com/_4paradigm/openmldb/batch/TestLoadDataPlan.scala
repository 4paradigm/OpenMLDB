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
import com._4paradigm.openmldb.proto.NS
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import scala.collection.JavaConverters.seqAsJavaListConverter

class TestLoadDataPlan extends SparkTestSuite {
  var openmldbSession: OpenmldbSession = _
  var openmldbConnector: SqlClusterExecutor = _
  val db = "batch_test"
  val table = "connector_load"

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
    //  DO NOT check the offline table info until task manager works.
    openmldbConnector.executeDDL(db, s"drop table $table;")
    openmldbConnector.executeDDL(db, s"create table $table(c1 int, c2 int64, " +
      s"c3 double);")
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

  test("Test Load to Openmldb Offline Storage") {
    // TODO(hw): check no offline table info here. Can't do now cuz drop table will fail, so here may have offline
    //  table info

    // NOTE: src csv files should be in the directory '/load_data_test_src'
    // reading the exact file will convert it to a dir on linux, but works fine on macos, sth weird
    val testFile = "file://" + getClass.getResource("/load_data_test_src").getPath
    // TODO(hw): offline address may not unique

    println("soft load data with overwrite mode")
    openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', mode='overwrite', deep_copy=false, null_value='123');")
    // getLatestTableInfo will refresh the table info cache
    val softInfo = getLatestTableInfo(db, table)
    assert(softInfo.hasOfflineTableInfo, s"no offline info $softInfo")
    // the offline info will be the same with load data options
    assert(softInfo.getOfflineTableInfo.getPath == testFile)
    assert(softInfo.getOfflineTableInfo.getFormat == "csv")
    assert(softInfo.getOfflineTableInfo.getOptionsMap.get("nullValue") == "123")
    assert(!softInfo.getOfflineTableInfo.getDeepCopy)

    println("simple deep load data, it'll overwrite the offline table info")
    var res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', foo='bar', header=false, mode='overwrite');")
    res.show()
    val info = getLatestTableInfo(db, table)
    assert(info.hasOfflineTableInfo, s"no offline info $info")
    assert(info.getOfflineTableInfo.getFormat == "parquet")
    assert(info.getOfflineTableInfo.getOptionsMap.isEmpty)
    assert(info.getOfflineTableInfo.getDeepCopy)

    println("deep load data with strange delimiter, but works")
    res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', foo='bar', header=false, delimiter='++', mode='append');")
    res.show()
    val info1 = getLatestTableInfo(db, table)
    assert(info1.hasOfflineTableInfo, s"no offline info $info1")
    // the offline info won't change
    assert(info1.getOfflineTableInfo == info.getOfflineTableInfo)

    println("deep load data when offline info is exist, and with 'errorifexists' mode")
    try {
      res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(format='csv', foo='bar', header=false, delimiter='++', mode='error_if_exists');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)

    }

    println("deep load data with invalid format option")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(format='txt', mode='overwrite');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

    println("soft load data when the offline info is exist, and with append mode")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(format='csv', mode='append', deep_copy=false);")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }
  }

  test("Test LoadData to Openmldb Online Storage") {
    openmldbSession.getOpenmldbBatchConfig.loadDataMode = "online"
    val testFile = "file://" + getClass.getResource("/load_data_test_src").getPath
    println("simple load to online storage")
    openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(mode='append', header=false);")

    println("online storage doesn't support soft copy")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(mode='append', header=false, deep_copy=false);")
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

  }
}
