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

class TestLoadDataPlan extends SparkTestSuite {
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

  ignore("Test Load to Openmldb Offline Storage") {
    val originInfo = getLatestTableInfo(db, table)
    assert(!originInfo.hasOfflineTableInfo, s"shouldn't have info $originInfo")
    // P.S. src csv files have header, and the col names are different with table schema, and no s
    // If soft-copy, we don't read data, can't do schema check. So we don't do restrict schema check now.
    // TODO(hw): do restrict schema check even when soft-copy?
    val testFile = "file://" + getClass.getResource("/load_data_test_src").getPath

    println("no offline table info now, soft load data with any mode")
    openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', deep_copy=false, null_value='123');")
    // getLatestTableInfo will refresh the table info cache
    val softInfo = getLatestTableInfo(db, table)
    assert(softInfo.hasOfflineTableInfo, s"no offline info $softInfo")
    // the offline info will be the same with load data options
    assert(softInfo.getOfflineTableInfo.getPath == testFile)
    assert(softInfo.getOfflineTableInfo.getFormat == "csv")
    assert(softInfo.getOfflineTableInfo.getOptionsMap.get("nullValue") == "123")
    assert(!softInfo.getOfflineTableInfo.getDeepCopy)

    println("soft offline table now, simple deep load data with append mode")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(foo='bar', mode='append');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

    println("soft offline table now, simple deep load data with overwrite mode")
    var res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', foo='bar', mode='overwrite');")
    res.show()
    val info = getLatestTableInfo(db, table)
    assert(info.hasOfflineTableInfo, s"no offline info $info")
    assert(info.getOfflineTableInfo.getFormat == "parquet")
    assert(info.getOfflineTableInfo.getOptionsMap.isEmpty)
    assert(info.getOfflineTableInfo.getDeepCopy)

    println("deep load data again with strange delimiter")
    // src csv files have header, if we read header with strange delimiter, the col name will contain invalid
    // character(s) among " ,;{}()\n\t=". Set header to false, to avoid this error.
    res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', foo='bar', delimiter='++', header=false, mode='append');")
    res.show()
    val info1 = getLatestTableInfo(db, table)
    assert(info1.hasOfflineTableInfo, s"no offline info $info1")
    // the offline info won't change
    assert(info1.getOfflineTableInfo == info.getOfflineTableInfo)

    println("deep load data when offline info is exist, and with 'errorifexists' mode")
    try {
      res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(foo='bar', mode='error_if_exists');")
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

    println("deep offline table now, soft load data with any mode")
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(deep_copy=false, mode='append');")
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
  }
}
