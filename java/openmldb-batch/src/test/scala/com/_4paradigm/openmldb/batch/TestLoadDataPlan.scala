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

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.proto.NS
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor

class TestLoadDataPlan extends SparkTestSuite {
  var openmldbSession: OpenmldbSession = _
  var openmldbConnector: SqlClusterExecutor = _
  val db = "batch_test"
  val table = "connector_load"

  override def customizedBefore(): Unit = {
    getSparkSession.conf.set("openmldb.test.print", "true")
    getSparkSession.conf.set("openmldb.zk.cluster", "127.0.0.1:6181")
    getSparkSession.conf.set("openmldb.zk.root.path", "/onebox")
    //      set("openmldb.loaddata.mode", "offline") // default is offline
    openmldbSession = new OpenmldbSession(getSparkSession)
    openmldbConnector = openmldbSession.openmldbCatalogService.sqlExecutor

    openmldbConnector.createDB(db)
    // to ensure the offline info is unset
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
    val testFile = getClass.getResource("/test.csv").getPath
    // TODO(hw): offline address is uncompleted, it's invariant now, so here use 'overwrite' mode to avoid save errors
    var res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', foo='bar', header=false, mode='overwrite');")
    res.show()

    // getLatestTableInfo will refresh the table info cache
    val info = getLatestTableInfo(db, table)
    assert(info.hasOfflineTableInfo, s"no offline info $info")
    assert(info.getOfflineTableInfo.getFormat == "parquet")
    assert(info.getOfflineTableInfo.getOptionsMap.isEmpty)
    assert(info.getOfflineTableInfo.getDeepCopy)

    // strange delimiter, but works
    res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', foo='bar', header=false, delimiter='++', mode='append');")
    res.show()
    val info1 = getLatestTableInfo(db, table)
    assert(info1.hasOfflineTableInfo, s"no offline info $info1")
    // the offline info won't change
    assert(info1.getOfflineTableInfo == info.getOfflineTableInfo)

    // offline info is exist, it'll failed in 'errorifexists' mode
    try {
      res = openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(format='csv', foo='bar', header=false, delimiter='++', mode='error_if_exists');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)

    }

    // invalid format option
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(format='txt', mode='overwrite');")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

    // the offline info is exist, we can't use append mode to do soft copy
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(format='csv', mode='append', deep_copy=false);")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

    // we can use overwrite to do soft copy
    openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(format='csv', mode='overwrite', deep_copy=false, null_value='123');")
    val softInfo = getLatestTableInfo(db, table)
    assert(softInfo.hasOfflineTableInfo, s"no offline info $softInfo")
    // the offline info won't change
    assert(softInfo.getOfflineTableInfo.getPath == testFile)
    assert(softInfo.getOfflineTableInfo.getFormat == "csv")
    assert(softInfo.getOfflineTableInfo.getOptionsMap.get("nullValue") == "123")
    assert(!softInfo.getOfflineTableInfo.getDeepCopy)
  }

  test("Test LoadData to Openmldb Online Storage") {
    openmldbSession.getOpenmldbBatchConfig.loadDataMode = "online"
    val testFile = getClass.getResource("/test.csv").getPath
    openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(mode='append', header=false);")

    // online storage doesn't support soft copy
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(mode='append', header=false, deep_copy=false);")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }
    // online storage doesn't support overwrite mode
    try {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(mode='overwrite', header=false);")
      fail("unreachable")
    } catch {
      case e: IllegalArgumentException => println("It should catch this: " + e.toString)
    }

  }
}
