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
import org.apache.spark.SparkException
import org.scalatest.Matchers
import scala.language.postfixOps
import scala.collection.JavaConverters.asScalaBufferConverter


class TestLoadDataPlan extends SparkTestSuite with Matchers {
  var openmldbSession: OpenmldbSession = _
  var openmldbConnector: SqlClusterExecutor = _
  val db = "batch_test"
  var table: String = _

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

    // NOTICE: test openmldb cluster doesn't have task manager now, so drop table will fail
    // To ensure the offline info is unset, we use unique table names. But we can't do cleanup.
    table = "load_data_test" + java.time.Instant.now.toEpochMilli
    println(s"load test on $db.$table")
    assert(!tableExists(db, table))

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
    a[IllegalArgumentException] should be thrownBy {
      planner.plan("load data infile 'foo.txt' into table t1;", Map("t1" -> t1))
      fail("unreachable")
    }
  }

  // Plz DO NOT split this offline test into multi tests, cuz the test is contextual.
  test("Test Load to Openmldb Offline Storage") {
    val originInfo = getLatestTableInfo(db, table)
    assert(!originInfo.hasOfflineTableInfo, s"shouldn't have offline info before the offline test, $originInfo")
    // P.S.
    // When the src **csv** files have header, and the col names are different with table schema,
    // use the table schema to read, header in csv will be ignored.
    val testFileWithHeader = "file://" + getClass.getResource("/load_data_test_src/test_with_any_header.csv").getPath

    println("no offline table info now, soft load data with any mode")
    openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
      "options(format='csv', deep_copy=false, null_value='123');")
    // getLatestTableInfo will refresh the table info cache
    var softInfo = getLatestTableInfo(db, table)
    assert(softInfo.hasOfflineTableInfo, s"no offline info $softInfo")
    // the offline info will be the same with load data options
    assert(softInfo.getOfflineTableInfo.getPath == "")
    assert(softInfo.getOfflineTableInfo.getSymbolicPathsList().asScala.toList(0) == testFileWithHeader)
    assert(softInfo.getOfflineTableInfo.getFormat == "csv")
    assert(softInfo.getOfflineTableInfo.getOptionsMap.get("nullValue") == "123")

    println("soft offline table now, soft load data with error_if_exists mode")
    a[IllegalArgumentException] should be thrownBy {
      openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(deep_copy=false, mode='error_if_exists');")
      fail("unreachable")
    }

    println("soft offline table now, soft load data with overwrite mode, will rewrite the info")
    openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
      "options(format='csv', deep_copy=false, null_value='456', mode='overwrite');")
    // getLatestTableInfo will refresh the table info cache
    softInfo = getLatestTableInfo(db, table)
    assert(softInfo.hasOfflineTableInfo, s"no offline info $softInfo")
    // the offline info will be the same with load data options
    assert(softInfo.getOfflineTableInfo.getPath == "")
    assert(softInfo.getOfflineTableInfo.getSymbolicPathsList().asScala.toList(0) == testFileWithHeader)
    assert(softInfo.getOfflineTableInfo.getFormat == "csv")
    assert(softInfo.getOfflineTableInfo.getOptionsMap.get("nullValue") == "456")

    println("soft offline table now, simple deep load data with append mode")
    a[IllegalArgumentException] should be thrownBy {
      openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(foo='bar', mode='append');")
      fail("unreachable")
    }


    println("soft offline table now, simple deep load data with overwrite mode")
    var res = openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
      "options(format='csv', foo='bar', mode='overwrite');")
    assert(res.count() == 0)
    val info = getLatestTableInfo(db, table)
    assert(info.hasOfflineTableInfo, s"no offline info $info")
    assert(info.getOfflineTableInfo.getFormat == "parquet")
    assert(info.getOfflineTableInfo.getOptionsMap.isEmpty)


    // after load, we can read the offline table data
    // only new openmldb session will register the latest offline tables
    val newOpenmldbSession = new OpenmldbSession(getSparkSession)
    val result = newOpenmldbSession.openmldbSql(s"select * from $db.$table")
    assert(result.count() == 2)

    // invalid load cases:
    println("deep load data again with strange delimiter, null at non-null column c3, fail")
    try {
      res = openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(format='csv', delimiter='++', mode='overwrite');")
      fail("unreachable")
    } catch {
      case e: SparkException => println("It should catch this: " + e.toString)
      // should equal("The 2th field 'c3' of input row cannot be null.")
    }

    println("deep load data when offline info is exist, and with 'error_if_exists' mode")
    a[IllegalArgumentException] should be thrownBy {
      res = openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(foo='bar', mode='error_if_exists');")
      fail("unreachable")
    }

    println("deep load data with invalid format option, catalog will throw exception")
    a[org.apache.spark.sql.catalyst.parser.ParseException] should be thrownBy {
      openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(format='txt', mode='overwrite');")
      fail("unreachable")
    }

    println("deep offline table now, soft load data with any mode")
    /* TODO: Support this case now and need to update case
    a[IllegalArgumentException] should be thrownBy {
      openmldbSession.openmldbSql(s"load data infile '$testFileWithHeader' into table $db.$table " +
        "options(deep_copy=false, mode='append');")
      fail("unreachable")
    }
    */
  }

  test("Test LoadData to Openmldb Online Storage") {
    openmldbSession.getOpenmldbBatchConfig.loadDataMode = "online"
    val testFile = "file://" + getClass.getResource("/load_data_test_src/test_with_any_header.csv").getPath
    println("simple load to online storage")
    openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
      "options(mode='append');")

    println("online storage doesn't support soft copy")
    a[IllegalArgumentException] should be thrownBy {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(mode='append', deep_copy=false);")
      fail("unreachable")
    }

    println("online storage doesn't support overwrite mode")
    a[IllegalArgumentException] should be thrownBy {
      openmldbSession.openmldbSql(s"load data infile '$testFile' into table $db.$table " +
        "options(mode='overwrite', header=false);")
      fail("unreachable")
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
