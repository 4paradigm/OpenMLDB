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

package com._4paradigm.openmldb.spark

import com._4paradigm.openmldb.sdk.SdkOption
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.FunSuite

import java.lang.Thread.currentThread
import java.util.Properties

class TestRead extends FunSuite {

  test("Test use connector to read openmldb online table") {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val prop = new Properties
    prop.load(getClass.getResourceAsStream("/test.properties"))
    val zkCluster = prop.getProperty("openmldb.zk.cluster", "127.0.0.1:6181")
    val zkPath = prop.getProperty("openmldb.zk.root.path", "/onebox")
    val db = "db"
    val table = "spark_read_test"
    val options = Map("db" -> db, "table" -> table, "zkCluster" -> zkCluster, "zkPath" -> zkPath)

    val option = new SdkOption
    option.setZkCluster(zkCluster)
    option.setZkPath(zkPath)
    val executor = new SqlClusterExecutor(option)
    executor.createDB(db)
    executor.executeDDL(db, s"drop table $table")
    executor.executeDDL(db, s"create table $table(c2 smallint, c3 int, c4 bigint, c5 float, c6 double," +
      "c7 string)")
    executor.executeSQL(db, "set @@SESSION.execute_mode='online'")
    executor.executeSQL(db, s"insert into $table values(1, 2, 3, 4.0, 5.0, 'foo')")

    val df = spark.read
      .format("openmldb")
      .options(options)
      .load()

    val row = df.collect()(0)
    assert(row.getShort(0) == 1)
    assert(row.getInt(1) == 2)
    assert(row.getLong(2) == 3L)
    assert(row.getFloat(3) == 4.0)
    assert(row.getDouble(4) == 5.0)
    assert(row.getString(5).equals("foo"))
  }
}
