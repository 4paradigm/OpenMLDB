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
import com._4paradigm.openmldb.batch.utils.SparkUtil
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType,
  StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}


class TestInsertPlan extends SparkTestSuite {
  var sparkSession: SparkSession = _
  var openmldbSession: OpenmldbSession = _
  var openmldbConnector: SqlClusterExecutor = _
  val db = "offline_insert_test"

  override def customizedBefore(): Unit = {
    sparkSession = getSparkSession()
    openmldbSession = new OpenmldbSession(sparkSession)
    openmldbConnector = openmldbSession.openmldbCatalogService.sqlExecutor
    openmldbConnector.createDB(db)
    openmldbConnector.refreshCatalog()
  }

  override def customizedAfter(): Unit = {
    val tables = openmldbConnector.getTableNames(db)
    tables.forEach(table => openmldbConnector.executeDDL(db, s"drop table $table;"))
    openmldbConnector.dropDB(db)
  }

  override def getSparkSession(): SparkSession = {
    val zkHost = "localhost:2181"
    val zkPath = "/openmldb"
    SparkSession.builder()
      .master("local")
      .config("openmldb.zk.cluster", zkHost)
      .config("openmldb.zk.root.path", zkPath)
      .getOrCreate()
  }

  test("Test multi data type") {
    val table = "t1"
    openmldbConnector.executeDDL(db,
      s"create table $table(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 timestamp," +
        s" c8 date, c9 bool);")
    openmldbConnector.refreshCatalog()
    assert(openmldbConnector.getTableInfo(db, table).getName.nonEmpty)

    val sql = s"insert into $db.$table values ('aa', 1, 5, 1.2, 2.4, '2024-04-08 12:00:00', 1712548801000, " +
      s"'2024-04-08', true)"
    openmldbSession.sql(sql)
    val querySess = new OpenmldbSession(sparkSession)
    val queryResult = querySess.sql(s"select * from $db.$table")

    val schema = StructType(Seq(
      StructField("c1", StringType, nullable = true),
      StructField("c2", IntegerType, nullable = true),
      StructField("c3", LongType, nullable = true),
      StructField("c4", FloatType, nullable = true),
      StructField("c5", DoubleType, nullable = true),
      StructField("c6", TimestampType, nullable = true),
      StructField("c7", TimestampType, nullable = true),
      StructField("c8", DateType, nullable = true),
      StructField("c9", BooleanType, nullable = true)
    ))
    val expectDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(Row("aa", 1, 5L, 1.2f, 2.4d, Timestamp.valueOf("2024-04-08 12:00:00"),
        Timestamp.valueOf("2024-04-08 12:00:01"), Date.valueOf("2024-04-08"), true))),
      schema)
    assert(SparkUtil.approximateDfEqual(expectDf, queryResult.getSparkDf()))
  }

  test("Test multi rows") {
    val table = "t2"
    openmldbConnector.executeDDL(db, s"create table $table(c1 string, c2 int);")
    openmldbConnector.refreshCatalog()
    assert(openmldbConnector.getTableInfo(db, table).getName.nonEmpty)

    val sql = s"insert into $db.$table values ('a', 1), ('b', 2)"
    openmldbSession.sql(sql)

    val querySess = new OpenmldbSession(sparkSession)
    val queryResult = querySess.sql(s"select * from $db.$table")

    val schema = StructType(Seq(
      StructField("c1", StringType, nullable = true),
      StructField("c2", IntegerType, nullable = true)
    ))
    val expectDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(Row("a", 1), Row("b", 2))),
      schema)
    assert(SparkUtil.approximateDfEqual(expectDf, queryResult.getSparkDf()))
  }

  test("Test random columns and empty column") {
    val table = "t3"
    openmldbConnector.executeDDL(db, s"create table $table(c1 string, c2 int);")
    openmldbConnector.refreshCatalog()
    assert(openmldbConnector.getTableInfo(db, table).getName.nonEmpty)

    val sql1 = s"insert into $db.$table (c2, c1) values (1, 'a')"
    openmldbSession.sql(sql1)
    val sql2 = s"insert into $db.$table (c1) values ('b')"
    openmldbSession.sql(sql2)

    val querySess = new OpenmldbSession(sparkSession)
    val queryResult = querySess.sql(s"select * from $db.$table")

    val schema = StructType(Seq(
      StructField("c1", StringType, nullable = true),
      StructField("c2", IntegerType, nullable = true)
    ))
    val expectDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(Row("a", 1), Row("b", null))),
      schema)
    assert(SparkUtil.approximateDfEqual(expectDf, queryResult.getSparkDf()))
  }

  test("Test exceptions") {
    val table = "t4"
    openmldbConnector.executeDDL(db, s"create table $table(c1 int not null, c2 int);")
    openmldbConnector.refreshCatalog()
    assert(openmldbConnector.getTableInfo(db, table).getName.nonEmpty)

    val sql1 = s"insert into $db.$table (c1, c2) values (1, 'a')"
    assertThrows[IllegalArgumentException](openmldbSession.sql(sql1))

    val sql2 = s"insert into $db.$table (c1, c3) values (1, 1)"
    assertThrows[IllegalArgumentException](openmldbSession.sql(sql2))

    val sql3 = s"insert into $db.$table values (1, 1, 1)"
    assertThrows[IllegalArgumentException](openmldbSession.sql(sql3))

    val sql4 = s"insert into $db.$table (c2) values (1)"
    assertThrows[IllegalArgumentException](openmldbSession.sql(sql4))

    val sql5 = s"insert into $db.$table (c1, c2) values (1)"
    assertThrows[IllegalArgumentException](openmldbSession.sql(sql5))

    val sql6 = s"insert into $db.$table (c1) values (1, 1)"
    assertThrows[IllegalArgumentException](openmldbSession.sql(sql6))
  }
}
