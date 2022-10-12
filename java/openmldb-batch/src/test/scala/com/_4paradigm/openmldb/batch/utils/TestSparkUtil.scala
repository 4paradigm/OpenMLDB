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

import com._4paradigm.hybridse.node.JoinType
import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.utils.SparkUtil.{addColumnByMonotonicallyIncreasingId, addColumnByZipWithIndex,
  addColumnByZipWithUniqueId, addIndexColumn, approximateDfEqual, checkSchemaIgnoreNullable, rddInternalRowToDf,
  smallDfEqual, supportNativeLastJoin}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp
import scala.collection.JavaConverters.seqAsJavaListConverter


class TestSparkUtil extends SparkTestSuite {

  val schemaTest1: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("time", TimestampType)
  ))
  val schemaTest2: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("length", IntegerType),
    StructField("time", TimestampType)
  ))
  val data = Seq(
    (0, Timestamp.valueOf("0001-01-01 0:0:0")),
    (0, Timestamp.valueOf("1899-04-01 0:0:0")),
    (0, Timestamp.valueOf("1900-01-01 0:0:0")),
    (0, Timestamp.valueOf("1969-01-01 0:0:0")),
    (0, Timestamp.valueOf("2000-08-01 0:0:0")),
    (0, Timestamp.valueOf("2019-09-11 0:0:0"))
  )

  test("Test supportNativeLastJoin") {
    assert(!supportNativeLastJoin(JoinType.kJoinTypeFull, hasOrderby = true))
    assert(!supportNativeLastJoin(JoinType.kJoinTypeFull, hasOrderby = false))
    assert(!supportNativeLastJoin(JoinType.kJoinTypeLast, hasOrderby = false))
  }
  test("Test addIndexColumn") {
    val Session: SparkSession = getSparkSession
    val table: DataFrame = Session.createDataFrame(data.map(Row.fromTuple(_)).asJava, schemaTest1)
    assert(addIndexColumn(Session,table,"Index","zipwithindex")
      .select("Index").distinct().count()==data.length)
    assert(addIndexColumn(Session,table,"UniqueId","zipwithuniqueid")
      .select("UniqueId").distinct().count()==data.length)
    assert(addIndexColumn(Session,table,"IncreasingId","monotonicallyincreasingid")
      .select("IncreasingId").distinct().count()==data.length)
    assertThrows[HybridSeException]{
      addIndexColumn(Session,table,"Unsupported","Unsupported method")
    }
  }
  test("Test addColumnByZipWithIndex") {
    val Session: SparkSession = getSparkSession
    val table: DataFrame = Session.createDataFrame(data.map(Row.fromTuple(_)).asJava, schemaTest1)
    assert(addColumnByZipWithIndex(Session,table,"Index")
      .select("Index").distinct().count()==data.length)
  }

  test("Test addColumnByZipWithUniqueId") {
    val Session: SparkSession = getSparkSession
    val table: DataFrame = Session.createDataFrame(data.map(Row.fromTuple(_)).asJava, schemaTest1)
    assert(addColumnByZipWithUniqueId(Session,table,"UniqueId")
      .select("UniqueId").distinct().count()==data.length)
  }

  test("Test addColumnByMonotonicallyIncreasingId") {
    val Session: SparkSession = getSparkSession
    val table: DataFrame = Session.createDataFrame(data.map(Row.fromTuple(_)).asJava, schemaTest1)
    assert(addColumnByMonotonicallyIncreasingId(Session,table,"IncreasingId")
      .select("IncreasingId").distinct().count()==data.length)
  }

  test("Test checkSchemaIgnoreNullable") {
    val schemaTest3: StructType = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", TimestampType)
    ))
    val schemaTest4: StructType = StructType(Seq(
      StructField("id", IntegerType,nullable = false),
      StructField("time", TimestampType)
    ))
    assert(!checkSchemaIgnoreNullable(schemaTest1,schemaTest2))
    assert(checkSchemaIgnoreNullable(schemaTest1,schemaTest3))
    assert(schemaTest1!=schemaTest4)
    assert(checkSchemaIgnoreNullable(schemaTest1,schemaTest4))
  }

  test("Test smallDfEqual") {
    val dataTest1 = Seq(
      (0, Timestamp.valueOf("1969-01-01 0:0:0")),
      (0, Timestamp.valueOf("2019-09-11 0:0:0"))
    )
    val dataTest2 = Seq(
      (0, Timestamp.valueOf("0001-01-01 0:0:0")),
      (0, Timestamp.valueOf("1899-04-01 0:0:0"))
    )
    val dataTest3 = Seq(
      (0, 1, Timestamp.valueOf("0001-01-01 0:0:0")),
      (0, 1, Timestamp.valueOf("1899-04-01 0:0:0"))
    )

    val Session: SparkSession = getSparkSession
    val tableTest1: DataFrame = Session.createDataFrame(dataTest1.map(Row.fromTuple(_)).asJava, schemaTest1)
    val tableTest2: DataFrame = Session.createDataFrame(dataTest2.map(Row.fromTuple(_)).asJava, schemaTest1)
    val tableTest3: DataFrame = Session.createDataFrame(dataTest3.map(Row.fromTuple(_)).asJava, schemaTest2)

    assert(smallDfEqual(tableTest1, tableTest1))
    assert(!smallDfEqual(tableTest1, tableTest2))
    assert(!smallDfEqual(tableTest1, tableTest3))
  }

  test("Test rddInternalRowToDf") {
    val dataTest1 = Seq(
      (0, Timestamp.valueOf("1969-01-01 0:0:0")),
      (0, Timestamp.valueOf("2019-09-11 0:0:0"))
    )
    val Session: SparkSession = getSparkSession
    val df1 = Session.createDataFrame(dataTest1.map(Row.fromTuple(_)).asJava, schemaTest1)
    val internalRow = df1.queryExecution.toRdd
    assert(rddInternalRowToDf(Session, internalRow, schemaTest1).collect() sameElements df1.collect())
  }

  test("Test approximateDfEqual") {
    val dataTest1 = Seq(
      (0, Timestamp.valueOf("1969-01-01 0:0:0")),
      (0, Timestamp.valueOf("2019-09-11 0:0:0"))
    )
    val dataTest2 = Seq(
      (0, Timestamp.valueOf("0001-01-01 0:0:0")),
      (0, Timestamp.valueOf("1899-04-01 0:0:0"))
    )
    val dataTest3 = Seq(
      (0, 1, Timestamp.valueOf("0001-01-01 0:0:0")),
      (0, 1, Timestamp.valueOf("1899-04-01 0:0:0"))
    )

    val Session: SparkSession = getSparkSession
    val tableTest1: DataFrame = Session.createDataFrame(dataTest1.map(Row.fromTuple(_)).asJava, schemaTest1)
    val tableTest2: DataFrame = Session.createDataFrame(dataTest2.map(Row.fromTuple(_)).asJava, schemaTest1)
    val tableTest3: DataFrame = Session.createDataFrame(dataTest3.map(Row.fromTuple(_)).asJava, schemaTest2)

    assert(approximateDfEqual(tableTest1, tableTest1))
    assert(!approximateDfEqual(tableTest1, tableTest2))
    assert(!approximateDfEqual(tableTest1, tableTest3))
  }

}

