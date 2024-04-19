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

import com._4paradigm.hybridse.HybridSeLibrary
import com._4paradigm.hybridse.sdk.SqlEngine
import com._4paradigm.hybridse.vm.{Engine, EngineOptions}
import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.utils.GraphvizUtil.{drawPhysicalPlan, getGraphNode, visitPhysicalOp}
import guru.nidi.graphviz.model.MutableNode
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import scala.collection.JavaConverters.seqAsJavaListConverter
import java.sql.Timestamp
import scala.collection.mutable

class TestGraphvizUtil extends SparkTestSuite {

  HybridSeLibrary.initCore()
  Engine.InitializeGlobalLLVM()
  val schema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("time", TimestampType),
    StructField("amt", DoubleType)
  ))
  val data = Seq(
    (1, Timestamp.valueOf("2001-01-01 0:0:0"), 1.0),
    (1, Timestamp.valueOf("2009-04-01 0:0:0"), 1.0),
    (1, Timestamp.valueOf("2900-01-01 0:0:0"), 1.0),
    (0, Timestamp.valueOf("2009-01-01 0:0:0"), 1.0),
    (0, Timestamp.valueOf("2000-08-01 0:0:0"), 1.0),
    (0, Timestamp.valueOf("2019-09-11 0:0:0"), 1.0)
  )
  val sql: String =
    """
      | SELECT id, sum(amt) amt_sum FROM t
      | GROUP BY id;
     """.stripMargin

  test("Test drawPhysicalPlan") {
    val engineOptions: EngineOptions = SqlEngine.createDefaultEngineOptions()
    val Session = getSparkSession
    val table = Session.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)

    val path = "target/test-classes/TestGraphvizUtil/drawPhysicalPlan.png"

    val engine = new SqlEngine(sql, HybridseUtil.getDatabase("spark_db", Map("t" -> table)), engineOptions)
    val root = engine.getPlan
    drawPhysicalPlan(root, path)

    if (engine != null) {
      engine.close()
    }

    assert(new File(path).exists())
  }

  test("Test getGraphNode") {
    val engineOptions: EngineOptions = SqlEngine.createDefaultEngineOptions()
    val sess = getSparkSession

    val table = sess.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)

    val engine = new SqlEngine(sql, HybridseUtil.getDatabase("spark_db", Map("t" -> table)), engineOptions)
    val root = engine.getPlan
    val mutablenode = getGraphNode(root)

    if (engine != null) {
      engine.close()
    }

    assert(mutablenode.toString == "[87]GroupAgg{}->[22]GroupBy::")
  }

  test("Test visitPhysicalOp") {
    val engineOptions: EngineOptions = SqlEngine.createDefaultEngineOptions()
    val sess = getSparkSession

    val table = sess.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)

    val engine = new SqlEngine(sql, HybridseUtil.getDatabase("spark_db", Map("t" -> table)), engineOptions)
    val root = engine.getPlan
    val children = mutable.ArrayBuffer[MutableNode]()
    val mutablenode = visitPhysicalOp(root, children.toArray)

    if (engine != null) {
      engine.close()
    }
    assert(mutablenode.toString == "[87]GroupAgg{}->")
  }
}
