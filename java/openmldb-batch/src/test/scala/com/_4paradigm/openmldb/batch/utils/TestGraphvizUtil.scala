package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.HybridSeLibrary
import com._4paradigm.hybridse.sdk.SqlEngine
import com._4paradigm.hybridse.vm.{Engine, EngineOptions}
import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.utils.GraphvizUtil.{drawPhysicalPlan, getGraphNode}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import scala.collection.JavaConverters.seqAsJavaListConverter
import java.sql.Timestamp

class TestGraphvizUtil extends SparkTestSuite {

  HybridSeLibrary.initCore()
  Engine.InitializeGlobalLLVM()

  test("Test drawPhysicalPlan") {
    val engineOptions: EngineOptions = SqlEngine.createDefaultEngineOptions()
    val Session = getSparkSession

    val sql ="""
               | SELECT id, `time`, amt, sum(amt) OVER w AS w_amt_sum FROM t
               | WINDOW w AS (
               |    PARTITION BY id
               |    ORDER BY `time`
               |    ROWS BETWEEN 3 PRECEDING AND 0 FOLLOWING);
     """.stripMargin

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", TimestampType),
      StructField("amt", DoubleType)
    ))

    val data = Seq(
      (0, Timestamp.valueOf("0001-01-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("1899-04-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("1900-01-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("1969-01-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("2000-08-01 0:0:0"), 1.0),
      (0, Timestamp.valueOf("2019-09-11 0:0:0"), 1.0)
    )

    val table = Session.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)

    val path = getClass.getResource("").getPath + "/TestGraphvizUtil/drawPhysicalPlan.png"

    val engine = new SqlEngine(sql, HybridseUtil.getDatabase("spark_db", Map("t" -> table)), engineOptions)
    val root = engine.getPlan
    drawPhysicalPlan(root,path)

    if (engine != null) {
      engine.close()
    }

    assert(new File(path).exists())
  }

  test("Test getGraphNode") {
    val engineOptions: EngineOptions = SqlEngine.createDefaultEngineOptions()
    val sess = getSparkSession

    val sql ="""
               | SELECT id, `time`, amt, sum(amt) OVER w AS w_amt_sum FROM t
               | GROUP BY id
               | WINDOW w AS (
               |    PARTITION BY id
               |    ORDER BY `time`
               |    ROWS BETWEEN 3 PRECEDING AND 0 FOLLOWING);
     """.stripMargin

    val schema = StructType(Seq(
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

    val table = sess.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)

    val engine = new SqlEngine(sql, HybridseUtil.getDatabase("spark_db", Map("t" -> table)), engineOptions)
    val root = engine.getPlan
    val mutablenode = getGraphNode(root)

    if (engine != null) {
      engine.close()
    }
    
    assert(mutablenode.toString=="[77]GroupAgg{}->[45]GroupBy::")
  }
}
