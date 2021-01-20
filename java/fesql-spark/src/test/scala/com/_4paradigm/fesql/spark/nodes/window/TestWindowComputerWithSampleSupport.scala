package com._4paradigm.fesql.spark.nodes.window

import com._4paradigm.fesql.spark.{FeSQLConfig, SparkPlanner, SparkRowCodec, SparkTestSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestWindowComputerWithSampleSupport extends SparkTestSuite {

  test("Test sample window data") {
    val config = new FeSQLConfig
    val samplePath = "src/test/resources/fesql_windows/"
    executeSpark(samplePath)
    val sampleExecutor = WindowSampleSupport.recover(config, samplePath + "/w/0")

    // run single compute
    val nativeOutput = sampleExecutor.run()

    // decode
    val outputSchemas = sampleExecutor.config.outputSchemaSlices
    val decoder = new SparkRowCodec(outputSchemas)
    val arr = new Array[Any](outputSchemas.map(_.size).sum)
    decoder.decode(nativeOutput, arr)
    println(arr.mkString(", "))
    assert(arr(0) == 0)
    assert(arr(1) == 3)
    assert(arr(2) == 3.0)
    assert(arr(3) == 6.0)
  }

  private def executeSpark(samplePath: String): Unit = {
    val sess = getSparkSession
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time", LongType),
      StructField("amt", DoubleType)
    ))
    val data = Seq(
      (0, 1L, 1.0),
      (0, 2L, 2.0),
      (0, 3L, 3.0)
    )
    val table = sess.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)
    val sql ="""
               | SELECT id, `time`, amt, sum(amt) OVER w AS w_amt_sum FROM t
               | WINDOW w AS (
               |    PARTITION BY id
               |    ORDER BY `time`
               |    ROWS BETWEEN 3 PRECEDING AND 0 FOLLOWING);"
             """.stripMargin

    val config = new FeSQLConfig
    config.groupPartitions = 1
    config.windowSampleMinSize = 3
    config.windowSampleOutputPath = samplePath

    val planner = new SparkPlanner(sess, config)
    val res = planner.plan(sql, Map("t" -> table))
    val output = res.getDf(sess)
    output.show()
  }
}
