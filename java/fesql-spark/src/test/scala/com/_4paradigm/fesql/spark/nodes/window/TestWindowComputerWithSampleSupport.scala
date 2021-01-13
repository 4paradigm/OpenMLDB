package com._4paradigm.fesql.spark.nodes.window

import com._4paradigm.fesql.spark.element.FesqlConfig
import com._4paradigm.fesql.spark.{SparkPlanner, SparkRowCodec, SparkTestSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestWindowComputerWithSampleSupport extends SparkTestSuite {

  test("Test sample window data") {
    val samplePath = "src/test/resources/fesql_windows/"
    executeSpark(samplePath)
    val sampleExecutor = WindowComputerWithSampleSupport.recover(samplePath, "w")

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
    val config =  Map(
      "spark.fesql.group.partitions" -> 1,
      "fesql.window.sampleMinSize" -> 3,
      "fesql.window.sampleOutputPath" -> samplePath
    )
    sess.conf.set("spark.fesql.group.partitions", "1")
    val planner = new SparkPlanner(sess, config)
    FesqlConfig.paritions = 1
    val res = planner.plan(sql, Map("t" -> table))
    val output = res.getDf(sess)
    output.show()
  }
}
