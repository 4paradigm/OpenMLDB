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

package com._4paradigm.hybridsql.spark.nodes.window

import com._4paradigm.hybridsql.spark.{SparkFeConfig, SparkPlanner, SparkRowCodec, SparkTestSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class TestWindowComputerWithSampleSupport extends SparkTestSuite {

  test("Test sample window data") {
    val config = new SparkFeConfig
    val samplePath = "src/test/resources/sparkfe_windows/"
    executeSpark(samplePath)
    val sampleExecutor = WindowSampleSupport.recover(config, samplePath + "/w/0", 0)

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

    val config = new SparkFeConfig
    config.groupPartitions = 1
    config.windowSampleMinSize = 3
    config.windowSampleOutputPath = samplePath
    config.windowSampleFilter = "id=0, time=3"
    config.windowSampleBeforeCompute = false
    config.print = true
    config.printRowContent = true
    config.printSampleInterval = 1

    val planner = new SparkPlanner(sess, config)
    val res = planner.plan(sql, Map("t" -> table))
    val output = res.getDf()
    output.show()
  }
}
