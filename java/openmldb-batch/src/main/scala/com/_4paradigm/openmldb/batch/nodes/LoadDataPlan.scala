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
package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.hybridse.vm.PhysicalLoadDataNode
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object LoadDataPlan {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def parseOptions(node: PhysicalLoadDataNode): (String, String, Map[String, String]) = {
    var format = "csv"
    var option = node.GetOption("format")
    if (option != null && option.GetStr() != null) {
      val f = option.GetStr()
      if (f.compareToIgnoreCase("csv") != 0 && f.compareToIgnoreCase("parquet") != 0) {
        throw new HybridSeException("file format unsupported")
      }
      format = f
    }

    var mode = "ErrorIfExists"
    option = node.GetOption("mode")
    if (option != null) {
      val m = option.GetStr().toLowerCase
      // overwrite/append need to reassign
      if (m != "error_if_exists") {
        mode = m
      }
    }

    val options: mutable.Map[String, String] = mutable.Map()
    // default values:
    // delimiter -> sep: ,
    // header: true(different with spark)
    // null_value -> nullValue: null(different with spark)
    // quote: '\0'(means no quote, the same with spark quote "empty string")
    options += ("header" -> "true")
    options += ("nullValue" -> "null")

    option = node.GetOption("delimiter")
    if (option != null) {
      options += ("sep" -> option.GetStr())
    }
    option = node.GetOption("header")
    if (option != null) {
      // boolean to str
      options += ("header" -> option.GetBool().toString)
    }
    option = node.GetOption("null_value")
    if (option != null) {
      options += ("nullValue" -> option.GetStr())
    }
    option = node.GetOption("quote")
    if (option != null) {
      options += ("quote" -> option.GetStr())
    }

    (format, mode, options.toMap)
  }

  def gen(ctx: PlanContext, node: PhysicalLoadDataNode): SparkInstance = {
    // TODO(hw): get offline address from nameserver by db.table, "hdfs://"
    val offlineAddress = "/tmp/test_dir"

    val inputFile = node.File()
    val spark = ctx.getSparkSession

    // read input file
    val (format, mode, options) = parseOptions(node)
    logger.info("format {}, mode {}, options {}", format, mode, options)
    val df = spark.read.options(options).format(format).load(inputFile)

    // write, offline address may contains some files, should choose the mode
    df.write.mode(mode).parquet(offlineAddress)

    SparkInstance.fromDataFrame(spark.emptyDataFrame)
  }
}
