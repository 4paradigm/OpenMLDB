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

import java.io.File

import com._4paradigm.hybridse.spark.api.FesqlSession
import com._4paradigm.hybridse.spark.utils.ArgumentParser
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source


object Main {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val inputSpecs = mutable.HashMap[String, String]()
  private val configs = mutable.HashMap[String, Any]()
  private var sql: String = _
  private var outputPath: String = _
  private var sparkMaster = "local"
  private var appName: String = _
  private var useSparkSQL = false
  private var jsonPath: String = _

  def main(args: Array[String]): Unit = {
    val parser = new ArgumentParser(args)
    parser.parseArgs {
      case "-i" | "--input" => inputSpecs += parser.parsePair()
      case "-s" | "--sql" => sql = parser.parseValue()
      case "-o" | "--output" => outputPath = parser.parseValue()
      case "-c" | "--conf" => configs += parser.parsePair()
      case "--master" => sparkMaster = parser.parseValue()
      case "--name" => appName = parser.parseValue()
      case "--spark-sql" => useSparkSQL = true
      case "--json" => jsonPath = parser.parseValue()
      case other =>
        logger.warn(s"Unknown argument: $other")
    }
    run()
  }

  def run(): Unit = {
    logger.info("Create FeSQL Spark Planner...")
    val sessionBuilder = SparkSession.builder().master(sparkMaster)
    if (appName != null) {
      sessionBuilder.appName(appName)
    }
    for ((k, v) <- configs) {
      if (k.startsWith("spark.")) {
        sessionBuilder.config(k, v.toString)
      }
    }
    val sparkSession = sessionBuilder.getOrCreate()

    val sess = new FesqlSession(sparkSession)

    logger.info("Resolve input tables...")
    for ((name, path) <- inputSpecs) {
      logger.info(s"Try load table $name from: $path")
      sess.read(path).createOrReplaceTempView(name)
    }

    if (sql == null) {
      throw new IllegalArgumentException("No sql script specified")
    }
    val sqlFile = new File(sql)
    if (sqlFile.exists()) {
      sql = Source.fromFile(sqlFile).mkString("")
    }
    logger.info("SQL Script:\n" + sql)

    var startTime = System.currentTimeMillis()
    val outputDf = if (useSparkSQL) {
      sess.sparksql(sql)
    } else {
      sess.sql(sql)
    }

    var endTime = System.currentTimeMillis()
    logger.info(f"Compile SQL time cost: ${(endTime - startTime) / 1000.0}%.2f seconds")

    startTime = System.currentTimeMillis()
    if (outputPath != null) {
      logger.info(s"Save result to: $outputPath")
      outputDf.write(outputPath)
    } else {
      val count = outputDf.getSparkDf().queryExecution.toRdd.count()
      logger.info(s"Result records count: $count")
    }
    endTime = System.currentTimeMillis()
    logger.info(f"Execution time cost: ${(endTime - startTime) / 1000.0}%.2f seconds")
  }
}
