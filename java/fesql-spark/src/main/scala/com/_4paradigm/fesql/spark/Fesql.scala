/*
 * java/fesql-spark/src/main/scala/com/_4paradigm/fesql/spark/Fesql.scala
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

package com._4paradigm.fesql.spark

import com._4paradigm.fesql.common.DDLEngine._
import com._4paradigm.fesql.element.SparkConfig
import com._4paradigm.fesql.utils.SqlUtils._
import com._4paradigm.fesql.spark.api.FesqlSession
import com._4paradigm.fesql.spark.utils.{FesqlUtil, HDFSUtil}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory



object Fesql {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val sparkMaster = "cluster"
  private val appName: String = "fesql"

  def main(args: Array[String]): Unit = {
    run(args)
  }

  def run(config: SparkConfig): Unit = {
    val sessionBuilder = SparkSession.builder()
    if (appName != null) {
      sessionBuilder.appName(appName)
    }

    import scala.collection.JavaConverters._
    for (e <- config.getSparkConfig.asScala) {
      val arg: Array[String]  = e.split("=")
      val k = arg(0)
      val v = arg(1)
      if (k.startsWith("spark.")) {
        sessionBuilder.config(k, v)
      }
    }
    val sparkSession = sessionBuilder.getOrCreate()

    val feSQLConfig = FeSQLConfig.fromSparkSession(sparkSession)

    val sess = new FesqlSession(sparkSession)
    sess.version()

    val sqlScript = config.getSql
    val inputTables = config.getTables
    for ((name, path) <- inputTables.asScala) {
      logger.info(s"Try load table $name from: $path")

      if (feSQLConfig.tinyData > 0) {
        sess.read(path).tiny(feSQLConfig.tinyData).createOrReplaceTempView(name)
      } else {
        val df = sess.read(path)
        df.createOrReplaceTempView(name)
        logger.info(s"schema=${df.sparkDf.schema.toDDL}")
      }
    }
    val feconfig = sql2Feconfig(sqlScript, FesqlUtil.getDatabase(feSQLConfig.configDBName, sess.registeredTables.toMap))//parseOpSchema(rquestEngine.getPlan)
    val tableInfoRDD = sess.getSparkSession.sparkContext.parallelize(Seq(feconfig)).repartition(1)
    HDFSUtil.deleteIfExist(config.getOutputPath + "/config")
    tableInfoRDD.saveAsTextFile(config.getOutputPath + "/config")

    val output = config.getOutputPath + "/data"
    val res = sess.fesql(sqlScript)
    logger.info(s"output schema:${res.sparkDf.schema.toDDL}")

    res.sparkDf.show(100)
    logger.info("fesql compute is done")


    if (config.getInstanceFormat.equals("parquet")) {
      res.sparkDf.write.mode("overwrite").parquet(output)
    }
    if (config.getInstanceFormat.equals("csv")) {
      res.sparkDf.write.mode("overwrite").csv(output)
    }
    sess.stop()
  }

  def run(args: Array[String]): Unit = {
    val path = args(0)
    val config = parseFeconfigJsonPath(path)
    run(config)
  }

}
