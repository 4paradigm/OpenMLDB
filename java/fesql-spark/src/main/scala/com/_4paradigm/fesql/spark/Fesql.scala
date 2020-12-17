package com._4paradigm.fesql.spark

import java.io.File
import com._4paradigm.fesql.utils.SqlUtils._

import com._4paradigm.fesql.spark.api.FesqlSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source


object Fesql {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val sparkMaster = "local"
  private val appName: String = "fesql"

  def main(args: Array[String]): Unit = {
    run(args)
  }

  def run(args: Array[String]): Unit = {
    val path = args(0)
    val config = parseFeconfigJsonPath(path)
    val sessionBuilder = SparkSession.builder().master(sparkMaster)
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

    val sess = new FesqlSession(sparkSession)

    val sqlScript = config.getSql
    val inputTables = config.getTables
    for ((name, path) <- inputTables.asScala) {
      logger.info(s"Try load table $name from: $path")
      sess.read(path).createOrReplaceTempView(name)
    }

    val output = config.getOutputPath
    val res = sess.fesql(sqlScript)
    if (config.getInstanceFormat.equals("parquet")) {
      res.sparkDf.write.mode("overwrite").parquet(output)
    }
    if (config.getInstanceFormat.equals("csv")) {
      res.sparkDf.write.mode("overwrite").csv(output)
    }
    sess.stop()
  }


}
