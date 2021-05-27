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

package com._4paradigm.hybridsql.spark.api

import com._4paradigm.hybridsql.spark.{SparkPlanner, SparkFeConfig}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
 * The class to provide SparkSession-like API.
 */
class SparkFeSession {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private var sparkSession: SparkSession = null
  private var sparkMaster: String = null

  val registeredTables = mutable.HashMap[String, DataFrame]()

  private var config: SparkFeConfig = _

  var planner: SparkPlanner = _

  /**
   * Construct with Spark session.
   *
   * @param sparkSession
   */
  def this(sparkSession: SparkSession) = {
    this()
    this.sparkSession = sparkSession
    this.config = SparkFeConfig.fromSparkSession(sparkSession)
    this.sparkSession.conf.set("spark.sql.session.timeZone", config.timeZone)
  }

  /**
   * Get or create the Spark session.
   *
   * @return
   */
  def getSparkSession: SparkSession = {
    this.synchronized {
      if (this.sparkSession == null) {

        if (this.sparkMaster == null) {
          this.sparkMaster = "local"
        }

        logger.debug("Create new SparkSession with master={}", this.sparkMaster)
        val sparkConf = new SparkConf()
        val sparkMaster = sparkConf.get("spark.master", this.sparkMaster)
        val builder = SparkSession.builder()

        // TODO: Need to set for official Spark 2.3.0 jars
        logger.debug("Set spark.hadoop.yarn.timeline-service.enabled as false")
        builder.config("spark.hadoop.yarn.timeline-service.enabled", value = false)

        this.sparkSession = builder.appName("SparkFeApp")
          .master(sparkMaster)
          .getOrCreate()
      }

      this.sparkSession
    }
  }

  /**
   * Read the file with get dataframe with Spark API.
   *
   * @param filePath
   * @param format
   * @return
   */
  def read(filePath: String, format: String = "parquet"): SparkFeDataframe = {
    val spark = this.getSparkSession

    val sparkDf = format match {
      case "parquet" => spark.read.parquet(filePath)
      case "csv" => spark.read.csv(filePath)
      case "json" => spark.read.json(filePath)
      case "text" => spark.read.text(filePath)
      case "orc" => spark.read.orc(filePath)
      case _ => null
    }

    new SparkFeDataframe(this, sparkDf)
  }


  /**
   * Read the Spark dataframe to SparkFE dataframe.
   *
   * @param sparkDf
   * @return
   */
  def readSparkDataframe(sparkDf: DataFrame): SparkFeDataframe = {
    SparkFeDataframe(this, sparkDf)
  }

  /**
   * Run sql.
   *
   * @param sqlText
   * @return
   */
  def sparkFeSql(sqlText: String): SparkFeDataframe = {
    var sql: String = sqlText
    if (!sql.trim.endsWith(";")) {
      sql = sql.trim + ";"
    }

    val planner = new SparkPlanner(getSparkSession, config)
    this.planner = planner
    val df = planner.plan(sql, registeredTables.toMap).getDf()
    new SparkFeDataframe(this, df)
  }

  /**
   * Run sql.
   *
   * @param sqlText
   * @return
   */
  def sql(sqlText: String): SparkFeDataframe = {
    sparkFeSql(sqlText)
  }

  /**
   * Run sql with Spark SQL API.
   *
   * @param sqlText
   * @return
   */
  def sparksql(sqlText: String): SparkFeDataframe = {
    // Use Spark internal implementation because we may override sql function in 4PD Spark distribution
    val tracker = new QueryPlanningTracker
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      getSparkSession.sessionState.sqlParser.parsePlan(sqlText)
    }

    // Call private method Dataset.ofRows()
    val datasetClass = Class.forName("org.apache.spark.sql.Dataset")
    val datasetOfRowsMethod = datasetClass
      .getDeclaredMethod(s"ofRows", classOf[SparkSession], classOf[LogicalPlan], classOf[QueryPlanningTracker])
    val outputDataset = datasetOfRowsMethod.invoke(null, getSparkSession, plan, tracker).asInstanceOf[Dataset[Row]]

    SparkFeDataframe(this, outputDataset)
  }

  /**
   * Get the version from git commit message.
   */
  def version(): Unit = {
    val stream = this.getClass.getClassLoader.getResourceAsStream("sparkfe_git.properties")
    if (stream == null) {
      logger.warn("Project version is missing")
    } else {
      IOUtils.copy(stream, System.out)
    }
  }

  /**
   * Record the registered tables to run.
   *
   * @param name
   * @param df
   */
  def registerTable(name: String, df: DataFrame): Unit = {
    registeredTables.put(name, df)
  }

  /**
   * Return the string of Spark session.
   *
   * @return
   */
  override def toString: String = {
    sparkSession.toString()
  }

  /**
   * Stop the Spark session.
   */
  def stop(): Unit = {
    sparkSession.stop()
  }

}
