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

package com._4paradigm.openmldb.batch.api

import com._4paradigm.openmldb.batch.catalog.OpenmldbCatalogService
import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, SparkPlanner}
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
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
class OpenmldbSession {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private var sparkSession: SparkSession = _
  private var sparkMaster: String = _

  // The map of "DatabaseName -> TableName -> Spark DataFrame"
  val registeredTables: mutable.Map[String, mutable.Map[String, DataFrame]] =
    mutable.HashMap[String, mutable.Map[String, DataFrame]]()

  private var config: OpenmldbBatchConfig = _

  var planner: SparkPlanner = _

  var openmldbCatalogService: OpenmldbCatalogService = _

  /**
   * Construct with Spark session.
   *
   * @param sparkSession the SparkSession object
   */
  def this(sparkSession: SparkSession) = {
    this()
    this.sparkSession = sparkSession
    this.config = OpenmldbBatchConfig.fromSparkSession(sparkSession)
    this.setDefaultSparkConfig()

    if (this.config.openmldbZkCluster.nonEmpty && this.config.openmldbZkRootPath.nonEmpty) {
      openmldbCatalogService = new OpenmldbCatalogService(this.config.openmldbZkCluster, this.config.openmldbZkRootPath,
        config.openmldbJsdkLibraryPath)
      registerOpenmldbOfflineTable(openmldbCatalogService)
    }
  }

  /**
   * Get the config of Openmldb session.
   *
   * @return
   */
  def getOpenmldbBatchConfig: OpenmldbBatchConfig = {
    this.config
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
        //logger.debug("Set spark.hadoop.yarn.timeline-service.enabled as false")
        //builder.config("spark.hadoop.yarn.timeline-service.enabled", value = false)

        this.sparkSession = builder.getOrCreate()
      }

      this.sparkSession
    }
  }

  def setDefaultSparkConfig(): Unit = {
    val sparkConf = this.sparkSession.conf
    // Set timezone
    sparkConf.set("spark.sql.session.timeZone", config.timeZone)
  }

  /**
   * Read the file with get dataframe with Spark API.
   *
   * @param filePath the path to read
   * @param format   the format of data
   * @return
   */
  def read(filePath: String, format: String = "parquet"): OpenmldbDataframe = {
    val spark = this.getSparkSession

    val sparkDf = format match {
      case "parquet" => spark.read.parquet(filePath)
      case "csv" => spark.read.csv(filePath)
      case "json" => spark.read.json(filePath)
      case "text" => spark.read.text(filePath)
      case "orc" => spark.read.orc(filePath)
      case _ => null
    }

    OpenmldbDataframe(this, sparkDf)
  }


  /**
   * Read the Spark dataframe to OpenMLDB dataframe.
   *
   * @param sparkDf the Spark DataFrame object
   * @return
   */
  def readSparkDataframe(sparkDf: DataFrame): OpenmldbDataframe = {
    OpenmldbDataframe(this, sparkDf)
  }

  /**
   * Run sql.
   *
   * @param sqlText the SQL script
   * @return
   */
  def openmldbSql(sqlText: String): OpenmldbDataframe = {
    if (config.disableOpenmldb) {
      return OpenmldbDataframe(this, sparksql(sqlText))
    }

    val planner = new SparkPlanner(this, config)
    this.planner = planner
    val df = planner.plan(sqlText, registeredTables).getDf()
    OpenmldbDataframe(this, df)
  }

  /**
   * Run sql.
   *
   * @param sqlText the SQL script
   * @return
   */
  def sql(sqlText: String): OpenmldbDataframe = {
    openmldbSql(sqlText)
  }

  /**
   * Run sql with Spark SQL API.
   *
   * @param sqlText the SQL script
   * @return
   */
  def sparksql(sqlText: String): DataFrame = {
    // Use Spark internal implementation because we may override sql function in 4PD Spark distribution
    val tracker = new QueryPlanningTracker
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      getSparkSession.sessionState.sqlParser.parsePlan(sqlText)
    }

    // Call private method Dataset.ofRows()
    val datasetClass = Class.forName("org.apache.spark.sql.Dataset")
    val datasetOfRowsMethod = datasetClass
      .getDeclaredMethod(s"ofRows", classOf[SparkSession], classOf[LogicalPlan], classOf[QueryPlanningTracker])
    datasetOfRowsMethod.invoke(null, getSparkSession, plan, tracker).asInstanceOf[Dataset[Row]]
  }

  /**
   * Get the version from git commit message.
   */
  def version(): String = {
    // Read OpenMLDB git properties which is added by maven plugin
    val stream = this.getClass.getClassLoader.getResourceAsStream("openmldb_git.properties")
    if (stream == null) {
      logger.error("OpenMLDB git properties is missing")
      s"${sparkSession.version}"
    } else {
      val gitInfo = IOUtils.toString(stream, "UTF-8")
      s"${sparkSession.version}\n$gitInfo"
    }
  }

  def registerTable(dbName: String, tableName: String, df: DataFrame): Unit = {
    if (!registeredTables.contains(dbName)) {
      registeredTables.put(dbName, new mutable.HashMap[String, DataFrame]())
    }
    registeredTables(dbName).put(tableName, df)
  }

  /**
   * Record the registered tables to run.
   *
   * @param tableName the registered name of table
   * @param df        the Spark DataFrame
   */
  def registerTable(tableName: String, df: DataFrame): Unit = {
    registerTable(config.defaultDb, tableName, df)
  }

  /**
   * Return the string of Spark session.
   *
   * @return
   */
  override def toString: String = {
    sparkSession.toString
  }

  /**
   * Stop the Spark session.
   */
  def stop(): Unit = {
    sparkSession.close()
  }

  def close(): Unit = stop()

  def registerOpenmldbOfflineTable(catalogService: OpenmldbCatalogService): Unit = {
    val databases = catalogService.getDatabases
    databases.map(dbName => {
      val tableInfos = catalogService.getTableInfos(dbName)
      tableInfos.map(tableInfo => {
        val tableName = tableInfo.getName
        val offlineTableInfo = tableInfo.getOfflineTableInfo

        if (offlineTableInfo != null) { // offlineTableInfo is always not null
          val path = offlineTableInfo.getPath
          val format = offlineTableInfo.getFormat

          // TODO: Ignore the register exception which occurs when switching local and yarn mode
          try {
            // default offlineTableInfo required members 'path' & 'format' won't be null
            if (path != null && path.nonEmpty && format != null && format.nonEmpty) {
              // Has offline table meta
              val df = format.toLowerCase match {
                case "parquet" => sparkSession.read.parquet(path)
                case "csv" => sparkSession.read.csv(path)
              }
              // TODO: Check schema
              registerTable(dbName, tableName, df)
            } else {
              // Register empty df for table
              logger.info(s"Register empty dataframe fof $dbName.$tableName")
              // TODO: Create empty df with schema
              registerTable(dbName, tableName, sparkSession.emptyDataFrame)
            }
          } catch {
            case e: Exception => logger.warn(s"Fail to register table $dbName.$tableName, error: ${e.getMessage}")
          }
        }
      })
    })
  }

}
