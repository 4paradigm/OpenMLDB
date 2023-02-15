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
import com._4paradigm.openmldb.batch.utils.{DataTypeUtil, VersionCli}
import com._4paradigm.openmldb.batch.utils.HybridseUtil.autoLoad
import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, SparkPlanner}
import org.apache.commons.io.IOUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMap, mapAsScalaMapConverter}

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
      logger.info(s"Try to connect OpenMLDB with zk ${this.config.openmldbZkCluster} and root path " +
        s"${this.config.openmldbZkRootPath}")
      try {
        openmldbCatalogService = new OpenmldbCatalogService(this.config.openmldbZkCluster,
          this.config.openmldbZkRootPath, config.openmldbJsdkLibraryPath)
        registerOpenmldbOfflineTable(openmldbCatalogService)
      } catch {
        case e: Exception => logger.warn("Fail to connect OpenMLDB cluster and register tables, " + e.getMessage)
      }
    } else {
      logger.warn("openmldb.zk.cluster or openmldb.zk.root.path is not set and do not register OpenMLDB tables")
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

  def isYarnMode(): Boolean = {
    getSparkSession.conf.get("spark.master").equalsIgnoreCase("yarn")
  }

  def isClusterMode(): Boolean = {
    getSparkSession.conf.get("spark.submit.deployMode", "client").equalsIgnoreCase("cluster")
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
    if (config.enableSparksql) {
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
    try {
      val openmldbBatchVersion = VersionCli.getVersion()
      s"$SPARK_VERSION\n$openmldbBatchVersion"
    } catch {
      case e: Exception => {
        logger.error("Fail to load OpenMLDB git properties " + e.getMessage)
        SPARK_VERSION
      }
    }
  }

  def registerTable(dbName: String, tableName: String, df: DataFrame): Unit = {
    // Register in OpenMLDB session
    registerTableInOpenmldbSession(dbName, tableName, df)

    // Register in Spark catalog
    df.createOrReplaceTempView(tableName)
  }

  def registerTableInOpenmldbSession(dbName: String, tableName: String, df: DataFrame): Unit = {
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

  def disableSparkLogs(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
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
          val options = offlineTableInfo.getOptionsMap.asScala.toMap

          // TODO: Ignore the register exception which occurs when switching local and yarn mode
          try {
            // default offlineTableInfo required members 'path' & 'format' won't be null
            if (path != null && path.nonEmpty && format != null && format.nonEmpty) {
              // Has offline table meta, use the meta and table schema to read data
              // hive load will use sparksql
              val df = autoLoad(this, path, format, options, tableInfo.getColumnDescList)
              registerTable(dbName, tableName, df)
            } else {
              // Register empty df for table
              val tableInfo = catalogService.getTableInfo(dbName, tableName)
              val columnDescList = tableInfo.getColumnDescList

              val schema = new StructType(columnDescList.asScala.map(colDesc => {
                StructField(colDesc.getName, DataTypeUtil.protoTypeToSparkType(colDesc.getDataType),
                  !colDesc.getNotNull)
              }).toArray)

              logger.info(s"Register empty dataframe of $dbName.$tableName with schema $schema")
              // Create empty df with schema
              val emptyDf = sparkSession.createDataFrame(sparkSession.emptyDataFrame.rdd, schema)

              registerTable(dbName, tableName, emptyDf)
            }
          } catch {
            case e: Exception => logger.warn(s"Fail to register table $dbName.$tableName, error: ${e.getMessage}")
          }
        }
      })
    })
  }

}
