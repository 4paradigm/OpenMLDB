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

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.openmldb.batch.catalog.OpenmldbCatalogService
import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, SparkPlanner}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
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

  val registeredTables: mutable.Map[String, DataFrame] = mutable.HashMap[String, DataFrame]()

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
    if (this.config.openmldbZkCluster.nonEmpty && this.config.openmldbZkPath.nonEmpty) {
      openmldbCatalogService = new OpenmldbCatalogService(this.config.openmldbZkCluster, this.config.openmldbZkPath)
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

        builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        builder.appName("App").master(sparkMaster)

        if (config.enableHiveMetaStore) {
          builder.enableHiveSupport()
        }

        this.sparkSession = builder.getOrCreate()

        this.setDefaultSparkConfig()
      }

      this.sparkSession
    }
  }

  def setDefaultSparkConfig(): Unit = {
    val sparkConf = this.sparkSession.conf
    // Set timezone
    sparkConf.set("spark.sql.session.timeZone", config.timeZone)

    // Set Iceberg catalog
    if (!config.hadoopWarehousePath.isEmpty) {
      sparkConf.set("spark.sql.catalog.%s".format(config.icebergHadoopCatalogName),
        "org.apache.iceberg.spark.SparkCatalog")
      sparkConf.set("spark.sql.catalog.%s.type".format(config.icebergHadoopCatalogName), "hadoop")
      sparkConf.set("spark.sql.catalog.%s.warehouse".format(config.icebergHadoopCatalogName),
        this.config.hadoopWarehousePath)
    }

    if (config.enableHiveMetaStore) {
      sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive")
      // TODO: Check if "hive.metastore.uris" is set or not
    }

  }

  /**
   * Read the file with get dataframe with Spark API.
   *
   * @param filePath the path to read
   * @param format the format of data
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

    var sql: String = sqlText
    if (!sql.trim.endsWith(";")) {
      sql = sql.trim + ";"
    }
    val planner = new SparkPlanner(getSparkSession, config)
    this.planner = planner
    val df = planner.plan(sql, registeredTables.toMap).getDf()
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

  /**
   * Record the registered tables to run.
   *
   * @param name the registered name of table
   * @param df the Spark DataFrame
   */
  def registerTable(name: String, df: DataFrame): Unit = {
    registeredTables.put(name, df)
  }

  /**
   * Read table from Spark catalog and databases to register in OpenMLDB engine.
   */
  def registerCatalogTables(): Unit = {
    val spark = this.sparkSession
    spark.catalog.listDatabases().collect().flatMap(db => {
      spark.catalog.listTables(db.name).collect().map(x => {
        val fullyQualifiedName = s"${db.name}.${x.name}"
        logger.info("Register table " + fullyQualifiedName)
        registerTable(fullyQualifiedName, spark.table(fullyQualifiedName))
      })
    })
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
    sparkSession.stop()
  }

  /**
   * Create table and import data from DataFrame to offline storage.
   */
  def importToOfflineStorage(databaseName: String, tableName: String, df: DataFrame): Unit = {
    createHiveTable(databaseName, tableName, df)
    appendHiveTable(databaseName, tableName, df)
  }

  /**
   * Create table in offline storage.
   */
  def createHiveTable(databaseName: String, tableName: String, df: DataFrame): Unit = {

    logger.info("Register the table %s to create table in offline storage".format(tableName))
    df.createOrReplaceTempView(tableName)

    val conf = getSparkSession.sessionState.newHadoopConf()
    val catalog = new HiveCatalog(conf)
    val icebergSchema = SparkSchemaUtil.schemaForTable(getSparkSession, tableName)
    val partitionSpec = PartitionSpec.builderFor(icebergSchema).build()
    val tableIdentifier = TableIdentifier.of(databaseName, tableName)

    // Create Iceberg database if not exists
    val namespace = Namespace.of(databaseName)
    try {
      catalog.createNamespace(namespace)
    } catch {
      case _: org.apache.iceberg.exceptions.AlreadyExistsException => {
        logger.warn("Database %s already exists".format(databaseName))
      }
    }

    // Check if table exists
    if(catalog.tableExists(tableIdentifier)) {
      catalog.close()
      logger.error("Table %s already exists".format(tableName))
      throw new HybridSeException("Table %s already exists, Please check the table name".format(tableName))
    }

    // Create Iceberg table
    catalog.createTable(tableIdentifier, icebergSchema, partitionSpec)
    catalog.close()

    // Register table in OpenMLDB engine
    registerTable(s"$databaseName.$tableName", df)
  }

  /**
   * Create table in offline storage.
   */
  def createHadoopCatalogTable(databaseName: String, tableName: String, df: DataFrame): Unit = {

    logger.info("Register the table %s to create table in offline storage".format(tableName))
    df.createOrReplaceTempView(tableName)

    val hadoopConfiguration = new Configuration()
    val hadoopCatalog = new HadoopCatalog(hadoopConfiguration, config.hadoopWarehousePath)
    val icebergSchema = SparkSchemaUtil.schemaForTable(this.getSparkSession, tableName)
    val partitionSpec = PartitionSpec.builderFor(icebergSchema).build()
    val tableIdentifier = TableIdentifier.of(databaseName, tableName)

    // Create Iceberg database if not exists
    val namespace = Namespace.of(databaseName)
    try {
      hadoopCatalog.createNamespace(namespace)
    } catch {
      case _: org.apache.iceberg.exceptions.AlreadyExistsException => {
        logger.warn("Database %s already exists".format(databaseName))
      }
    }

    // Check if table exists
    if(hadoopCatalog.tableExists(tableIdentifier)) {
      hadoopCatalog.close()
      logger.error("Table %s already exists".format(tableName))
      throw new HybridSeException("Table %s already exists, Please check the table name".format(tableName))
    }

    // Create Iceberg table
    hadoopCatalog.createTable(tableIdentifier, icebergSchema, partitionSpec)
    hadoopCatalog.close()
  }

  /**
   * Append data from DataFrame to offline storage.
   */
  def appendHiveTable(databaseName: String, tableName: String, df: DataFrame): Unit = {
    logger.info("Register the table %s to append data in offline storage".format(tableName))
    df.createOrReplaceTempView(tableName)

    // TODO: Support other catalog name if possible
    val icebergTableName = "%s.%s.%s".format("spark_catalog", databaseName, tableName)

    // Insert parquet to Iceberg table
    getSparkSession.table(tableName).writeTo(icebergTableName).append()
  }

  /**
   * Append data from DataFrame to offline storage.
   */
  def appendHadoopCatalogTable(databaseName: String, tableName: String, df: DataFrame): Unit = {
    logger.info("Register the table %s to append data in offline storage".format(tableName))
    df.createOrReplaceTempView(tableName)

    val icebergTableName = "%s.%s.%s".format(config.icebergHadoopCatalogName, databaseName, tableName)

    // Insert parquet to Iceberg table
    this.getSparkSession.table(tableName).writeTo(icebergTableName).append()
  }

}
