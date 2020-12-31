package com._4paradigm.fesql.spark.api

import com._4paradigm.fesql.spark.SparkPlanner
import com._4paradigm.fesql.spark.element.FesqlConfig
import com._4paradigm.fesql.vm.PhysicalOpNode
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.QueryPlanningTracker

import scala.collection.mutable


/**
 * The class to provide SparkSession-like API.
 */
class FesqlSession {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private var sparkSession: SparkSession = null
  private var sparkMaster: String = null

  val registeredTables = mutable.HashMap[String, DataFrame]()
//  private var configs: mutable.HashMap[String, Any] = _
  private var scalaConfig: Map[String, Any] = Map()
  var planner: SparkPlanner = _

  /**
   * Construct with Spark session.
   *
   * @param sparkSession
   */
  def this(sparkSession: SparkSession) = {
    this()
    this.sparkSession = sparkSession
    this.sparkSession.conf.set(FesqlConfig.configTimeZone, FesqlConfig.timeZone)

    for ((k, v) <- this.sparkSession.conf.getAll) {
      logger.info(s"fesql config: ${k} = ${v}")
      scalaConfig += (k -> v)
      k match {
        case FesqlConfig.configSkewRadio => FesqlConfig.skewRatio = v.toDouble
        case FesqlConfig.configSkewLevel => FesqlConfig.skewLevel = v.toInt
        case FesqlConfig.configSkewCnt => FesqlConfig.skewCnt = v.toInt
        case FesqlConfig.configSkewCntName => FesqlConfig.skewCntName = v.asInstanceOf[String]
        case FesqlConfig.configSkewTag => FesqlConfig.skewTag = v.asInstanceOf[String]
        case FesqlConfig.configSkewPosition => FesqlConfig.skewPosition = v.asInstanceOf[String]
        case FesqlConfig.configMode => FesqlConfig.mode = v.asInstanceOf[String]
        case FesqlConfig.configPartitions => FesqlConfig.paritions = v.toInt
        case FesqlConfig.configTimeZone => FesqlConfig.timeZone = v.asInstanceOf[String]
        case FesqlConfig.configTinyData => FesqlConfig.tinyData = v.toLong
        case _ => ""
      }
    }
  }

  /**
   * Construct with Spark master string.
   *
   *
   */
//  def this(sparkMaster: String) = {
//    this()
//    this.sparkMaster = sparkMaster
//  }

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
        builder.config(FesqlConfig.configSparkEnable, value = false)

        this.sparkSession = builder.appName("FesqlApp")
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
  def read(filePath: String, format: String = "parquet"): FesqlDataframe = {
    val spark = this.getSparkSession

    val sparkDf = format match {
      case "parquet" => spark.read.parquet(filePath)
      case "csv" => spark.read.csv(filePath)
      case "json" => spark.read.json(filePath)
      case "text" => spark.read.text(filePath)
      case "orc" => spark.read.orc(filePath)
      case _ => null
    }

    new FesqlDataframe(this, sparkDf)
  }



  /**
   * Read the Spark dataframe to Fesql dataframe.
   *
   * @param sparkDf
   * @return
   */
  def readSparkDataframe(sparkDf: DataFrame): FesqlDataframe = {
    new FesqlDataframe(this, sparkDf)
  }

  /**
   * Run sql with FESQL.
   *
   * @param sqlText
   * @return
   */
  def fesql(sqlText: String): FesqlDataframe = {
    var sql: String = sqlText
    if (!sql.trim.endsWith(";")) {
      sql = sql.trim + ";"
    }

    val planner = new SparkPlanner(getSparkSession, scalaConfig)
    this.planner = planner
    val df = planner.plan(sql, registeredTables.toMap).getDf(getSparkSession)
    new FesqlDataframe(this, df)
  }

  /**
   * Run sql with FESQL.
   *
   * @param sqlText
   * @return
   */
  def sql(sqlText: String): FesqlDataframe = {
    fesql(sqlText)
  }

  /**
   * Run sql with Spark SQL API.
   *
   * @param sqlText
   * @return
   */
  def sparksql(sqlText: String): FesqlDataframe = {
    // Use Spark internal implementation because we may override sql function in 4PD Spark distribution
    val tracker = new QueryPlanningTracker
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      getSparkSession.sessionState.sqlParser.parsePlan(sqlText)
    }

    // Call private method Dataset.ofRows()
    val datasetClass = Class.forName("org.apache.spark.sql.Dataset")
    val datasetOfRowsMethod = datasetClass
      .getDeclaredMethod(s"ofRows", classOf[SparkSession], classOf[LogicalPlan], classOf[QueryPlanningTracker])
    val outputDataset =  datasetOfRowsMethod.invoke(null, getSparkSession, plan, tracker).asInstanceOf[Dataset[Row]]

    FesqlDataframe(this, outputDataset)
  }

  /**
   * Get the version from git commit message.
   */
  def version(): Unit = {
    val stream = this.getClass.getClassLoader.getResourceAsStream("fesql_git.properties")
    if (stream == null) {
      logger.warn("Project version is missing")
    } else {
      IOUtils.copy(stream, System.out)
    }
  }

  /**
   * Record the registered tables for fesql to run.
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
