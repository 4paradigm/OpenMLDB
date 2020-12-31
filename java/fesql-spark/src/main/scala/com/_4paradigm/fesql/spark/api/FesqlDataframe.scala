package com._4paradigm.fesql.spark.api

import com._4paradigm.fesql.spark.SchemaUtil
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.slf4j.LoggerFactory

case class FesqlDataframe(fesqlSession: FesqlSession, sparkDf: DataFrame) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var tableName: String = "table"

  /**
   * Register the dataframe with name which can be used for sql.
   *
   * @param name
   */
  def createOrReplaceTempView(name: String): Unit = {
    tableName = name
    // Register for Spark SQL
    sparkDf.createOrReplaceTempView(name)

    // Register for FESQL
    fesqlSession.registerTable(name, sparkDf)
  }

  def tiny(number: Long): FesqlDataframe = {
    sparkDf.createOrReplaceTempView(tableName)
    val sqlCode = s"select * from ${tableName} limit ${number};"
    new FesqlDataframe(fesqlSession, fesqlSession.sparksql(sqlCode).sparkDf)
  }

  /**
   * Save the dataframe to file with Spark API.
   *
   * @param path
   * @param format
   * @param mode
   * @param renameDuplicateColumns
   * @param partitionNum
   */
  def write(path: String,
           format: String = "parquet",
           mode: String = "overwrite",
           renameDuplicateColumns: Boolean = true,
           partitionNum: Int = -1): Unit = {

    var df = sparkDf
    if (renameDuplicateColumns) {
      df = SchemaUtil.renameDuplicateColumns(df)
    }

    if (partitionNum > 0) {
      df = df.repartition(partitionNum)
    }

    format.toLowerCase match {
      case "parquet" => df.write.mode(mode).parquet(path)
      case "csv" => df.write.mode(mode).csv(path)
      case "json" => df.write.mode(mode).json(path)
      case "text" => df.write.mode(mode).text(path)
      case "orc" => df.write.mode(mode).orc(path)
      case _ => Unit
    }
  }

  /**
   * Run Spark job without other operators.
   */
  def run(): Unit = {
    fesqlSession.getSparkSession.sparkContext.runJob(sparkDf.rdd, { _: Iterator[_] => })
  }

  /**
   * Show with Spark API.
   */
  def show(): Unit = {
    sparkDf.show()
  }

  /**
   * Count with Spark API.
   *
   * @return
   */
  def count(): Long = {
    sparkDf.count()
  }

  /**
   * Sample with Spark API.
   *
   * @param fraction
   * @param seed
   * @return
   */
  def sample(fraction: Double, seed: Long): FesqlDataframe = {
    new FesqlDataframe(fesqlSession, sparkDf.sample(fraction, seed))
  }

  /**
   * Sample with Spark API.
   *
   * @param fraction
   * @return
   */
  def sample(fraction: Double): FesqlDataframe = {
    new FesqlDataframe(fesqlSession, sparkDf.sample(fraction))
  }

  /**
   * Describe with Spark API.
   *
   * @param cols
   * @return
   */
  def describe(cols: String*): FesqlDataframe = {
    new FesqlDataframe(fesqlSession, sparkDf.describe(cols:_*))
  }

  /**
   * Print Spark plan with Spark API.
   *
   * @param extended
   */
  def explain(extended: Boolean = false): Unit = {
    sparkDf.explain(extended)
  }

  def summary(): FesqlDataframe = {
    new FesqlDataframe(fesqlSession, sparkDf.summary())
  }

  /**
   * Cache the dataframe with Spark API.
   *
   * @return
   */
  def cache(): FesqlDataframe = {
    new FesqlDataframe(fesqlSession, sparkDf.cache())
  }

  /**
   * Collect the dataframe with Spark API.
   *
   * @return
   */
  def collect(): Array[Row] = {
    sparkDf.collect()
  }

  /**
   * Return the string of Spark dataframe.
   *
   * @return
   */
  override def toString: String = {
    sparkDf.toString()
  }

  /**
   * Get Spark dataframe object.
   *
   * @return
   */
  def getSparkDf(): DataFrame = {
    sparkDf
  }

  /**
   * Get fesql session object.
   *
   * @return
   */
  def getFesqlSession(): FesqlSession = {
    fesqlSession
  }

  /**
   * Get Spark session object.
   *
   * @return
   */
  def getSparkSession(): SparkSession = {
    fesqlSession.getSparkSession
  }

  /**
   * Get Spark dataframe scheme json string.
   *
   * @return
   */
  def schemaJson: String = {
    sparkDf.queryExecution.analyzed.schema.json
  }

  /**
   * Print Spark codegen string.
   */
  def printCodegen: Unit = {
    sparkDf.queryExecution.debug.codegen
  }

}
