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

import com._4paradigm.hybridsql.spark.SchemaUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory


case class SparkFeDataframe(sparkFeSession: SparkFeSession, sparkDf: DataFrame) {

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

    // Register for SparkFE SQL
    sparkFeSession.registerTable(name, sparkDf)
  }

  def tiny(number: Long): SparkFeDataframe = {
    sparkDf.createOrReplaceTempView(tableName)
    val sqlCode = s"select * from ${tableName} limit ${number};"
    new SparkFeDataframe(sparkFeSession, sparkFeSession.sparksql(sqlCode).sparkDf)
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
    sparkFeSession.getSparkSession.sparkContext.runJob(sparkDf.rdd, { _: Iterator[_] => })
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
  def sample(fraction: Double, seed: Long): SparkFeDataframe = {
    new SparkFeDataframe(sparkFeSession, sparkDf.sample(fraction, seed))
  }

  /**
   * Sample with Spark API.
   *
   * @param fraction
   * @return
   */
  def sample(fraction: Double): SparkFeDataframe = {
    new SparkFeDataframe(sparkFeSession, sparkDf.sample(fraction))
  }

  /**
   * Describe with Spark API.
   *
   * @param cols
   * @return
   */
  def describe(cols: String*): SparkFeDataframe = {
    new SparkFeDataframe(sparkFeSession, sparkDf.describe(cols: _*))
  }

  /**
   * Print Spark plan with Spark API.
   *
   * @param extended
   */
  def explain(extended: Boolean = false): Unit = {
    sparkDf.explain(extended)
  }

  def summary(): SparkFeDataframe = {
    new SparkFeDataframe(sparkFeSession, sparkDf.summary())
  }

  /**
   * Cache the dataframe with Spark API.
   *
   * @return
   */
  def cache(): SparkFeDataframe = {
    new SparkFeDataframe(sparkFeSession, sparkDf.cache())
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
   * Get session object.
   *
   * @return
   */
  def getSparkFeSession(): SparkFeSession = {
    sparkFeSession
  }

  /**
   * Get Spark session object.
   *
   * @return
   */
  def getSparkSession(): SparkSession = {
    sparkFeSession.getSparkSession
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
