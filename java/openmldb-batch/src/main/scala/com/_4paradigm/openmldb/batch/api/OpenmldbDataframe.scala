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

import com._4paradigm.openmldb.batch.SchemaUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


case class OpenmldbDataframe(openmldbSession: OpenmldbSession, sparkDf: DataFrame) {

  private var tableName: String = "table"

  /**
   * Register the dataframe with name which can be used for sql.
   *
   * @param name the name of registered table
   */
  def createOrReplaceTempView(name: String): Unit = {
    tableName = name
    // Register for Spark SQL
    sparkDf.createOrReplaceTempView(name)

    // Register for OpenMLDB SQL
    openmldbSession.registerTable(name, sparkDf)
  }

  def tiny(number: Long): OpenmldbDataframe = {
    sparkDf.createOrReplaceTempView(tableName)
    val sqlCode = s"select * from $tableName limit $number;"
    OpenmldbDataframe(openmldbSession, openmldbSession.sparksql(sqlCode))
  }

  /**
   * Save the dataframe to file with Spark API.
   *
   * @param path the path to write
   * @param format the format of file
   * @param mode the mode of SavedMode
   * @param renameDuplicateColumns if it renames the duplicated columns
   * @param partitionNum the number of partitions
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
    openmldbSession.getSparkSession.sparkContext.runJob(sparkDf.rdd, { _: Iterator[_] => })
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
   * @param fraction the fraction to sample
   * @param seed the seed of sample
   * @return
   */
  def sample(fraction: Double, seed: Long): OpenmldbDataframe = {
    OpenmldbDataframe(openmldbSession, sparkDf.sample(fraction, seed))
  }

  /**
   * Sample with Spark API.
   *
   * @param fraction the fraction to sample
   * @return
   */
  def sample(fraction: Double): OpenmldbDataframe = {
    OpenmldbDataframe(openmldbSession, sparkDf.sample(fraction))
  }

  /**
   * Describe with Spark API.
   *
   * @param cols the columns to describe
   * @return
   */
  def describe(cols: String*): OpenmldbDataframe = {
    OpenmldbDataframe(openmldbSession, sparkDf.describe(cols: _*))
  }

  /**
   * Print Spark plan with Spark API.
   *
   * @param extended if extended the output
   */
  def explain(extended: Boolean = false): Unit = {
    sparkDf.explain(extended)
  }

  def summary(): OpenmldbDataframe = {
    OpenmldbDataframe(openmldbSession, sparkDf.summary())
  }

  /**
   * Cache the dataframe with Spark API.
   *
   * @return
   */
  def cache(): OpenmldbDataframe = {
    OpenmldbDataframe(openmldbSession, sparkDf.cache())
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
  override def toString(): String = {
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
  def getOpenmldbSession(): OpenmldbSession = {
    openmldbSession
  }

  /**
   * Get Spark session object.
   *
   * @return
   */
  def getSparkSession(): SparkSession = {
    openmldbSession.getSparkSession
  }

  /**
   * Get Spark dataframe scheme json string.
   *
   * @return
   */
  def schemaJson(): String = {
    sparkDf.queryExecution.analyzed.schema.json
  }

  /**
   * Print Spark codegen string.
   */
  def printCodegen(): Unit = {
    sparkDf.queryExecution.debug.codegen
  }

}
