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

import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, SchemaUtil}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.HttpHeaders
import org.apache.http.entity.StringEntity
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringEscapeUtils
import scala.collection.mutable.ArrayBuffer

import java.sql.Timestamp

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

  /**
   * Send df parts to taskmanager http
   */
  def sendResult(): Unit = {
    val url = openmldbSession.config.saveJobResultHttp
    val resultId = openmldbSession.config.saveJobResultId
    val rowPerPost = openmldbSession.config.saveJobResultRowPerPost
    val postTimeouts = openmldbSession.config.saveJobResultPostTimeouts.split(",").map(_.toInt)
    val schemaLine = sparkDf.schema.map(structField => { structField.name }).mkString(",")
    println(s"send result to ${url}, result id ${resultId}")
    sparkDf.foreachPartition { (partition: Iterator[Row]) => {
      val client = HttpClientBuilder.create().build()
      val post = new HttpPost(url)
      post.setHeader("Content-type", "application/json");
      val requestConfig = RequestConfig.custom().setConnectionRequestTimeout(postTimeouts(0))
        .setConnectTimeout(postTimeouts(1)).setSocketTimeout(postTimeouts(2)).build()
      post.setConfig(requestConfig);
      while (partition.hasNext) {
        val arr = new ArrayBuffer[String]()
        var i = 0
        while (i < rowPerPost && partition.hasNext) {
          // print each column value, if null, print null, if string, print escaped string
          arr.append(
            partition
              .next()
              .toSeq
              .map(x => {
                if (x == null) { "null" }
                else if(x.isInstanceOf[Timestamp]) { s""""${x.toString}"""" }
                else if (x.isInstanceOf[String]) {
                  s""""${StringEscapeUtils.escapeJson(x.toString)}""""
                } else { x.toString }
              })
              .mkString(",")
          )
          i += 1
        }
        // use json load to rebuild two dim array?
        // just send a raw csv string "<schema>\n<row1>\n<row2>...",
        // so we need to quote the whole csv string(escape again)
        val data = StringEscapeUtils.escapeJson(arr.mkString("\n"))
        val json_str = s"""{"json_data": "${schemaLine}\\n${data}", "result_id": ${resultId}}"""
        post.setEntity(new StringEntity(json_str))
        val response = client.execute(post)
        val entity = response.getEntity()
        if( response.getStatusLine.getStatusCode() != 200) {
          println(s"send http failed, ${response.getStatusLine.getStatusCode()}-" +
            s"${response.getStatusLine.getReasonPhrase()}")
        } else {
          println(IOUtils.toString(entity.getContent()))
        }
        }
      }
    }

    // send an empty data to let the result reader know that it can read now
    val client = HttpClientBuilder.create().build()
    val post = new HttpPost(url)
    post.setHeader("Content-type", "application/json");
    val requestConfig = RequestConfig.custom().setConnectionRequestTimeout(postTimeouts(0))
      .setConnectTimeout(postTimeouts(1)).setSocketTimeout(postTimeouts(2)).build()
    post.setConfig(requestConfig);
    val json_str = s"""{"json_data": "", "result_id": ${resultId}}"""
    post.setEntity(new StringEntity(json_str))
    val response = client.execute(post)
    val entity = response.getEntity()
    if( response.getStatusLine.getStatusCode() != 200) {
      println(s"send last http failed, ${response.getStatusLine.getStatusCode()}-" +
        s"${response.getStatusLine.getReasonPhrase()}")
    } else {
      println("last response: " + IOUtils.toString(entity.getContent()))
    }
  }

}
