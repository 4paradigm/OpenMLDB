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

package com._4paradigm.openmldb.taskmanager

import com._4paradigm.openmldb.taskmanager.dao.JobInfo
import com._4paradigm.openmldb.taskmanager.spark.SparkJobManager
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

object OpenmldbBatchjobManager {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Run the Spark job to print the OpenMLDB Spark version.
   */
  def showBatchVersion(): JobInfo = {
    val jobType = "ShowBatchVersion"
    val mainClass = "com._4paradigm.openmldb.batchjob.ShowBatchVersion"

    SparkJobManager.submitSparkJob(jobType, mainClass)
  }

  /**
   * Run the SparkSQL job and save output to specified path.
   *
   * @param sql the SQL text
   * @param outputPath the output path
   * @return the Yarn AppId in String format
   */
  def runBatchSql(sql: String, outputPath: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "RunBatchSql"
    val mainClass = "com._4paradigm.openmldb.batchjob.RunBatchSql"
    val args = List(sql, outputPath)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, sparkConf.asScala.toMap, defaultDb)
  }

  def runBatchAndShow(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "RunBatchAndShow"
    val mainClass = "com._4paradigm.openmldb.batchjob.RunBatchAndShow"
    val args = List(sql)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, sparkConf.asScala.toMap, defaultDb)
  }

  def importOnlineData(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "ImportOnlineData"
    val mainClass = "com._4paradigm.openmldb.batchjob.ImportOnlineData"
    val args = List(sql)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, sparkConf.asScala.toMap, defaultDb)
  }

  def importOfflineData(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "ImportOfflineData"
    val mainClass = "com._4paradigm.openmldb.batchjob.ImportOfflineData"
    val args = List(sql)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, sparkConf.asScala.toMap, defaultDb)
  }

}