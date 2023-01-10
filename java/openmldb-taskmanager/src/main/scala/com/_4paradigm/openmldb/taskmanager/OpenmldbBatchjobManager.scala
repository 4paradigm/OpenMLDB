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
import com._4paradigm.openmldb.taskmanager.util.SqlFileUtil
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}
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
   * @return the Yarn AppId in String format
   */
  def runBatchSql(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): String = {
    val jobType = "RunBatchSql"
    val mainClass = "com._4paradigm.openmldb.batchjob.RunBatchSql"

    val tempSqlFile = SqlFileUtil.createTempSqlFile(sql)
    val args = List(tempSqlFile.getAbsolutePath)

    val jobInfo = SparkJobManager.submitSparkJob(jobType, mainClass, args, tempSqlFile.getAbsolutePath,
      sparkConf.asScala.toMap, defaultDb, blocking=true)

    LogManager.getJobLog(jobInfo.getId)
  }

  def runBatchAndShow(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "RunBatchAndShow"
    val mainClass = "com._4paradigm.openmldb.batchjob.RunBatchAndShow"

    val tempSqlFile = SqlFileUtil.createTempSqlFile(sql)
    val args = List(tempSqlFile.getAbsolutePath)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, tempSqlFile.getAbsolutePath, sparkConf.asScala.toMap,
      defaultDb)
  }

  def importOnlineData(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "ImportOnlineData"
    val mainClass = "com._4paradigm.openmldb.batchjob.ImportOnlineData"

    val tempSqlFile = SqlFileUtil.createTempSqlFile(sql)
    val args = List(tempSqlFile.getAbsolutePath)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, tempSqlFile.getAbsolutePath, sparkConf.asScala.toMap,
      defaultDb)
  }

  def importOfflineData(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "ImportOfflineData"
    val mainClass = "com._4paradigm.openmldb.batchjob.ImportOfflineData"

    val tempSqlFile = SqlFileUtil.createTempSqlFile(sql)
    val args = List(tempSqlFile.getAbsolutePath)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, tempSqlFile.getAbsolutePath, sparkConf.asScala.toMap,
      defaultDb)
  }

  def exportOfflineData(sql: String, sparkConf: java.util.Map[String, String], defaultDb: String): JobInfo = {
    val jobType = "ExportOfflineData"
    val mainClass = "com._4paradigm.openmldb.batchjob.ExportOfflineData"

    val tempSqlFile = SqlFileUtil.createTempSqlFile(sql)
    val args = List(tempSqlFile.getAbsolutePath)

    SparkJobManager.submitSparkJob(jobType, mainClass, args, tempSqlFile.getAbsolutePath, sparkConf.asScala.toMap,
      defaultDb)
  }

}
