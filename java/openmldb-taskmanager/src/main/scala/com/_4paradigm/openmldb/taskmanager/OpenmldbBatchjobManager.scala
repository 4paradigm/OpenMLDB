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

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import com._4paradigm.openmldb.taskmanager.spark.SparkLauncherUtil
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil
import org.slf4j.LoggerFactory

object OpenmldbBatchjobManager {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Run the Spark job to print the Spark version.
   */
  def showSparkVersion(): Unit = {
    val mainClass = "com._4paradigm.openmldb.batchjob.SparkVersionApp"
    SparkLauncherUtil.submitSparkAndWait(mainClass)
  }

  /**
   * Run the SparkSQL job and save output as new table.
   *
   * @param sql the SQL text
   * @param dbName the database name of output table
   * @param outputTableName the table name of output table
   * @return the Yarn AppId in String format
   */
  def batchRunSql(sql: String, dbName: String, outputTableName: String): String = {
    val mainClass = "com._4paradigm.openmldb.batchjob.BatchRunSql"
    val args = List(TaskManagerConfig.HIVE_METASTORE_ENDPOINT, sql, dbName, outputTableName)

    SparkLauncherUtil.submitSparkGetAppId(mainClass, args.toArray)
  }

  /**
   * Run the Spark job to import HDFS files as new table.
   *
   * @param fileType the file type
   * @param filePath the path of input file
   * @param dbName the database name of output table
   * @param outputTableName the table name of output table
   * @return the Yarn AppId in String format
   */
  def importHdfsFile(fileType: String, filePath: String, dbName: String, outputTableName: String): String = {
    val mainClass = "com._4paradigm.openmldb.batchjob.ImportHdfsFile"
    val args = List(TaskManagerConfig.HIVE_METASTORE_ENDPOINT, fileType, filePath, dbName, outputTableName)

    SparkLauncherUtil.submitSparkGetAppId(mainClass, args.toArray)
  }

  /**
   * Get Spark job state from Yarn app id.
   *
   * @param appIdStr the string app id
   * @return the string state
   */
  def getJobState(appIdStr: String): String = {
    YarnClientUtil.getYarnJobState(appIdStr).toString
  }

}