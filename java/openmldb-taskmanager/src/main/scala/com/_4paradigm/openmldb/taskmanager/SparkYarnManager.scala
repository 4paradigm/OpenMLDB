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

import java.util.concurrent.CountDownLatch

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.LoggerFactory

object SparkYarnManager {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def createSparkLauncher(mainClass: String): SparkLauncher = {
    new SparkLauncher()
      .setAppResource(TaskManagerConfig.BATCHJOB_JAR_PATH)
      .setMainClass(mainClass)
      .setMaster("yarn")
      .setDeployMode("cluster")
      .setConf("spark.yarn.jars", TaskManagerConfig.SPARK_YARN_JARS)
      .setConf("spark.yarn.maxAppAttempts", "1")
  }

  def showSparkVersion(): Unit = {
    val lock = new CountDownLatch(1)

    val mainClass = "com._4paradigm.openmldb.batchjob.SparkVersionApp"

    val sparkLauncher = createSparkLauncher(mainClass)

    val sparkAppHandle = sparkLauncher.startApplication(new SparkAppHandle.Listener() {
      override def stateChanged(sparkAppHandle: SparkAppHandle): Unit = {
        if(sparkAppHandle.getState.isFinal) {
          lock.countDown()
        }
      }

      override def infoChanged(sparkAppHandle: SparkAppHandle): Unit = {
      }
    })

    lock.await()

    logger.info(s"Final state: ${sparkAppHandle.getState}")
  }

  def batchRunSql(sql: String, dbName: String, outputTableName: String): String = {
    val mainClass = "com._4paradigm.openmldb.batchjob.BatchRunSql"
    val args = List(TaskManagerConfig.HIVE_METASTORE_ENDPOINT, sql, dbName, outputTableName)

    submitSparkGetAppId(mainClass, args.toArray)
  }

  def importHdfsFile(fileType: String, filePath: String, dbName: String, tableName: String): String = {
    val mainClass = "com._4paradigm.openmldb.batchjob.ImportHdfsFile"
    val args = List(TaskManagerConfig.HIVE_METASTORE_ENDPOINT, fileType, filePath, dbName, tableName)

    submitSparkGetAppId(mainClass, args.toArray)
  }

  def submitSparkGetAppId(mainClass: String, args: Array[String]): String = {
    val lock = new CountDownLatch(1)

    val sparkLauncher = createSparkLauncher(mainClass)
    sparkLauncher.addAppArgs(args:_*)

    val sparkAppHandle = sparkLauncher.startApplication(new SparkAppHandle.Listener() {
      override def stateChanged(sparkAppHandle: SparkAppHandle): Unit = {
        if(sparkAppHandle.getState == SparkAppHandle.State.SUBMITTED && sparkAppHandle.getAppId != null) {
          lock.countDown()
        }
      }

      override def infoChanged(sparkAppHandle: SparkAppHandle): Unit = {
      }
    })

    lock.await()

    logger.info(s"Final state: ${sparkAppHandle.getState}, appId: ${sparkAppHandle.getAppId}")
    sparkAppHandle.getAppId
  }



  def main(args: Array[String]): Unit = {


    //showSparkVersion()

    val fileType = "parquet"
    val filePath = "hdfs:///Users/tobe/data/taxi_tour_parquet_all/"
    val dbName = "taxitour5"
    val tableName = "t10"

    importHdfsFile(fileType, filePath, dbName, tableName)
  }


}