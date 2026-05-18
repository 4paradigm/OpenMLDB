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

package com._4paradigm.openmldb.taskmanager.spark

import com._4paradigm.openmldb.taskmanager.{JobInfoManager, LogManager}
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import com._4paradigm.openmldb.taskmanager.dao.JobInfo
import com._4paradigm.openmldb.taskmanager.udf.ExternalFunctionManager
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil
import org.apache.spark.launcher.SparkLauncher
import org.slf4j.LoggerFactory
import java.nio.file.Paths

object SparkJobManager {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def createSparkLauncher(mainClass: String): SparkLauncher = {

    val env: java.util.Map[String, String] =
      new java.util.HashMap[String, String](System.getenv())

    if (TaskManagerConfig.getHadoopConfDir != null && TaskManagerConfig.getHadoopConfDir.nonEmpty) {
      env.put("HADOOP_CONF_DIR", TaskManagerConfig.getHadoopConfDir)
    }

    if (TaskManagerConfig.getHadoopUserName != null && TaskManagerConfig.getHadoopUserName.nonEmpty) {
      env.put("HADOOP_USER_NAME", TaskManagerConfig.getHadoopUserName)
    }

    val launcher = new SparkLauncher(env)
      .setAppResource(TaskManagerConfig.getBatchjobJarPath)
      .setMainClass(mainClass)

    if (TaskManagerConfig.getSparkHome != null && TaskManagerConfig.getSparkHome.nonEmpty) {
      launcher.setSparkHome(TaskManagerConfig.getSparkHome)
    }

    if (TaskManagerConfig.getSparkMaster.startsWith("local")) {
      launcher.setMaster(TaskManagerConfig.getSparkMaster)
    } else {
      TaskManagerConfig.getSparkMaster.toLowerCase match {
        case "yarn" | "yarn-cluster" =>
          launcher.setMaster("yarn").setDeployMode("cluster")
        case "yarn-client" =>
          launcher.setMaster("yarn").setDeployMode("client")
        case _ => throw new Exception(s"Unsupported Spark master ${TaskManagerConfig.getSparkMaster}")
      }
    }

    if (TaskManagerConfig.getSparkYarnJars != null && TaskManagerConfig.getSparkYarnJars.nonEmpty) {
      launcher.setConf("spark.yarn.jars", TaskManagerConfig.getSparkYarnJars)
    }

    launcher
  }

  def submitSparkJob(jobType: String, mainClass: String,
                     args: List[String] = List(),
                     sql: String = "",
                     localSqlFile: String = "",
                     sparkConf: Map[String, String] = Map(),
                     defaultDb: String = "",
                     blocking: Boolean = false): JobInfo = {

    val jobInfoArgs = if (sql.nonEmpty) {
      List(sql)
    } else {
      args
    }

    val jobInfo = JobInfoManager.createJobInfo(jobType, jobInfoArgs, sparkConf)

    val launcher = createSparkLauncher(mainClass)

    if (args.nonEmpty) {
      launcher.addAppArgs(args:_*)
    }

    if (localSqlFile.nonEmpty) {
      logger.info("Add the local SQL file: " + localSqlFile)
      launcher.addFile(localSqlFile)
    }

    if (TaskManagerConfig.getSparkEventlogDir.nonEmpty) {
      launcher.setConf("spark.eventLog.enabled", "true")
      launcher.setConf("spark.eventLog.dir", TaskManagerConfig.getSparkEventlogDir)
    }

    if (TaskManagerConfig.getSparkYarnMaxappattempts >= 1) {
      launcher.setConf("spark.yarn.maxAppAttempts", TaskManagerConfig.getSparkYarnMaxappattempts.toString)
    }

    val defaultSparkConfs = TaskManagerConfig.getSparkDefaultConf.split(";")
    defaultSparkConfs.map(sparkConf => {
      if (sparkConf.nonEmpty) {
        val kvList = sparkConf.split("=")
        val key = kvList(0)
        val value = kvList.drop(1).mkString("=")
        launcher.setConf(key, value)
      }
    })

    if (TaskManagerConfig.getZkCluster.nonEmpty && TaskManagerConfig.getZkRootPath.nonEmpty) {
      launcher.setConf("spark.openmldb.zk.cluster", TaskManagerConfig.getZkCluster)
      launcher.setConf("spark.openmldb.zk.root.path", TaskManagerConfig.getZkRootPath)

      launcher.setConf("spark.openmldb.user", TaskManagerConfig.getUser)
      launcher.setConf("spark.openmldb.password", TaskManagerConfig.getPassword)
    }

    if (defaultDb.nonEmpty) {
      launcher.setConf("spark.openmldb.default.db", defaultDb)
    }

    if (TaskManagerConfig.getOfflineDataPrefix.nonEmpty) {
      launcher.setConf("spark.openmldb.offline.data.prefix", TaskManagerConfig.getOfflineDataPrefix)
    }

    val absoluteExternalFunctionDir = if (TaskManagerConfig.getExternalFunctionDir.startsWith("/")) {
      TaskManagerConfig.getExternalFunctionDir
    } else {
      Paths.get(Paths.get(".").toAbsolutePath.toString, TaskManagerConfig.getExternalFunctionDir).toString
    }

    launcher.setConf("spark.openmldb.taskmanager.external.function.dir", absoluteExternalFunctionDir)

    for ((k, v) <- sparkConf) {
      logger.info("Get Spark config key: " + k + ", value: " + v)
      launcher.setConf(k, v)
    }

    if (TaskManagerConfig.getJobLogPath.nonEmpty) {
      launcher.redirectOutput(LogManager.getJobLogFile(jobInfo.getId))
      launcher.redirectError(LogManager.getJobErrorLogFile(jobInfo.getId))
    }

    if (TaskManagerConfig.getEnableHiveSupport) {
      launcher.setConf("spark.sql.catalogImplementation", "hive")
    }

    ExternalFunctionManager.getAllLibraryFilePaths().forEach(filePath => launcher.addFile(filePath))

    val sparkAppHandler = launcher.startApplication(new SparkJobListener(jobInfo))

    if (blocking) {
      while (!sparkAppHandler.getState().isFinal()) {
        Thread.sleep(3000L)
      }
    }

    jobInfo
  }

  def stopSparkYarnJob(jobInfo: JobInfo): Unit = {
    if (jobInfo.isFinished) {
      // TODO: return error message
    } else if (jobInfo.getApplicationId == null) {

    } else {
      YarnClientUtil.killYarnJob(jobInfo.getApplicationId)
    }
  }
}