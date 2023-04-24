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

  /**
   * Create the SparkLauncher object with pre-set parameters like yarn-cluster.
   *
   * @param mainClass the full-qualified Java class name
   * @return the SparkLauncher object
   */
  def createSparkLauncher(mainClass: String): SparkLauncher = {

    val launcher = new SparkLauncher()
      .setAppResource(TaskManagerConfig.BATCHJOB_JAR_PATH)
      .setMainClass(mainClass)

    if (TaskManagerConfig.SPARK_HOME != null && TaskManagerConfig.SPARK_HOME.nonEmpty) {
      launcher.setSparkHome(TaskManagerConfig.SPARK_HOME)
    }

    if (TaskManagerConfig.SPARK_MASTER.toLowerCase.startsWith("local")) {
      launcher.setMaster(TaskManagerConfig.SPARK_MASTER)
    } else {
      TaskManagerConfig.SPARK_MASTER.toLowerCase match {
        case "yarn" | "yarn-cluster" =>
          launcher.setMaster("yarn").setDeployMode("cluster")
        case "yarn-client" =>
          launcher.setMaster("yarn").setDeployMode("client")
        case _ => throw new Exception(s"Unsupported Spark master ${TaskManagerConfig.SPARK_MASTER}")
      }
    }

    if (TaskManagerConfig.SPARK_YARN_JARS != null && TaskManagerConfig.SPARK_YARN_JARS.nonEmpty) {
      launcher.setConf("spark.yarn.jars", TaskManagerConfig.SPARK_YARN_JARS)
    }

    launcher
  }

  def submitSparkJob(jobType: String, mainClass: String,
                     args: List[String] = List(),
                     localSqlFile: String = "",
                     sparkConf: Map[String, String] = Map(),
                     defaultDb: String = "",
                     blocking: Boolean = false): JobInfo = {
    val jobInfo = JobInfoManager.createJobInfo(jobType, args, sparkConf)

    // Submit Spark application with SparkLauncher
    val launcher = createSparkLauncher(mainClass)

    if (args.nonEmpty) {
      launcher.addAppArgs(args:_*)
    }

    if (localSqlFile.nonEmpty) {
      logger.info("Add the local SQL file: " + localSqlFile)
      launcher.addFile(localSqlFile)
    }

    // TODO: Avoid using zh_CN to load openmldb jsdk so
   
    if (TaskManagerConfig.SPARK_EVENTLOG_DIR.nonEmpty) {
      launcher.setConf("spark.eventLog.enabled", "true")
      launcher.setConf("spark.eventLog.dir", TaskManagerConfig.SPARK_EVENTLOG_DIR)
    }

    if (TaskManagerConfig.SPARK_YARN_MAXAPPATTEMPTS >= 1 ) {
      launcher.setConf("spark.yarn.maxAppAttempts", TaskManagerConfig.SPARK_YARN_MAXAPPATTEMPTS.toString)
    }

    // Set default Spark conf by TaskManager configuration file
    val defaultSparkConfs = TaskManagerConfig.SPARK_DEFAULT_CONF.split(";")
    defaultSparkConfs.map(sparkConf => {
      if (sparkConf.nonEmpty) {
        val kvList = sparkConf.split("=")
        val key = kvList(0)
        val value = kvList.drop(1).mkString("=")
        launcher.setConf(key, value)
      }
    })

    // Set ZooKeeper config for openmldb-batch jobs
    if (TaskManagerConfig.ZK_CLUSTER.nonEmpty && TaskManagerConfig.ZK_ROOT_PATH.nonEmpty) {
      launcher.setConf("spark.openmldb.zk.cluster", TaskManagerConfig.ZK_CLUSTER)
      launcher.setConf("spark.openmldb.zk.root.path", TaskManagerConfig.ZK_ROOT_PATH)
    }

    // Set ad-hoc Spark configuration
    if (defaultDb.nonEmpty) {
      launcher.setConf("spark.openmldb.default.db", defaultDb)
    }

    if (TaskManagerConfig.OFFLINE_DATA_PREFIX.nonEmpty) {
      launcher.setConf("spark.openmldb.offline.data.prefix", TaskManagerConfig.OFFLINE_DATA_PREFIX)
    }

    // Set external function dir for offline jobs
    val absoluteExternalFunctionDir = if (TaskManagerConfig.EXTERNAL_FUNCTION_DIR.startsWith("/")) {
      TaskManagerConfig.EXTERNAL_FUNCTION_DIR
    } else {
      // TODO: The current path is incorrect if running in IDE, please set `external.function.dir` with absolute path
      // Concat to generate absolute path
      Paths.get(Paths.get(".").toAbsolutePath.toString, TaskManagerConfig.EXTERNAL_FUNCTION_DIR).toString
    }
    launcher.setConf("spark.openmldb.taskmanager.external.function.dir", absoluteExternalFunctionDir)

    for ((k, v) <- sparkConf) {
      logger.info("Get Spark config key: " + k + ", value: " + v)
      launcher.setConf(k, v)
    }

    if (TaskManagerConfig.JOB_LOG_PATH.nonEmpty) {
      // Create local file and redirect the log of job into files
      launcher.redirectOutput(LogManager.getJobLogFile(jobInfo.getId))
      launcher.redirectError(LogManager.getJobErrorLogFile(jobInfo.getId))
    }

    if(TaskManagerConfig.ENABLE_HIVE_SUPPORT) {
      launcher.setConf("spark.sql.catalogImplementation", "hive")
    }

    if (TaskManagerConfig.AWS_ACCESS_KEY.nonEmpty) {
      launcher.setConf("spark.fs.s3a.access.key", TaskManagerConfig.AWS_ACCESS_KEY)
    }

    if (TaskManagerConfig.AWS_SECRET_KEY.nonEmpty) {
      launcher.setConf("spark.fs.s3a.secret.key", TaskManagerConfig.AWS_SECRET_KEY)
    }

    // Add the external function library files
    // TODO(tobe): Handle the same file names
    ExternalFunctionManager.getAllLibraryFilePaths().forEach(filePath => launcher.addFile(filePath))

    // Submit Spark application and watch state with custom listener
    val sparkAppHandler = launcher.startApplication(new SparkJobListener(jobInfo))

    if (blocking) {
      while (!sparkAppHandler.getState().isFinal()) {
        // TODO: Make this configurable
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
