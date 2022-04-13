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
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil
import org.apache.spark.launcher.SparkLauncher
import org.slf4j.LoggerFactory

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

    TaskManagerConfig.SPARK_MASTER.toLowerCase match {
      case "local" =>
        launcher.setMaster("local")
      case "yarn" | "yarn-cluster" =>
        launcher.setMaster("yarn").setDeployMode("cluster")
      case "yarn-client" =>
        launcher.setMaster("yarn").setDeployMode("client")
      case _ => throw new Exception(s"Unsupported Spark master ${TaskManagerConfig.SPARK_MASTER}")
    }

    if (TaskManagerConfig.SPARK_YARN_JARS != null && TaskManagerConfig.SPARK_YARN_JARS.nonEmpty) {
      launcher.setConf("spark.yarn.jars", TaskManagerConfig.SPARK_YARN_JARS)
    }

    launcher
  }

  def submitSparkJob(jobType: String, mainClass: String, args: List[String] = List(),
                     sparkConf: Map[String, String] = Map(), defaultDb: String = "",
                     blocking: Boolean = false): JobInfo = {
    val jobInfo = JobInfoManager.createJobInfo(jobType, args, sparkConf)

    // Submit Spark application with SparkLauncher
    val launcher = createSparkLauncher(mainClass)
    if (args != null) {
      launcher.addAppArgs(args:_*)
    }

    // TODO: Avoid using zh_CN to load openmldb jsdk so
    launcher.setConf("spark.yarn.appMasterEnv.LANG", "en_US.UTF-8")
    launcher.setConf("spark.yarn.appMasterEnv.LC_ALL", "en_US.UTF-8")
    launcher.setConf("spark.yarn.executorEnv.LANG", "en_US.UTF-8")
    launcher.setConf("spark.yarn.executorEnv.LC_ALL", "en_US.UTF-8")

    if (TaskManagerConfig.SPARK_EVENTLOG_DIR.nonEmpty) {
      launcher.setConf("spark.eventLog.enabled", "true")
      launcher.setConf("spark.eventLog.dir", TaskManagerConfig.SPARK_EVENTLOG_DIR)
    }

    if (TaskManagerConfig.SPARK_YARN_MAXAPPATTEMPTS >= 1 ) {
      launcher.setConf("spark.yarn.maxAppAttempts", TaskManagerConfig.SPARK_YARN_MAXAPPATTEMPTS.toString)
    }

    // TODO: Support escape delimiter
    // Set default Spark conf by TaskManager configuration file
    val defaultSparkConfs = TaskManagerConfig.SPARK_DEFAULT_CONF.split(",")
    defaultSparkConfs.map(sparkConf => {
      if (sparkConf.nonEmpty) {
        val kvList = sparkConf.split("=")
        launcher.setConf(kvList(0), kvList(1))
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

    for ((k, v) <- sparkConf) {
      logger.info("Get Spark config key: " + k + ", value: " + v)
      launcher.setConf(k, v)
    }

    if (TaskManagerConfig.JOB_LOG_PATH.nonEmpty) {
      // Create local file and redirect the log of job into files
      launcher.redirectOutput(LogManager.getJobLogFile(jobInfo.getId))
      launcher.redirectError(LogManager.getJobErrorLogFile(jobInfo.getId))
    }

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
