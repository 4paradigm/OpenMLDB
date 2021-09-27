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

object SparkLauncherUtil {

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

    TaskManagerConfig.SPARK_MASTER.toLowerCase match {
      case "local" => {
        launcher.setMaster("local")
      }
      case "yarn" => {
        launcher.setMaster("yarn")
          .setDeployMode("cluster")
          .setConf("spark.yarn.jars", TaskManagerConfig.SPARK_YARN_JARS)
          .setConf("spark.yarn.maxAppAttempts", "1")
      }
      case _ => throw new Exception(s"Unsupported Spark master ${TaskManagerConfig.SPARK_MASTER}")
    }

  }

  /**
   * Submit the Spark job and wait its job to be done.
   *
   * @param mainClass the full-qualified Java class name
   * @param args the arguments of Java class main function
   */
  def submitSparkAndWait(mainClass: String, args: Array[String] = null): Unit = {
    val lock = new CountDownLatch(1)

    val sparkLauncher = createSparkLauncher(mainClass)
    if (args != null) {
      sparkLauncher.addAppArgs(args:_*)
    }

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

    logger.info(s"Final state: ${sparkAppHandle.getState}, appId: ${sparkAppHandle.getAppId}")
  }

  /**
   * Submit the Spark job then wait its state to be SUBMITTED and get yarn AppId.
   *
   * @param mainClass the full-qualified Java class name
   * @param args the arguments of Java class main function
   * @return the Yarn AppId in String format
   */
  def submitSparkGetAppId(mainClass: String, args: Array[String] = null): String = {
    val lock = new CountDownLatch(1)

    val sparkLauncher = createSparkLauncher(mainClass)
    if (args != null) {
      sparkLauncher.addAppArgs(args:_*)
    }

    val sparkAppHandle = sparkLauncher.startApplication(new SparkAppHandle.Listener() {
      override def stateChanged(sparkAppHandle: SparkAppHandle): Unit = {
        // For yarn-cluster return when get application id
        // For local mode, return when finished
        if((sparkAppHandle.getState == SparkAppHandle.State.SUBMITTED && sparkAppHandle.getAppId != null) || sparkAppHandle.getState.isFinal) {
          logger.info(s"Get Spark job state: ${sparkAppHandle.getState}")
          logger.warn(sparkAppHandle.getError.toString)
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

}