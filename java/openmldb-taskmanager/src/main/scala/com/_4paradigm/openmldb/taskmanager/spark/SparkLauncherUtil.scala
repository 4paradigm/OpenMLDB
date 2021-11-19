package com._4paradigm.openmldb.taskmanager.spark

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.LoggerFactory

import java.util.concurrent.CountDownLatch

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

    if (TaskManagerConfig.SPARK_HOME != null && TaskManagerConfig.SPARK_HOME.nonEmpty) {
      launcher.setSparkHome(TaskManagerConfig.SPARK_HOME)
    }

    TaskManagerConfig.SPARK_MASTER.toLowerCase match {
      case "local" => {
        launcher.setMaster("local")
      }
      case "yarn" => {
        launcher.setMaster("yarn")
          .setDeployMode("cluster")
          .setConf("spark.yarn.maxAppAttempts", "1")
      }
      case _ => throw new Exception(s"Unsupported Spark master ${TaskManagerConfig.SPARK_MASTER}")
    }

    if (TaskManagerConfig.SPARK_YARN_JARS != "") {
      launcher.setConf("spark.yarn.jars", TaskManagerConfig.SPARK_YARN_JARS)
    }

    launcher
  }

  /**
   * Submit the Spark job and wait its job to be done.
   *
   * @param mainClass the full-qualified Java class name
   * @param args      the arguments of Java class main function
   */
  def submitSparkAndWait(mainClass: String, args: Array[String] = null): Unit = {
    val lock = new CountDownLatch(1)

    val sparkLauncher = createSparkLauncher(mainClass)
    if (args != null) {
      sparkLauncher.addAppArgs(args: _*)
    }

    val sparkAppHandle = sparkLauncher.startApplication(new SparkAppHandle.Listener() {
      override def stateChanged(sparkAppHandle: SparkAppHandle): Unit = {
        if (sparkAppHandle.getState.isFinal) {
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
   * @param args      the arguments of Java class main function
   * @return the Yarn AppId in String format
   */
  def submitSparkGetAppId(mainClass: String, args: Array[String] = null): String = {
    val lock = new CountDownLatch(1)

    val sparkLauncher = createSparkLauncher(mainClass)
    if (args != null) {
      sparkLauncher.addAppArgs(args: _*)
    }

    val sparkAppHandle = sparkLauncher.startApplication(new SparkAppHandle.Listener() {
      override def stateChanged(sparkAppHandle: SparkAppHandle): Unit = {
        // For yarn-cluster return when get application id
        // For local mode, return when finished
        if ((sparkAppHandle.getState == SparkAppHandle.State.SUBMITTED && sparkAppHandle.getAppId != null) || sparkAppHandle.getState.isFinal) {
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
