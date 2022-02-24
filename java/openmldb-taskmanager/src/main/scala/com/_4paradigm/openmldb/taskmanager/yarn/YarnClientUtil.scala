package com._4paradigm.openmldb.taskmanager.yarn

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.slf4j.LoggerFactory

import java.io.File

object YarnClientUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Parse app id from string to ApplicationId object.
   *
   * @param appIdStr the string app id
   * @return the ApplicationId object
   */
  def parseAppIdStr(appIdStr: String): ApplicationId = {
    val appIdStrSplit = appIdStr.split("_")
    ApplicationId.newInstance(appIdStrSplit(1).toLong, appIdStrSplit(2).toInt)
  }

  def createYarnClient(): YarnClient = {
    val configuration = new Configuration()
    configuration.addResource(new Path(TaskManagerConfig.HADOOP_CONF_DIR + File.separator + "core-site.xml"));
    configuration.addResource(new Path(TaskManagerConfig.HADOOP_CONF_DIR + File.separator + "hdfs-site.xml"));
    configuration.addResource(new Path(TaskManagerConfig.HADOOP_CONF_DIR + File.separator + "yarn-site.xml"));

    // Create yarn client
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(configuration)
    yarnClient.start()

    yarnClient
  }

  /**
   * Get Yarn job report from string app id.
   *
   * @param appIdStr the string app id
   * @return the ApplicationReport object
   */
  def getYarnJobReport(appIdStr: String): ApplicationReport = {
    val yarnClient = createYarnClient()

    val appId = parseAppIdStr(appIdStr)
    yarnClient.getApplicationReport(appId)

  }

  /**
   * Get Yarn job state from string app id.
   *
   * @param appIdStr the string app id
   * @return the YarnApplicationState object
   */
  def getYarnJobState(appIdStr: String): YarnApplicationState = {
    val appReport = getYarnJobReport(appIdStr)
    appReport.getYarnApplicationState
  }

  def killYarnJob(appIdStr: String): Unit = {
    val yarnClient = createYarnClient()
    yarnClient.killApplication(parseAppIdStr(appIdStr))
  }

}
