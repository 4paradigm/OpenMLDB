package com._4paradigm.openmldb.taskmanager.yarn

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.slf4j.LoggerFactory
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.logaggregation.{ContainerLogsRequest, LogCLIHelpers}
import java.io.PrintStream
import java.nio.charset.StandardCharsets


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
    val config = new Configuration()
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "core-site.xml"))
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "hdfs-site.xml"))
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "yarn-site.xml"))
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "mapred-site.xml"))

    // Create yarn client
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(config)
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


  /**
   * Get the yarn job log.
   *
   * Notice that it uses the CLI package which works for custom version like 3.x.
   * Notice that this only works for completed job and requires Yarn to enable log aggregation.
   *
   * @param appIdStr
   * @return
   */
  def getAppLog(appIdStr: String): String = {
    val appId = parseAppIdStr(appIdStr)
    logger.info(s"Try to get yarn log for app $appId")

    val config = new YarnConfiguration()
    // TODO: Load config file in better way
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "core-site.xml"))
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "hdfs-site.xml"))
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "yarn-site.xml"))
    config.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "mapred-site.xml"))

    val logCliHelper = new LogCLIHelpers
    logCliHelper.setConf(config)

    val appOwner = UserGroupInformation.getCurrentUser.getShortUserName

    val logCliHelperOptions = new ContainerLogsRequest()
    logCliHelperOptions.setAppId(appId)
    logCliHelperOptions.setAppOwner(appOwner)

    var content = ""

    try {
      // Get the YARN log for a completed application
      logCliHelper.dumpAllContainersLogs(logCliHelperOptions)

      val baos = new ByteArrayOutputStream()
      content = new String(baos.toByteArray, StandardCharsets.UTF_8)
    } catch {
      case e: Exception => logger.error(s"Fail to get yarn job log for app id $appId, get error $e")
    }

    content
  }

}
