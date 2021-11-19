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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.slf4j.LoggerFactory

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
    // TODO: Read yarn config from environment
    val confPath = Thread.currentThread.getContextClassLoader.getResource("").getPath + File.separator + "conf"
    val configuration = new Configuration()
    System.out.println(confPath + File.separator + "core-site.xml");
    configuration.addResource(new Path(confPath + File.separator + "core-site.xml"));
    configuration.addResource(new Path(confPath + File.separator + "hdfs-site.xml"));
    configuration.addResource(new Path(confPath + File.separator + "yarn-site.xml"));

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