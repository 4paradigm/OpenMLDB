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

import com._4paradigm.openmldb.taskmanager.JobInfoManager
import SparkLauncherUtil.createSparkLauncher
import com._4paradigm.openmldb.taskmanager.dao.JobInfo
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil

import java.util.Calendar

object SparkJobManager {

  def submitSparkJob(jobInfo: JobInfo, mainClass: String, args: Array[String],
                     sparkConf: Map[String, String]): Unit = {
    // TODO: Request NameServer to write jobInfo into system table
    JobInfoManager.addJobInfo(jobInfo)

    // Submit Spark application with SparkLauncher
    val launcher = createSparkLauncher(mainClass)
    if (args != null) {
      launcher.addAppArgs(args:_*)
    }

    // Set ad-hoc Spark configuration
    for ((k, v) <- sparkConf) {
      launcher.setConf(k, v)
    }

    // Submit Spark application and watch state with custom listener
    launcher.startApplication(new SparkJobListener(jobInfo))
  }

  def submitSparkJob(mainClass: String, jobType: String, cluster: String, args: Array[String]=null,
                     sparkConf: Map[String, String]=Map()): Unit = {
    // TODO: Generate unique job id
    val jobId = 1
    val startTime = new java.sql.Timestamp(Calendar.getInstance.getTime().getTime())

    // Generate JobInfo
    val jobInfo = new JobInfo()
    jobInfo.setId(jobId)
    jobInfo.setJobType(jobType)
    jobInfo.setStartTime(startTime)
    jobInfo.setCluster(cluster)

    submitSparkJob(jobInfo, mainClass, args, sparkConf)
  }

  def stopSparkYarnJob(jobInfo: JobInfo): Unit = {
    if (jobInfo.isFinal) {
      // TODO: return error message
    } else if (jobInfo.getApplicationId == null) {

    } else {
      YarnClientUtil.killYarnJob(jobInfo.getApplicationId)
    }
  }

}
