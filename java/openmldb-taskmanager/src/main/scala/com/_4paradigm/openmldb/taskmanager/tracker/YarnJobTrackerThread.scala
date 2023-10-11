package com._4paradigm.openmldb.taskmanager.tracker


import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import com._4paradigm.openmldb.taskmanager.dao.JobInfo
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil.parseAppIdStr
import org.apache.spark.launcher.SparkAppHandle.State

class YarnJobTrackerThread(job: JobInfo) extends Thread {

  override def run() {
    val yarnClient = YarnClientUtil.createYarnClient()

    if (job.getApplicationId.isEmpty) {
      job.setState(State.LOST.toString)
      job.sync()
      return
    }

    val appId = parseAppIdStr(job.getApplicationId)
    val appReport = yarnClient.getApplicationReport(appId)
    var lastYarnState = appReport.getYarnApplicationState.toString.toLowerCase()

    while(true) {
      // Get final state and exit
      if (JobInfo.FINAL_STATE.contains(lastYarnState)) {
        job.setState(lastYarnState)
        job.sync()
        return
      }

      // Sleep for interval time
      Thread.sleep(TaskManagerConfig.getJobTrackerInterval * 1000)

      val currentYarnState = appReport.getYarnApplicationState.toString.toLowerCase()

      // Check if state changes
      if (!currentYarnState.equals(lastYarnState)) {
        job.setState(currentYarnState)
        job.sync()
        if (JobInfo.FINAL_STATE.contains(currentYarnState)) {
          return
        }

        lastYarnState = currentYarnState
      }

    }

  }

}