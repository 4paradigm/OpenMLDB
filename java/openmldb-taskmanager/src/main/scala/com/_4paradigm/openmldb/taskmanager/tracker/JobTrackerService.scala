package com._4paradigm.openmldb.taskmanager.tracker

import com._4paradigm.openmldb.taskmanager.JobInfoManager
import org.apache.spark.launcher.SparkAppHandle.State
import org.slf4j.LoggerFactory

object JobTrackerService {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def startTrackerThreads(): Unit = {
    // Get unfinished jobs
    val unfinishedJobs = JobInfoManager.getUnfinishedJobs()

    for (job <- unfinishedJobs) {
      if (job.isYarnClusterJob && job.getApplicationId.nonEmpty) {
        logger.info("Start tracker thread to track job: " + job)

        // Get submitted yarn jobs
        val trackerThread = new YarnJobTrackerThread(job)

        // Start thread to track state
        trackerThread.start()
      } else {
        // Can not track local job, yarn-client job or job without application id, set state as LOST
        logger.info("Unable to track job state: " + job)

        job.setState(State.LOST.toString)
        job.sync()
      }
    }

  }

}
