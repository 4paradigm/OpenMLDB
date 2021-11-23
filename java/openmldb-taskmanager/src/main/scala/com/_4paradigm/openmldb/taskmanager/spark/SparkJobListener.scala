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

import com._4paradigm.openmldb.taskmanager.dao.JobInfo
import org.apache.spark.launcher.SparkAppHandle
import org.slf4j.LoggerFactory

import java.util.Calendar

class SparkJobListener(jobInfo: JobInfo) extends SparkAppHandle.Listener{

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def stateChanged(handle: SparkAppHandle): Unit = {
    val state = handle.getState

    // TODO: Sync job info with system table
    // Set state
    jobInfo.setState(state.toString)
    logger.info("Job(id=%d) state change to %s".format(jobInfo.getId, state))

    // Set application id
    if (handle.getAppId != null) {
      jobInfo.setApplicationId(handle.getAppId)
    }

    if (state.isFinal) {

      // Set end time
      val endTime = new java.sql.Timestamp(Calendar.getInstance.getTime().getTime())
      jobInfo.setEndTime(endTime)

      // Set error
      if (handle.getError.isPresent) {
        jobInfo.setError(handle.getError.get().getMessage)
      }

    }

  }

  override def infoChanged(handle: SparkAppHandle): Unit = {
    // TODO: Check if we can update applicationId from callback
  }

}
