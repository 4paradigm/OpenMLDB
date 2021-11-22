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

import com._4paradigm.openmldb.taskmanager.dao.JobInfo
import scala.collection.mutable

object JobInfoManager {

  // TODO: persist in system table instead of memory
  val memoryJobInfos = new mutable.ArrayBuffer[JobInfo]()

  def getJobInfos(): mutable.ArrayBuffer[JobInfo] = {
    memoryJobInfos
  }

  def getJobInfo(id: Int): Option[JobInfo] = {
    memoryJobInfos.find(p => p.getId == id)
  }

  def addJobInfo(jobInfo: JobInfo): Unit = {
    memoryJobInfos.append(jobInfo)
  }

}
