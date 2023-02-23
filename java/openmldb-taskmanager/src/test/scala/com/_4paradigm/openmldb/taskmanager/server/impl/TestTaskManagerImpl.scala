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

package com._4paradigm.openmldb.taskmanager.server.impl

import com._4paradigm.openmldb.proto.TaskManager
import com._4paradigm.openmldb.taskmanager.server.StatusCode
import org.scalatest.FunSuite

class TestTaskManagerImpl extends FunSuite {

  test("Test ShowJobs and check status") {
    val impl = new TaskManagerImpl()

    val request = TaskManager.ShowJobsRequest.newBuilder.build
    val response = impl.ShowJobs(request)

    assert(response.getCode == StatusCode.SUCCESS)
  }

  test("Test GetVersion") {
    val impl = new TaskManagerImpl()

    val request = TaskManager.EmptyMessage.newBuilder().build()
    val response = impl.GetVersion(request)

    assert(!response.getBatchVersion.equals("unknown"))
    // Notice that we can not get taskmanager version in unit test
    assert(response.getTaskmanagerVersion.equals("unknown"))
  }

  test("Test ShowJob with non-existent id") {
    val impl = new TaskManagerImpl()

    val jobId = -1
    val request = TaskManager.ShowJobRequest.newBuilder.setId(jobId).build
    val response = impl.ShowJob(request)

    assert(response.getCode() == StatusCode.FAILED)
  }

}
